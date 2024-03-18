from abc import ABC, abstractmethod
from psycopg import sql, ProgrammingError
from psycopg_pool import AsyncConnectionPool
from typing import Any


Schema = Table = Column = str
Columns = DTypes = tuple[str]
ColumnsWithDTypes = tuple[Columns, DTypes]


def _make_conditions(condition_columns: Columns, condition_operators: tuple[str]) -> sql.SQL:
    return sql.SQL(" AND ").join(
        map(
            lambda x, y: sql.SQL("{} {} {}").format(
                sql.Identifier(x), sql.SQL(y), sql.Placeholder()
            ),
            zip(condition_columns, condition_operators),
        )
    )


def _make_columns(columns: Columns) -> sql.SQL:
    return sql.SQL(", ").join(map(sql.Identifier, columns))


def _make_columns_eq(columns: Columns) -> sql.SQL:
    return sql.SQL(", ").join(
        map(
            lambda x: sql.SQL("{} = {}").format(sql.Identifier(x), sql.Placeholder()),
            columns,
        )
    )


def _make_columns_dtype(columns: ColumnsWithDTypes) -> sql.SQL:
    return sql.SQL(", ").join(
        map(
            lambda x: sql.SQL("{} {}").format(sql.Identifier(x[0]), sql.SQL(x[1])),
            columns,
        )
    )


class CommandABC(ABC):
    QUERY_STUB: sql.SQL 
    
    @abstractmethod
    def make(self, **kwargs) -> sql.SQL:
        return self.QUERY_STUB.format(
            **{k: sql.Identifier(v) if isinstance(v, str) else v
               for k, v 
               in kwargs.items()}
            )


class CreateSchema(CommandABC):
    QUERY_STUB = sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema};")

    def make(self, schema: Schema):
        return super().make(schema=schema)


class CreateTable(CommandABC):
    QUERY_STUB = sql.SQL("CREATE TABLE IF NOT EXISTS {schema}.{table} (")
    
    def make(self, schema: Schema, table: Table, columns: ColumnsWithDTypes):
        return super().make(schema=schema, table=table) + _make_columns_dtype(columns) + sql.SQL(');')


class SelectTable(CommandABC):
    QUERY_STUB = sql.SQL("SELECT * FROM {schema}.{table};")
    
    def make(self, schema: Schema, table: Table):
        return super().make(schema=schema, table=table)


class SelectColumns(CommandABC):
    QUERY_STUB = sql.SQL("SELECT ({columns}) FROM {schema}.{table};")
    
    def make(self, schema: Schema, table: Table, columns: Columns):
        return super().make(schema=schema, table=table, columns=_make_columns(columns))


class SelectRecords(CommandABC):
    QUERY_STUB = sql.SQL("SELECT * FROM {schema}.{table} WHERE ")
    
    def make(self, 
             schema: Schema, 
             table: Table, 
             condition_columns: Columns, 
             condition_operators: tuple[str]):
        conditions = _make_conditions(condition_columns, condition_operators)
        return super().make(schema=schema, table=table) + conditions + sql.SQL(';')


class SelectColumnsRecords(CommandABC):
    QUERY_STUB = sql.SQL("SELECT ({columns}) FROM {schema}.{table} WHERE ")
    
    def make(self, 
             schema: Schema, 
             table: Table, 
             columns: Columns, 
             condition_columns: Columns, 
             condition_operators: tuple[str]):
        columns_sql = _make_columns(columns)
        conditions = _make_conditions(condition_columns, condition_operators)
        return super().make(schema=schema, table=table, columns=columns_sql) + conditions + sql.SQL(';')


SelectRecordsColumns = SelectColumnsRecords


class UpdateTable(CommandABC):
    QUERY_STUB = sql.SQL("UPDATE {schema}.{table} SET ")

    def make(self, 
             schema: Schema, 
             table: Table, 
             set_columns: Columns, 
             condition_columns: Columns, 
             condition_operators: tuple[str]):
        set_columns_sql = _make_columns_eq(set_columns)
        conditions = _make_conditions(condition_columns, condition_operators)
        return super().make(schema=schema, table=table) + set_columns_sql + conditions + sql.SQL(';')


class InsertRow(CommandABC):
    QUERY_STUB = sql.SQL("INSERT INTO {schema}.{table} ({columns}) VALUES (")
    
    def make(self, schema: Schema, table: Table, columns: Columns):
        values = sql.SQL(', ').join([sql.Placeholder()] * len(columns))
        return super().make(schema=schema, table=table, columns=_make_columns(columns)) + values + sql.SQL(');')


class CopyRows(CommandABC):
    QUERY_STUB = sql.SQL("COPY {schema}.{table} ({columns}) FROM STDIN;")
    
    def make(self, schema: Schema, table: Table, columns: Columns):
        return super().make(schema=schema, table=table, columns=_make_columns(columns))


class DeleteSchema(CommandABC):
    QUERY_STUB = sql.SQL("DROP SCHEMA IF EXISTS {schema} CASCADE;")

    def make(self, schema: Schema):
        return super().make(schema=schema)
    
    
class DeleteTable(CommandABC):
    QUERY_STUB = sql.SQL("DROP TABLE IF EXISTS {schema}.{table} CASCADE;")

    def make(self, schema: Schema, table: Table):
        return super().make(schema=schema, table=table)
    
    
class DeleteRow(CommandABC):
    QUERY_STUB = sql.SQL("DELETE FROM {schema}.{table} WHERE ")
    
    def make(self, schema: Schema, table: Table, condition_columns: Columns, condition_operators: tuple[str]):
        conditions = _make_conditions(condition_columns, condition_operators)
        return super().make(schema=schema, table=table) + conditions + sql.SQL(';')
    

class ListSchemas(CommandABC):
    QUERY_STUB = sql.SQL("SELECT schema_name FROM information_schema.schemata;")
    
    def make(self):
        return super().make()
    
    
class ListTables(CommandABC):
    QUERY_STUB = sql.SQL("SELECT table_name FROM information_schema.tables WHERE table_schema = {schema};")
    
    def make(self, schema: Schema):
        return super().make(schema=schema)
    
    
class ListColumns(CommandABC):
    QUERY_STUB = sql.SQL("""
                         SELECT column_name 
                         FROM information_schema.columns 
                         WHERE table_schema = {schema} 
                         AND table_name = {table};
                         """)
    
    def make(self, schema: Schema, table: Table):
        return super().make(schema=schema, table=table)


class CheckRecords(CommandABC):
    QUERY_STUB = sql.SQL("SELECT EXISTS (SELECT 1 FROM {schema}.{table} WHERE ")
    
    def make(self, schema: Schema, table: Table, condition_columns: Columns, condition_operators: tuple[str]):
        conditions = _make_conditions(condition_columns, condition_operators)
        return super().make(schema=schema, table=table) + conditions + sql.SQL(');')


class Database:
    QUERIES = {
        "create_schema": CreateSchema(),
        "create_table": CreateTable(),
        "select_table": SelectTable(),
        "select_columns": SelectColumns(),
        "select_records": SelectRecords(),
        "select_columns_records": SelectColumnsRecords(),
        "update_table": UpdateTable(),
        "insert_row": InsertRow(),
        "copy_rows": CopyRows(),
        "delete_schema": DeleteSchema(),
        "delete_table": DeleteTable(),
        "delete_row": DeleteRow(),
        "list_schemas": ListSchemas(),
        "list_tables": ListTables(),
        "list_columns": ListColumns(),
        "check_records": CheckRecords(),
    }
        
    def __init__(self, dbname: str, host: str, port: int, user: str, password: str):
        self._pool =  AsyncConnectionPool(
            conninfo=f"dbname={dbname} user={user} host={host} port={port} password={password}", 
            open=False
            )
        del password
        
    async def _execute_one(self, query_stub: sql.SQL, data: tuple | None = None) -> Any | None:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cursor: 
                if data:
                    await cursor.execute(query_stub, data[0])
                else:
                    await cursor.execute(query_stub)
                try: 
                    return await cursor.fetchall()
                except ProgrammingError:
                    return None

    async def _execute_many(self, query_stub: sql.SQL, data: tuple[tuple] | None = None) -> Any | None:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cursor: 
                await cursor.executemany(query_stub, data)
                return await cursor.fetchall()
    
    async def _execute_bulk(self, query_stub: sql.SQL, data: tuple[tuple]) -> None:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cursor: 
                async with cursor.copy(query_stub) as copy:
                    await copy.write('\n'.join(map(lambda x: '\t'.join(map(str, x)), data)))

    async def query(self, which: str, *placeholder_args, **keyword_kwargs) -> None | tuple[tuple[Any]]:
        try:
            if self._pool.closed:
                await self._pool.open()

            match which: 
                case "insert_row":
                    if isinstance(placeholder_args[0], tuple):
                        return await self._execute_many(self.QUERIES[which].make(**keyword_kwargs), *placeholder_args)
                    else: 
                        return await self._execute_one(self.QUERIES[which].make(**keyword_kwargs), *placeholder_args)
                case "copy_rows":
                    return await self._execute_bulk(self.QUERIES[which].make(**keyword_kwargs), *placeholder_args)
                case _:
                    if which in self.QUERIES:
                        return await self._execute_one(self.QUERIES[which].make(**keyword_kwargs), *placeholder_args)
                    else:
                        raise ValueError(f"Unsupported query: '{which}' must be one of {self.QUERIES.keys()}")
        except Exception as e:
            await self._pool.close()
            raise e
        