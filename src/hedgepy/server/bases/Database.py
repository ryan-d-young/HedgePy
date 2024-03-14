from abc import ABC, abstractmethod
from psycopg import sql
from psycopg_pool import AsyncConnectionPool
from typing import Any


Schema = Table = str
Columns = DTypes = tuple[str]
ColumnsWithDTypes = tuple[Columns, DTypes]


QUERY_STUBS = {
    'insert': sql.SQL("INSERT INTO {schema}.{table} ({columns}) VALUES (%s);"),
    'insert_bulk': sql.SQL("COPY {schema}.{table} ({columns}) FROM STDIN;"),
    'update': sql.SQL("UPDATE {schema}.{table} SET ("), 
    'delete_schema': sql.SQL("DROP SCHEMA IF EXISTS {schema} CASCADE;"), 
    'delete_table': sql.SQL("DROP TABLE IF EXISTS {schema}.{table} CASCADE;"),
    'delete_records': sql.SQL("DELETE FROM {schema}.{table} "), 
    'check_table': sql.SQL("""
                           SELECT EXISTS 
                           (SELECT 1 FROM information_schema.tables WHERE table_schema = {schema} AND table_name = {table});
                           """), 
    'check_schema': sql.SQL("""
                            SELECT EXISTS 
                            (SELECT 1 FROM information_schema.schemata WHERE schema_name = {schema});
                            """),
    'check_columns': sql.SQL("""
                            SELECT column_name 
                            FROM information_schema.columns 
                            WHERE table_schema = {schema} AND table_name = {table};
                            """), 
    'check_connection': sql.SQL("SELECT 1;"),
}


def _make_conditions(condition_columns: tuple[str], condition_operators: tuple[str]) -> sql.SQL:
    return sql.SQL('WHERE ') + sql.SQL(' AND ').join(
        map(lambda x: sql.SQL("{} {} {}").format(sql.Identifier(x[0]), sql.SQL(x[1]), sql.Placeholder()), 
            zip(condition_columns, condition_operators))
    )


def _make_columns(columns: Columns) -> sql.SQL:
    return sql.SQL(', ').join(map(sql.Identifier, columns))


def _make_columns_eq(columns: Columns, values: tuple[Any]) -> sql.SQL:
    return sql.SQL(', ').join(
        map(lambda x: sql.SQL("{} = {}").format(sql.Identifier(x[0]), sql.Placeholder()), 
            zip(columns, values)
            )
        )

def _make_columns_dtype(columns: Columns) -> sql.SQL:
    return (
        (sql.SQL('{} ') + sql.Placeholder() + sql.SQL(', ')) 
        * len(columns)
        ).format(
            *map(sql.Identifier, columns)
            )
    

def _make_columns_conditions(columns: Columns, condition_columns: Columns, condition_operators: tuple[str]) -> sql.SQL:
    columns_sql = _make_columns(columns)
    conditions = _make_conditions(condition_columns, condition_operators)
    return columns_sql + conditions


class _Command(ABC):
    QUERY_STUB: sql.SQL 
    
    @abstractmethod
    def make(self, **kwargs) -> sql.SQL:
        return self.QUERY_STUB.format(
            **{k: sql.Identifier(v) 
               for k, v 
               in kwargs.items()}
            )


class CreateSchema(_Command):
    QUERY_STUB = sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema};")

    def make(self, schema: Schema):
        return super().make(schema=schema)


class CreateTable(_Command):
    QUERY_STUB = sql.SQL("CREATE TABLE IF NOT EXISTS {schema}.{table} (")
    
    def make(self, schema: Schema, table: Table, columns: Columns):
        return super().make(schema=schema, table=table) + _make_columns_dtype(columns) + sql.SQL(');')


class SelectTable(_Command):
    QUERY_STUB = sql.SQL("SELECT * FROM {schema}.{table};")
    
    def make(self, schema: Schema, table: Table):
        return super().make(schema=schema, table=table)


class SelectColumns(_Command):
    QUERY_STUB = sql.SQL("SELECT ({columns}) FROM {schema}.{table};")
    
    def make(self, schema: Schema, table: Table, columns: Columns):
        return super().make(schema=schema, table=table, columns=_make_columns(columns))
    
    
class SelectRecords(_Command):
    QUERY_STUB = sql.SQL("SELECT * FROM {schema}.{table} WHERE ")
    
    def make(self, 
             schema: Schema, 
             table: Table, 
             condition_columns: Columns, 
             condition_operators: tuple[str]):
        conditions = _make_conditions(condition_columns, condition_operators)
        return super().make(schema=schema, table=table) + conditions + sql.SQL(';')


class SelectColumnsRecords(_Command):
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


class UpdateTable(_Command):
    QUERY_STUB = sql.SQL("UPDATE {schema}.{table} SET ")

    def make(self, 
             schema: Schema, 
             table: Table, 
             set_columns: Columns, 
             condition_columns: Columns, 
             condition_operators: tuple[str]):
        set_columns_sql = sql.SQL(', ').join(
            sql.SQL("{} = {}").format(sql.Identifier(col), sql.Placeholder()) for col in set_columns
        )
        conditions = _make_conditions(condition_columns, condition_operators)
        return super().make(schema=schema, table=table) + set_columns_sql + conditions + sql.SQL(';')

class Database:    
    def __init__(self, dbname: str, host: str, port: int, user: str, password: str):
        self._pool =  AsyncConnectionPool(
            conninfo=f"dbname={dbname} user={user} host={host} port={port} password={password}", 
            open=False
            )
        del password
        
    async def _execute_one(self, query_stub: sql.SQL, data: tuple | None = None) -> Any | None:
        async with self.pool.cursor() as cursor:
            await cursor.execute(query_stub, data[0])
            return await cursor.fetchall()

    async def _execute_many(self, query_stub: sql.SQL, data: tuple[tuple] | None = None) -> Any | None:
        async with self.pool.cursor() as cursor:
            await cursor.executemany(query_stub, data)
            return await cursor.fetchall()
        
    async def _execute_bulk(self, query_stub: sql.SQL, data: tuple[tuple]) -> None:
        async with self.pool.cursor() as cursor:
            async with cursor.copy(query_stub) as copy:
                await copy.write('\n'.join(map(lambda x: '\t'.join(map(str, x)), data)))

    async def query(
        self, 
        which: str = "check_connection", 
        bulk: bool = False,
        *placeholder_args,
        **keyword_kwargs
        ) -> None | tuple[tuple[Any]]:
        query_stub = _make_query_stub(which, **keyword_kwargs)
        if bulk:
            query_result = await self._execute_bulk(query_stub, *placeholder_args)
        else: 
            query_result = await self._execute_many(query_stub, *placeholder_args)          
        return query_result
        
    async def start(self):
        await self._pool.open()

    async def create_schema(identifiers: tuple[sql.Identifier | sql.SQL], pool: AsyncConnectionPool) -> None:
        schema, table, columns = identifiers
        return QUERY_STUBS['create_schema'].format(schema)
        
    async def create_table(identifiers: tuple[tuple[sql.Identifier | sql.SQL]], 
                        py_dtypes: tuple[type], 
                        pool: AsyncConnectionPool) -> None:
        schema, table, columns = identifiers
        columns = sql.SQL(', ').join(
            sql.SQL("{} {}").format(
                sql.Identifier(col_name), 
                sql.SQL(PY_TO_DB[py_dtype])
            ) for col_name, py_dtype in zip(columns, py_dtypes)
        )
        identifiers = (schema, table, columns)
        query = _make_stub('create_table', identifiers)
        _execute_query(query, pool)


    async def insert(identifiers: tuple[tuple[sql.Identifier | sql.SQL]], 
                    pool: AsyncConnectionPool, 
                    data: tuple[tuple]) -> None:
        query = _make_stub('insert', identifiers) + sql.SQL(', ').join([sql.Placeholder()]*len(data[0])) + sql.SQL(');')
        _execute_query(query, pool, data)


    async def insert_bulk(identifiers: tuple[tuple[sql.Identifier | sql.SQL]], 
                        pool: AsyncConnectionPool, 
                        data: tuple[tuple]) -> None:
        query = _make_stub('insert_bulk', identifiers)
        _execute_bulk_insert(query, pool, data)


    async def update(identifiers: tuple[tuple[sql.Identifier | sql.SQL]], 
                    pool: AsyncConnectionPool, 
                    data: tuple[tuple],
                    condition_columns: tuple[str] | None = None,
                    condition_values: tuple | Any | None = None) -> None:
        schema, table, columns = identifiers
        query = _make_stub('update', (schema, table)) + sql.SQL(', ').join(columns) 
        query += sql.SQL(') = (') + sql.SQL(', ').join([sql.Placeholder()]*len(data[0])) + sql.SQL(') ')
        query += _make_conditions(condition_columns, condition_values) + sql.SQL(';')
        _execute_query(query, pool, (condition_values,))
        

    async def select(identifiers: tuple[tuple[sql.Identifier | sql.SQL]], 
                    pool: AsyncConnectionPool, 
                    condition_columns: tuple[str] | None = None,
                    condition_values: tuple | Any | None = None) -> None:
        schema, table, columns = identifiers
        if columns: 
            query = _make_stub('select', identifiers)
        else: 
            query = _make_stub('select_all', identifiers)
        if condition_columns and condition_values:
            query += _make_conditions(condition_columns, condition_values) 
        query += sql.SQL(';')
        return await _execute_query(query, pool, (condition_values,))


    async def delete_schema(identifiers: tuple[sql.Identifier | sql.SQL], pool: AsyncConnectionPool) -> None:
        schema, table, columns = identifiers
        query = _make_stub('delete_schema', (schema,))
        _execute_query(query, pool)
        

    async def delete_table(identifiers: tuple[sql.Identifier | sql.SQL], pool: AsyncConnectionPool) -> None:
        schema, table, columns = identifiers
        query = _make_stub('delete_table', (schema, table))
        _execute_query(query, pool)
        
        
    async def delete_records(identifiers: tuple[tuple[sql.Identifier | sql.SQL]], 
                            pool: AsyncConnectionPool, 
                            condition_columns: tuple[str] | None = None,
                            condition_values: tuple | Any | None = None) -> None:
        schema, table, columns = identifiers
        query = _make_stub('delete_records', (schema, table))
        if condition_columns and condition_values:
            query += _make_conditions(condition_columns, condition_values) 
        query += sql.SQL(';')
        _execute_query(query, pool, (condition_values,))
    
    
    async def check_table(identifiers: tuple[sql.Identifier | sql.SQL], pool: AsyncConnectionPool) -> bool:
        schema, table, columns = identifiers
        query = _make_stub('check_table', (schema, table))
        return await _execute_query(query, pool, (schema, table))


    async def check_schema(identifiers: tuple[sql.Identifier | sql.SQL], pool: AsyncConnectionPool) -> bool:
        schema, table, columns = identifiers
        query = _make_stub('check_schema', (schema,))
        return await _execute_query(query, pool, (schema,))


    async def check_columns(identifiers: tuple[sql.Identifier | sql.SQL], pool: AsyncConnectionPool) -> tuple[str]:
        schema, table, columns = identifiers
        query = _make_stub('check_columns', (schema, table))
        return await _execute_query(query, pool, (schema, table))