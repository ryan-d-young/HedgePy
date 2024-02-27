import datetime
from psycopg import sql
from psycopg_pool import AsyncConnectionPool
from hedgepy.bases.vendor import APIFormattedResponse
from typing import Any

DB_TYPE = ["text",  "bool", "null", "int", "float", "date", "time", "timestamp", "interval"]
PY_TYPE = [str, bool, None, int, float, datetime.date, datetime.time, datetime.datetime, datetime.timedelta]
DB_TO_PY = dict(zip(DB_TYPE, PY_TYPE))
PY_TO_DB = dict(zip(PY_TYPE, DB_TYPE))
QUERY_STUBS = {
    'create_schema': sql.SQL("CREATE SCHEMA IF NOT EXISTS {};"),
    'create_table': sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({});"),
    'insert': sql.SQL("INSERT INTO {}.{} ({}) VALUES ("),
    'insert_bulk': sql.SQL("COPY {}.{} ({}) FROM STDIN;"),
    'update': sql.SQL("UPDATE {}.{} SET ("), 
    'select': sql.SQL("SELECT ({}) FROM {}.{} "), 
    'select_all': sql.SQL("SELECT * FROM {}.{} "),
    'delete_schema': sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE;"), 
    'delete_table': sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE;"),
    'delete_records': sql.SQL("DELETE FROM {}.{} "), 
    'check_table': sql.SQL("""
                           SELECT EXISTS 
                           (SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s);
                           """), 
    'check_schema': sql.SQL("""
                            SELECT EXISTS 
                            (SELECT 1 FROM information_schema.schemata WHERE schema_name = %s);
                            """),
    'check_columns': sql.SQL("""
                            SELECT column_name 
                            FROM information_schema.columns 
                            WHERE table_schema = %s AND table_name = %s;
                            """)
}


def make_identifiers(schema: str | None, 
                     table: str | None, 
                     columns: tuple[str] | None) -> tuple[sql.Identifier | sql.SQL | None]:
    schema = sql.Identifier(schema) if schema else schema
    table = sql.Identifier(table) if table else table
    columns = sql.SQL(', ').join(map(sql.Identifier, columns)) if columns else columns
    return schema, table, columns


def parse_response(response: APIFormattedResponse
                    ) -> tuple[tuple[sql.Identifier | sql.SQL], tuple[type], tuple[tuple]]:
    schema, table, columns = response.vendor_name, response.endpoint_name, response.fields
    identifiers = make_identifiers(schema=schema, table=table, columns=columns)
    py_dtypes = tuple(map(lambda x: x[1], response.fields))
    return identifiers, py_dtypes, response.data


def validate_response_data(py_dtypes: tuple[type], data: tuple[tuple]) -> None:
    record_len = len(data[0])
    for record in data:
        for ix, (value, dtype) in enumerate(zip(record, py_dtypes)):
            if not isinstance(value, dtype):
                try: 
                    value = dtype(value)
                except ValueError:
                    raise TypeError(f"Value {value} at index {ix} is not of type {dtype} and cannot be coerced")
            assert len(record) == record_len, f"Record {ix} has wrong length ({len(record)} / {record_len} expected)"


def _make_stub(which: str, identifiers: tuple[tuple[sql.Identifier | sql.SQL]]) -> sql.SQL:
    return QUERY_STUBS[which].format(*identifiers)


def _make_conditions(condition_columns: tuple[str], 
                     condition_values: tuple[Any]) -> sql.SQL:
    return sql.SQL('WHERE ') + sql.SQL(' AND ').join(
        map(lambda x: sql.SQL("{} = {}").format(sql.Identifier(x[0]), sql.Placeholder()), 
            zip(condition_columns, condition_values))
    )


async def _execute_query(query: sql.SQL, 
                         pool: AsyncConnectionPool, 
                         data: tuple[tuple] | None = None) -> Any | None:
    async with pool.cursor() as cursor:
        if data:
            if len(data) > 1:
                await cursor.executemany(query, data)
            else: 
                await cursor.execute(query, data[0])
        else:
            await cursor.execute(query)
        return await cursor.fetchall()


async def _execute_bulk_insert(query: sql.SQL, pool: AsyncConnectionPool, data: tuple[tuple]) -> None:
    async with pool.cursor() as cursor:
        async with cursor.copy(query) as copy:
            await copy.write('\n'.join(map(lambda x: '\t'.join(map(str, x)), data)))


async def create_schema(identifiers: tuple[sql.Identifier | sql.SQL], pool: AsyncConnectionPool) -> None:
    schema, table, columns = identifiers
    query = _make_stub('create_schema', (schema,))
    _execute_query(query, pool)


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
 
 
QUERIES = dict(zip(QUERY_STUBS.keys(), 
                   (create_schema,
                    create_table, 
                    insert, 
                    insert_bulk, 
                    update, 
                    select, 
                    delete_schema, 
                    delete_table, 
                    delete_records)))
 