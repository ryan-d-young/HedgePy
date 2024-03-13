import asyncio
import datetime
from psycopg import sql
from psycopg_pool import AsyncConnectionPool
from typing import Any, Callable
from functools import partial

from hedgepy.common import API


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
 
 

class DatabaseManager:
    _QUERIES = dict(zip(
        QUERY_STUBS.keys(), 
        (create_schema,
         create_table, 
         insert, 
         insert_bulk, 
         update, 
         select, 
         delete_schema, 
         delete_table, 
         delete_records)
        ))

    def __init__(self, 
                 api_instance: 'API_Instance',
                 dbname: str,
                 host: str,
                 port: int,
                 user: str, 
                 password: str):
        self._api_instance = api_instance
        self._pool =  AsyncConnectionPool(
            conninfo=f"dbname={dbname} user={user} host={host} port={port} password={password}", 
            open=False
            )
        del password

        self._bind_queries(self._pool)
                
    def _bind_queries(self) -> dict[str, Callable]:
        queries = {}
        for query, func in self.QUERIES.items():
            queries[query] = partial(func, pool=self._pool)
        self._queries = queries
    
    def query(self, query: str, *args, **kwargs):
        return self._queries[query](*args, **kwargs)
    
    def check_preexisting_data(self, endpoint: API.Endpoint, meth: str, *args, **kwargs
                               ) -> tuple[tuple, tuple[tuple]] | None:
        raise NotImplementedError("To do")
    
    def postprocess_response(self, response: API.FormattedResponse) -> tuple[tuple, tuple]:
        endpoint = self._api_instance.vendors[response.vendor_name]
        meth = getattr(endpoint.getters, response.endpoint_name)
        fields, data = response.fields, response.data
        
        if meth.discard:
            fields, data = self._discard(response.fields, response.data, meth.discard)
            
        return fields, data

    def _discard(self, fields: tuple[tuple[str, type]], data: tuple[tuple], discard: tuple[str] | None
                 ) -> tuple[tuple[str, type], tuple[tuple]]:
        discard_ix = tuple(map(lambda x: fields.index(x), discard))
        fields = tuple(filter(lambda x: x[0] not in discard_ix, enumerate(fields)))
        data = tuple(filter(lambda x: x[0] not in discard_ix, enumerate(data)))
        return fields, data
            
    def stage_response(self, response: API.FormattedResponse, dtypes: tuple[type], data: tuple[tuple]
                       ) -> tuple[tuple, tuple[tuple]]:        
        schema, table, columns = make_identifiers(schema=response.vendor_name, 
                                                  table=response.endpoint_name, 
                                                  columns=response.fields)
                                
        schema_exists = self.query('check_schema', (schema,))
        if not schema_exists:
            self.query('create_schema', (schema,))

        table_exists = self.query('check_table', (schema, table))
        if not table_exists:
            self.query('create_table', (schema, table, columns), dtypes)            

        return (schema, table, columns), data
        
    async def start(self):
        print(f"DatabaseManager.start: {asyncio.get_event_loop()}")
        await self._pool.open()
