import datetime
from psycopg import sql
from psycopg_pool import AsyncConnectionPool
from hedgepy.bases.vendor import APIFormattedResponse

DB_TYPE = ["text",  "bool", "null", "int", "float", "date", "time", "timestamp", "interval"]
PY_TYPE = [str, bool, None, int, float, datetime.date, datetime.time, datetime.datetime, datetime.timedelta]
DB_TO_PY = dict(zip(DB_TYPE, PY_TYPE))
PY_TO_DB = dict(zip(PY_TYPE, DB_TYPE))
QUERY_STUBS = {
    'create_table': sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({});"),
    'insert': sql.SQL("INSERT INTO {}.{} ({}) VALUES ("),
    'insert_bulk': sql.SQL("COPY {}.{} ({}) FROM STDIN;")
}


def _parse_response(response: APIFormattedResponse
                    ) -> tuple[tuple[sql.Identifier, sql.Identifier, sql.SQL], tuple[type], tuple[tuple]]:
    schema = sql.Identifier(response.vendor_name)
    table = sql.Identifier(response.endpoint_name)
    columns = sql.SQL(', ').join(map(lambda x: sql.Identifier(x[0]), response.fields))
    identifiers = (schema, table, columns)
    py_dtypes = tuple(map(lambda x: x[1], response.fields))
    return identifiers, py_dtypes, response.data


def _validate_response_data(py_dtypes: tuple[type], data: tuple[tuple]) -> None:
    record_len = len(data[0])
    for record in data:
        for ix, (value, dtype) in enumerate(zip(record, py_dtypes)):
            if not isinstance(value, dtype):
                raise TypeError(f"Value {value} at index {ix} is not of type {dtype}")
            assert len(record) == record_len, f"Record {ix} has wrong length ({len(record)} versus {record_len} expected)"


def _make_stub(which: str, identifiers: tuple[sql.Identifier, sql.Identifier, sql.SQL]) -> sql.SQL:
    return QUERY_STUBS[which].format(*identifiers)


async def create_table(identifiers: tuple[sql.Identifier, sql.Identifier, sql.SQL], 
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
    async with pool.cursor() as cursor:
        await cursor.execute(query)


async def insert(identifiers: tuple[sql.Identifier, sql.Identifier, sql.SQL], pool: AsyncConnectionPool, data: tuple[tuple], bulk: bool = False) -> None:
    if bulk:
        query = _make_stub('insert_bulk', identifiers)
        async with pool.cursor() as cursor:
            async with cursor.copy(query) as copy:
                await copy.write('\n'.join(map(lambda x: '\t'.join(map(str, x)), data)))
    else:
        query = _make_stub('insert', identifiers) + sql.SQL(', ').join([sql.Placeholder()]*len(data[0])) + sql.SQL(');')
        async with pool.cursor() as cursor:
            for record in data:
                await cursor.execute(query, record)  
