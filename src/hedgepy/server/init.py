from hedgepy.server.bases import Data
from hedgepy.server.bases.Server import Server
from hedgepy.server.bases.Database import Database


async def make_tables(schema: str, tables: tuple[str], columns: tuple[tuple[str, type]], database: Database):
    for table, cols in zip(tables, columns):
        cols = tuple((x, Data.resolve_py_type(y)) for x, y in cols)
        cols = cols + (('date', 'date'),) if 'date' not in (x for x, _ in cols) else cols
        await database.query(which='create_table', schema=schema, table=table, columns=cols)


async def make_schemas(schemas: tuple[str], database: Database):
    for schema in schemas: 
        await database.query(which='create_schema', schema=schema)


def gather_required(server: Server):
    required = {vendor: {} for vendor in server.vendors}
    for vendor, endpoint in server.vendors.items():
        for getter in endpoint.getters:
            required[vendor][getter.__name__] = getter.fields
    return required


async def reset_database(schemas: tuple[str], database: Database):
    for schema in schemas:
        await database.query(which='drop_schema', schema=schema)


async def main(server: Server, database: Database, reset_first=False):
    if reset_first:
        await reset_database(server.vendors.keys(), database)
    required = gather_required(server)
    for vendor, endpoints in required.items():
        await make_schemas((vendor,), database)
        await make_tables(vendor, endpoints.keys(), endpoints.values(), database)
    return True
