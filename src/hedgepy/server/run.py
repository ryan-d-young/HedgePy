import asyncio
from hedgepy.server import routines
from hedgepy.server.bases.Server import Server
from hedgepy.server.bases.Database import Database
from hedgepy.server.bases.Schedule import Daemon, Schedule
from hedgepy.common.bases import API
from hedgepy.common.utils import config


async def prepare(server: Server, db: Database) -> tuple[list[API.Request], Schedule]:
    json_requests = routines.parse_templates()
    expected_db_struct = routines.generate_expected_db_struct(json_requests, server)
    actual_db_struct = await db.struct()
    missing, orphaned, common = routines.diff(actual_db_struct, expected_db_struct)
    preplan = routines.plan(missing, server)
    requests = routines.generate_requests(json_requests, server)
    plan = routines.generate_schedule(requests, config.get("api", "start"), config.get("api", "stop"))
    return preplan, plan


async def run_first(server: Server, db: Database, daemon: Daemon, preplan: list[API.Request])


async def main():
    server, db, daemon = await routines.init()
    preplan, plan = await prepare(server, db)
    await run_first(server, db, daemon, preplan)

    
    daemon.set_schedule(schedule.items)
    await asyncio.gather(server.start(), daemon.start())
    
if __name__ == '__main__':
    asyncio.run(main(), debug=True)
    