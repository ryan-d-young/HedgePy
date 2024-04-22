import asyncio
from hedgepy.server import init, routines


async def main():
    server, db, daemon = await init.init()
    schedule = routines.parse(daemon.start, daemon.stop, server)
    
    daemon.set_schedule(schedule.items)
    await asyncio.gather(server.start(), daemon.start())
    
if __name__ == '__main__':
    asyncio.run(main(), debug=True)
    