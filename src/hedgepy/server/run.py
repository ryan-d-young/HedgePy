import asyncio
from hedgepy.server import init


async def main():
    server, db, daemon, schedule = await init.init()
    daemon.set_schedule(schedule.items)
    
    tasks = await server._ainit()
#    await asyncio.gather(server.run(), daemon.start(), *tasks)
    await asyncio.gather(server.run(), *tasks)    
    
if __name__ == '__main__':
    asyncio.run(main())
    