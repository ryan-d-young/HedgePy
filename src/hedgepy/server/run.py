import asyncio
from hedgepy.server import init


async def main():
    server, db, daemon, schedule = await init.init()
    daemon.set_schedule(schedule.items)
    
    await server.start()
#    await asyncio.gather(server.run(), daemon.start(), *tasks)
    
if __name__ == '__main__':
    asyncio.run(main(), debug=True)
    