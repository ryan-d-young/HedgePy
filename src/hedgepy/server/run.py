import asyncio
from hedgepy.server import init


async def main():
    server, db, daemon, schedule = await init.init()
    daemon.set_schedule(schedule.items)
    
    server = await server._ainit()
    await server.run()
    await daemon.start()
    
    
if __name__ == '__main__':
    asyncio.run(main())