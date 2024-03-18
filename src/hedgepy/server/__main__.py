import asyncio
import os
from pathlib import Path
from hedgepy.server import init, parse, process
from hedgepy.server.bases import Server, Database


async def main():
    server = Server.Server(Path(os.getcwd()) / 'src' / 'hedgepy')
    db = Database.Database('hedgepy_dev', 'localhost', 5432, 'ryan', 'curly9-radio5')
    await init.main(server, db)
    

if __name__ == "__main__":
    asyncio.run(main())
    