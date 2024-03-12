import asyncio
import dotenv
from hedgepy.client.bases import Client
from hedgepy.client.bridge import Bridge

bridge = Bridge()
env = dotenv.dotenv_values()
args = (Client, env['SERVER_HOST'], env['SERVER_PORT'])

if __name__ == "__main__":
    asyncio.run(bridge.ainit(*args))
else: 
    app = asyncio.run(bridge.ainit(*args))
    