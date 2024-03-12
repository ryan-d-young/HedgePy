import aiohttp
from textual.app import App as TextualApp


class Bridge:
    def __init__(self):
        self.api_instance: aiohttp.ClientSession = None
        self.app_instance: TextualApp = None    

    async def _start_api(self, server_host: str, server_port: int) -> aiohttp.ClientSession:
        session = aiohttp.ClientSession()
        return session

    async def _start_app(self, app_instance: TextualApp):
        await app_instance.run_async()

    async def ainit(self, app_cls, server_host: str, server_port: int):
        self.api_instance = await self._start_api(server_host, server_port)
        self.app_instance = app_cls(bridge=self)
        await self._start_app(self.app_instance)
        