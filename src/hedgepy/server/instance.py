import asyncio
import json
from aiohttp import web
from pathlib import Path
from importlib import import_module
from uuid import UUID

from hedgepy.common import API
from hedgepy.server.bases.Database import DatabaseManager
from hedgepy.server.bases.Server import ServerManager, Task
    

class API_Instance:
    RETRY_MS = 1000
    MAX_RETRIES = 60

    def __init__(self, root: str, password: str):
        self._root = root
        self._event_loop = asyncio.get_event_loop()
        
        self.vendors: dict[str, API.Endpoint] = self._load_vendors(
            Path(root) / 'src' / 'hedgepy' / 'common' / 'vendors')

        self._database_manager = DatabaseManager(self, password)
        self._server_manager = ServerManager(self)
        
        self._retries: dict[UUID, int] = {}

    async def start(self):
        for routine in (
            self._init_vendors, 
            self._server_manager.start, 
            self._database_manager.start, 
            self._request_manager.start):
            await routine()
        
    def _load_vendors(self, vendor_root: Path) -> dict[str, API.Endpoint]:
        vendors = {}                    
        for vendor in vendor_root.iterdir():
            if vendor.is_dir() and not vendor.stem.startswith('_'):
                vendors[vendor.stem] = import_module(f'hedgepy.common.vendors.{vendor.stem}').endpoint    
        return vendors
    
    async def _init_vendors(self):
        for endpoint in self.vendors.values():
            if endpoint.app_constructor and endpoint.loop:
                app = endpoint.construct_app()
                asyncio.create_task(
                    endpoint.loop.start_fn(
                        app, 
                        *endpoint.loop.start_fn_args, 
                        **endpoint.loop.start_fn_kwargs
                    )
                )
            elif endpoint.loop:
                asyncio.create_task(
                    endpoint.loop.start_fn(
                        *endpoint.loop.start_fn_args, 
                        **endpoint.loop.start_fn_kwargs
                    )
                )

    def _request(self, task: Task, urgent: bool) -> UUID:
        self._request_manager._put_queue(task, urgent=urgent)

    def _check_retries(self, corr_id: UUID) -> None:
        if corr_id in self._retries:
            retries = self._retries[corr_id] = self._retries[corr_id] + 1
            if retries > self.MAX_RETRIES:
                raise ValueError(f"Fetching response {corr_id} has exceeded the maximum number of retries")
        else:
            self._retries[corr_id] = 1
    
    async def response(self, request: web.Request) -> API.FormattedResponse:
        request_js = json.loads(await request.text())
        corr_id = UUID(request_js['corr_id'])
        
        try: 
            return await self._response_manager.pop(corr_id)
        except KeyError:
            self._retries = self._check_retries(corr_id)
            await asyncio.sleep(self.RETRY_MS/1e3)
            return await self.response(corr_id)

    async def request(self, request: web.Request) -> UUID:
        request_js = json.loads(await request.text())    
        corr_id = self._request(**request_js)
        return web.json_response({'corr_id': corr_id})

    async def status(self, _) -> web.Response:
        return web.json_response({
            'server': 'running',  # if a response is received, the server is running
            'api': 'running' if self._request_manager._running else 'stopped',
            'db': 'running' if self._database_manager._pool._open else 'stopped',
        })
        