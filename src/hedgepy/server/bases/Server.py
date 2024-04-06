import asyncio
from aiohttp import web
from pathlib import Path
from functools import partial
from uuid import UUID, uuid4
from collections import UserDict
from inspect import signature, Parameter
from typing import Coroutine, Callable

from hedgepy.common.api.bases import API


class Task(asyncio.Task):    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._corr_id = str(uuid4())
    
    @staticmethod
    def _bind_param(func: Callable, param: Parameter, request: dict, endpoint: API.VendorSpec) -> Callable:
        if param.name in request:
            return partial(func, **{param.name: request[param.name]})
        elif param.default != param.empty:
            return partial(func, **{param.name: param.default})
        elif param.name == "app":
            if endpoint.app_instance:
                func = partial(func, app=endpoint.app_instance)
            else: 
                raise RuntimeError(f"Missing app instance for {endpoint}")           
        else:
            raise ValueError(f"Missing required argument: {param.name}")
        return func
    
    @classmethod
    def from_request(cls, request: API.RequestParams | dict, endpoint: API.VendorSpec) -> "Task":
        if isinstance(request, API.RequestParams):
            request = request.js
        
        func_name = request['endpoint']
        func = endpoint.getters.get(func_name)

        for param in signature(func).parameters.values():
            func = cls._bind_param(func, param, request, endpoint)
        
        if asyncio.iscoroutine(func):
            return cls(func())
        else:
            async def _coro(func):
                return func()
            return cls(_coro(func))
    
    @property
    def corr_id(self) -> str:
        return self._corr_id


class ResponseManager(UserDict):
    def __init__(self):
        super().__init__()
        self._lock = asyncio.Lock()
        
    async def __getitem__(self, key: UUID) -> API.FormattedResponse:
        async with self._lock:
            return super().__getitem__(key)
            
    async def __setitem__(self, key: UUID, value: API.FormattedResponse) -> None:
        async with self._lock:
            super().__setitem__(key, value)      
            
    async def pop(self, key: UUID) -> Task:
        async with self._lock:
            return super().pop(key)        


class VendorMixin:
    @staticmethod
    def load_vendors(root: str):
        vendors = {}
        for vendor in (Path(root) / 'common' / 'vendors').iterdir():
            vendors[vendor.stem] = API.Vendor.from_module(f"hedgepy.common.vendors.{vendor.stem}")
        return vendors

    async def stop_vendors(self):
        for vendor in self.vendors.values():
            await vendor.stop()

    async def start_vendors(self) -> tuple[Coroutine]:
        tasks = filter(
            lambda coro_or_none: coro_or_none is not None,
            map(lambda vendor: vendor.start(), self.vendors.values()),
        )
        return tuple(tasks)


class WebMixin:
    async def start_server(self):
        server = web.Server(self._handler)
        self._runner = web.ServerRunner(server)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, 'localhost', 8080)
        await self._site.start()
    
    async def stop_server(self):
        await self._site.stop()
        await self._runner.cleanup()

    async def _handle_put(self, request: web.BaseRequest) -> web.Response:
        request_bytes = await request.read()
        ...

    async def _handle_get(self, request: web.BaseRequest) -> web.Response:
        request_js = await request.json()
        if request_js['corr_id'] in self._responses:
            response = await self._responses.pop(request_js['corr_id'])
            response_js = response.js
            return web.json_response(response_js)
        else:
            return web.Response(status=404)

    async def _handle_post(self, request: web.BaseRequest) -> web.Response:
        request_js = await request.json()
        endpoint = self._vendors[request_js['vendor']]
        request_task = Task.from_request(request=request_js, endpoint=endpoint)
        await self._request_queue.put(request_task)
        return web.json_response({'corr_id': request_task.corr_id})

    async def _handler(self, request: web.BaseRequest):
        try: 
            match request.method:
                case "GET":
                    return await self._handle_get(request)
                case "POST":
                    return await self._handle_post(request)
                case "PUT":
                    return await self._handle_put(request)
                case _:
                    return web.Response(status=405)
        except Exception as e:
            return web.Response(status=500, text=str(e))


class Server(VendorMixin, WebMixin):
    CYCLE_MS = 50
    LONG_CYCLE_MS = 1e3

    def _cleanup(self):
        self.vendors: dict[str, API.VendorSpec] = {}
        self._running: bool = False
        self._started: bool = False
        self._site: web.TCPSite | None = None
        self._runner: web.ServerRunner | None = None
        self._request_queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self._responses: ResponseManager = ResponseManager()

    def __init__(self, root: str):
        self._cleanup()
        self.vendors = self.load_vendors(root)

    async def _next_request(self) -> Coroutine | None:
        try:
            return await self._request_queue.get()
        except asyncio.QueueEmpty:
            return None

    async def _cycle(self):
        if request := await self._next_request():
            response = await request
            await self._responses.__setitem__(request.corr_id, response)
        
    async def start(self):
        await self.start_server()
        await asyncio.gather(
            self.run(),
            self.start_vendors(),
        )

    async def run(self):
        self._running = True
        while self._running:
            print("Server cycle")
            if not self._request_queue.empty():
                try: 
                    await self._cycle() 
                except KeyboardInterrupt:
                    await self.stop()
                finally: 
                    await asyncio.sleep(Server.CYCLE_MS / 1e3)
            else: 
                await asyncio.sleep(Server.LONG_CYCLE_MS / 1e3)            

    async def stop(self):
        self._running = False
        await self.stop_server()
        await self.stop_vendors()
        self._cleanup()
