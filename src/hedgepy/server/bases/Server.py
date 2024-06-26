import asyncio
from aiohttp import web
from pathlib import Path
from functools import partial
from uuid import UUID, uuid4
from collections import UserDict
from importlib import import_module
from inspect import signature

from hedgepy.common import API


class Task(asyncio.Task):    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._corr_id = str(uuid4())
    
    @classmethod
    def from_request(cls, request: API.Request | dict, endpoint: API.Endpoint) -> "Task":
        if isinstance(request, API.Request):
            request = request.js        
        
        func_name = request['endpoint']
        func = endpoint.getters.get(func_name)

        for param in signature(func).parameters.values():
            if param.name in request:
                func = partial(func, **{param.name: request[param.name]})
            
            elif param.default != param.empty:
                func = partial(func, **{param.name: param.default})
            
            elif param.name == "app":
                if endpoint.app_instance:
                    func = partial(func, app=endpoint.app_instance)
                else: 
                    raise RuntimeError(f"Missing app instance for {endpoint}")           
                
            elif param.name in ("kwargs", "args"):
                pass
            
            else:
                raise ValueError(f"Missing required argument: {param.name}")
        
        if asyncio.iscoroutinefunction(func):
            return cls(func())
        else:
            async def afunc():
                return func()
            return cls(afunc())
    
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


class Server:
    CYCLE_MS = 50

    def _cleanup(self):
        self._running: bool = False
        self._started: bool = False
        self._server: web.Server | None = None
        self._runner: web.ServerRunner | None = None
        self._site: web.TCPSite | None = None
        self._request_queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self._responses: ResponseManager = ResponseManager()

    def __init__(self, root: str):
        self._cleanup()        
        self._vendors: dict[str, API.Endpoint] = {
            vendor.stem: import_module(
                f'hedgepy.common.vendors.{vendor.stem}'
                ).endpoint for vendor in (Path(root) / 'common' / 'vendors').iterdir()
            }

    async def _ainit(self):
        self._server = web.Server(self._handler)
        self._runner = web.ServerRunner(self._server)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, 'localhost', 8080)
        await self._site.start()

        tasks = []
        for endpoint in self._vendors.values():
            if endpoint.app_constructor and endpoint.loop:
                app = endpoint.construct_app()
                task = asyncio.create_task(
                    endpoint.loop.start_fn(
                        app,
                        *endpoint.loop.start_fn_args,
                        **endpoint.loop.start_fn_kwargs,
                    )
                )
                tasks.append(task)
            elif endpoint.loop:
                task = asyncio.create_task(
                    endpoint.loop.start_fn(
                        *endpoint.loop.start_fn_args, 
                        **endpoint.loop.start_fn_kwargs
                    )
                )
                tasks.append(task)
        self._started = True
        return tasks

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
        self._request_queue.put_nowait(request_task)
        return web.json_response({'corr_id': request_task.corr_id})        

    async def _handler(self, request: web.BaseRequest):
        try: 
            match request.method:
                case "GET":
                    return await self._handle_get(request)
                case 'POST':
                    return await self._handle_post(request)
                case _:
                    return web.Response(status=405)
        except Exception as e:
            return web.Response(status=500, text=str(e))

    async def _next_request(self) -> Task | None:
        if not self._request_queue.empty():
            return self._request_queue.get_nowait()
        else:
            await asyncio.sleep(self.CYCLE_MS/1e3)
            return None

    @property
    def started(self) -> bool:
        return self._started

    @property
    def running(self) -> bool:
        return self._started and self._running

    def vendor(self, name: str) -> API.Endpoint:
        return self._vendors[name]

    @property
    def vendors(self) -> dict[str, API.Endpoint]:
        return self._vendors

    async def run(self):
        if self.started:
            self._running = True
            while self.running:
                print("Server cycle")
                if request := await self._next_request():
                    response = await request
                    await self._responses.__setitem__(request.corr_id, response)
        else:
            raise RuntimeError('Server not running')

    async def stop(self):
        self._running = False
        await self._site.stop()
        await self._runner.cleanup()
        self._cleanup()
