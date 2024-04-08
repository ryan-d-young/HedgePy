import asyncio
from aiohttp import web
from uuid import UUID
from collections import UserDict
from abc import ABC, abstractmethod

from hedgepy.common.api.bases import API
from hedgepy.common.utils import config


class ResponseManager(UserDict):
    def __init__(self):
        super().__init__()
        self._lock = asyncio.Lock()
        
    async def __getitem__(self, key: UUID) -> API.Response:
        async with self._lock:
            return super().__getitem__(key)
            
    async def __setitem__(self, key: UUID, value: API.Response) -> None:
        async with self._lock:
            super().__setitem__(key, value)      
            
    async def pop(self, key: UUID) -> API.Response:
        async with self._lock:
            return super().pop(key)        


class LogicMixin(ABC):
    CYCLE_MS = 50
    LONG_CYCLE_MS = 1e3

    def _cleanup(self):
        self._running: bool = False
        self._request_queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self._responses: ResponseManager = ResponseManager()

    @abstractmethod
    async def cycle(self):
        ...
        
    async def start_loop(self):
        self._running = True
        await self.run()

    async def run(self):
        while self._running:
            try: 
                print("Server cycle")
                await self.cycle() 
            except KeyboardInterrupt:
                await self.stop_loop()
            finally: 
                await asyncio.sleep(LogicMixin.CYCLE_MS / 1e3)                

    async def stop_loop(self):
        self._running = False
        self._cleanup()


class BaseServer(LogicMixin, ABC):
    def __init__(self):
        self._runner: web.ServerRunner = None
        self._site: web.TCPSite = None
    
    async def start_server(self):
        server = web.Server(self._handler)
        self._runner = web.ServerRunner(server)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, config.get("server.host"), config.get("server.port"))
        await self._site.start()
    
    async def stop_server(self):
        await self._site.stop()
        await self._runner.cleanup()

    @abstractmethod
    async def _handle_get(self, response: web.BaseRequest) -> web.Response:
        ...

    @abstractmethod
    async def _handle_post(self, request: web.BaseRequest) -> web.Response:
        ...

    async def _handler(self, request: web.BaseRequest):
        try: 
            match request.method:
                case "GET":
                    return await self._handle_get(request)
                case "POST":
                    return await self._handle_post(request)
                case _:
                    return web.Response(status=405)
        except Exception as e:
            return web.Response(status=500, text=str(e))


class Server(BaseServer):
    def __init__(self):
        super().__init__()
        self._cleanup()
        self._vendors = API.Vendors()
        
    @property
    def vendors(self):
        return self._vendors
        
    async def cycle(self):
        try: 
            request: API.Request = await self._request_queue.get()
    
            if request: 
                vendor = self.vendors[request.vendor]
                fn = vendor[request.endpoint]
                result = await fn(vendor.app, request.params, vendor.context)
    
                if hasattr(result, "json"):
                    result = await result.json()                

                response = API.Response(request=request, data=result)

                if fn.formatter:
                    response = fn.formatter(response)

                self._responses[request.corr_id] = response
    
        except asyncio.QueueEmpty:
            await asyncio.sleep(LogicMixin.LONG_CYCLE_MS / 1e3)
        except Exception as e:
            raise e  # TODO: error handling
    
    async def _handle_get(self, response: web.BaseRequest) -> web.Response:
        response_js = await response.json()
        if response_js['corr_id'] in self._responses:
            response = await self._responses.pop(response_js['corr_id'])
            return web.json_response(response.data)
        else:
            return web.Response(status=404)

    async def _handle_post(self, request: web.BaseRequest) -> web.Response:
        request_js = await request.json()
        request_obj = API.Request(**request_js)
        await self._request_queue.put(request_obj)
        return web.json_response({'corr_id': request_obj.corr_id})
    