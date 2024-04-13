import asyncio
from aiohttp import web, ClientResponse
from uuid import UUID
from collections import UserDict
from abc import ABC, abstractmethod
from time import time
from typing import Awaitable

from hedgepy.common.bases import API
from hedgepy.common.utils import config, dtwrapper, logger


LOGGER = logger.get(__name__)


class ResponseManager(UserDict):
    def __init__(self):
        super().__init__()
        self._lock = asyncio.Lock()
        
    async def __getitem__(self, key: UUID) -> API.Response:
        async with self._lock:
            return super().__getitem__(key)

    get = __getitem__
            
    async def __setitem__(self, key: UUID, value: API.Response) -> None:
        async with self._lock:
            super().__setitem__(key, value)      
            
    set = __setitem__
            
    async def pop(self, key: UUID) -> API.Response:
        async with self._lock:
            return super().pop(key)        


class LogicMixin(ABC):
    CYCLE_MS = 50
    LONG_CYCLE_MS = 1e3 - CYCLE_MS

    def _cleanup(self):
        self._running: bool = False
        self._request_queue: asyncio.Queue = asyncio.Queue()
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
    def requests(self) -> asyncio.Queue:
        return self._request_queue
    
    @property
    def responses(self) -> ResponseManager:
        return self._responses
        
    @property
    def vendors(self) -> API.Vendors:
        return self._vendors.vendors
    
    async def start(self):
        LOGGER.info("Starting server")
        await asyncio.gather(
            *self._vendors.start_vendors(), 
            self.start_server(), 
            self.start_loop()
            )
        
    async def stop(self):
        LOGGER.info("Stopping server")
        await asyncio.gather(
            self._vendors.stop_vendors(), 
            self.stop_server(), 
            self.stop_loop()
            )
        
    async def cycle(self):
        try: 
            request: API.Request = self._request_queue.get_nowait()
            vendor: API.Vendor = self.vendors[request.vendor]
            fn: API.Target = vendor[request.endpoint]
            LOGGER.debug(f"Processing request {request}")
            
            response = fn(vendor.app, request, vendor.context)

            if isinstance(response, Awaitable):
                response = await response

            if fn.formatter:
                response = fn.formatter(response)

            await self.responses.set(key=request.corr_id, value=response)
    
        except asyncio.QueueEmpty:
            await asyncio.sleep(LogicMixin.LONG_CYCLE_MS / 1e3)
        except Exception as e:
            LOGGER.error(f"Error processing request: {e}")
            raise e  # TODO: error handling
    
    async def _handle_get(self, request: web.BaseRequest) -> web.Response:
        LOGGER.debug(f"Received GET request {request}")
        request_js = await request.json()
        
        if request_js['corr_id'] in self.responses:
            response = await self.responses.pop(request_js['corr_id'])
            return web.json_response(response.to_js())
        else:
            return web.Response(status=404)

    async def _handle_post(self, request: web.BaseRequest) -> web.Response:
        LOGGER.debug(f"Received POST request {request}")
        request_js = await request.json()
        vendor = self.vendors[request_js['vendor']]
        corr_id = vendor.corr_id_fn(vendor.app)        
        request_obj = API.Request.from_js(request_js, corr_id)
        await self.requests.put(request_obj)
        
        return web.json_response({'corr_id': corr_id})
    