import asyncio
from aiohttp import web, ClientResponse
from uuid import UUID
from collections import UserDict
from abc import ABC, abstractmethod
from time import time
from typing import Awaitable
from importlib import import_module

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
        
        
class VendorMixin(ABC):
    @property
    def vendors(self) -> dict[str, API.Vendor]:
        return self._vendors
    
    def load_vendors(self):
        self._vendors = {}
        for vendor in (config.SOURCE_ROOT / "common" / "vendors").iterdir():
            mod = import_module(f"hedgepy.common.vendors.{vendor.stem}")
            self._vendors[vendor.stem] = API.Vendor.from_spec(mod.spec)
            
    async def stop_vendors(self):
        for vendor in self._vendors.values():
            if hasattr(vendor, "stop"):  # ClientSession does not have a stop method
                if asyncio.iscoroutine(vendor.stop):
                    await vendor.stop()
                else:
                    vendor.stop()

    def start_vendors(self) -> tuple[Awaitable]:
        tasks = []
        for vendor in self._vendors.values():
            if vendor.runner:
                tasks.append(vendor.runner(vendor.app))
        return tuple(tasks)  # to be awaited via asyncio.gather(*tasks)


class BaseServer(LogicMixin, VendorMixin, ABC):
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
        except Exception as e:  # TODO: error handling
            return web.Response(status=500, text=str(e))


class Server(BaseServer):
    def __init__(self):
        super().__init__()
        self._cleanup()
        self.load_vendors()
        
    @property
    def requests(self) -> asyncio.Queue:
        return self._request_queue
    
    @property
    def responses(self) -> ResponseManager:
        return self._responses
    
    async def start(self):
        LOGGER.info("Starting server")
        await asyncio.gather(
            *self.start_vendors(), 
            self.start_server(), 
            self.start_loop()
            )
        
    async def stop(self):
        LOGGER.info("Stopping server")
        await asyncio.gather(
            self.stop_vendors(), 
            self.stop_server(), 
            self.stop_loop()
            )
        
    async def cycle(self):
        try: 
            request: API.Request = self._request_queue.get_nowait()
            LOGGER.debug(f"Processing request {request}")
            
            vendor: API.Vendor = self.vendors[request.vendor]
            fn: API.Target = vendor[request.endpoint]
            response: ClientResponse | API.Response = await fn(vendor.app, request, vendor.context)

            if fn.formatter:
                response = fn.formatter(request, response)        
                if isinstance(response, Awaitable):
                    response = await response

            await self.responses.set(key=request.corr_id, value=response)
    
        except asyncio.QueueEmpty:
            LOGGER.debug("Request queue empty")
            await asyncio.sleep(LogicMixin.LONG_CYCLE_MS / 1e3)
        except Exception as e:
            LOGGER.error(f"Error processing request: {e}")
            raise e  # TODO: error handling
    
    async def _handle_get(self, request: web.BaseRequest) -> web.Response:
        LOGGER.debug(f"Received GET request {request}")
        
        request_js = await request.json()
        corr_id = request_js.get("corr_id", None)
        
        if corr_id:
            if corr_id in self.responses:
                response: API.Response = await self.responses.pop(corr_id)
                return web.json_response(response.js())
            else:
                return web.Response(status=404)
        else:
            return web.json_response({
                "pending_requests": self.requests.qsize(), 
                "pending_responses": len(self.responses)
                })
            
    async def _handle_post(self, request: web.BaseRequest) -> web.Response:
        LOGGER.debug(f"Received POST request {request}")
        
        request_js = await request.json()
        vendor = self.vendors[request_js['vendor']]
        corr_id = vendor.corr_id_fn(vendor.app)   
        request_js['corr_id'] = corr_id

        if resource_handle := request_js['params'].pop('resource', None):
            qualname, *field_values = resource_handle.split("_")
            *_, vendor, cls_name = qualname.split(".")
            cls = self.vendors[vendor].resources[cls_name]
            resource_handle = "_".join(field_values)
            resource = cls.decode(resource_handle)

        request_obj = API.Request.decode(request_js).bind_resource(resource)
        await self.requests.put(request_obj)
        
        return web.json_response({'corr_id': corr_id})
    