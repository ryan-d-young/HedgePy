import asyncio
import dotenv
import datetime
import json
from aiohttp import web
from psycopg_pool import AsyncConnectionPool
from pathlib import Path
from importlib import import_module
from typing import Callable, Literal
from dataclasses import dataclass
from collections import UserDict
from functools import partial
from uuid import uuid4, UUID

from hedgepy.common import API, template
from dev.src.hedgepy.server.query import make_identifiers, QUERIES


@dataclass
class Resource:
    vendor: str | None = None
    endpoint: str | None = None
    start: datetime.datetime | None = None
    end: datetime.datetime | None = None
    resolution: str | None = None
    orientation: Literal['wide', 'long'] = 'wide'
    symbol: tuple[str] | None = None
    

@dataclass
class Task:
    bound_func: Callable
    
    def __post_init__(self):
        self.corr_id = uuid4()


class ResponseManager(UserDict):
    def __init__(self):
        super().__init__()
        self._lock = asyncio.Lock()
        
    async def __getitem__(self, key: API.FormattedResponse) -> API.FormattedResponse:
        async with self._lock:
            return super().__getitem__(key)
            
    async def __setitem__(self, key: API.FormattedResponse, value: API.FormattedResponse) -> None:
        async with self._lock:
            super().__setitem__(key, value)      
            
    async def pop(self, key: API.FormattedResponse) -> Task:
        async with self._lock:
            return super().pop(key)


class RequestManager:
    CYCLE_SLEEP_MS = 50
    
    def __init__(self, api_instance: 'API_Instance'):
        self._api_instance = api_instance
        self._task_queue_urgent = asyncio.PriorityQueue()
        self._task_queue_normal = asyncio.LifoQueue()
        self._running = False

    def _put_queue(self, task: Task, urgent: bool = False):
        if urgent:
            self._task_queue_urgent.put_nowait(task)
        else:
            self._task_queue_normal.put_nowait(task)

    async def _process_task(self, task: Task, queue: asyncio.Queue) -> API.FormattedResponse:
        func = partial(getattr(task.endpoint.getters, task.method), *task.args, **task.kwargs)
        response = await self._api_instance.event_loop.run_in_executor(func=func)        
        queue.task_done()
        return response

    @staticmethod
    def _poll_queue(queue: asyncio.Queue) -> Task | None:
        try: 
            return queue.get_nowait()
        except asyncio.QueueEmpty:
            return None
    
    async def cycle(self):
        for queue in (self._task_queue_urgent, self._task_queue_normal):
            while task := self._poll_queue(queue):
                response: API.FormattedResponse = await self._process_task(task, queue)
                self._api_instance.response_manager[response] = response
                await asyncio.sleep(0)

        print(f"cycle: {datetime.datetime.now()}")
        await asyncio.sleep(self.CYCLE_SLEEP_MS/1e3)

    async def run(self):
        while self._running: 
            await self.cycle()

    async def start(self):
        self._running = True
        await self.run()
        
    async def stop(self):
        self._running = False
        

class DatabaseManager:
    def __init__(self, api_instance: 'API_Instance', password: str):
        self._api_instance = api_instance

        user = dotenv.get_key(Path(self._api_instance._root) / '.env', 'SQL_USER')
        host = dotenv.get_key(Path(self._api_instance._root) / '.env', 'SQL_HOST')
        port = dotenv.get_key(Path(self._api_instance._root) / '.env', 'SQL_PORT')
        dbname = dotenv.get_key(Path(self._api_instance._root) / '.env', 'SQL_DBNAME')
        self._pool =  AsyncConnectionPool(
            conninfo=f"dbname={dbname} user={user} host={host} port={port} password={password}", 
            open=False)
        del password

        self.queries = self._bind_queries(self._pool)
                
    def _bind_queries(self, pool: AsyncConnectionPool) -> dict[str, Callable]:
        queries = {}
        for query, func in QUERIES.items():
            queries[query] = partial(func, pool=self._pool)
        return queries
    
    def query(self, query: str, *args, **kwargs):
        return self.queries[query](*args, **kwargs)
    
    def check_preexisting_data(self, endpoint: API.Endpoint, meth: str, *args, **kwargs
                               ) -> tuple[tuple, tuple[tuple]] | None:
        raise NotImplementedError("To do")
    
    def postprocess_response(self, response: API.FormattedResponse) -> tuple[tuple, tuple]:
        endpoint = self._api_instance.vendors[response.vendor_name]
        meth = getattr(endpoint.getters, response.endpoint_name)
        fields, data = response.fields, response.data
        
        if meth.discard:
            fields, data = self._discard(response.fields, response.data, meth.discard)
            
        return fields, data

    def _discard(self, fields: tuple[tuple[str, type]], data: tuple[tuple], discard: tuple[str] | None
                 ) -> tuple[tuple[str, type], tuple[tuple]]:
        discard_ix = tuple(map(lambda x: fields.index(x), discard))
        fields = tuple(filter(lambda x: x[0] not in discard_ix, enumerate(fields)))
        data = tuple(filter(lambda x: x[0] not in discard_ix, enumerate(data)))
        return fields, data
            
    def stage_response(self, response: API.FormattedResponse, dtypes: tuple[type], data: tuple[tuple]
                       ) -> tuple[tuple, tuple[tuple]]:        
        schema, table, columns = make_identifiers(schema=response.vendor_name, 
                                                  table=response.endpoint_name, 
                                                  columns=response.fields)
                                
        schema_exists = self.query('check_schema', (schema,))
        if not schema_exists:
            self.query('create_schema', (schema,))

        table_exists = self.query('check_table', (schema, table))
        if not table_exists:
            self.query('create_table', (schema, table, columns), dtypes)            

        return (schema, table, columns), data
        
    async def start(self):
        print(f"DatabaseManager.start: {asyncio.get_event_loop()}")
        await self._pool.open()


class ServerManager:
    def __init__(self, api_instance: 'API_Instance'):
        self._api_instance = api_instance
        self._web_server = self._init_web_server()
        
    def _init_web_server(self):
        app = web.Application()
        app.router.add_post('/request', self._api_instance.request)
        app.router.add_get('/response', self._api_instance.response)
        app.router.add_get('/status', self._api_instance.status)
        return app
        
    async def start(self):
        runner = web.AppRunner(self._web_server)
        await runner.setup()
        site = web.TCPSite(runner, 'localhost', 8080)
        await site.start()
        print("server started")
        
    async def stop(self):
        await self._web_server.shutdown()
        await self._web_server.cleanup()


class API_Instance:
    RETRY_MS = 1000
    MAX_RETRIES = 60

    def __init__(self, root: str, password: str):
        self._root = root
        self._event_loop = asyncio.get_event_loop()
        
        self.vendors: dict[str, API.Endpoint] = self._load_vendors(
            Path(root) / 'src' / 'hedgepy' / 'common' / 'vendors')

        self._response_manager = ResponseManager()
        self._request_manager = RequestManager(self)
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

    def _request(self, vendor: str, method: str, *args, **kwargs) -> UUID:
        task = Task(endpoint=self.vendors[vendor], method=method, args=args, kwargs=kwargs)
        self._request_manager._put_queue(task)
        return task.corr_id

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

        try:
            template.validate(instance=request_js)
        except template.ValidationError as e:
            raise web.HTTPBadRequest(reason=str(e))
    
        corr_id = self._request(**request_js)
    
        return web.json_response({'corr_id': corr_id})

    async def status(self, _) -> web.Response:
        return web.json_response({
            'server': 'running',  # if a response is received, the server is running
            'api': 'running' if self._request_manager._running else 'stopped',
            'db': 'running' if self._database_manager._pool._open else 'stopped',
        })
        