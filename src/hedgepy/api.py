import asyncio
import dotenv
from psycopg_pool import AsyncConnectionPool
from pathlib import Path
from importlib import import_module
from typing import Callable, Any
from dataclasses import dataclass
from collections import UserDict
from functools import partial
from uuid import uuid4, UUID

from hedgepy.bases import API
from hedgepy.bases.database import make_identifiers, QUERIES


@dataclass
class Task:
    endpoint: API.Endpoint
    method: str
    args: tuple | None = None
    kwargs: dict | None = None
    
    def __post_init__(self):
        if not self.args:
            self.args = ()
        if not self.kwargs:
            self.kwargs = {}
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
    
    def __init__(self, parent: 'API'):
        self._parent = parent
        self._task_queue_urgent = asyncio.PriorityQueue()
        self._task_queue_normal = asyncio.LifoQueue()
        self._started = False

    def _put_queue(self, task: Task, urgent: bool = False):
        if urgent:
            self._task_queue_urgent.put_nowait(task)
        else:
            self._task_queue_normal.put_nowait(task)

    async def _process_task(self, task: Task, queue: asyncio.Queue) -> API.FormattedResponse:
        func = getattr(task.endpoint.getters, task.method)
        func = partial(func, *task.args, **task.kwargs)
        response = await self._parent.event_loop.run_in_executor(func=func)        
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
            for task in self._poll_queue(queue):
                response: API.FormattedResponse = await self._process_task(task, queue)
                self.response_manager[response] = response

        await asyncio.sleep(self.CYCLE_SLEEP_MS/1e3)

    async def run(self):
        while self._started: 
            await self.cycle()

    async def start(self):
        self._started = True
        await self.run()
        
    async def stop(self):
        self._started = False
        

class DatabaseManager:
    def __init__(self, parent: 'API', password: str):
        self._parent = parent
        user = dotenv.get_key(Path(self._parent._root) / '.env', 'SQL_USER')
        host = dotenv.get_key(Path(self._parent._root) / '.env', 'SQL_HOST')
        port = dotenv.get_key(Path(self._parent._root) / '.env', 'SQL_PORT')
        dbname = dotenv.get_key(Path(self._parent._root) / '.env', 'SQL_DBNAME')
        self._pool =  AsyncConnectionPool(
            conninfo=f"dbname={dbname} user={user} host={host} port={port} password={password}", 
            open=False)
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
        endpoint = self._parent.vendors[response.vendor_name]
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
        
    def start(self):
        self._pool.open()


class API:
    WAIT_FOR_RESPONSE_MS = 1000

    def __init__(self, root: str, password: str):
        self._root = root

        self.event_loop = asyncio.get_event_loop()
        self.vendors: dict[str, API.Endpoint] = self._load_vendors(Path(root) / 'src' / 'hedgepy' / 'vendors')

        self._response_manager = ResponseManager()
        self._request_manager = RequestManager(self)
        self._database_manager = DatabaseManager(self, password)

    async def start(self):
        await self._init_vendors()
        await self._request_manager.start()
        await self._database_manager.start()
        
    def _load_vendors(self, vendor_root: Path) -> dict[str, API.Endpoint]:
        vendors = {}                    
        for vendor in vendor_root.iterdir():
            if vendor.is_dir() and not vendor.stem.startswith('_'):
                vendors[vendor.stem] = import_module(f'hedgepy.vendors.{vendor.stem}').endpoint    
        return vendors
    
    async def _init_vendors(self):
        for endpoint in self.vendors.values():
            if endpoint.app_constructor and endpoint.loop:
                app = endpoint.app_constructor(*endpoint.app_constructor_args, **endpoint.app_constructor_kwargs)
                await self.event_loop.run_in_executor(
                    None,
                    endpoint.loop.start_fn,
                    app, 
                    *endpoint.loop.start_fn_args, 
                    **endpoint.loop.start_fn_kwargs)
            elif endpoint.loop:
                await self.event_loop.run_in_executor(
                    None,
                    endpoint.loop.start_fn,
                    *endpoint.loop.start_fn_args, 
                    **endpoint.loop.start_fn_kwargs)

    def request(self, vendor: str, method: str, *args, **kwargs) -> UUID:
        task = Task(endpoint=self.vendors[vendor], method=method, args=args, kwargs=kwargs)
        self._request_manager._put_queue(task)
        return task.corr_id
    
    async def response(self, corr_id: UUID) -> API.FormattedResponse:
        try: 
            return await self._response_manager.pop(corr_id)
        except KeyError:
            await asyncio.sleep(self.WAIT_FOR_RESPONSE_MS/1e3)
            return await self.response(corr_id)
