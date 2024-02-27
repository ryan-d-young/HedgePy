import asyncio
from psycopg_pool import AsyncConnectionPool
from pathlib import Path
from importlib import import_module
from typing import Callable
from dataclasses import dataclass
from collections import UserDict
from functools import partial
from uuid import uuid4, UUID

from hedgepy.bases.vendor import APIEndpoint, APIEventLoop, APIFormattedResponse
from hedgepy.bases.database import make_identifiers, parse_response, validate_response_data, QUERIES


    
    

@dataclass
class Task:
    endpoint: APIEndpoint
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
        
    async def __getitem__(self, key: APIFormattedResponse) -> APIFormattedResponse:
        async with self._lock:
            super().__getitem__(key)
            
    async def __setitem__(self, key: APIFormattedResponse, value: APIFormattedResponse) -> None:
        async with self._lock:
            super().__setitem__(key, value)      
            
    async def pop(self, key: APIFormattedResponse) -> Task:
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

    async def _process_task(self, task: Task, queue: asyncio.Queue) -> APIFormattedResponse:
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
                response: APIFormattedResponse = await self._process_task(task, queue)
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
    def __init__(self, parent: 'API'):
        self._parent = parent
        self._pool = pool = AsyncConnectionPool()
        self.queries = self._bind_queries(pool)
                
    def _bind_queries(self, pool: AsyncConnectionPool) -> dict[str, Callable]:
        queries = {}
        for query, func in QUERIES.items():
            queries[query] = partial(func, pool=self._pool)
        return queries
    
    def query(self, query: str, *args, **kwargs):
        return self.queries[query](*args, **kwargs)
    
    def postprocess_response(self, response: APIFormattedResponse) -> tuple[tuple, tuple]:
        identifiers, _, data = parse_response(response)
        schema_name, table_name, _ = identifiers

        endpoint = self._parent.vendors[response.vendor_name]
        meth = getattr(endpoint.getters, response.endpoint_name)
        fields_all, fields_discard = meth.fields, meth.discard
        fields_out = ((field_name, field_dtype) for field_name, field_dtype in fields_all 
                        if field_name not in fields_discard)
        fields_out_names = tuple(map(lambda x: x[0], fields_out))
        fields_out_dtypes = tuple(map(lambda x: x[1], fields_out))

        if fields_discard:        
            data_out = ()
            discard_ix = tuple(map(lambda x: fields_out_names.index(x), fields_discard))
            fields_out_names = tuple(filter(lambda x: x[0] not in fields_discard, enumerate(fields_out_names)))
            fields_out_dtypes = tuple(filter(lambda x: x[0] not in discard_ix, enumerate(fields_out_dtypes)))
            for record in data:
                data_out += (tuple(filter(lambda x: x[0] not in discard_ix, enumerate(record))),)
        else: 
            data_out = data

        return schema_name, table_name, fields_out_names, fields_out_dtypes, data
            
    def prepare_to_store_response(self, 
                                  schema_name: str, 
                                  table_name: str, 
                                  fields_out_names: tuple[str], 
                                  fields_out_dtypes: tuple[type], 
                                  data: tuple[tuple]) -> tuple[tuple, tuple[tuple]]:        
        schema, table, columns = make_identifiers(schema_name, table_name, fields_out_names)

        schema_exists = self.query('check_schema', (schema,))
        if not schema_exists:
            self.query('create_schema', (schema,))

        table_exists = self.query('check_table', (schema, table))
        if not table_exists:
            self.query('create_table', (schema, table, columns), fields_out_dtypes)            

        validate_response_data(fields_out_dtypes, data)

        identifiers = schema, table, columns
        return identifiers, data
    
    def check_existing_data(self, task: Task):
        raise NotImplementedError("To do")
    

class API:
    WAIT_FOR_RESPONSE_MS = 1000

    def __init__(self, root: str):
        self.event_loop = asyncio.get_event_loop()
        self.vendors: dict[str, APIEndpoint] = self._load_vendors(Path(root) / 'vendors')

        self._response_manager = ResponseManager()
        self._request_manager = RequestManager(self)
        self._database_manager = DatabaseManager(self)

    async def start(self):
        await self._init_vendors()
        await self._request_manager.start()
        
    def _load_vendors(self, vendor_root: Path) -> dict[str, APIEndpoint]:
        vendors = {}                    
        for vendor in vendor_root.iterdir():
            if vendor.is_dir() and not vendor.stem.startswith('_'):
                vendors[vendor.stem] = import_module(f'hedgepy.vendors.{vendor.stem}').endpoint    
        return vendors
    
    async def _init_vendors(self):
        for endpoint in self.vendors.values():
            if endpoint.app_constructor and endpoint.loop:
                app = endpoint.app_constructor(*endpoint.app_constructor_args, **endpoint.app_constructor_kwargs)
                await self.event_loop.create_task(
                    endpoint.loop.start_fn(
                        app=app, *endpoint.loop.start_fn_args, **endpoint.loop.start_fn_kwargs))
            elif endpoint.loop:
                await self.event_loop.create_task(
                    endpoint.loop.start_fn(
                        *endpoint.loop.start_fn_args, **endpoint.loop.start_fn_kwargs))

    def request(self, vendor: str, method: str, *args, **kwargs) -> UUID:
        task = Task(endpoint=self.vendors[vendor], method=method, args=args, kwargs=kwargs)
        self._request_manager._put_queue(task)
        return task.corr_id
    
    async def response(self, corr_id: UUID) -> APIFormattedResponse:
        try: 
            return await self._response_manager.pop(corr_id)
        except KeyError:
            await asyncio.sleep(self.WAIT_FOR_RESPONSE_MS/1e3)
            return await self.response(corr_id)
