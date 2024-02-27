import asyncio
from psycopg_pool import AsyncConnectionPool
from pathlib import Path
from importlib import import_module
from typing import Callable
from dataclasses import dataclass
from collections import UserDict
from functools import partial

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


class ResponseManager(UserDict):
    def __init__(self):
        super().__init__()
        self._lock = asyncio.Lock()
        
    def __getitem__(self, key: APIFormattedResponse) -> APIFormattedResponse:
        with self._lock:
            super().__getitem__(key)
            
    def __setitem__(self, key: APIFormattedResponse, value: APIFormattedResponse) -> None:
        with self._lock:
            super().__setitem__(key, value)      


class RequestManager:
    CYCLE_SLEEP_MS = 50
    
    def __init__(self, root: str, event_loop: asyncio.BaseEventLoop):
        self._parent_loop = event_loop
        self._task_queue_urgent = asyncio.PriorityQueue()
        self._task_queue_normal = asyncio.LifoQueue()

        self.vendors: dict[str, APIEndpoint] = self._load_vendors(Path(root) / 'vendors')
        self.loops: dict[str, APIEventLoop] = self._init_vendors()
        
        self.response_manager: ResponseManager = ResponseManager()
        
                
    def _load_vendors(self, vendor_root: Path) -> dict[str, APIEndpoint]:
        vendors = {}                    
        for vendor in vendor_root.iterdir():
            if vendor.is_dir() and not vendor.stem.startswith('_'):
                vendors[vendor.stem] = import_module(f'hedgepy.vendors.{vendor.stem}').endpoint    
        return vendors
    
    def _init_vendors(self) -> dict[str, APIEventLoop]:
        loops = {}
        for vendor, endpoint in self.vendors.items():
            if endpoint.app_constructor and endpoint.loop:
                app = endpoint.app_constructor(*endpoint.app_constructor_args, **endpoint.app_constructor_kwargs)
                loop = self._parent_loop.create_task(
                    endpoint.loop.start_fn(
                        app=app, *endpoint.loop.start_fn_args, **endpoint.loop.start_fn_kwargs))
            elif endpoint.loop:
                loop = self._parent_loop.create_task(
                    endpoint.loop.start_fn(
                        *endpoint.loop.start_fn_args, **endpoint.loop.start_fn_kwargs))
            else: 
                loop = None
            loops[vendor] = loop
        return loops

    def _put_queue(self, task: Task, urgent: bool = False):
        if urgent:
            self._task_queue_urgent.put_nowait(task)
        else:
            self._task_queue_normal.put_nowait(task)

    async def _process_task(self, task: Task, queue: asyncio.Queue) -> APIFormattedResponse:
        response = await self._parent_loop.run_in_executor(None, request, task)        
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
        while True: 
            await self.cycle()

    def start(self):
        self._parent_loop.run_until_complete(self.run())


class DatabaseManager:
    def __init__(self):
        self._pool = pool = AsyncConnectionPool()
        self.queries = self._bind_queries(pool)
                
    def _bind_queries(self, pool: AsyncConnectionPool) -> dict[str, Callable]:
        queries = {}
        for query, func in QUERIES.items():
            queries[query] = partial(func, pool=self.pool)
        return queries
    
    def query(self, query: str, *args, **kwargs):
        return self.queries[query](*args, **kwargs)
    
    def _check_wide_table(self, task: Task):
        
        
    
    def check_existing_data(self, task: Task):
        func = getattr(task.endpoint.getters, task.method)
        match func.table_type:
            case 'wide':
                self._check_wide_table(task)
            case 'long':
                self._check_long_table(task)


class API:
    def __init__(self, root: str):
        event_loop = asyncio.get_event_loop()
        self._request_manager = RequestManager(event_loop=event_loop, root=root)
        self._database_manager = DatabaseManager()

    