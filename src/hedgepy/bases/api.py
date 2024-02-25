import asyncio
from pathlib import Path
from importlib import import_module
from typing import Callable, Literal
from dataclasses import dataclass
from collections import UserDict

from hedgepy.bases.vendor import APIEndpoint, APIFormattedResponse


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


def request(task: Task) -> APIFormattedResponse: 
    getter: Callable = next(filter(lambda x: x.__name__ == task.method, task.endpoint.getters))
    response = getter(*task.args, **task.kwargs)
    return response


class ResponseManager(UserDict):
    def __init__(self):
        super().__init__()
        self.data = {}
        self._lock = asyncio.Lock()


class RequestManager:
    SLEEP_MS = 50
    
    def __init__(self, root: str):
        self._parent_loop = asyncio.get_event_loop()
        self.vendors = self._load_vendors(Path(root) / 'vendors')
        self.children: dict[str, asyncio.BaseEventLoop] = {}
        self._task_queue_urgent = asyncio.PriorityQueue()
        self._task_queue_normal = asyncio.LifoQueue()
        self.queues_in = {'urgent': self._task_queue_urgent, 'normal': self._task_queue_normal}
                
    def _load_vendors(self, vendor_root: Path) -> dict[str, APIEndpoint]:
        vendors = {}                    
        for vendor in vendor_root.iterdir():
            if vendor.is_dir() and not vendor.stem.startswith('_'):
                vendors[vendor.stem] = import_module(f'hedgepy.vendors.{vendor.stem}').endpoint    
        return vendors

    def _put_queue(self, task: Task, queue: Literal['urgent', 'normal']):
        self.queues_in[queue].put_nowait(task)

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
        for queue in self.queues.values():
            task = await self._poll_queue(queue)
            if isinstance(task, Task):
                response = await self._process_task(task, queue)
                return response
        await asyncio.sleep(self.SLEEP_MS/1e3)

    async def run(self):
        while True: 
            await self.cycle()

    def start(self):
        self._parent_loop.run_until_complete(self.run())
