import asyncio
from aiohttp import web
from functools import partial
from uuid import UUID, uuid4
from typing import Callable
from dataclasses import dataclass
from collections import UserDict
from datetime import datetime

from hedgepy.common import API


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

        print(f"cycle: {datetime.now()}")
        await asyncio.sleep(self.CYCLE_SLEEP_MS/1e3)

    async def run(self):
        while self._running: 
            await self.cycle()

    async def start(self):
        self._running = True
        await self.run()
        
    async def stop(self):
        self._running = False
        
        
class ServerManager(RequestManager, ResponseManager):
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
