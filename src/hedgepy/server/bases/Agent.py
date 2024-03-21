import datetime
import asyncio
from dataclasses import dataclass
from uuid import UUID
from aiohttp import ClientSession

from hedgepy.common import API


@dataclass
class ScheduleItem:
    request: API.Request
    interval: int | None = None


@dataclass
class Schedule:
    start: datetime.timedelta 
    stop: datetime.timedelta 
    interval: datetime.timedelta 
    items: tuple[ScheduleItem] | None = None

    @property
    def cycles(self):
        return (self.stop - self.start) // self.interval


class Consumer:
    def __init__(self, env: dict):
        self._url = f"http://{env['SERVER_HOST']}:{env['SERVER_PORT']}"
        self._session = ClientSession()
    
    async def post(self, request: API.Request) -> UUID:
        async with self._session.post(self._url, json=request.js) as response:
            resp = await response.json()
            return UUID(resp['corr_id'])
        
    async def get(self, corr_id: UUID) -> API.Response:
        async with self._session.get(self._url, json={'corr_id': corr_id}) as response:
            return await response.json()

    
class Daemon(Consumer):            
    def __init__(self, env: dict):
        super().__init__(env)
        self._running = False
        self._cycle = 0
        self._schedule = Schedule(start=env['DAEMON_START'], stop=env['DAEMON_STOP'], interval=env['DAEMON_INTERVAL'])
        
    def set_schedule(self, items: tuple[ScheduleItem]):
        self._schedule.items = items
        
    async def consume(self) -> tuple[UUID]:
        corr_ids = ()
        for item in self._schedule.items:
            corr_ids += (await self.post(item.request),)
        self._cycle += 1
        return corr_ids
        
    async def run(self):
        while self._running:
            print("Daemon cycle:", self._cycle)
            if self._cycle < self._schedule.cycles:
                await self.consume()
            else: 
                await self.stop()
            await asyncio.sleep(self._schedule.INTERVAL.total_seconds())
            
    async def start(self):
        self._running = True
        await self.run()
        
    async def stop(self):
        self._running = False
        await self._session.close()
        