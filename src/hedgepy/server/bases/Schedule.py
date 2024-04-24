import datetime
import asyncio
from dataclasses import dataclass
from uuid import UUID

from hedgepy.common.bases import API, Consumer


@dataclass
class ScheduleItem:
    request: API.Request
    interval: int | None = None

    @classmethod
    def from_request(cls, request: API.Request):
        return cls(request=request, interval=request.params.resolution)
        
        
@dataclass
class Schedule:
    start: datetime.timedelta 
    stop: datetime.timedelta 
    interval: datetime.timedelta 
    items: tuple[ScheduleItem] | None = None

    @property
    def cycles(self):
        return (self.stop - self.start) // self.interval
    
    def struct(self, vendors: dict[str, API.Vendor]):
        di = {vendor: {} for vendor in vendors.keys()}
        for item in self.items:
            if (long_handle := item.request.params.resource.encode()) not in di[item.request.vendor]:
                di[item.request.vendor][long_handle] = []
            endpoint_name = item.request.endpoint
            endpoint = vendors[item.request.vendor].getters[endpoint_name]
            fields = endpoint.returns
            field_names = list(zip(*fields))[0]
            di[item.request.vendor][long_handle].append({
                "fields": field_names,
                "endpoint": endpoint_name,
                "date_range": (item.request.params.start, item.request.params.end),
                "resolution": item.request.params.resolution
            })
        return di


class Daemon(Consumer.BaseConsumer):
    START_OFFSET_S = 5
    
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
        await asyncio.sleep(Daemon.START_OFFSET_S)
        while self._running:
            print("Daemon cycle:", self._cycle)
            if self._cycle < self._schedule.cycles:
                await self.consume()
            else: 
                await self.stop()
            await asyncio.sleep(self._schedule.interval.total_seconds())
            
    async def start(self):
        self._running = True
        await self.run()
        
    async def stop(self):
        self._running = False
        await self._session.close()
        