import asyncio
from uuid import UUID
from time import time
from typing import AsyncGenerator

from aiohttp import ClientSession, web

from hedgepy.common.bases import API


class BaseConsumer:
    def __init__(self, env: dict):
        self._url = f"http://{env['SERVER_HOST']}:{env['SERVER_PORT']}"
        self._session = ClientSession()
        self.pending: dict[UUID, float] = {}
    
    async def post(self, request: API.Request) -> UUID:
        async with self._session.post(self._url, json=request.to_js()) as response:
            resp = await response.json()
            return resp['corr_id']
        
    async def get(self, corr_id: API.CorrID) -> API.Response | None:
        async with self._session.get(self._url, json={'corr_id': corr_id}) as response:
            if response.status == 404:
                return None
            else:
                response_js = await response.json()
                return API.Response.from_js(response_js)
        
    async def _roundtrip(self, request: API.Request) -> API.Response:
        corr_id = await self.post(request)
        return await self.get(corr_id)
    
    async def request(self, request: API.Request, roundtrip=True) -> API.Response:
        if roundtrip:
            return await self._roundtrip(request)
        else:
            corr_id = await self.post(request)
            self.pending[corr_id] = time()                
            
    async def flush(self) -> AsyncGenerator[API.Response, None]:
        for corr_id in self.pending:
            if response := await self.get(corr_id):
                self.pending.pop(corr_id)
                yield response
            