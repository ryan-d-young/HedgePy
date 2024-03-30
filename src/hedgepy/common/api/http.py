import yarl
import aiohttp
from typing import Any, Callable, Coroutine
from dataclasses import dataclass, asdict
from uuid import uuid4, UUID

from hedgepy.common.utils import config


@dataclass
class SessionSpec:
    host: str
    scheme: str = "http"
    port: int | None = None
    key: str | None = None
    headers: dict[str, str] | None = None
    cookies: dict[str, str] | None = None

    def __post_init__(self):
        self.url = yarl.URL.build(scheme=self.scheme, host=self.host, port=self.port)
        
        
@dataclass
class Request:
    method: str = "GET"
    headers: dict[str, str] | None = None
    params: dict[str, str] | None = None
    data: aiohttp.FormData | None = None
    
    def __post_init__(self):
        self._corr_id: UUID = uuid4()

        
class Session:
    def __init__(self, spec: SessionSpec):
        self.url: yarl.URL = spec.url
        self._spec: SessionSpec = spec
        self._session: aiohttp.ClientSession | None = None
        
    async def _ainit(self):
        self._session = aiohttp.ClientSession(
            cookies=self._spec.cookies,
            headers=self._spec.headers
            )
        
    async def request(self, req: Request) -> Coroutine:
        kwargs = asdict(req)
        return self._session.request(url=self.url, **kwargs)    
