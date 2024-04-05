from typing import Any

import aiohttp
from yarl import URL

from .bases import API, Data


def session(spec: API.HTTPSessionSpec) -> aiohttp.ClientSession:
    return aiohttp.ClientSession(
        base_url=URL(host=spec.host, scheme=spec.scheme, port=spec.port),
        headers=spec.headers,
        cookies=spec.cookies
    )


def request(
    session: aiohttp.ClientSession,
    corr_id: API.CorrID,
    url: str,
    **kwargs,
) -> tuple[API.CorrID, aiohttp.client._RequestContextManager]:
    return corr_id, session.request(method="get", url=url, **kwargs)


async def get(ctx_mgr: aiohttp.client._RequestContextManager, corr_id: API.CorrID) -> API.Response:
    async with ctx_mgr as response:
        data: Any = await response.json()
        return API.Response(corr_id=corr_id, data=data)


def format(response: API.Response, data: Data.Tbl) -> API.Response:
    return API.Response(corr_id=response.corr_id, data=data)
