from hedgepy.common.bases import API
from aiohttp import ClientSession, ClientResponse
from typing import Awaitable
from functools import partial


class Series(API.Resource):
    VARIABLE = ((API.Field("series_id", str), True, API.NO_DEFAULT),
                (API.Field("offset", int), False, 0),)


class Release(API.Resource):
    VARIABLE = ((API.Field("release_id", str), True, API.NO_DEFAULT),
                (API.Field("offset", int), False, 0),)


async def format(request: API.Request, response: ClientResponse, index: str) -> API.Response:
    async with response:
        data = await response.json()
    return API.Response(
        request=request,
        data=tuple(tuple(record.values()) for record in data[index]),
    )


def merge_params(params: dict, **kwargs) -> dict:
    merged_params = params.copy()
    merged_params.update(kwargs)
    return merged_params


@API.register_getter(
    returns=(
        ("id", str),
        ("realtime_start", str),
        ("realtime_end", str),
        ("title", str),
        ("observation_start", str),
        ("observation_end", str),
        ("frequency", str),
        ("frequency_short", str),
        ("units", str),
        ("units_short", str),
        ("seasonal_adjustment", str),
        ("seasonal_adjustment_short", str),
        ("last_updated", str),
        ("popularity", int),
        ("notes", str),
    ),
    formatter=partial(format, index="seriess"),
)
def get_series(
    app: ClientSession, params: API.RequestParams, context: API.Context
) -> Awaitable[API.Response]:
    params = merge_params(context.params, series_id=params.resource)
    return app.get(url="/fred/series", params=params)


@API.register_getter(
    returns=(
        ("realtime_start", str),
        ("realtime_end", str),
        ("date", str),
        ("value", str),
    ),
    formatter=partial(format, index="observations"),
)
def get_series_observations(
    app: ClientSession,
    params: API.RequestParams,
    context: API.Context,
    offset: int = 0,
) -> Awaitable[API.Response]:
    params = merge_params(
        context.params,
        series_id=params.resource,
        observation_start=params.start,
        observation_end=params.end,
        offset=offset,
    )
    return app.get(url="/fred/series/observations", params=params)


@API.register_getter(
    returns=(("vintage_date", str),), formatter=partial(format, index="vintage_dates")
)
def get_series_vintage_dates(
    app: ClientSession,
    params: API.RequestParams,
    context: API.Context,
    offset: int = 0,
) -> Awaitable[API.Response]:
    params = merge_params(context.params, series_id=params.resource, offset=offset)
    return app.get(url="/fred/series/vintagedates", params=params)


@API.register_getter(
    returns=(
        ("id", str),
        ("realtime_start", str),
        ("realtime_end", str),
        ("name", str),
        ("press_release", bool),
        ("link", str),
    ),
    formatter=partial(format, index="releases"),
)
def get_series_release(
    app: ClientSession, params: API.RequestParams, context: API.Context
) -> Awaitable[API.Response]:
    params = merge_params(context.params, series_id=params.resource)
    return app.get(url="/fred/series/release", params=params)


@API.register_getter(
    returns=(
        ("id", str),
        ("realtime_start", str),
        ("realtime_end", str),
        ("name", str),
        ("press_release", bool),
        ("link", str),
    ),
    formatter=partial(format, index="releases"),
)
def get_releases(
    app: ClientSession, params: API.RequestParams, context: API.Context
) -> Awaitable[API.Response]:
    return app.get(url="/fred/releases", params=context.params)


@API.register_getter(
    returns=(
        ("id", str),
        ("realtime_start", str),
        ("realtime_end", str),
        ("name", str),
        ("press_release", bool),
        ("link", str),
    ),
    formatter=partial(format, index="releases"),
)
def get_release(
    app: ClientSession, params: API.RequestParams, context: API.Context
) -> Awaitable[API.Response]:
    params = merge_params(context.params, release_id=params.resource)
    return app.get(url="/fred/release", params=params)


@API.register_getter(
    returns=(
        ("id", str),
        ("realtime_start", str),
        ("realtime_end", str),
        ("title", str),
        ("observation_start", str),
        ("observation_end", str),
        ("frequency", str),
        ("frequency_short", str),
        ("units", str),
        ("units_short", str),
        ("seasonal_adjustment", str),
        ("seasonal_adjustment_short", str),
        ("last_updated", str),
        ("popularity", int),
        ("group_popularity", int),
        ("notes", str),
    ),
    formatter=partial(format, index="seriess"),
)
def get_release_series(
    app: ClientSession,
    params: API.RequestParams,
    context: API.Context,
    offset: int = 0,
) -> Awaitable[API.Response]:
    params = merge_params(context.params, release_id=params.resource, offset=offset)
    return app.get(url="/fred/release/series", params=params)


@API.register_getter(
    returns=(
        ("release_id", str),
        ("date", str),
    ),
    formatter=partial(format, index="release_dates"),
)
def get_release_dates(
    app: ClientSession,
    params: API.RequestParams,
    context: API.Context,
    offset: int = 0,
) -> Awaitable[API.Response]:
    params = merge_params(context.params, offset=offset)
    return app.get(url="/fred/release/dates", params=params)
