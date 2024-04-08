from hedgepy.common.api.bases import API
from aiohttp import ClientSession
from typing import Awaitable
from functools import partial


def format(response: API.Response, index: str) -> API.Response:
    return API.Response(
        request=response.request,
        data=tuple(tuple(record.values()) for record in response.data[index]),
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
) -> Awaitable:
    params = merge_params(context.params, series_id=params.symbol)
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
) -> Awaitable:
    params = merge_params(
        context.params,
        series_id=params.symbol,
        observation_start=params.start,
        observation_end=params.end,
        offset=offset,
    )
    return app.get(url="/fred/series/observations", params=params)


@API.register_getter(
    returns=(("vintage_date", str)), formatter=partial(format, index="vintage_dates")
)
def get_series_vintage_dates(
    app: ClientSession,
    params: API.RequestParams,
    context: API.Context,
    offset: int = 0,
) -> Awaitable:
    params = merge_params(context.params, series_id=params.symbol, offset=offset)
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
) -> Awaitable:
    params = merge_params(context.params, series_id=params.symbol)
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
) -> Awaitable:
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
) -> Awaitable:
    params = merge_params(context.params, release_id=params.symbol)
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
) -> Awaitable:
    params = merge_params(context.params, release_id=params.symbol, offset=offset)
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
) -> Awaitable:
    params = merge_params(context.params, offset=offset)
    return app.get(url="/fred/release/dates", params=params)
