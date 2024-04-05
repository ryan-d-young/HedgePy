import json
from functools import wraps
from typing import Any, Callable, Awaitable
from dataclasses import dataclass, asdict
from uuid import uuid4, UUID
from yarl import URL

from hedgepy.common.utils import config


@dataclass
class EnvironmentVariable:
    name: str
    
    def __post_init__(self):
        self._value = config.get(self.name)

    @property
    def value(self):
        return self._value


@dataclass
class RequestParams:
    start: str | None = None
    end: str | None = None
    resolution: str | None = None
    symbol: tuple[str] | None = None

    def __post_init__(self):
        self._kwargs = {k: v for k, v in asdict(self).items() if v is not None}
    
    @property
    def kwargs(self) -> dict:
        return self._kwargs


CorrID = str | int | UUID


class Request:
    def __init__(self, params: RequestParams, vendor: str, endpoint: str, corr_id: CorrID | None = None):
        self.params: dict = params.kwargs
        self.vendor = vendor
        self.endpoint = endpoint
        self.corr_id = corr_id if corr_id else uuid4()

    def js(self):
        return {
            "params": self.params,
            "vendor": self.vendor,
            "endpoint": self.endpoint,
            "corr_id": str(self.corr_id) if isinstance(self.corr_id, UUID) else self.corr_id
        }
        
    def encode(self) -> str:
        return json.dumps(self.js())


@dataclass
class Response:
    corr_id: CorrID
    data: tuple[tuple[Any]] | None = None

    @property
    def js(self):
        return asdict(self)


@dataclass
class HTTPSessionSpec:
    host: str
    scheme: str = "http"
    port: int | None = None
    headers: dict[str, str] | None = None
    cookies: dict[str, str] | None = None
    params: dict[str, str] | None = None
    
    @property
    def url(self):
        return URL.build(
            scheme=self.scheme,
            host=self.host,
            port=self.port,
        )


Getter = Callable[[Request], Awaitable[Response]]


def register_getter(
    returns: tuple[tuple[str, type]],
    streams: bool = False,
    formatter: Callable[[Response], Response] | None = None
) -> Getter:
    """
    Decorator function to register an API endpoint.

    Args:
        returns (tuple[tuple[str, type]]): A tuple of field names and their corresponding types.
        streams (bool, optional): Indicates if the endpoint streams. Defaults to False.

    Returns:
        Getter: The decorated function.

    """
    def decorator(getter: Getter) -> Getter:
        @wraps(getter)
        def wrapper(request: Request) -> Awaitable[Response]:
            return getter(request)
        wrapper.returns = returns
        wrapper.streams = streams
        wrapper.formatter = formatter
        return wrapper
    return decorator


@dataclass
class VendorSpec:
    getters: dict[str, Getter]
    app_constructor: Callable[[None], Any] | None = None
    app_constructor_kwargs: dict | HTTPSessionSpec | None = None
    app_runner: Callable[[None], Awaitable] | None = None

    def __post_init__(self):
        self.name = self.__module__.split(".")[-2]
