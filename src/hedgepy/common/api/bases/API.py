import json
from asyncio import iscoroutine
from functools import wraps, partial
from typing import Any, Callable, Coroutine
from dataclasses import dataclass, asdict
from uuid import uuid4, UUID
from importlib import import_module

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
                    

def register_getter(
    fields: tuple[tuple[str, type]],
    formatter: Callable[[Response], Response] | None = None,
    streaming: bool = False,
) -> Callable[[Request], Coroutine[Response]]:
    """
    Decorator function to register an API endpoint.

    Args:
        fields (tuple[tuple[str, type]]): A tuple of field names and their corresponding types.
        formatter (Callable[[Response], FormattedResponse] | None, optional): A function to format the response. 
        Defaults to None.
        streaming (bool, optional): Indicates if the endpoint streams. Defaults to False.

    Returns:
        Callable[..., FormattedResponse]: The decorated function.

    """
    def decorator(getter: Callable[..., Coroutine[Response]]) -> Callable[..., FormattedResponse]:
        @wraps(getter)
        def wrapper(*args, **kwargs) -> Response:
            raw_response: Response = getter(*args, **kwargs)
            response = formatter(raw_response) if formatter else raw_response
            return response

        wrapper.fields = fields
        wrapper.streaming = streaming
        return wrapper

    return decorator


@dataclass
class VendorSpec:
    endpoints: dict[str, Endpoint]
    http_session_spec: HTTPSessionSpec | None = None
    app_constructor: Callable | None = None
    app_constructor_kwargs: dict | None = None
    app_runner: Coroutine | None = None

    def __post_init__(self):
        self.name = self.__module__.split(".")[-2]
        if self.app_constructor: 
            if not self.app_constructor_kwargs:
                self.app_constructor_kwargs = {}
            if self.http_session_spec:
                raise ValueError("Cannot have both an app constructor and an http session spec.")
        elif not self.http_session_spec:
            raise ValueError("Must have either an app constructor or an http session spec.")


class Vendor:
    def __init__(
        self,
        name: str,
        getters: dict[str, Callable[[Request], Coroutine[Response]]],
        app_instance: Any | None = None,
        app_runner: Callable[[Any], Coroutine] | None = None,
    ):
        self.name = name
        self.endpoints = getters
        self.app_instance = app_instance
        self.app_runner = app_runner

    @classmethod
    def from_spec(cls, spec: VendorSpec) -> "Vendor":
        if spec.app_constructor:
            app_instance = spec.app_constructor(**spec.app_constructor_kwargs)
            endpoints = {}
            for endpoint_key, endpoint in spec.endpoints.items():
                endpoints[endpoint_key] = partial(endpoint, app=app_instance)
            return cls(name=spec.name, endpoints=endpoints, app_instance=app_instance, app_runner=spec.app_runner)
        else:
            return cls(name=spec.name, endpoints=spec.endpoints)

    @classmethod
    def from_module(cls, module: str) -> "Vendor":
        return cls.from_spec(spec=import_module(module).spec)

    async def run(self):
        if self._runner:
            await self._runner(self.app_instance)

    async def stop(self):
        if self.app_instance: 
            if hasattr(self.app_instance, "stop"):
                coro_or_none = self.app_instance.stop()
                if iscoroutine(coro_or_none):
                    await coro_or_none
