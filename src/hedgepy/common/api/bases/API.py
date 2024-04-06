import json
from functools import wraps
from typing import Any, Callable, Awaitable, Self, Generator
from dataclasses import dataclass, asdict
from uuid import uuid4, UUID
from collections import UserString

from aiohttp import ClientSession
from yarl import URL

from hedgepy.common.utils import config


App = ClientSession | Any
AppConstructor = Callable[["Context"], App]
AppRunner = Callable[[App], Awaitable]
CorrID = str | int | UUID
CorrIDFn = Callable[[App], CorrID]
Getter = Callable[[App, "RequestParams", "Context"], Awaitable["Response"]]
Getters = dict[str, Getter]
Formatter = Callable[["Response"], "Response"]
Field = tuple[str, type]
Fields = tuple[Field]


@dataclass
class EnvironmentVariable:
    name: str
    
    def __post_init__(self):
        self._value = config.get(f"${self.name}")

    @property
    def value(self):
        return self._value


class Context:
    def __init__(
        self, 
        static_vars: dict[str, str | int | float] | None = None, 
        derived_vars: dict[str, Callable[[Self], str | int | float]] | None = None
        ):
        if static_vars: 
            for key, value in static_vars.items():
                setattr(self, key, value)

            if derived_vars:
                for key, value in derived_vars.items():
                    if callable(value):
                        setattr(self, key, value(self))
                    else: 
                        raise ValueError(f"Derived variable {key} must be a callable that takes self as an argument")
                    
        elif derived_vars:
            raise ValueError("Derived variables require static variables")
                    
        self.__setattr__ = self._immutable
        self.__delattr__ = self._immutable

    @staticmethod
    def _immutable():
        raise AttributeError("Context is immutable")


class Symbol(UserString):
    def split(self) -> tuple[str]:
        return super().split(":")


@dataclass
class RequestParams:
    start: str | None = None
    end: str | None = None
    resolution: str | None = None
    symbol: tuple[Symbol] | Symbol | None = None

    def __post_init__(self):
        self._kwargs = {k: v for k, v in asdict(self).items() if v is not None}
    
    @property
    def kwargs(self) -> dict:
        return self._kwargs
    
    def chunk(self) -> Generator["RequestParams", None, None]:
        if isinstance(self.symbol, tuple):
            return (RequestParams(
                start=self.start, end=self.end, resolution=self.resolution, symbol=symbol) for symbol in self.symbol)
        else:
            return (self,)


class Request:
    def __init__(
        self,
        vendor: str,
        endpoint: str,
        context: Context,
        params: RequestParams,
        corr_id: CorrID | None = None,
    ):
        self.params: dict = params.kwargs
        self.vendor = vendor
        self.endpoint = endpoint
        self.corr_id = corr_id if corr_id else uuid4()
        self.context = context

    def prepare(self):
        return {
            "params": self.params,
            "vendor": self.vendor,
            "endpoint": self.endpoint,
            "corr_id": str(self.corr_id) if isinstance(self.corr_id, UUID) else self.corr_id
        }

    def encode(self) -> str:
        return json.dumps(self.prepare())


@dataclass
class Response:
    corr_id: CorrID
    data: tuple[tuple[Any]] | None = None

    @classmethod
    def from_request(cls, request: Request, data: Any) -> Self:
        return cls(corr_id=request.corr_id, data=data)


def register_getter(returns: Fields, streams: bool = False, formatter: Formatter | None = None) -> Getter:
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
        def wrapper(app: Any, params: RequestParams, *args, **kwargs) -> Awaitable[Response]:
            return getter(app, params, *args, **kwargs)
        wrapper.returns = returns
        wrapper.streams = streams
        wrapper.formatter = formatter
        return wrapper
    return decorator


@dataclass
class HTTPSessionSpec:
    host: str = ""  # empty string required for yarl
    scheme: str = ""  # empty string required for yarl
    port: int | None = None
    headers: dict[str, str] | None = None
    cookies: dict[str, str] | None = None
   
    def url(self):
        _url = URL.build(scheme=self.scheme, host=self.host, port=self.port)
        return _url if _url != URL("") else None  # hack required for yarl <> aiohttp compatibility
    
    def __call__(self, context: Context) -> ClientSession:
        return ClientSession(
            base_url=self.url(),
            headers=self.headers, 
            cookies=self.cookies
        )


@dataclass
class VendorSpec:
    getters: dict[str, Getter]
    app_constructor: Callable[[Context], App] | HTTPSessionSpec | None = None
    app_runner: Callable[[App], Awaitable] | None = None
    context: Context | None = None
    corr_id_fn: Callable[[App], CorrID] | None = None

    def __post_init__(self):
        self.name = self.__module__.split(".")[-2]


class Vendor:
    def __init__(
        self,
        app: Any,
        context: Context,
        getters: Getters,
        runner: AppRunner | None = None,
        corr_id_fn: CorrIDFn | None = None,
    ):
        self.app = app
        self.context = context
        self.gettters = getters
        self.runner = runner
        self.corr_id_fn = corr_id_fn if corr_id_fn else lambda _: uuid4()

    @classmethod
    def from_spec(cls, spec: VendorSpec) -> Self:
        app = spec.app_constructor(spec.context)
        return cls(app, spec.context, spec.getters, spec.app_runner, spec.corr_id_fn)

    def request(self, endpoint: str, params: RequestParams) -> Request:
        if endpoint in self.getters:
            return Request(
                vendor=self.name, 
                endpoint=endpoint, 
                params=params, 
                context=self.context, 
                corr_id=self.corr_id_fn(self.app)
                )
        else:
            raise ValueError(f"Endpoint {endpoint} not found in {self.name}")
        
    def response(self, request: Request, data: Any) -> Response:
        return Response.from_request(request, data)
