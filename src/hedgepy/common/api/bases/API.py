import json
import asyncio
from abc import ABC, abstractmethod
from functools import wraps
from typing import Any, Callable, Awaitable, Self, Generator
from dataclasses import dataclass, asdict
from uuid import uuid4, UUID
from collections import UserString
from pathlib import Path
from importlib import import_module

from yarl import URL

from hedgepy.common.utils import config


AppConstructor = Callable[["Context"], "App"]
AppRunner = Callable[["App"], Awaitable]
CorrID = str | int | UUID
CorrIDFn = Callable[["App"], CorrID]
Getter = Callable[["App", "RequestParams", "Context"], Awaitable["Response"]]
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
            return (
                RequestParams(
                    start=self.start, 
                    end=self.end, 
                    resolution=self.resolution, 
                    symbol=symbol
                    ) for symbol in self.symbol)
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
        self.vendor = vendor
        self.endpoint = endpoint
        self.context = context
        self.params = params
        self.corr_id = corr_id if corr_id else uuid4()

    @property
    def js(self):
        return {
            "params": self.params,
            "vendor": self.vendor,
            "endpoint": self.endpoint,
            "corr_id": self.corr_id
            }

    def encode(self) -> str:
        return json.dumps(self.js())


@dataclass
class Response:
    request: Request
    data: tuple[tuple[Any]] | None = None


def register_getter(returns: Fields, streams: bool = False, formatter: Formatter | None = None) -> Getter:
    """
    Decorator function to register an API endpoint.

    Args:
        returns (Fields): Field names and their corresponding types.
        streams (bool): Indicates if the endpoint streams. Defaults to False.
        formatter (Formatter, optional): A function to format the response. Defaults to None.

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
    

class App(ABC):
    @abstractmethod
    def start(self): 
        ...
        
    @abstractmethod
    def stop(self):
        ...
        
    @abstractmethod
    def get(self, *args, **kwargs) -> Awaitable[Response]:
        ...


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
        app: App,
        context: Context,
        getters: Getters,
        runner: AppRunner | None = None,
        corr_id_fn: CorrIDFn | None = None,
    ):
        self.app = app
        self.context = context
        self.gettters = getters
        self.runner = runner
        self.corr_id_fn = corr_id_fn if corr_id_fn else uuid4

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


class Vendors:
    def __init__(self, root: str):
        self._vendors = self.load_vendors(root)
    
    @staticmethod
    def load_vendors(root: str):
        vendors = {}
        for vendor in (Path(root) / 'common' / 'vendors').iterdir():
            mod = import_module(f"hedgepy.common.vendors.{vendor.stem}")
            vendors[vendor.stem] = Vendor.from_spec(mod.spec)
        return vendors

    async def stop_vendors(self):
        for vendor in self._vendors.values():
            if hasattr(vendor, "stop"):  # ClientSession does not have a stop method
                if asyncio.iscoroutine(vendor.stop):
                    await vendor.stop()
                else:
                    vendor.stop()

    def start_vendors(self) -> tuple[Awaitable]:
        tasks = filter(lambda coro_or_none: coro_or_none is not None,
                       map(lambda vendor: vendor.start(), 
                           filter(lambda vendor: hasattr(vendor, "start"), self._vendors.values())  # ClientSession does
                           )                                                                        # not have a start
                       )                                                                            # method
        return tuple(tasks)  # to be awaited via asyncio.gather(*tasks)
    