import json
import asyncio
from abc import ABC, abstractmethod
from functools import wraps
from itertools import chain
from typing import Any, Callable, Awaitable, Self, Generator
from dataclasses import dataclass, asdict
from uuid import uuid4, UUID
from collections import UserString, deque
from pathlib import Path
from importlib import import_module

from yarl import URL
from aiohttp import ClientSession

from hedgepy.common.utils import config, dtwrapper, logger


LOGGER = logger.get(__name__)


App = Any | ClientSession
AppConstructor = Callable[["Context"], "App"]
AppRunner = Callable[["App"], Awaitable]
CorrID = str | int | UUID
CorrIDFn = Callable[["App"], CorrID]
Target = Callable[["App", "Request", "Context"], Awaitable["Response"]]
Getters = dict[str, Target | "RateLimitedGetter"]
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
    def _immutable(*args, **kwargs):
        raise AttributeError("Context is immutable")


@dataclass
class RequestParams:
    start: dtwrapper.datetime | None = None
    end: dtwrapper.datetime | None = None
    resolution: dtwrapper.timedelta | None = None
    symbol: str | None = None

    def __post_init__(self):
        for attr in ("start", "end"):
            if getattr(self, attr) and not isinstance(getattr(self, attr), dtwrapper.datetime):
                try:
                    setattr(self, attr, dtwrapper.str_to_dt(getattr(self, attr)))
                except ValueError:
                    raise ValueError(f"Invalid datetime format for {attr}")
        if isinstance(self.resolution, str):
            try:
                self.resolution = dtwrapper.str_to_td(self.resolution)
            except ValueError:
                raise ValueError(f"Invalid timedelta format for {self.resolution}")
        self._kwargs = {k: v for k, v in asdict(self).items() if v is not None}
        
    @property
    def kwargs(self) -> dict:
        return self._kwargs
    
    @classmethod
    def from_js(cls, js: dict) -> Self:
        dtwrapper.UDt.select_fmt()
        if js.get("start"):
            js["start"] = dtwrapper.UDt.convert(js["start"])
        if js.get("end"):
            js["end"] = dtwrapper.UDt.convert(js["end"])
        if js.get("resolution"):
            js["resolution"] = dtwrapper.UDur.convert(js["resolution"])
        return cls(**js)
    
    def to_js(self) -> dict:
        dtwrapper.UDt.select_fmt()
        return {
            "start": dtwrapper.UDt.convert(self.start),
            "end": dtwrapper.UDt.convert(self.end),
            "resolution": dtwrapper.UDur.convert(self.resolution),
            "symbol": self.symbol
        }


@dataclass
class Request:
    def __init__(
        self,
        vendor: str,
        endpoint: str,
        params: RequestParams,
        corr_id: CorrID | None = None,
    ):
        self.vendor = vendor
        self.endpoint = endpoint
        self.params = params
        self.corr_id = corr_id  # corr_id is only set server-side

    def to_js(self):
        return {
            "vendor": self.vendor,
            "endpoint": self.endpoint,
            "params": self.params.to_js(),
            }  # to_js is only called client-side, so corr_id is not included

    @classmethod
    def from_js(cls, js: dict, corr_id: CorrID | None = None) -> Self:  # from_js is only called server-side
        params = js.pop("params", {})
        return cls(**js, corr_id=corr_id, params=RequestParams.from_js(params))
    
    @classmethod
    def from_template(cls, common: dict, template: dict) -> Self:
        request_js, params_js = {}, {}
        for k, v in chain(common.items(), template.items()):
            if k in ("start", "end", "resolution", "symbol"):
                params_js[k] = v
            elif k in ("vendor", "endpoint"):
                request_js[k] = v
            else: 
                raise ValueError(f"Invalid request parameter {k}={v}")
        request_js["params"] = params_js
        return cls.from_js(request_js)


@dataclass
class Response:
    request: Request
    data: tuple[tuple[Any]] | None = None
    
    def to_js(self):
        return {
            "request": self.request.to_js(),
            "data": self.data
            }
        
    @classmethod
    def from_js(cls, js: dict) -> Self:
        request_js = js.pop("request")
        corr_id = request_js.pop("corr_id")
        request = Request.from_js(request_js, corr_id)
        return cls(request=request, **js)


def register_getter(returns: Fields, streams: bool = False, formatter: Formatter | None = None) -> Target:
    """
    Decorator function to register an API endpoint.

    Args:
        returns (Fields): Field names and their corresponding types.
        streams (bool): Indicates if the endpoint streams. Defaults to False.
        formatter (Formatter, optional): A function to format the response. Defaults to None.

    Returns:
        Getter: The decorated function.

    """
    def decorator(getter: Target) -> Target:
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


class Getter:
    def __init__(self, target: Target):
        self.target = target
        self._lock = asyncio.Lock()
        
    @property
    def returns(self) -> Fields:
        return self.target.returns
    
    @property
    def formatter(self) -> Formatter:
        return self.target.formatter
    
    @property
    def streams(self) -> bool:
        return self.target.streams
        
    async def __call__(self, app: App, request: Request, context: Context, **kwargs) -> Response:
        async with self._lock:
            return await self.target(app, request, context, **kwargs)


class RateLimiter(Getter):
    def __init__(self, target: Target, max_requests: int, interval: str):
        super().__init__(target)
        self.interval = dtwrapper.str_to_td(interval).total_seconds()
        self.history = deque(maxlen=max_requests)

    async def __call__(self, app: App, request: Request, context: Context) -> Response:
        time = dtwrapper.now()
        
        try:
            elapsed = time - self.history.popleft()
        except IndexError:  # history is empty
            
            elapsed = self.interval
        if elapsed < self.interval:
            sleep_time = self.interval - elapsed
            LOGGER.info(f"Rate limit exceeded for {request.vendor} {request.endpoint}, waiting {sleep_time}s")
            await asyncio.sleep(sleep_time)
        response = await self.target.__call__(app, request, context)
        self.history.append(time)

        return response


class TimeChunker(Getter):
    def __init__(self, target: Target, chunk_schedule: dict[str, str], corr_id_fn: CorrIDFn):  # dict[resolution, max_duration]
        super().__init__(target)
        self.chunk_schedule = dict(zip(
            map(dtwrapper.str_to_td, chunk_schedule.keys()), 
            map(dtwrapper.str_to_td, chunk_schedule.values())))
        self._corr_id_fn = corr_id_fn
        
    async def _chunk(self, app: App, request: Request, context: Context, 
                     n_chunks: int, max_duration: dtwrapper.timedelta) -> dict[CorrID, Response]:
        responses = {}        

        corr_id, request_start = request.corr_id, request.params.start
        request_end = request_start + max_duration

        for _ in range(n_chunks - 1):
            request = Request(
                corr_id=corr_id,
                vendor=request.vendor, 
                endpoint=request.endpoint,
                params=RequestParams(
                    start=request_start, 
                    end=request_end, 
                    resolution=request.params.resolution, 
                    symbol=request.params.symbol)
                )
            
            responses[request.corr_id] = await self.target.__call__(app, request, context)

            request_start = request_end + request.params.resolution
            request_end = request_start + max_duration
            corr_id = self._corr_id_fn(app)
            
        stub_request = Request(
            corr_id=corr_id,
            vendor=request.vendor, 
            endpoint=request.endpoint,
            params=RequestParams(
                start=request_end + request.params.resolution, 
                end=request.params.end, 
                resolution=request.params.resolution, 
                symbol=request.params.symbol)
            )
        
        responses[stub_request.corr_id] = await self.target.__call__(app, stub_request, context)
        return responses
    
    async def __call__(self, app: App, request: Request, context: Context) -> Response:
        request_end = request.params.end if request.params.end else dtwrapper.timestamp()
        request_duration = request_end - request.params.start
        responses = None

        for resolution, max_duration in self.chunk_schedule.items():
            if request.params.resolution <= resolution:                
                if (n_chunks := request_duration // max_duration) > 1:
                    LOGGER.info(f"Chunking {request.vendor} {request.endpoint} into {n_chunks} chunks")
                    responses = await self._chunk(app, request, context, n_chunks, max_duration)

        if not responses:  # the first if statement never evaluated to True; no chunking required
            responses = await self.target.__call__(app, request, context)

        return responses
           

@dataclass
class VendorSpec:
    getters: Getters
    app_constructor: AppConstructor | None = None
    app_runner: AppRunner | None = None
    context: Context | None = None
    corr_id_fn: CorrIDFn | None = None

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
        self.getters = getters
        self.runner = runner
        self.corr_id_fn = corr_id_fn if corr_id_fn else uuid4
        
    def __getitem__(self, endpoint: str) -> Target:
        return self.getters[endpoint]

    @classmethod
    def from_spec(cls, spec: VendorSpec) -> Self:
        if isinstance(spec.app_constructor, HTTPSessionSpec):
            app = ClientSession(
                base_url=spec.app_constructor.url(), 
                headers=spec.app_constructor.headers, 
                cookies=spec.app_constructor.cookies
                )
        else: 
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
    def __init__(self):
        self._vendors = self.load_vendors(root=config.SOURCE_ROOT)
        
    @property
    def vendors(self):
        return self._vendors

    @staticmethod
    def load_vendors(root: str):
        vendors = {}
        for vendor in (Path(root) / 'common' / 'vendors').iterdir():
            mod = import_module(f"hedgepy.common.vendors.{vendor.stem}")
            vendors[vendor.stem] = Vendor.from_spec(mod.spec)
            if hasattr(mod.spec.context, "DTFMT"):
                dtwrapper.UDt.register_fmt(vendor.stem, mod.spec.context.DTFMT)
        return vendors

    async def stop_vendors(self):
        for vendor in self._vendors.values():
            if hasattr(vendor, "stop"):  # ClientSession does not have a stop method
                if asyncio.iscoroutine(vendor.stop):
                    await vendor.stop()
                else:
                    vendor.stop()

    def start_vendors(self) -> tuple[Awaitable]:
        tasks = []
        for vendor in self._vendors.values():
            if vendor.runner:
                tasks.append(vendor.runner(vendor.app))
        return tuple(tasks)  # to be awaited via asyncio.gather(*tasks)
    