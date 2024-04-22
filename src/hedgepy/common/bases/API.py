import json
import asyncio
from abc import ABC, abstractmethod
from functools import wraps
from itertools import chain
from typing import Any, Callable, Awaitable, Self, Generator
from dataclasses import dataclass, asdict
from uuid import uuid4, UUID
from collections import UserDict, namedtuple, deque
from pathlib import Path
from importlib import import_module

from yarl import URL
from aiohttp import ClientSession

from hedgepy.common.utils import config, dtwrapper, logger


LOGGER = logger.get(__name__)

NO_DEFAULT = object()

App = Any | ClientSession
AppConstructor = Callable[["Context"], "App"]
AppRunner = Callable[["App"], Awaitable]
CorrID = str | int | UUID
CorrIDFn = Callable[["App"], CorrID]
Target = Callable[["App", "Request", "Context"], Awaitable["Response"] | "Response"]
Getters = dict[str, "Getter"]
Formatter = Callable[["Response"], "Response"]
Field = namedtuple("Field", ["name", "dtype"])
Fields = tuple[Field]
Parameter = tuple[Field, bool, Any]
Parameters = tuple[Parameter]


@dataclass
class EnvironmentVariable:
    name: str
    
    def __post_init__(self):
        self._value = config.get(f"${self.name}")

    @property
    def value(self):
        return self._value


class _ImmutableDict(UserDict):
    def __init__(self, **kwargs):
        super().__init__(kwargs)
        self._name = self.__class__.__name__
        self._mod = self.__module__
        self.__setitem__ = self._immutable
        self.__delitem__ = self._immutable

    @staticmethod
    def _immutable(*args, **kwargs):
        raise AttributeError("Resource is immutable")
    
    @property
    def name(self) -> str:
        return self._name
    
    @property
    def qualname(self) -> str:
        return f"{self._mod}.{self._name}"


class Context(_ImmutableDict):
    def __init__(
        self, 
        static_vars: dict[str, str | int | float] | None = None, 
        derived_vars: dict[str, Callable[[Self], str | int | float]] | None = None
        ):
        di = static_vars if static_vars else {}
        super().__init__(**di)
        if derived_vars:
            for key, value in derived_vars.items():
                if callable(value):
                    self[key] = value(self)
                else: 
                    raise ValueError(f"Derived variable {key} must be a callable that takes self as an argument")        


class Resource(_ImmutableDict):
    CONSTANT: Parameters = ()
    VARIABLE: Parameters = ()
    
    def __init__(self, **kwargs: dict[str, Any]):
        di = {}
        
        for field, required, default in chain(self.CONSTANT, self.VARIABLE):
            if field.name in kwargs:
                arg_value = kwargs.pop(field.name)
            elif required:
                if default is NO_DEFAULT:
                    raise ValueError(f"Missing required argument {field.name}")
                else:
                    arg_value = default

            if isinstance(arg_value, field.dtype):
                di[field.name] = arg_value
            else:
                try:
                    di[field.name] = field.dtype(arg_value)
                except ValueError:
                    raise ValueError(f"Invalid type for argument {field.name}")

        if len(kwargs) > 0:
            raise ValueError(f"Invalid keyword argument(s) provided {kwargs}")
            
        super().__init__(**di)


@dataclass
class RequestParams:
    start: dtwrapper.datetime | None = None
    end: dtwrapper.datetime | None = None
    resolution: dtwrapper.timedelta | None = None
    resource: Resource | None = None

    def encode(self) -> dict:
        return {
            "start": dtwrapper.dt_to_str(self.start),
            "end": dtwrapper.dt_to_str(self.end),
            "resolution": dtwrapper.td_to_str(self.resolution),
            "resource": self.resource
        }
        
    @classmethod
    def decode(cls, js: dict) -> Self:
        return cls(
            start=dtwrapper.str_to_dt(js.get("start", None)), 
            end=dtwrapper.str_to_dt(js.get("end", None)),
            resolution=dtwrapper.str_to_td(js.get("resolution", None)),
            resource=js.get("resource", None))


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
        self.corr_id = corr_id  # corr_id is set server-side

    def encode(self) -> dict:
        return {
            "vendor": self.vendor,
            "endpoint": self.endpoint,
            "params": self.params.encode(), 
            "corr_id": self.corr_id
        }
        
    @classmethod
    def decode(cls, js: dict) -> Self:
        return cls(
            vendor=js.get("vendor", None), 
            endpoint=js.get("endpoint", None), 
            params=RequestParams.decode(js.get("params", {})), 
            corr_id=js.get("corr_id", None)
        )
    
    @classmethod
    def from_template(cls, common: dict, request: dict) -> Self:
        merged = {**common, **request}
        return cls(
            vendor=merged.pop("vendor"), 
            endpoint=merged.pop("endpoint"), 
            params=RequestParams.decode(merged)
            )


@dataclass
class Response:
    request: Request
    data: tuple[tuple[Any]] | None = None
    
    def js(self):
        return {
            "request": self.request.js(),
            "data": self.data
            }
        
    @classmethod
    def from_js(cls, js: dict) -> Self:
        request_js = js.pop("request")
        request = Request.from_js(request_js)
        return cls(request=request, **js)


def register_getter(returns: Fields, streams: bool = False, formatter: Formatter | None = None) -> Target:
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
        
    def _merge(self, request: Request, responses: dict[CorrID, Response]) -> Response:
        data = tuple(chain.from_iterable((response.data for response in responses.values())))
        return Response(request=request, data=data)
        
    async def _chunk(self, app: App, request: Request, context: Context, 
                     n_chunks: int, max_duration: dtwrapper.timedelta) -> Response:
        responses = {}
        original_request = request

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
                    resource=request.params.resource)
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
                resource=request.params.resource)
            )
        
        responses[stub_request.corr_id] = await self.target.__call__(app, stub_request, context)
        response = self._merge(request=original_request, responses=responses)
        return response
    
    async def __call__(self, app: App, request: Request, context: Context) -> Response:
        request_end = request.params.end if request.params.end else dtwrapper.timestamp()
        request_duration = request_end - request.params.start
        response = None

        for resolution, max_duration in self.chunk_schedule.items():
            if request.params.resolution <= resolution:                
                if (n_chunks := request_duration // max_duration) > 1:
                    LOGGER.info(f"Chunking {request.vendor} {request.endpoint} into {n_chunks} chunks")
                    response = await self._chunk(app, request, context, n_chunks, max_duration)

        if not response:  # the first if statement never evaluated to True; no chunking required
            response = await self.target.__call__(app, request, context)

        return response
           

@dataclass
class VendorSpec:
    getters: Getters
    app_constructor: AppConstructor | None = None
    app_runner: AppRunner | None = None
    context: Context | None = None
    corr_id_fn: CorrIDFn | None = None
    resources: tuple[Resource] | None = None

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
        resources: tuple[Resource] | None = None
    ):
        self.app = app
        self.context = context
        self.getters = getters
        self.runner = runner
        self.corr_id_fn = corr_id_fn if corr_id_fn else uuid4
        self.resources = resources
        
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
        return cls(app, spec.context, spec.getters, spec.app_runner, spec.corr_id_fn, spec.resources)

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
