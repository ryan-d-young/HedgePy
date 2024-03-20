import json
import requests
from functools import wraps, partial
from typing import Any, Callable
from dataclasses import dataclass, asdict
from uuid import uuid4, UUID

from hedgepy.common.utils import config


@dataclass
class EnvironmentVariable:
    name: str
    value: str
    
    @classmethod
    def from_config(cls, key: str):
        return cls(name=key, value=config.get(key))


@dataclass
class EventLoop:
    start_fn: Callable
    stop_fn: Callable | None = None
    start_fn_args: tuple | None = None
    start_fn_kwargs: dict | None = None
    stop_fn_args: tuple | None = None
    stop_fn_kwargs: dict | None = None
    app = None
    
    def __post_init__(self):
        self.running = False
        for attr in ('start_fn_args', 'stop_fn_args'):
            if not (tup := getattr(self, attr)):
                setattr(self, attr, ())
            else: 
                setattr(self, attr, config.replace(tup))
        for attr in ('start_fn_kwargs', 'stop_fn_kwargs'):
            if not (di := getattr(self, attr)):
                setattr(self, attr, {})
            else: 
                setattr(self, attr, config.replace(di))


@dataclass
class EndpointMetadata:
    date_format: str | None = None
    time_format: str | None = None
    datetime_format: str | None = None
    
    def __post_init__(self):
        if not self.date_format:
            self.date_format = '%Y-%m-%d'
        if not self.time_format:
            self.time_format = '%H:%M:%S'
        if not self.datetime_format:
            self.datetime_format = f'{self.date_format} {self.time_format}'


@dataclass
class Endpoint:
    app_instance: object = None
    app_constructor: Callable | None = None
    app_constructor_args: tuple | None = None
    app_constructor_kwargs: dict | None = None
    loop: EventLoop | None = None
    getters: dict[str, Callable] | None = None
    environment_variables: tuple[EnvironmentVariable] | None = None
    metadata: EndpointMetadata | None = None
    
    def __post_init__(self):
        if not self.metadata:
            self.metadata = EndpointMetadata()
        if self.app_constructor and not self.app_constructor_args:
            self.app_constructor_args = ()
        if self.app_constructor and not self.app_constructor_kwargs:
            self.app_constructor_kwargs = {}
        if not (self.app_constructor or self.getters):
            raise ValueError('APIEndpoint must have either an app_constructor or getters')
        
    def construct_app(self):
        if self.app_constructor and not self.app_instance:
            self.app_instance = self.app_constructor(*self.app_constructor_args, **self.app_constructor_kwargs)
        return self.app_instance


@dataclass
class Request:
    vendor: str | None = None
    endpoint: str | None = None
    start: str | None = None
    end: str | None = None
    resolution: str | None = None
    symbol: tuple[str] | None = None
    
    def __post_init__(self):
        self.corr_id: UUID = uuid4()
        
    @property
    def js(self):
        return asdict(self)
        
    def encode(self) -> str:
        return json.dumps(self.js)


@dataclass
class ResponseMetadata:
    request: requests.PreparedRequest
    page: int = 0
    num_pages: int = 0

    def __post_init__(self):
        self.remaining_pages = self.num_pages - self.page
        
        protocol, url = self.request.url.split('://')
        
        try:    
            base_url, tags = url.split('?')
        except ValueError:
            base_url, tags = url, None

        base_url, *directory = base_url.split('/')

        tag_dict = {}
        if tags:
            tag_pairs = tags.split('&')
            tag_dict = {tag: value for tag, value in map(lambda x: x.split('='), tag_pairs)}

        self._url = {'protocol': protocol, 'base_url': base_url, 'directory': directory, 'tags': tag_dict}
        
    @property
    def url(self):
        return self._url


@dataclass
class Response:
    data: tuple[tuple[Any]]
    fields: tuple[tuple[str, type]]
    corr_id: str | None = None
    metadata: ResponseMetadata | None = None
    
    def __post_init__(self):
        if not self.corr_id:
            self.corr_id = str(uuid4())
        assert len(self.fields) == len(self.data[0]), "Fields and data have different lengths"
                

"""Note: we cannot subclass APIResponse in APIFormattedResponse as doing so clashes with dataclass inheritance"""
"""Unfortunately this means manually copiying APIResponse's __init__ signature before expanding upon it"""


@dataclass
class FormattedResponse:
    data: tuple[tuple[Any]]
    fields: tuple[tuple[str, type]]
    vendor_name: str
    endpoint_name: str
    metadata: ResponseMetadata | None = None
    corr_id: str | None = None
    
    @classmethod
    def format(cls, 
               response: Response, 
               vendor_name: str, 
               endpoint_name: str, 
               fields: tuple[tuple[str, type]] | None = None
               ) -> 'FormattedResponse':
        return cls(data=response.data,
                   fields=fields,  
                   vendor_name=vendor_name, 
                   endpoint_name=endpoint_name, 
                   metadata=response.metadata, 
                   corr_id=response.corr_id)

    @property
    def js(self):
        return asdict(self)


def rest_get(base_url: str, 
             headers: dict[str, str] | None = None,
             suffix: str | None = None,
             directory: tuple[str] | None = None, 
             tags: dict[str, str] | None = None
        ) -> requests.Response:
    url = base_url + '/'

    if directory: 
        url += '/'.join(directory)
    
    if suffix:
        url += suffix
    
    if tags: 
        for tag, value in tags.items(): 
            url += f'&{tag}={value}'
    
    response = requests.get(url, headers=headers)

    match status_code := response.status_code:
        case 200: 
            return response
        case _:
            raise ConnectionError("Error making API request: \n"
                                  f"STATUS CODE: {status_code}\n"
                                  "MESSAGE:\n"
                                  f"{response.text}")


def bind_rest_get(base_url: str, **kwargs) -> Callable:
    if kwargs: 
        kwargs = config.replace(kwargs)
    return partial(rest_get, base_url=base_url, **kwargs)


def format_rest_response(response: requests.Response) -> tuple[tuple[Any]]:
    data = tuple()
    for key, value in response.json(): 
        if isinstance(value, dict):
            record = (key, *tuple(value.values))
        elif isinstance(value, list) or isinstance(value, tuple):
            record = (key, *value)
        else: 
            raise TypeError('Formatting failed due to incorrect record type:\n'
                            f'RECORD: {record}\n'
                            f'TYPE: {type(record)} (allowed: list, tuple, dict)\n'
                            'Please consider writing a custom formatting function and registering it '
                            'via generic_rest.register')
        data += record
    return data


def register_endpoint(formatter: Callable[[requests.Response], Response], 
                      fields: tuple[tuple[str, type]] | None = None, 
                      streaming: bool = False
                      ) -> Callable[..., FormattedResponse]:
    def decorator(endpoint: Callable[..., requests.Response]): 
        @wraps(endpoint)
        def wrapper(*args, **kwargs) -> FormattedResponse:            
            vendor_name: str = endpoint.__module__.split('.')[-1]
            endpoint_name: str = endpoint.__name__
            raw_response = endpoint(*args, **kwargs)            

            interim_response = formatter(raw_response)

            final_response = FormattedResponse.format(interim_response, 
                                                         vendor_name, 
                                                         endpoint_name, 
                                                         fields)

            return final_response
        
        wrapper.streaming = streaming
        wrapper.fields = fields
        
        return wrapper
    return decorator


def validate_response_data(py_dtypes: tuple[type], data: tuple[tuple]) -> None:
    record_len = len(data[0])
    for record in data:
        for ix, (value, dtype) in enumerate(zip(record, py_dtypes)):
            if not isinstance(value, dtype):
                try: 
                    value = dtype(value)
                except ValueError:
                    raise TypeError(f"Value {value} at index {ix} is not of type {dtype} and cannot be coerced")
            assert len(record) == record_len, f"Record {ix} has wrong length ({len(record)} / {record_len} expected)"
