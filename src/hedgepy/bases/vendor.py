import dotenv
import requests
from pathlib import Path
from functools import wraps, partial
from typing import Any, Callable
from dataclasses import dataclass
from uuid import uuid4


@dataclass
class APIEnvironmentVariable:
    name: str
    value: str
    
    @classmethod
    def from_dotenv(cls, key: str, dotenv_path: str = '.env'):
        return cls(name=key, 
                   value=dotenv.get_key(Path(dotenv_path), key))


@dataclass
class APIEventLoop:
    start_fn: Callable
    stop_fn: Callable | None = None
    start_fn_args: tuple | None = None
    start_fn_kwargs: dict | None = None
    stop_fn_args: tuple | None = None
    stop_fn_kwargs: dict | None = None
    
    def __post_init__(self):
        self.started = False

    def start(self):
        if not self.started: 
            self.start_fn()
            self.started = True

    def stop(self):
        if self.started: 
            self.stop_fn()
            self.started = False


@dataclass
class APIEndpointMetadata:
    date_format: str | None = None
    time_format: str | None = None
    datetime_format: str | None = None
    
    def __post__init__(self):
        if not self.date_format:
            self.date_format = '%Y-%m-%d'
        if not self.time_format:
            self.time_format = '%H:%M:%S'
        if not self.datetime_format:
            self.datetime_format = f'{self.date_format} {self.time_format}'


@dataclass
class APIEndpoint:
    app_constructor: Callable | None = None
    app_constructor_args: tuple | None = None
    app_constructor_kwargs: dict | None = None
    loops: tuple[APIEventLoop] | None = None
    getters: tuple[Callable] | None = None
    environment_variables: tuple[APIEnvironmentVariable] | None = None
    metadata: APIEndpointMetadata | None = None
    
    def __post_init__(self):
        if not self.metadata:
            self.metadata = APIEndpointMetadata()
        if self.app_constructor and not self.app_constructor_args:
            self.app_constructor_args = ()
        if self.app_constructor and not self.app_constructor_kwargs:
            self.app_constructor_kwargs = {}
        if not (self.app_constructor or self.getters):
            raise ValueError('APIEndpoint must have either an app_constructor or getters')
    

@dataclass
class APIRequest:
    vendor: str
    endpoint: str
    args: tuple | None = None
    kwargs: dict | None = None
    corr_id: str | None = None
    
    def __post_init__(self):
        if not self.corr_id:
            self.corr_id = str(uuid4())
        if not self.args:
            self.args = ()
        if not self.kwargs:
            self.kwargs = {}


@dataclass
class APIResponseMetadata:
    request: requests.PreparedRequest
    page: int = 0
    num_pages: int = 0
    other: dict | None = None

    def __post_init__(self):
        self.remaining_pages = self.num_pages - self.page


@dataclass
class APIResponse:
    data: tuple[tuple[Any]]
    fields: tuple[tuple[str, type]] | None = None
    metadata: APIResponseMetadata | None = None
    corr_id: str | None = None    
    
    def __post_init__(self):
        if not self.corr_id:
            self.corr_id = str(uuid4())


"""Note: due to the non-default argument 'metadata' in Response, we cannot subclass it under FormattedResponse"""
"""as doing so clashes with the init logic for dataclass, so instead we 'manually' subclass by copy/pasting..."""


@dataclass
class APIFormattedResponse:
    fields: tuple[tuple[str, type]]
    data: tuple[tuple[Any]]
    vendor_name: str
    endpoint_name: str
    metadata: APIResponseMetadata | None = None
    id: str | None = None
    
    @classmethod
    def format(cls, response: APIResponse, vendor_name: str, endpoint_name: str) -> 'APIFormattedResponse':
        return cls(fields=response.fields, 
                   data=response.data, 
                   vendor_name=vendor_name, 
                   endpoint_name=endpoint_name, 
                   metadata=response.metadata, 
                   id=response.id)


def rest_get(base_url: str, 
             headers: dict[str, str] | None = None,
             suffix: str | None = None,
             directory: tuple[str] | None = None, tags: dict[str, str] | None = None
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
    return partial(rest_get, base_url=base_url, **kwargs)


def format_rest_response(response: requests.Response) -> tuple[tuple[Any]]:
    data = tuple()
    for key, value in response.json(): 
        if isinstance(value, dict):
            record = (key, *tuple(value.values))
        elif isinstance(value, list) | isinstance(value, tuple):
            record = (key, *value)
        else: 
            raise TypeError('Formatting failed due to incorrect record type:\n'
                            f'RECORD: {record}\n'
                            f'TYPE: {type(record)} (allowed: list, tuple, dict)\n'
                            'Please consider writing a custom formatting function and registering it via generic_rest.register')
        data += record
    return data


def register_endpoint(formatter: Callable[[requests.Response], APIResponse]) -> Callable[..., APIFormattedResponse]:
    def decorator(endpoint: Callable[..., requests.Response]): 
        @wraps(endpoint)
        def wrapper(*args, **kwargs) -> APIFormattedResponse:            
            vendor_name: str = endpoint.__module__.split('.')[-1]
            endpoint_name: str = endpoint.__name__
            raw_response = endpoint(*args, **kwargs)            
            interim_response = formatter(raw_response)
            final_response = APIFormattedResponse.format(interim_response, vendor_name, endpoint_name)
            return final_response
        return wrapper
    return decorator
