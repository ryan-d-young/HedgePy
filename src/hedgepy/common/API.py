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
class VendorSpec:
    app_constructor: Callable | None = None
    app_constructor_kwargs: dict | None = None
    app_runner: Callable | None = None
    getters: dict[str, Callable] | None = None
    
    def __post_init__(self):
        if not (self.app_constructor or self.getters):
            raise ValueError('VendorSpec must have an app_constructor and/or getter(s)')
        if self.app_constructor and not self.app_constructor_kwargs:
            self.app_constructor_kwargs = {}

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
    
    @property
    def kwargs(self):
        return {
            'start': self.start,
            'end': self.end,
            'resolution': self.resolution,
            'symbol': self.symbol
        }


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
    data: tuple[tuple[Any]] | None = None
    corr_id: str | int | None = None
    metadata: ResponseMetadata | None = None
    
    def __post_init__(self):
        if not self.corr_id:
            self.corr_id = str(uuid4())
                

"""Note: we cannot subclass APIResponse in APIFormattedResponse as doing so clashes with dataclass inheritance"""
"""Unfortunately this means manually copiying APIResponse's __init__ signature before expanding upon it"""


@dataclass
class FormattedResponse:
    data: tuple[tuple[Any]]
    vendor_name: str
    endpoint_name: str
    corr_id: str | None = None
    metadata: ResponseMetadata | None = None
    
    @classmethod
    def format(cls, 
               response: Response, 
               vendor_name: str, 
               endpoint_name: str, 
               ) -> 'FormattedResponse':
        return cls(data=response.data,
                   vendor_name=vendor_name, 
                   endpoint_name=endpoint_name, 
                   corr_id=response.corr_id,
                   metadata=response.metadata
                   ) 

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


def register_endpoint(fields: tuple[tuple[str, type]],
                      formatter: Callable[[requests.Response], Response] | None = None, 
                      streaming: bool = False
                      ) -> Callable[..., FormattedResponse]:
    """
    Decorator function that registers an API endpoint.

    Args:
        formatter (Callable[[requests.Response], Response]): A function that formats the raw response from the endpoint.
        fields (tuple[tuple[str, type]] | None, optional): A tuple of field names and types for the formatted response. Defaults to None.
        streaming (bool, optional): Indicates whether the endpoint supports streaming. Defaults to False.

    Returns:
        Callable[..., FormattedResponse]: The decorated endpoint function.

    """
    def decorator(endpoint: Callable[..., requests.Response]): 
        @wraps(endpoint)
        def wrapper(*args, **kwargs) -> FormattedResponse:
            vendor_name: str = endpoint.__module__.split('.')[-1]
            endpoint_name: str = endpoint.__name__

            raw_response = endpoint(*args, **kwargs)
            response = formatter(raw_response) if formatter else raw_response
            return FormattedResponse.format(
                response, vendor_name, endpoint_name)
                
        wrapper.fields = fields
        wrapper.streaming = streaming
        
        return wrapper
    return decorator
