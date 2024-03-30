import json
from functools import wraps
from typing import Any, Callable
from dataclasses import dataclass, asdict
from uuid import uuid4, UUID


@dataclass
class VendorSpec:
    app_constructor: Callable | None = None
    app_constructor_kwargs: dict | None = None
    app_runner: Callable | None = None
    app_instance: object | None = None
    getters: dict[str, Callable] | None = None

    def __post_init__(self):
        if not (self.app_constructor or self.getters):
            raise ValueError("VendorSpec must have an app_constructor and/or getter(s)")
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
            "start": self.start,
            "end": self.end,
            "resolution": self.resolution,
            "symbol": self.symbol,
        }


@dataclass
class Response:
    corr_id: str | UUID
    data: tuple[tuple[Any]] | None = None

    def __post_init__(self):
        self.corr_id = str(self.corr_id)


@dataclass
class FormattedResponse:
    data: tuple[tuple[Any]]
    vendor_name: str
    endpoint_name: str
    corr_id: str | int | None = None

    @classmethod
    def format(
        cls,
        response: Response,
        vendor_name: str,
        endpoint_name: str,
    ) -> "FormattedResponse":
        return cls(
            data=response.data,
            vendor_name=vendor_name,
            endpoint_name=endpoint_name,
            corr_id=response.corr_id,
        )

    @property
    def js(self):
        return asdict(self)


def register_getter(
    fields: tuple[tuple[str, type]],
    formatter: Callable[[Response], FormattedResponse] | None = None,
    streaming: bool = False,
) -> Callable[..., FormattedResponse]:
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
    def decorator(getter: Callable[..., Response]) -> Callable[..., FormattedResponse]:
        @wraps(getter)
        def wrapper(*args, **kwargs) -> Response:
            vendor_name: str = getter.__module__.split(".")[-1]
            endpoint_name: str = getter.__name__
            raw_response: Response = getter(*args, **kwargs)
            response = formatter(raw_response) if formatter else raw_response
            return FormattedResponse.format(response, vendor_name, endpoint_name)

        wrapper.fields = fields
        wrapper.streaming = streaming
        return wrapper

    return decorator
