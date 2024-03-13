import inspect
from typing import Callable
from functools import partial

from hedgepy.common import API
from dev.src.hedgepy.server.instance import Task


def _bind_request(request: API.Request, vendor: API.Endpoint) -> tuple:
    meth = getattr(vendor.getters, request.endpoint)
    for param in inspect.signature(meth).parameters.values():
        if param.name in request:
            value = request[param.name]
        
        elif param.default != param.empty:
            value = param.default
        
        elif param.name == "app":
            if vendor.app_instance:
                value = vendor.app_instance            
            else: 
                raise RuntimeError(f"Missing app instance for {vendor}")                
        
        else:
            raise ValueError(f"Missing required argument: {param.name}")

        meth = partial(meth, **{param.name: value})
    
    return meth


def ingest_request(request: API.Request, vendor: API.Endpoint) -> Task:
    bound_resource = _bind_request(request, vendor)
    return Task(bound_func=bound_resource)


def ingest_requests(
    requests: dict[str, tuple[API.Request]], 
    vendors: dict[str, API.Endpoint]
    ) -> dict[str, tuple[Task]]:
    tasks = {vendor_name: tuple() for vendor_name in vendors.keys()}
    for template_name, requests in requests.items():
        tasks[template_name] = tuple()
        for request in requests:
            vendor = vendors[request.vendor]
            task = ingest_request(request, vendor)
            tasks[request.vendor] += (task,)
    return tasks
