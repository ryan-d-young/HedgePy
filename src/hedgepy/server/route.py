import inspect
from typing import Callable
from functools import partial

from hedgepy.common import API
from hedgepy.server.bases import Resource, Task


def _map_args(resource: Resource, vendor: API.Endpoint) -> tuple:
    meth = getattr(vendor.getters, resource.endpoint)
    for param in inspect.signature(meth).parameters.values():
        if param.name in resource:
            value = resource[param.name]
        
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

def _ingest_resource(resource: Resource, vendor: API.Endpoint) -> Task:
    bound_meth = _map_args(resource, vendor)
    return Task(bound_func=bound_meth)


def ingest(resources: dict[str, tuple[Resource]], vendors: dict[str, API.Endpoint]) -> dict[str, tuple[Task]]:
    tasks = {vendor_name: tuple() for vendor_name in vendors.keys()}
    for template_name, resources in resources.items():
        tasks[template_name] = tuple()
        for resource in resources:
            vendor = vendors[resource.vendor]
            task = _ingest_resource(resource, vendor)
            tasks[resource.vendor] += (task,)
    return tasks
