import asyncio
import argparse
import pprint
from importlib import import_module
from typing import Any, Callable

from aiohttp import ClientSession

from hedgepy.common.bases import API


parser = argparse.ArgumentParser(prog="HedgePy", description="HedgePy CLI")
parser.add_argument("vendor", type=str, help="Vendor name")
parser.add_argument("endpoint", type=str, help="Endpoint name")
parser.add_argument("-y", type=str, help="Symbol", default=None)
parser.add_argument("-s", type=str, help="Start date", default=None)
parser.add_argument("-e", type=str, help="End date", default=None)
parser.add_argument("-r", type=str, help="Resolution", default=None)

args = parser.parse_args()


def load_vendor(vendor: str) -> API.VendorSpec:
    return import_module(f"hedgepy.common.vendors.{vendor}").spec


def load_endpoint(vendor_spec: API.VendorSpec, endpoint: str) -> Callable:
    return vendor_spec.getters.get(endpoint)


async def make_session(vendor_spec: API.VendorSpec) -> ClientSession | Any:
    if vendor_spec.app_constructor: 
        app = vendor_spec.app_constructor(**vendor_spec.app_constructor_kwargs)
        if vendor_spec.app_runner:
            await vendor_spec.app_runner(app)
    else:
        app = None
        if kwargs := vendor_spec.app_constructor_kwargs:
            app = ClientSession(base_url=kwargs.url(), headers=kwargs.headers, cookies=kwargs.cookies)
    return app


async def get(
    app: ClientSession | Any,
    request: API.Request,
) -> API.Response:
    return await request.endpoint(app, request.params, request.context)


async def main():
    vendor_spec = load_vendor(args.vendor)
    endpoint = load_endpoint(vendor_spec, args.endpoint)
    app = await make_session(vendor_spec)
    
    request = vendor_spec.request(
        endpoint, API.RequestParams(resource=args.y, start=args.s, end=args.e, resolution=args.r))

    raw_response = await get(app, request)
    data = await raw_response.json()
    response = API.Response.from_request(request, data=data)

    if endpoint.formatter:
        response = endpoint.formatter(response)

    pprint.pprint(response.data)

    await app.close()


if __name__ == "__main__":
    asyncio.run(main())
