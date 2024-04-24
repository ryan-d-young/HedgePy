from hedgepy.common.bases import API
from hedgepy.server.bases.Server import Server

from itertools import product


def _generate_resource(vendor: API.Vendor, resource_name: str) -> tuple[API.Resource, str]:
    resource_cls_name, resource_handle, duration_str = resource_name.split("$")
    resource_cls = vendor.resources[resource_cls_name]
    resource_inst = resource_cls.decode(resource_handle)
    return resource_inst, duration_str


def _locate_endpoints(vendor: API.Vendor, columns: list[str]):
    def _matches(vendor: API.Vendor, columns: list[str]) -> dict:
        matches = {}
        for endpoint_name, endpoint in vendor.getters.items():
            endpoint_columns = list(zip(*endpoint.returns))[0]
            if len([column for column in columns if column not in endpoint_columns]) == 0:  # no extras
                matches[endpoint_name] = [column for column in columns if column in endpoint_columns]
        return matches
    
    def _match(matched_columns: dict, remaining_columns: list[str]) -> tuple[str, list[str]]:
        best, score = None, 0
        for endpoint_name, endpoint_columns in matched_columns.items():
            endpoint_score = len(set(remaining_columns) & set(endpoint_columns))
            if endpoint_score > score:
                best, score = endpoint_name, endpoint_score
                remaining_columns = [column for column in remaining_columns if column not in endpoint_columns]
        return best, remaining_columns
                    
    endpoints = {}
    remaining_columns = columns.copy()
    while len(remaining_columns) > 0:
        matches = _matches(vendor, remaining_columns)
        prev_remaining_columns = remaining_columns.copy()
        match, remaining_columns = _match(matches, remaining_columns)
        if len(remaining_columns) == len(prev_remaining_columns):
            break
        endpoints[match] = matches[match]
    
    if len(remaining_columns) == 0:
        return endpoints
    else:
        raise ValueError(f"Could not locate endpoints for columns: {remaining_columns}")        


def _locate_endpoint(vendor: API.Vendor, columns: list[str]):
    endpoint = None
    for endpoint_name, endpoint in vendor.getters.items():
        endpoint_columns = list(zip(*endpoint.returns))[0]
        if all(column in endpoint_columns for column in columns):
            endpoint = endpoint_name
            break
    if not endpoint:
        return _locate_endpoints(vendor, columns)
    else:
        return {endpoint: columns}


def _generate_date_ranges(missing_resource_date_range: dict) -> tuple[str, str]:
    ranges = ()
    if missing_resource_date_range["start"]:
        expected_start, actual_start = missing_resource_date_range["start"]
        ranges += ((expected_start, actual_start),)
    if missing_resource_date_range["end"]:
        actual_end, expected_end = missing_resource_date_range["end"]
        ranges += ((actual_end, expected_end),)
    return ranges


def _generate_requests(
    resource_inst: API.Resource, 
    duration_str: str, 
    vendor: API.Vendor, 
    vendor_name: str,
    missing_resource: dict) -> list[API.Request]:
    requests = []
    endpoint = _locate_endpoint(vendor, missing_resource["columns"])
    date_ranges = _generate_date_ranges(missing_resource["date_range"])
    for endpoint_name, date_range in product(endpoint, date_ranges):
        start, end = date_range
        request = API.Request(
            vendor=vendor_name, 
            endpoint=endpoint_name, 
            params=API.RequestParams(
                start=start,
                end=end,
                resource=resource_inst.encode(), 
                resolution=duration_str
                )
            )
        requests.append(request)
    return requests


def plan(missing: dict, server_inst: Server) -> list[dict]:
    requests = []
    for vendor_name, missing_resources in missing.items():
        if len(missing_resources) > 0:
            vendor = server_inst.vendors[vendor_name]
            for resource_name, missing_resource in missing_resources.items():
                resource_inst, duration_str = _generate_resource(vendor, resource_name)
                requests.extend(
                    _generate_requests(
                        resource_inst, duration_str, vendor, vendor_name, missing_resource))                
    return requests
