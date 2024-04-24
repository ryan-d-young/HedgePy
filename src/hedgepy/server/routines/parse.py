"""Converts templates in the templates directory to a schedule of tasks"""

from datetime import timedelta

from hedgepy.common.bases import Template
from hedgepy.common.bases import API
from hedgepy.server.bases import Server
from hedgepy.server.bases.Schedule import ScheduleItem, Schedule


def _resolve_request(template_common: dict, request_in: dict) -> dict:
    request_out = template_common.copy()
    request_out.update(request_in)
    return request_out


def _format_request(request: dict) -> dict:
    return {
        "vendor": request.get("vendor"),
        "endpoint": request.get("endpoint"),
        "params": {
            "start": request.get("start", None),
            "end": request.get("end", None),
            "resource": request.get("resource", None),
            "resolution": request.get("resolution", None)
        }
    }
    
    
def parse_resource(request: dict, server_inst: Server) -> API.Resource | None:
    if resource_handle := request["params"]["resource"]:
        vendor_inst = server_inst.vendors[request["vendor"]]
        resource_cls_name, resource_handle = resource_handle.split("$")
        resource_cls = vendor_inst.resources[resource_cls_name]
        resource_inst = resource_cls.decode(resource_handle)
        return resource_inst
    return None


def generate_json_requests(templates: dict[str, dict]) -> list[dict]:
    json_requests = []
    for template in templates.values():
        template_common = template.pop("common", {})
        for request_in in template.get("templates", []):
            request = _resolve_request(template_common, request_in)
            formatted_request = _format_request(request)
            json_requests.append(formatted_request)
    return json_requests
            
            
def generate_requests(json_requests: list[dict], server_inst: Server) -> list[API.Request]:
    requests = []
    for request in json_requests:
        resource_obj = parse_resource(request, server_inst)
        request_obj = API.Request.decode(request).bind_resource(resource_obj)
        requests.append(request_obj)
    return requests


def generate_schedule(requests: list[API.Request], first_cycle: timedelta, last_cycle: timedelta) -> Schedule:
    schedule_items = [ScheduleItem.from_request(request) for request in requests]
    return Schedule(start=first_cycle, stop=last_cycle, items=schedule_items)


def generate_expected_db_struct(json_requests: list[dict], server_inst: Server) -> dict:
    expected_db_struct = {}
    for request in json_requests:
        expected_schema_name = request["vendor"]        
        if expected_schema_name not in expected_db_struct:
            expected_db_struct[expected_schema_name] = {}
        
        expected_table_name = "$".join((request["params"]["resource"], request["params"]["resolution"]))
        if expected_table_name not in expected_db_struct[expected_schema_name]:
            expected_db_struct[expected_schema_name][expected_table_name] = {"columns": [], "date_range": (None, None)}
            
        expected_columns = server_inst.vendors[expected_schema_name].getters[request["endpoint"]].returns
        expected_columns_names = list(zip(*expected_columns))[0]
        expected_db_struct[expected_schema_name][expected_table_name]["columns"].extend(expected_columns_names)

        request_expected_start, request_expected_end \
            = request["params"]["start"], request["params"]["end"]
        current_expected_start, current_expected_end \
            = expected_db_struct[expected_schema_name][expected_table_name]["date_range"]
        if request_expected_start and not current_expected_start:
            current_expected_start = request_expected_start
        elif request_expected_start and current_expected_start:
            current_expected_start = min(request_expected_start, current_expected_start)
        if request_expected_end and not current_expected_end:
            current_expected_end = request_expected_end
        elif request_expected_end and current_expected_end:
            current_expected_end = max(request_expected_end, current_expected_end)
        expected_db_struct[expected_schema_name][expected_table_name]["date_range"] \
            = (current_expected_start, current_expected_end)
        
    return expected_db_struct


def parse_templates() -> list[dict]:
    templates = Template.get_templates()
    json_requests = generate_json_requests(templates)
    return json_requests
