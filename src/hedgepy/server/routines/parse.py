"""Converts templates in the templates directory to a schedule of tasks"""

from datetime import timedelta

from hedgepy.common.bases import Template
from hedgepy.common.bases import API
from hedgepy.server.bases import Server
from hedgepy.server.bases.Schedule import ScheduleItem, Schedule


def generate_schedule(requests: tuple[API.RequestParams], first_cycle: timedelta, last_cycle: timedelta) -> Schedule:
    min_interval = min(request.interval for request in requests)
    return Schedule(start=first_cycle, stop=last_cycle, interval=min_interval, items=requests)


def _resolve_request(template_common: dict, request_in: dict) -> dict:
    request_out = template_common.copy()
    request_out.update(request_in)
    return request_out


def _decode_request(encoded_request: dict, server_inst: Server) -> dict:
    if (resource_di := encoded_request.pop("resource", None)) is not None:
        vendor_inst = server_inst.vendors[encoded_request["vendor"]]
        resource_cls_name = resource_di["class"]
        resource_cls = vendor_inst.resources[resource_cls_name]
        resource_inst = resource_cls.decode(resource_di["handle"])
        encoded_request["resource"] = resource_inst
    return encoded_request


def flatten(templates: dict[str, dict], server_inst: Server) -> tuple[ScheduleItem]:
    schedule_items = ()
    for template in templates.values():
        template_common = template.pop("common", {})
        for request_in in template.get("templates", []):
            encoded_request = _resolve_request(template_common, request_in)
            decoded_request = _decode_request(encoded_request, server_inst)
            request = API.Request.from_flat_dict(decoded_request)
            schedule_items += (ScheduleItem.from_request(request),)
    return schedule_items


def parse(daemon_start: timedelta, daemon_stop: timedelta, server_inst: Server) -> Schedule:
    templates = Template.get_templates()
    flattened = flatten(templates, server_inst)
    schedule = generate_schedule(flattened, daemon_start, daemon_stop)  
    return schedule


if __name__ == "__main__":
    parse(daemon_start=timedelta(hours=0), daemon_stop=timedelta(hours=18))
    