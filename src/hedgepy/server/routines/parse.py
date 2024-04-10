"""Converts templates in the templates directory to a schedule of tasks"""

from datetime import timedelta

from hedgepy.common.bases import Template
from hedgepy.common.bases import API
from hedgepy.common.bases.Data import cast
from hedgepy.server.bases.Schedule import ScheduleItem, Schedule


def generate_schedule(requests: tuple[API.RequestParams], first_cycle: timedelta, last_cycle: timedelta) -> Schedule:
    min_interval = min(request.interval for request in requests)
    return Schedule(start=first_cycle, stop=last_cycle, interval=min_interval, items=requests)


def make_request(**kwargs) -> API.Request:
    vendor = kwargs.pop("vendor")
    endpoint = kwargs.pop("endpoint")
    return API.Request(vendor=vendor, endpoint=endpoint, params=API.RequestParams(**kwargs))

def flatten(templates: dict[str, dict]) -> tuple[ScheduleItem]:
    schedule_items = ()
    for template in templates.values():
        template_common = template.pop("common", {})
        for request_in in template.get("templates", []):
            request_out = template_common.copy()
            request_out.update(**request_in)
            api_request_out = make_request(**request_out)
            interval = request_out.get("resolution", None)
            schedule_items += (ScheduleItem(api_request_out, interval),)
    return schedule_items


def parse(daemon_start: timedelta, daemon_stop: timedelta) -> Schedule:
    templates = Template.get_templates()
    flattened = flatten(templates)
    schedule = generate_schedule(flattened, daemon_start, daemon_stop)  
    return schedule


if __name__ == "__main__":
    parse(daemon_start=timedelta(hours=0), daemon_stop=timedelta(hours=18))
    