"""Converts templates in the templates directory to a schedule of tasks"""

from datetime import timedelta

from hedgepy.common.bases import Template
from hedgepy.common.bases import API
from hedgepy.server.bases.Schedule import ScheduleItem, Schedule


def generate_schedule(requests: tuple[API.RequestParams], first_cycle: timedelta, last_cycle: timedelta) -> Schedule:
    min_interval = min(request.interval for request in requests)
    return Schedule(start=first_cycle, stop=last_cycle, interval=min_interval, items=requests)


def flatten(templates: dict[str, dict]) -> tuple[ScheduleItem]:
    schedule_items = ()
    for template in templates.values():
        template_common = template.pop("common", {})
        for request_in in template.get("templates", []):
            api_request_out = API.Request.from_template(template_common, request_in)
            schedule_items += (ScheduleItem.from_request(api_request_out),)
    return schedule_items


def parse(daemon_start: timedelta, daemon_stop: timedelta) -> Schedule:
    templates = Template.get_templates()
    flattened = flatten(templates)
    schedule = generate_schedule(flattened, daemon_start, daemon_stop)  
    return schedule


if __name__ == "__main__":
    parse(daemon_start=timedelta(hours=0), daemon_stop=timedelta(hours=18))
    