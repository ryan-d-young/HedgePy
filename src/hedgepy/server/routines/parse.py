"""Converts templates in the templates directory to a schedule of tasks"""

from datetime import timedelta

from hedgepy.common import API, template
from hedgepy.server.bases.Data import cast
from hedgepy.server.bases.Agent import ScheduleItem, Schedule


Template = dict[str, dict]


def generate_schedule(requests: tuple[API.Request], first_cycle: timedelta, last_cycle: timedelta) -> Schedule:
    min_interval = min(request.interval for request in requests)
    return Schedule(start=first_cycle, stop=last_cycle, interval=min_interval, items=requests)


def flatten(templates: dict[str, Template]) -> tuple[ScheduleItem]:
    schedule_items = ()
    for template in templates.values():
        template_common = template.pop("common", {})
        for request_in in template.get("templates", []):
            request_out = template_common.copy()
            request_out.update(**request_in)
            api_request_out = API.Request(**request_out)
            interval = request_out.get("resolution", None)
            schedule_items += (ScheduleItem(api_request_out, interval),)
    return schedule_items


def parse(daemon_start: timedelta, daemon_stop: timedelta) -> Schedule:
    templates = template.get_templates()
    flattened = flatten(templates)
    schedule = generate_schedule(flattened, daemon_start, daemon_stop)  
    return schedule


if __name__ == "__main__":
    parse(daemon_start=timedelta(hours=0), daemon_stop=timedelta(hours=18))
    