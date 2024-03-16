"""Converts templates in the templates directory to a schedule of tasks"""

from hedgepy.common import API, template
from hedgepy.server.bases.Data import cast
from hedgepy.server.bases.Agent import ScheduleItem, Schedule


Template = dict[str, dict]


def generate_schedule(requests: tuple[API.Request]) -> Schedule:
    schedule_items = ()
    for request in requests:
        start = request.start.time()
        stop = request.stop.time() if request.stop else None
        interval = request.resolution if request.resolution else None
        schedule_items += (ScheduleItem(request, start, stop, interval),)
    return Schedule(items=schedule_items)
    

def _cast_dtypes(request: dict) -> dict:
    to_cast = {'start', 'stop', 'resolution'}
    return {k: cast(v) if k in to_cast else v for k, v in request.items()}


def flatten(templates: dict[str, Template]) -> tuple[API.Request]:
    requests = ()
    for template in templates.values():
        template_common = template.get("common", {})
        for request_in in template.values():
            request_out = template_common.copy()
            request_out.update(request_in)
            request_out = _cast_dtypes(request_out)
            requests += (API.Request(**request_out),)
    return requests


def main():
    templates = template.get_templates()
    flattened = flatten(templates)
    schedule = generate_schedule(flattened)
    return schedule


if __name__ == "__main__":
    main()
    