import datetime
import time
import calendar


def time_to_timedelta(time: str) -> datetime.timedelta:
    return datetime.timedelta(seconds=calendar.timegm(time.struct_time(time)))

