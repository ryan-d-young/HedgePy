import datetime
import time
import calendar


DFMT = "%Y-%m-%d"
TFMT = "%H:%M:%S.%f"
DTFMT = f"{DFMT}T{TFMT}"
DATE_RE = r"(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})"
TIME_RE = r"(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2}).(?P<microsecond>\d{6})"
DATETIME_RE = DATE_RE + r"T" + TIME_RE
DURATION_RE = r"P(?:(?P<days>\d+)D)?(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+(?:\.\d+)?)S)?)?"



def time_to_timedelta(time: str) -> datetime.timedelta:
    return datetime.timedelta(seconds=calendar.timegm(time.struct_time(time)))


def now() -> datetime.datetime:
    return datetime.datetime.now().strftime(DTFMT)
