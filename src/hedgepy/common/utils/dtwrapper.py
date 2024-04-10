import re
import math
import datetime
import time
import calendar


DFMT = "%Y-%m-%d"
TFMT = "%H:%M:%S"
DTFMT = f"{DFMT}T{TFMT}"
DATE_RE = r"(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})"
TIME_RE = r"(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2}).(?P<microsecond>\d{6})"
DATETIME_RE = DATE_RE + r"T" + TIME_RE
DURATION_RE = r"""
    P
    (?:(?P<years>\d+)Y)?
    (?:(?P<months>\d+)M)?
    (?:(?P<weeks>\d+)W)?
    (?:(?P<days>\d+)D)?
    (?:T
        (?:(?P<hours>\d+)H)?
        (?:(?P<minutes>\d+)M)?
        (?:(?P<seconds>\d+(?:\.\d+)?)S)?
    )?
    """


def d_or_dt_to_dt(d_or_dt: datetime.date | datetime.datetime) -> datetime.datetime:
    if isinstance(d_or_dt, datetime.date):
        return datetime.datetime.combine(d_or_dt, datetime.time(0, 0, 0, 0))
    else: 
        return d_or_dt


def time_to_td(t: str) -> datetime.timedelta:
    return datetime.timedelta(seconds=calendar.timegm(time.struct_time(t)))


def _str_to_td(match: re.Match):
    args = ("year", "month", "week", "day", "hour", "minute", "second")
    args_dict = dict(zip(args, tuple(map(lambda s: int(match.group(f"{s}s") or 0) + 1, args))))
    weeks = args_dict.pop("week") or 0
    args_dict["day"] += 7 * weeks
    return datetime.datetime(**args_dict) - datetime.datetime(1, 1, 8, 1, 1, 1)


def str_to_td(s: str) -> datetime.timedelta:
    pattern = re.compile(DURATION_RE, re.VERBOSE)
    match = pattern.match(s)
    return _str_to_td(match)
    
    
def str_to_dt(s: str | None, fmt: str = DTFMT) -> datetime.datetime:
    return datetime.datetime.strptime(s, fmt) if s else None


def dt_to_str(dt: datetime.datetime | None, fmt: str = DTFMT) -> str:
    return dt.strftime(fmt) if dt else None


def format(start: str | None, end: str | None) -> tuple[datetime.datetime, datetime.datetime]:
    return str_to_dt(start), str_to_dt(end)
    