import re
import calendar
import holidays
import time as timelib
from datetime import date, datetime, timedelta, time
from dateutil.relativedelta import relativedelta, MO, TU, WE, TH, FR, SA, SU
from dateutil.rrule import rrule, SECONDLY, MINUTELY, HOURLY, DAILY, WEEKLY, MONTHLY, YEARLY
from dateutil.parser import parse as parse_date


DAYS_IN_YEAR = 365


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


def d_or_dt_to_dt(d_or_dt: date | datetime) -> datetime:
    if isinstance(d_or_dt, date):
        return datetime.combine(d_or_dt, time(0, 0, 0, 0))
    else: 
        return d_or_dt

def dt_to_rd(dt: datetime, anchor_dt: datetime | None = None):
    anchor_dt = anchor_dt if anchor_dt else now()
    return relativedelta(anchor_dt, dt)


def rd_to_str(rd: relativedelta) -> str:
    res = "P"
    if rd.years > 0:
        res += f"{rd.years}Y"
    if rd.months > 0:
        res += f"{rd.months}M"
    if rd.days > 0:
        res += f"{rd.days}D"
    if rd.hours > 0 or rd.minutes > 0 or rd.seconds > 0:
        res += "T"
        if rd.hours > 0:
            res += f"{rd.hours}H"
        if rd.minutes > 0:
            res += f"{rd.minutes}M"
        if rd.seconds > 0:
            res += f"{rd.seconds}S"
    return res


def str_to_rd(s: str) -> relativedelta:
    pattern = re.compile(DURATION_RE, re.VERBOSE)
    match = pattern.match(s)
    args = ("years", "months", "weeks", "days", "hours", "minutes", "seconds")
    args_dict = dict(zip(args, tuple(map(lambda s: int(match.group(s) or 0), args))))
    args_dict["days"] += 7 * args_dict.pop("weeks")
    return relativedelta(**args_dict)


def _str_to_td(match: re.Match) -> timedelta:
    args = ("year", "month", "week", "day", "hour", "minute", "second")
    args_dict = dict(zip(args, tuple(map(lambda s: int(match.group(f"{s}s") or 0) + 1, args))))
    args_dict["day"] += 7 * args_dict.pop("week")
    return datetime(**args_dict) - datetime(1, 1, 8, 1, 1, 1)


def str_to_td(s: str) -> timedelta:
    pattern = re.compile(DURATION_RE, re.VERBOSE)
    match = pattern.match(s)
    return _str_to_td(match)
    
    
def _td_to_str(days: float) -> str:
    years, days = divmod(days, DAYS_IN_YEAR)
    months, days = divmod(days, (DAYS_IN_YEAR / 12))
    weeks, days = divmod(days, (DAYS_IN_YEAR / 52))
    days, hours = divmod(days, 1)
    hours, minutes = divmod(hours * 24, 1)
    minutes, seconds = divmod(minutes * 60, 1)
    seconds *= 60
    years, months, weeks, days, hours, minutes, seconds = map(
        int, (years, months, weeks, days, hours, minutes, seconds))
    
    res = "P"
    if years > 0:
        res += f"{years}Y"
    if months > 0:
        res += f"{months}M"
    if weeks > 0:
        res += f"{weeks}W"
    if days > 0:
        res += f"{days}D"
    if hours > 0 or minutes > 0 or seconds > 0:
        res += "T"
        if hours > 0:
            res += f"{hours}H"
        if minutes > 0:
            res += f"{minutes}M"
        if seconds > 0:
            res += f"{seconds}S"
    return res


def td_to_str(td: timedelta) -> str:
    days = td.days + (td.seconds / (24*(60**2)))
    return _td_to_str(days)

    
def str_to_dt(s: str | None, fmt: str = DTFMT) -> datetime:
    return datetime.strptime(s, fmt) if s else None


def dt_to_str(dt: datetime | None, fmt: str = DTFMT) -> str:
    return dt.strftime(fmt) if dt else None


def format(start: str | None, end: str | None) -> tuple[datetime, datetime]:
    return str_to_dt(start), str_to_dt(end)

    
def now() -> time:
    return timelib.time()


def today() -> date:
    return date.today()


def timestamp() -> datetime:
    return datetime.now()


class UDt:
    FMT: dict[str, str] = {None: DTFMT}
    SEL: str | None = None
    
    @classmethod
    def register_fmt(cls, vendor_name: str, fmt: str) -> None:
        cls.FMT[vendor_name] = fmt
        
    @classmethod
    def select_fmt(cls, vendor_name: str | None = None) -> None:
        if vendor_name in cls.FMT:
            cls.SEL = vendor_name
        else:
            cls.SEL = None
        
    @classmethod
    def convert(cls, dt_or_str: str | datetime) -> datetime | str:
        if isinstance(dt_or_str, datetime):
            return dt_to_str(dt_or_str, cls.FMT[cls.SEL])
        elif isinstance(dt_or_str, str):
            return str_to_dt(dt_or_str, cls.FMT[cls.SEL])
        elif dt_or_str is None:
            return None
        else:
            raise ValueError("Invalid argument type")

    def __init__(self):
        raise NotImplementedError("UDt cannot be instantiated directly")
    
    
class UDur:
    @classmethod
    def convert(cls, td_or_rd_or_str: str | timedelta) -> timedelta | relativedelta | str:
        if isinstance(td_or_rd_or_str, timedelta):
            return td_to_str(td_or_rd_or_str)
        elif isinstance(td_or_rd_or_str, relativedelta):
            return rd_to_str(td_or_rd_or_str)
        elif "Y" in td_or_rd_or_str or "M" in td_or_rd_or_str or "W" in td_or_rd_or_str:
            return str_to_rd(td_or_rd_or_str)
        else:
            return str_to_td(td_or_rd_or_str)
        
    def __init__(self):
        raise NotImplementedError("UDur cannot be instantiated directly")
    