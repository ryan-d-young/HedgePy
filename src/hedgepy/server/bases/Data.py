import re
import datetime
from typing import TypeVar, Any


DFMT = "%Y-%m-%d"
TFMT = "%H:%M:%S.%f"
DTFMT = f"{DFMT}T{TFMT}"
DURFMT = f"P{DTFMT}"

TEXT_RE = r"(?P<str>.*)"
BOOL_RE = r"(?P<bool>true|false)"
NULL_RE = r"(?P<none>NULL)"
INT_RE = r"(?P<sign>\-)?(?P<int>[0-9]*)"
FLOAT_RE = r"(?P<sign>\-)?(?P<integer>[0-9]*)(?P<dec>\.)(?P<fraction>[0-9]*)?"
DATE_RE = r"(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})"
TIME_RE = r"(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2}).(?P<microsecond>\d{6})"
DATETIME_RE = DATE_RE + r"T" + TIME_RE
DURATION_RE = r"P(?:(?P<days>\d+)D)?(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+(?:\.\d+)?)S)?)?"

DB_TYPE = ["text",  "bool", "null", "int", "float", "date", "time", "timestamp", "interval"]
PY_TYPE = [str, bool, None, int, float, datetime.date, datetime.time, datetime.datetime, datetime.timedelta]
RE = [TEXT_RE, BOOL_RE, NULL_RE, INT_RE, FLOAT_RE, DATE_RE, TIME_RE, DATETIME_RE, DURATION_RE]

DB_TO_PY = dict(zip(DB_TYPE, PY_TYPE))
PY_TO_DB = dict(zip(PY_TYPE, DB_TYPE))
RE_TO_PY = dict(zip(RE, PY_TYPE))

PyType = TypeVar("PyType")
PyValue = TypeVar("PyValue", bound=PY_TYPE)
DBType = TypeVar("DBType")
DBValue = TypeVar("DBValue", bound=str)


def resolve_re(value: str) -> tuple[re.Match, PyType]:
    for re_type, py_type in reversed(RE_TO_PY.items()):
        pattern = re.compile(re_type)
        if re_match := pattern.match(value):
            if re_match.group() == value:
                return re_match, py_type


def resolve_py_type(py_type: PyType) -> DBType:
    return PY_TO_DB[py_type]


def resolve_db_type(db_type: DBType) -> PyType:
    return DB_TO_PY[db_type]


def _cast_re_text(re_match: re.Match) -> str:
    return re_match.group(0)


def _cast_re_bool(re_match: re.Match) -> bool:
    return bool(re_match.group(0))


def _cast_re_null(re_match: re.Match) -> None:
    return


def _cast_re_int(re_match: re.Match) -> int:
    sign, value = re_match.groups()
    return -int(value) if sign == "-" else int(value)


def _cast_re_float(re_match: re.Match) -> float:
    sign, integer, _, fraction = re_match.groups()
    value = float(integer) if not fraction else float(f"{integer}.{fraction}")
    return -value if sign == "-" else value


def _cast_re_date(re_match: re.Match) -> datetime.date:
    return datetime.date(*(map(int, re_match.groups())))


def _cast_re_time(re_match: re.Match) -> datetime.time:
    return datetime.time(*(map(int, re_match.groups())))


def _cast_re_datetime(re_match: re.Match) -> datetime.datetime:
    return datetime.datetime(*(map(int, re_match.groups())))


def _cast_re_duration(re_match: re.Match) -> datetime.timedelta:
    days, hours, minutes, seconds = map(lambda x: int(x) if x else 0, re_match.groups())
    return datetime.timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)


def cast_re(re_match: re.Match, py_type: PyType) -> PyType:
    match py_type.__name__:
        case "str":
            return _cast_re_text(re_match)
        case "bool":
            return _cast_re_bool(re_match)
        case "None":
            return _cast_re_null(re_match)
        case "int":
            return _cast_re_int(re_match)
        case "float":
            return _cast_re_float(re_match)
        case "date" | "datetime.date":
            return _cast_re_date(re_match)
        case "time" | "datetime.time":
            return _cast_re_time(re_match)
        case "datetime" | "datetime.datetime":
            return _cast_re_datetime(re_match)
        case "timedelta" | "datetime.timedelta":
            return _cast_re_duration(re_match)
        case _:
            raise ValueError(f"Unsupported type: {py_type}")


def cast(value: DBValue) -> PyType:
    re_match, py_type = resolve_re(value)
    return cast_re(re_match, py_type)


class Data(tuple):
    def __new__(
        cls,
        data: tuple[tuple[Any]],
        cols: tuple[tuple[str, DBType]] | tuple[tuple[str, PyType]] = None,
        idx: str | tuple[Any] | None = None
        ) -> "Data":
        return super(Data, cls).__new__(cls, data)
       
    @staticmethod
    def _validate_data(data: tuple[tuple[Any]]) -> int:
        n_records, len_first_row = len(data), len(data[0])
       
        assert all(map(
            lambda row: len(row) == len_first_row,
            data
            )),\
        "Not all rows have equivalent length"
       
        return n_records, len_first_row

    @staticmethod
    def _validate_cols(
        cols: tuple[tuple[Any, DBType]] | tuple[tuple[Any, PyType]],
        len_first_row: int
        ) -> tuple[tuple[Any], tuple[DBType] | tuple[PyType]]:
        names, types = tuple(zip(*cols))
        assert all(map(
            lambda typ: (
                (isinstance(typ, str) and typ in DB_TYPE)
                or
                (isinstance(typ, type) and typ in PY_TYPE)
                ),
                types
            )),\
        "Columns failed type validation"

        assert len(cols) == len_first_row, "Number of columns does not match length of rows"

        return names, types

    @staticmethod
    def _validate_idx(idx: Any | tuple[Any], col_names, col_types) -> int:
        if not isinstance(idx, tuple):
            if col_names:
                assert idx in col_names, "Index label not in columns"
            return 1
        elif col_names:
            assert all(map(lambda lbl: lbl in col_names, idx)), "At least one index label not in columns"
        return len(idx)

    def __init__(self, data, cols = None, idx = None):
        self._cols = cols
        self._idx = idx

        n_records, n_cols = self._validate_data(data)
        col_names, col_types = self._validate_cols(cols, n_cols) if cols else None, None
        n_idx = self._validate_idx(idx, col_names, col_types) if idx else None

        self._col_names: tuple[Any] = col_names
        self._col_types: tuple[DBType] | tuple[PyType] = col_types
        self._n_records: int = n_records
        self._n_cols: int = n_cols
        self._n_idx: int = n_idx
        