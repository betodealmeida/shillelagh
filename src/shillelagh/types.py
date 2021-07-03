import datetime
import inspect
import time
from typing import Any

from shillelagh.fields import Field
from typing_extensions import Literal


class DBAPIType:
    def __init__(self, name: str):
        self.name = name

    def __eq__(self, other: Any) -> bool:
        if inspect.isclass(other) and issubclass(other, Field):
            return bool(self.name == other.db_api_type)

        return NotImplemented


STRING = DBAPIType("STRING")
BINARY = DBAPIType("BINARY")
NUMBER = DBAPIType("NUMBER")
DATETIME = DBAPIType("DATETIME")
ROWID = DBAPIType("ROWID")


def Date(year: int, month: int, day: int) -> str:
    return datetime.date(year, month, day).isoformat()


def Time(hour: int, minute: int, second: int) -> str:
    return datetime.time(hour, minute, second, tzinfo=datetime.timezone.utc).isoformat()


def Timestamp(
    year: int,
    month: int,
    day: int,
    hour: int,
    minute: int,
    second: int,
) -> str:
    return datetime.datetime(
        year,
        month,
        day,
        hour,
        minute,
        second,
        tzinfo=datetime.timezone.utc,
    ).isoformat()


def DateFromTicks(ticks: int) -> str:
    return Date(*time.gmtime(ticks)[:3])


def TimeFromTicks(ticks: int) -> str:
    return Time(*time.gmtime(ticks)[3:6])


def TimestampFromTicks(ticks: int) -> str:
    return Timestamp(*time.gmtime(ticks)[:6])


def Binary(string: str) -> bytes:
    return string.encode()
