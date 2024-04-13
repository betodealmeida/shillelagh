"""DB API 2.0 types for Shillelagh."""

import datetime
import inspect
import time
from typing import Any

from shillelagh.fields import Field


class DBAPIType:  # pylint: disable=too-few-public-methods
    """
    Constructor for the required DB API 2.0 types.
    """

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


def Date(  # pylint: disable=invalid-name
    year: int,
    month: int,
    day: int,
) -> datetime.date:
    """Constructs an object holding a date value."""
    return datetime.date(year, month, day)


def Time(  # pylint: disable=invalid-name
    hour: int,
    minute: int,
    second: int,
) -> datetime.time:
    """Constructs an object holding a time value."""
    return datetime.time(hour, minute, second, tzinfo=datetime.timezone.utc)


def Timestamp(  # pylint: disable=invalid-name, too-many-arguments
    year: int,
    month: int,
    day: int,
    hour: int,
    minute: int,
    second: int,
) -> datetime.datetime:
    """Constructs an object holding a timestamp value."""
    return datetime.datetime(
        year,
        month,
        day,
        hour,
        minute,
        second,
        tzinfo=datetime.timezone.utc,
    )


def DateFromTicks(ticks: int) -> datetime.date:  # pylint: disable=invalid-name
    """
    Constructs an object holding a date value from the given ticks value.

    Ticks should be in number of seconds since the epoch.
    """
    return Date(*time.gmtime(ticks)[:3])


def TimeFromTicks(ticks: int) -> datetime.time:  # pylint: disable=invalid-name
    """
    Constructs an object holding a time value from the given ticks value.

    Ticks should be in number of seconds since the epoch.
    """
    return Time(*time.gmtime(ticks)[3:6])


def TimestampFromTicks(ticks: int) -> datetime.datetime:  # pylint: disable=invalid-name
    """
    Constructs an object holding a timestamp value from the given ticks value.

    Ticks should be in number of seconds since the epoch.
    """
    return Timestamp(*time.gmtime(ticks)[:6])


def Binary(string: str) -> bytes:  # pylint: disable=invalid-name
    """constructs an object capable of holding a binary (long) string value."""
    return string.encode()
