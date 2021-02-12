import datetime
import time
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Type
from typing import Union

# A value corresponding to a constraint is one of:
#     None
#         This means you have no index for that constraint. SQLite will have to
#         iterate over every row for it.
#     integer
#         This is the argument number for the constraintargs being passed
#         into the Filter() function of your cursor.
#     (integer, boolean)
#         By default SQLite will check what you return. If you set the boolean
#         to False then SQLite won’t do that double checking.
Constraint = Union[None, int, Tuple[int, bool]]

# A row of data
Row = Dict[str, Any]

# An index is a tuple with a column index and an operator to filter it
Index = Tuple[int, int]


class DBAPIType:
    pass


class STRING(DBAPIType):
    pass


class BINARY(DBAPIType):
    pass


class NUMBER(DBAPIType):
    pass


class DATETIME(DBAPIType):
    pass


class ROWID(DBAPIType):
    pass


# Cursor description
Description = Optional[
    List[
        Tuple[
            str,
            Type[DBAPIType],
            Optional[str],
            Optional[str],
            Optional[str],
            Optional[str],
            Optional[bool],
        ]
    ]
]


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
