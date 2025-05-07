"""
An adapter for data generated on-the-fly.
"""

import itertools
import string
import urllib.parse
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from shillelagh.adapters.base import Adapter
from shillelagh.fields import Boolean, DateTime, Field, Float, Integer, Order, String
from shillelagh.filters import Filter
from shillelagh.typing import RequestedOrder, Row


def int_to_base26(n: int) -> str:
    """
    Convert 0→a, 1→b, …, 25→z, 26→aa, etc.
    """
    s = ""
    n += 1
    while n > 0:
        n, rem = divmod(n - 1, 26)
        s = string.ascii_lowercase[rem] + s
    return s


def make_seq(delta: Any) -> Iterator:
    """
    Generate a sequence of values starting from 0, incrementing by delta.
    """
    return (i * delta for i in itertools.count())  # pragma: no cover


DEFAULT_NUMBER_OF_ROWS = 10
TIME_RESOLUTIONS = {
    "microsecond": timedelta(microseconds=1),
    "millisecond": timedelta(milliseconds=1),
    "second": timedelta(seconds=1),
    "minute": timedelta(minutes=1),
    "hour": timedelta(hours=1),
    "day": timedelta(days=1),
    "week": timedelta(weeks=1),
    "month": timedelta(days=30),
    "quarter": timedelta(days=91),
    "year": timedelta(days=365),
    "decade": timedelta(days=3650),
    "century": timedelta(days=36500),
    "millennium": timedelta(days=365000),
}
TYPES = {  # pragma: no cover
    "int": itertools.count(),
    "float": make_seq(0.1),
    "str": (int_to_base26(i) for i in itertools.count()),
    "bool": (itertools.cycle([True, False])),
}
for type_, timedelta_ in TIME_RESOLUTIONS.items():
    TYPES[type_] = make_seq(timedelta_)


class VirtualMemory(Adapter):
    """
    An adapter for data generated on-the-fly:

        🍀> SELECT * FROM "virtual://?cols=id:int,t:day&rows=10";

    """

    safe = True

    supports_limit = False
    supports_offset = False
    supports_requested_columns = False

    @staticmethod
    def supports(uri: str, fast: bool = True, **kwargs: Any) -> Optional[bool]:
        parsed = urllib.parse.urlparse(uri)
        return parsed.scheme == "virtual"

    @staticmethod
    def parse_uri(uri: str) -> tuple[dict[str, str], str, int]:
        parsed = urllib.parse.urlparse(uri)
        query_string = urllib.parse.parse_qs(parsed.query)

        cols = dict(
            col.split(":", 1)
            for col in (
                query_string["cols"][0].split(",") if "cols" in query_string else []
            )
        )
        cols = {k: v for k, v in cols.items() if v in TYPES}
        time_cols = {k: v for k, v in cols.items() if v in TIME_RESOLUTIONS}

        # default start to today, truncated to resolution
        if "start" in query_string:
            start = datetime.fromisoformat(query_string["start"][0])
        else:
            start = datetime.now(tz=timezone.utc)
            truncated = start.timestamp()
            for v in time_cols.values():
                truncated -= truncated % TIME_RESOLUTIONS[v].total_seconds()
            start = datetime.fromtimestamp(truncated, tz=timezone.utc)

        # if end is set, try to compute the number of rows
        if "end" in query_string and time_cols:
            end = datetime.fromisoformat(query_string["end"][0])
            resolution = max(TIME_RESOLUTIONS[v] for v in time_cols.values())
            rows = (end - start) // resolution

        # otherwise use the passed number of rows or default value
        else:
            rows = (
                int(query_string["rows"][0])
                if "rows" in query_string
                else DEFAULT_NUMBER_OF_ROWS
            )

        return cols, start.isoformat(), rows

    def __init__(
        self,
        cols: dict[str, str],
        start: str,
        rows: int,
    ):
        super().__init__()

        self.cols = cols
        self.start = datetime.fromisoformat(start)
        self.rows = rows

        self._set_columns()

    def _set_columns(self) -> None:
        type_map: dict[str, Field] = {
            "int": Integer(filters=None, order=Order.ASCENDING, exact=False),
            "float": Float(filters=None, order=Order.ASCENDING, exact=False),
            "str": String(filters=None, order=Order.ASCENDING, exact=False),
            "bool": Boolean(filters=None, order=Order.ASCENDING, exact=False),
        }
        self.columns = {
            k: type_map.get(
                v,
                DateTime(filters=None, order=Order.ASCENDING, exact=False),
            )
            for k, v in self.cols.items()
        }

    def get_columns(self) -> dict[str, Field]:
        return self.columns

    def get_data(
        self,
        bounds: dict[str, Filter],
        order: list[tuple[str, RequestedOrder]],
        **kwargs: Any,
    ) -> Iterator[Row]:
        for row_id in range(self.rows):
            # pylint: disable=stop-iteration-return
            values = {v: next(TYPES[v]) for v in set(self.cols.values())}
            row = {"rowid": row_id}
            for k, v in self.cols.items():
                row[k] = self.start + values[v] if v in TIME_RESOLUTIONS else values[v]
            yield row
