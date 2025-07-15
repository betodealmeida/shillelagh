# pylint: disable=invalid-name, c-extension-no-member, unused-import
"""
A base DB API 2.0 implementation.
"""

import itertools
import re
from collections.abc import Iterator
from functools import wraps
from typing import Any, Callable, Generic, Optional, TypeVar, cast

from shillelagh.adapters.base import Adapter
from shillelagh.exceptions import (  # nopycln: import; pylint: disable=redefined-builtin
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
)
from shillelagh.types import (
    BINARY,
    DATETIME,
    NUMBER,
    ROWID,
    STRING,
    Binary,
    Date,
    DateFromTicks,
    Time,
    TimeFromTicks,
    Timestamp,
    TimestampFromTicks,
)
from shillelagh.typing import Description

__all__ = [
    "DatabaseError",
    "DataError",
    "Error",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "OperationalError",
    "BINARY",
    "DATETIME",
    "NUMBER",
    "ROWID",
    "STRING",
    "Binary",
    "Date",
    "DateFromTicks",
    "Time",
    "TimeFromTicks",
    "Timestamp",
    "TimestampFromTicks",
    "Warning",
    "apilevel",
    "threadsafety",
    "paramstyle",
]

apilevel = "2.0"
threadsafety = 2
paramstyle = "qmark"

NO_SUCH_TABLE = re.compile("no such table: (?P<uri>.*)")
DEFAULT_SCHEMA = "main"

CURSOR_METHOD = TypeVar("CURSOR_METHOD", bound=Callable[..., Any])


def check_closed(method: CURSOR_METHOD) -> CURSOR_METHOD:
    """Decorator that checks if a connection or cursor is closed."""

    @wraps(method)
    def wrapper(self: "Cursor", *args: Any, **kwargs: Any) -> Any:
        if self.closed:
            raise ProgrammingError(f"{self.__class__.__name__} already closed")
        return method(self, *args, **kwargs)

    return cast(CURSOR_METHOD, wrapper)


def check_result(method: CURSOR_METHOD) -> CURSOR_METHOD:
    """Decorator that checks if the cursor has results from ``execute``."""

    @wraps(method)
    def wrapper(self: "Cursor", *args: Any, **kwargs: Any) -> Any:
        if self._results is None:  # pylint: disable=protected-access
            raise ProgrammingError("Called before ``execute``")
        return method(self, *args, **kwargs)

    return cast(CURSOR_METHOD, wrapper)


class Cursor:  # pylint: disable=too-many-instance-attributes
    """
    Connection cursor.
    """

    def __init__(  # pylint: disable=too-many-arguments, too-many-positional-arguments
        self,
        adapters: list[type[Adapter]],
        adapter_kwargs: dict[str, dict[str, Any]],
        schema: str = DEFAULT_SCHEMA,
    ):
        self._adapters = adapters
        self._adapter_kwargs = adapter_kwargs
        self.schema = schema

        # This read/write attribute specifies the number of rows to fetch at a
        # time with .fetchmany(). It defaults to 1 meaning to fetch a single
        # row at a time.
        self.arraysize = 1

        self.closed = False

        # this is updated only after a query
        self.description: Description = None

        # this is set to an iterator of rows after a successful query
        self._results: Optional[Iterator[tuple[Any, ...]]] = None
        self._rowcount = -1

        self.operation: Optional[str] = None

    @property  # type: ignore
    @check_closed
    def rowcount(self) -> int:
        """
        Return the number of rows after a query.
        """
        try:
            results = list(self._results)  # type: ignore
        except TypeError:
            return -1

        n = len(results)
        self._results = iter(results)
        return max(0, self._rowcount) + n

    @check_closed
    def close(self) -> None:
        """
        Close the cursor.
        """
        self.closed = True

    @check_closed
    def execute(
        self,
        operation: str,
        parameters: Optional[tuple[Any, ...]] = None,
    ) -> "Cursor":
        """
        Execute a query using the cursor.
        """
        raise NotImplementedError()

    @check_closed
    def executemany(
        self,
        operation: str,
        seq_of_parameters: Optional[list[tuple[Any, ...]]] = None,
    ) -> "Cursor":
        """
        Execute multiple statements.

        Currently not supported.
        """
        raise NotSupportedError(
            "``executemany`` is not supported, use ``execute`` instead",
        )

    @check_result
    @check_closed
    def fetchone(self) -> Optional[tuple[Any, ...]]:
        """
        Fetch the next row of a query result set, returning a single sequence,
        or ``None`` when no more data is available.
        """
        try:
            row = self.next()
        except StopIteration:
            return None

        self._rowcount = max(0, self._rowcount) + 1

        return row

    @check_result
    @check_closed
    def fetchmany(self, size=None) -> list[tuple[Any, ...]]:
        """
        Fetch the next set of rows of a query result, returning a sequence of
        sequences (e.g. a list of tuples). An empty sequence is returned when
        no more rows are available.
        """
        size = size or self.arraysize
        results = list(itertools.islice(self, size))

        return results

    @check_result
    @check_closed
    def fetchall(self) -> list[tuple[Any, ...]]:
        """
        Fetch all (remaining) rows of a query result, returning them as a
        sequence of sequences (e.g. a list of tuples). Note that the cursor's
        arraysize attribute can affect the performance of this operation.
        """
        results = list(self)

        return results

    @check_closed
    def setinputsizes(self, sizes: int) -> None:
        """
        Used before ``execute`` to predefine memory areas for parameters.

        Currently not supported.
        """

    @check_closed
    def setoutputsizes(self, sizes: int) -> None:
        """
        Set a column buffer size for fetches of large columns.

        Currently not supported.
        """

    @check_result
    @check_closed
    def __iter__(self) -> Iterator[tuple[Any, ...]]:
        for row in self._results:  # type: ignore
            self._rowcount = max(0, self._rowcount) + 1
            yield row

    @check_result
    @check_closed
    def __next__(self) -> tuple[Any, ...]:
        return next(self._results)  # type: ignore

    next = __next__


ConnectionCursor = TypeVar("ConnectionCursor", bound=Cursor)


class Connection(
    Generic[ConnectionCursor],
):  # pylint: disable=too-many-instance-attributes
    """Connection."""

    def __init__(  # pylint: disable=too-many-arguments, too-many-positional-arguments
        self,
        adapters: list[type[Adapter]],
        adapter_kwargs: dict[str, dict[str, Any]],
        schema: str = DEFAULT_SCHEMA,
        safe: bool = False,
    ):
        self._adapters = adapters
        self._adapter_kwargs = adapter_kwargs
        self.schema = schema
        self.safe = safe

        self.closed = False
        self.cursors: list[ConnectionCursor] = []

    @check_closed
    def close(self) -> None:
        """Close the connection now."""
        self.closed = True
        for cursor in self.cursors:
            if not cursor.closed:
                cursor.close()

    @check_closed
    def commit(self) -> None:
        """Commit any pending transaction to the database."""

    @check_closed
    def rollback(self) -> None:
        """Rollback any transactions."""

    @check_closed
    def cursor(self) -> ConnectionCursor:
        """Return a new Cursor Object using the connection."""
        raise NotImplementedError(
            "Subclasses must implement the `cursor` method",
        )

    @check_closed
    def execute(
        self,
        operation: str,
        parameters: Optional[tuple[Any, ...]] = None,
    ) -> "Cursor":
        """
        Execute a query on a cursor.
        """
        cursor = self.cursor()
        return cursor.execute(operation, parameters)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.commit()
        self.close()
