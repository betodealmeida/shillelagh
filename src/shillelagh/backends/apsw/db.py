import itertools
import json
import urllib.parse
from functools import wraps
from typing import Any
from typing import Callable
from typing import cast
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple
from typing import Type
from typing import TypeVar

import apsw
from pkg_resources import iter_entry_points
from shillelagh.adapters.base import Adapter
from shillelagh.backends.apsw.vt import VTModule
from shillelagh.exceptions import Error
from shillelagh.exceptions import NotSupportedError
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Blob
from shillelagh.fields import Field
from shillelagh.fields import type_map
from shillelagh.lib import quote
from shillelagh.lib import serialize
from shillelagh.types import Description

apilevel = "2.0"
threadsafety = 2
paramstyle = "qmark"
sqlite_version_info = tuple(
    int(number) for number in apsw.sqlitelibversion().split(".")
)

NO_SUCH_TABLE = "SQLError: no such table: "

F = TypeVar("F", bound=Callable[..., Any])


def check_closed(method: F) -> F:
    """Decorator that checks if a connection or cursor is closed."""

    @wraps(method)
    def wrapper(self: "Cursor", *args: Any, **kwargs: Any) -> Any:
        if self.closed:
            raise ProgrammingError(f"{self.__class__.__name__} already closed")
        return method(self, *args, **kwargs)

    return cast(F, wrapper)


def check_result(method: F) -> F:
    """Decorator that checks if the cursor has results from `execute`."""

    @wraps(method)
    def wrapper(self: "Cursor", *args: Any, **kwargs: Any) -> Any:
        if self._results is None:
            raise ProgrammingError("Called before `execute`")
        return method(self, *args, **kwargs)

    return cast(F, wrapper)


def get_type_code(type_name: str) -> Type[Field]:
    return type_map.get(type_name, Blob)


class Cursor(object):

    """Connection cursor."""

    def __init__(
        self,
        cursor: "apsw.Cursor",
        adapters: List[Type[Adapter]],
        adapter_args: Dict[str, Any],
        isolation_level: Optional[str] = None,
    ):
        self._cursor = cursor
        self._adapters = adapters
        self._adapter_args = adapter_args

        self.in_transaction = False
        self.isolation_level = isolation_level

        # This read/write attribute specifies the number of rows to fetch at a
        # time with .fetchmany(). It defaults to 1 meaning to fetch a single
        # row at a time.
        self.arraysize = 1

        self.closed = False

        # this is updated only after a query
        self.description: Description = None

        # this is set to an iterator of rows after a successful query
        self._results: Optional[Iterator[Tuple[Any, ...]]] = None
        self._rowcount = -1

    @property  # type: ignore
    @check_closed
    def rowcount(self) -> int:
        try:
            results = list(self._results)  # type: ignore
        except TypeError:
            return -1

        n = len(results)
        self._results = iter(results)
        return self._rowcount + n

    @check_closed
    def close(self) -> None:
        """Close the cursor."""
        self._cursor.close()
        self.closed = True

    @check_closed
    def execute(
        self,
        operation: str,
        parameters: Optional[Tuple[Any, ...]] = None,
    ) -> "Cursor":
        if not self.in_transaction and self.isolation_level:
            self._cursor.execute(f"BEGIN {self.isolation_level}")
            self.in_transaction = True

        self.description = None
        self._rowcount = -1

        # this is where the magic happens: instead of forcing users to register
        # their virtual tables explicitly, we do it for them when they first try
        # to access them and it fails because the table doesn't exist yet
        while True:
            try:
                self._cursor.execute(operation, parameters)
                self.description = self._get_description()
                self._results = self._convert(self._cursor)
                break
            except apsw.SQLError as exc:
                message = exc.args[0]
                if not message.startswith(NO_SUCH_TABLE):
                    raise exc

                # create the virtual table
                uri = message[len(NO_SUCH_TABLE) :]
                self._create_table(uri)

        return self

    def _convert(self, cursor: "apsw.Cursor") -> Iterator[Tuple[Any, ...]]:
        if not self.description:
            return

        for row in cursor:
            yield tuple(desc[1].parse(col) for col, desc in zip(row, self.description))

    def _create_table(self, uri: str) -> None:
        for adapter in self._adapters:
            if adapter.supports(uri):
                break
        else:
            raise ProgrammingError(f"Unsupported table: {uri}")

        # collect arguments from URI and connection and serialize them
        args = [
            serialize(arg)
            for arg in adapter.parse_uri(uri)
            + self._adapter_args.get(adapter.__name__.lower(), ())
        ]
        formatted_args = ", ".join(args)
        table_name = quote(uri)
        self._cursor.execute(
            f'CREATE VIRTUAL TABLE "{table_name}" USING {adapter.__name__}({formatted_args})',
        )

    def _get_description(self) -> Description:
        try:
            description = self._cursor.getdescription()
        except apsw.ExecutionCompleteError:
            return None

        return [
            (
                name,
                get_type_code(type_name),
                None,
                None,
                None,
                None,
                True,
            )
            for name, type_name in description
        ]

    @check_closed
    def executemany(
        self,
        operation: str,
        seq_of_parameters: Optional[Tuple[Any, ...]] = None,
    ) -> "Cursor":
        raise NotSupportedError("`executemany` is not supported, use `execute` instead")

    @check_result
    @check_closed
    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        """
        Fetch the next row of a query result set, returning a single sequence,
        or `None` when no more data is available.
        """
        try:
            row = self.next()
        except StopIteration:
            return None

        self._rowcount = max(0, self._rowcount) + 1

        return row

    @check_result
    @check_closed
    def fetchmany(self, size=None) -> List[Tuple[Any, ...]]:
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
    def fetchall(self) -> List[Tuple[Any, ...]]:
        """
        Fetch all (remaining) rows of a query result, returning them as a
        sequence of sequences (e.g. a list of tuples). Note that the cursor's
        arraysize attribute can affect the performance of this operation.
        """
        results = list(self)
        return results

    @check_closed
    def setinputsizes(self, sizes: int) -> None:
        # not supported
        pass

    @check_closed
    def setoutputsizes(self, sizes: int) -> None:
        # not supported
        pass

    @check_result
    @check_closed
    def __iter__(self) -> Iterator[Tuple[Any, ...]]:
        for row in self._results:  # type: ignore
            self._rowcount = max(0, self._rowcount) + 1
            yield row

    @check_result
    @check_closed
    def __next__(self) -> Tuple[Any, ...]:
        return next(self._results)  # type: ignore

    next = __next__


class Connection(object):

    """Connection to a Google Spreadsheet."""

    def __init__(
        self,
        path: str,
        adapters: List[Type[Adapter]],
        adapter_args: Dict[str, Any],
        isolation_level: Optional[str] = None,
    ):
        # create underlying APSW connection
        self._connection = apsw.Connection(path)
        self.isolation_level = isolation_level

        # register adapters
        for adapter in adapters:
            self._connection.createmodule(adapter.__name__, VTModule(adapter))
        self._adapters = adapters
        self._adapter_args = adapter_args

        self.closed = False
        self.cursors: List[Cursor] = []

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
        for cursor in self.cursors:
            if cursor.in_transaction:
                cursor._cursor.execute("COMMIT")
                cursor.in_transaction = False

    @check_closed
    def rollback(self) -> None:
        """Rollback any transactions."""
        for cursor in self.cursors:
            if cursor.in_transaction:
                cursor._cursor.execute("ROLLBACK")
                cursor.in_transaction = False

    @check_closed
    def cursor(self) -> Cursor:
        """Return a new Cursor Object using the connection."""
        cursor = Cursor(
            self._connection.cursor(),
            self._adapters,
            self._adapter_args,
            self.isolation_level,
        )
        self.cursors.append(cursor)

        return cursor

    @check_closed
    def execute(
        self,
        operation: str,
        parameters: Optional[Tuple[Any, ...]] = None,
    ) -> Cursor:
        cursor = self.cursor()
        return cursor.execute(operation, parameters)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.commit()
        self.close()


def connect(
    path: str,
    adapters: Optional[List[str]] = None,
    adapter_args: Optional[Dict[str, Any]] = None,
    safe: bool = False,
    isolation_level: Optional[str] = None,
) -> Connection:
    """
    Constructor for creating a connection to the database.

        >>> conn = connect("database.sqlite", ["csv", "weatherapi"])
        >>> curs = conn.cursor()
        >>> curs.execute("SELECT * FROM 'csv:///path/to/file.csv'")

    """
    enabled_adapters = [
        adapter.load()
        for adapter in iter_entry_points("shillelagh.adapter")
        if adapters is None or adapter.name in adapters
    ]
    if safe:
        enabled_adapters = [adapter for adapter in enabled_adapters if adapter.safe]

    return Connection(path, enabled_adapters, adapter_args or {}, isolation_level)
