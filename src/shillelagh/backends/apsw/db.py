import datetime
import itertools
import json
import urllib.parse
from collections import Counter
from functools import partial
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
from shillelagh import functions
from shillelagh.adapters.base import Adapter
from shillelagh.backends.apsw.vt import VTModule
from shillelagh.exceptions import Error
from shillelagh.exceptions import InterfaceError
from shillelagh.exceptions import NotSupportedError
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Blob
from shillelagh.fields import Field
from shillelagh.fields import type_map
from shillelagh.lib import combine_args_kwargs
from shillelagh.lib import quote
from shillelagh.lib import serialize
from shillelagh.types import Description
from shillelagh.types import SQLiteValidType

apilevel = "2.0"
threadsafety = 2
paramstyle = "qmark"
sqlite_version_info = tuple(
    int(number) for number in apsw.sqlitelibversion().split(".")
)

NO_SUCH_TABLE = "SQLError: no such table: "
SCHEMA = "main"

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


def convert_binding(binding: Any) -> SQLiteValidType:
    """
    Convert a binding to a SQLite type.

    Eg, if the user is filtering a timestamp column we need to convert the
    `datetime.datetime` object in the binding to a string.
    """
    if isinstance(binding, bool):
        return int(binding)
    if isinstance(binding, (int, float, str, bytes, type(None))):
        return binding
    if isinstance(binding, (datetime.datetime, datetime.date, datetime.time)):
        return binding.isoformat()
    return str(binding)


class Cursor(object):

    """Connection cursor."""

    def __init__(
        self,
        cursor: "apsw.Cursor",
        adapters: List[Type[Adapter]],
        adapter_kwargs: Dict[str, Dict[str, Any]],
        isolation_level: Optional[str] = None,
    ):
        self._cursor = cursor
        self._adapters = adapters
        self._adapter_kwargs = adapter_kwargs

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

        # convert parameters (bindings) to types accepted by SQLite
        if parameters:
            parameters = tuple(convert_binding(parameter) for parameter in parameters)

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
            yield tuple(
                # convert from SQLite types to native Python types
                type_map[desc[1].type]().parse(col)
                for col, desc in zip(row, self.description)
            )

    def _create_table(self, uri: str) -> None:
        prefix = f"{SCHEMA}."
        if uri.startswith(prefix):
            uri = uri[len(prefix) :]

        for adapter in self._adapters:
            key = adapter.__name__.lower()
            kwargs = self._adapter_kwargs.get(key, {})
            if adapter.supports(uri, **kwargs):
                break
        else:
            raise ProgrammingError(f"Unsupported table: {uri}")

        # collect arguments from URI and connection and serialize them
        key = adapter.__name__.lower()
        args = adapter.parse_uri(uri)
        kwargs = self._adapter_kwargs.get(key, {})
        formatted_args = ", ".join(
            serialize(arg) for arg in combine_args_kwargs(adapter, *args, **kwargs)
        )
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

    """Connection."""

    def __init__(
        self,
        path: str,
        adapters: List[Type[Adapter]],
        adapter_kwargs: Dict[str, Dict[str, Any]],
        isolation_level: Optional[str] = None,
    ):
        # create underlying APSW connection
        self._connection = apsw.Connection(path)
        self.isolation_level = isolation_level

        # register adapters
        for adapter in adapters:
            self._connection.createmodule(adapter.__name__, VTModule(adapter))
        self._adapters = adapters
        self._adapter_kwargs = adapter_kwargs

        # register functions
        available_functions = {
            "sleep": functions.sleep,
            "version": functions.version,
            "get_metadata": partial(
                functions.get_metadata,
                self._adapter_kwargs,
                adapters,
            ),
        }
        for name, function in available_functions.items():
            self._connection.createscalarfunction(name, function)

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
            self._adapter_kwargs,
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
    adapter_kwargs: Optional[Dict[str, Dict[str, Any]]] = None,
    safe: bool = False,
    isolation_level: Optional[str] = None,
) -> Connection:
    """
    Constructor for creating a connection to the database.

        >>> conn = connect("database.sqlite", ["csv", "weatherapi"])
        >>> curs = conn.cursor()
        >>> curs.execute("SELECT * FROM 'csv:///path/to/file.csv'")

    """
    adapter_kwargs = adapter_kwargs or {}

    all_adapters = []
    for entry_point in iter_entry_points("shillelagh.adapter"):
        try:
            adapter = entry_point.load()
        except (ImportError, ModuleNotFoundError):
            continue
        all_adapters.append((entry_point.name, adapter))

    all_adapters_names = [name for name, adapter in all_adapters]

    # check if there are any repeated names, to prevent malicious adapters
    if safe:
        repeated = {
            name for name, count in Counter(all_adapters_names).items() if count > 1
        }
        if repeated:
            raise InterfaceError(f'Repeated adapter names found: {", ".join(repeated)}')

    adapters = adapters or ([] if safe else all_adapters_names)
    enabled_adapters = [
        adapter
        for (name, adapter) in all_adapters
        if name in adapters and (adapter.safe or not safe)
    ]

    # replace entry point names with class names
    mapping = {
        name: adapter.__name__.lower()
        for name, adapter in all_adapters
        if adapter in enabled_adapters
    }
    adapter_kwargs = {mapping[k]: v for k, v in adapter_kwargs.items()}

    return Connection(
        path,
        enabled_adapters,
        adapter_kwargs,
        isolation_level,
    )
