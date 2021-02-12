import urllib.parse
from functools import wraps
from typing import Any
from typing import Callable
from typing import cast
from typing import Dict
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
from shillelagh.fields import type_map
from shillelagh.types import BINARY
from shillelagh.types import DBAPIType
from shillelagh.types import Description

apilevel = "2.0"
threadsafety = 2
paramstyle = "pyformat"

NO_SUCH_TABLE = "SQLError: no such table: "

F = TypeVar("F", bound=Callable[..., Any])


def check_closed(method: F) -> F:
    """Decorator that checks if a connection or cursor is closed."""

    @wraps(method)
    def wrapper(self, *args, **kwargs):
        if self.closed:
            raise ProgrammingError(f"{self.__class__.__name__} already closed")
        return method(self, *args, **kwargs)

    return cast(F, wrapper)


def check_result(method: F) -> F:
    """Decorator that checks if the cursor has results from `execute`."""

    @wraps(method)
    def wrapper(self, *args, **kwargs):
        if self._results is None:
            raise ProgrammingError("Called before `execute`")
        return method(self, *args, **kwargs)

    return cast(F, wrapper)


def get_type_code(type_name: str) -> Type[DBAPIType]:
    return type_map.get(type_name, BINARY)


class Cursor(object):

    """Connection cursor."""

    def __init__(self, cursor: "apsw.Cursor", adapters: List[Type[Adapter]]):
        self._cursor = cursor
        self._adapters = adapters

        self.in_transaction = False

        # This read/write attribute specifies the number of rows to fetch at a
        # time with .fetchmany(). It defaults to 1 meaning to fetch a single
        # row at a time.
        self.arraysize = 1

        self.closed = False

        # this is updated only after a query
        self.description: Description = None

        # this is set to a list of rows after a successful query
        self._results: Optional[List[Any]] = None
        self._rowcount = -1

    @property  # type: ignore
    @check_result
    @check_closed
    def rowcount(self) -> int:
        return self._rowcount

    @check_closed
    def close(self) -> None:
        """Close the cursor."""
        self._cursor.close()
        self.closed = True

    @check_closed
    def execute(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> "Cursor":
        if not self.in_transaction:
            self._cursor.execute("BEGIN")
            self.in_transaction = True

        self.description = None
        query = apply_parameters(operation, parameters or {})
        try:
            self._cursor.execute(query)
            self.description = self._get_description()
            self._results = list(self._cursor)
        except apsw.SQLError as exc:
            message = exc.args[0]
            if not message.startswith(NO_SUCH_TABLE):
                raise exc

            # create the virtual table
            uri = message[len(NO_SUCH_TABLE) :]
            self._create_table(uri)

            # try again
            self._cursor.execute(query)
            self.description = self._get_description()
            self._results = list(self._cursor)

        self._rowcount = len(self._results)

        return self

    def _create_table(self, uri: str) -> None:
        for adapter in self._adapters:
            if adapter.supports(uri):
                break
        else:
            raise ProgrammingError(f"Unsupported table: {uri}")

        table_name = uri.replace('"', '""')
        args = ", ".join(adapter.parse_uri(uri))
        self._cursor.execute(
            f'CREATE VIRTUAL TABLE "{table_name}" USING {adapter.__name__}({args})',
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
    def executemany(self, operation, seq_of_parameters=None):
        raise NotSupportedError("`executemany` is not supported, use `execute` instead")

    @check_result
    @check_closed
    def fetchone(self):
        """
        Fetch the next row of a query result set, returning a single sequence,
        or `None` when no more data is available.
        """
        try:
            return self._results.pop(0)
        except IndexError:
            return None

    @check_result
    @check_closed
    def fetchmany(self, size=None):
        """
        Fetch the next set of rows of a query result, returning a sequence of
        sequences (e.g. a list of tuples). An empty sequence is returned when
        no more rows are available.
        """
        size = size or self.arraysize
        out = self._results[:size]
        self._results = self._results[size:]
        return out

    @check_result
    @check_closed
    def fetchall(self):
        """
        Fetch all (remaining) rows of a query result, returning them as a
        sequence of sequences (e.g. a list of tuples). Note that the cursor's
        arraysize attribute can affect the performance of this operation.
        """
        out = self._results[:]
        self._results = []
        return out

    @check_closed
    def setinputsizes(self, sizes):
        # not supported
        pass

    @check_closed
    def setoutputsizes(self, sizes):
        # not supported
        pass

    @check_closed
    def __iter__(self):
        return iter(self._results)


class Connection(object):

    """Connection to a Google Spreadsheet."""

    def __init__(self, path: str, adapters: List[Type[Adapter]]):
        # create underlying APSW connection
        self._connection = apsw.Connection(path)

        # register adapters
        for adapter in adapters:
            self._connection.createmodule(adapter.__name__, VTModule(adapter))
        self._adapters = adapters

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
        cursor = Cursor(self._connection.cursor(), self._adapters)
        self.cursors.append(cursor)

        return cursor

    @check_closed
    def execute(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> Cursor:
        cursor = self.cursor()
        return cursor.execute(operation, parameters)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.commit()
        self.close()


def connect(path: str, adapters: Optional[str] = None) -> Connection:
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
    return Connection(path, enabled_adapters)


def apply_parameters(operation: str, parameters: Dict[str, Any]) -> str:
    escaped_parameters = {key: escape(value) for key, value in parameters.items()}
    return operation % escaped_parameters


def escape(value: Any) -> str:
    if value == "*":
        return cast(str, value)
    elif isinstance(value, str):
        quoted_value = value.replace("'", "''")
        return f"'{quoted_value}'"
    elif isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, (list, tuple)):
        combined_value = ", ".join(escape(element) for element in value)
        return f"({combined_value})"
    else:
        raise ProgrammingError(f"Unable to escape value: {value!r}")
