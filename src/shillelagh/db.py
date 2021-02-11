import urllib.parse
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import apsw
from pkg_resources import iter_entry_points

from shillelagh.adapters.base import Adapter
from shillelagh.backends.apsw.vt import VTModule
from shillelagh.exceptions import Error, NotSupportedError, ProgrammingError

NO_SUCH_TABLE = "SQLError: no such table: "


def check_closed(f):
    """Decorator that checks if connection/cursor is closed."""

    def g(self, *args, **kwargs):
        if self.closed:
            raise Error("{klass} already closed".format(klass=self.__class__.__name__))
        return f(self, *args, **kwargs)

    return g


def check_result(f):
    """Decorator that checks if the cursor has results from `execute`."""

    def g(self, *args, **kwargs):
        if self._results is None:
            raise Error("Called before `execute`")
        return f(self, *args, **kwargs)

    return g


class Cursor(object):

    """Connection cursor."""

    def __init__(self, cursor: "apsw.Cursor", adapters: List[Adapter]):
        self._cursor = cursor
        self._adapters = adapters

        self.in_transaction = False

        # This read/write attribute specifies the number of rows to fetch at a
        # time with .fetchmany(). It defaults to 1 meaning to fetch a single
        # row at a time.
        self.arraysize = 1

        self.closed = False

        # this is updated only after a query
        self.description = None

        # this is set to a list of rows after a successful query
        self._results = None

    @property
    @check_result
    @check_closed
    def rowcount(self) -> int:
        return len(self._results)

    @check_closed
    def close(self) -> None:
        """Close the cursor."""
        self._cursor.close()
        self.closed = True

    @check_closed
    def execute(self, operation: str, parameters: Optional[Dict[str, Any]] = None):
        if not self.in_transaction:
            self._cursor.execute("BEGIN")
            self.in_transaction = True

        self.description = None
        query = apply_parameters(operation, parameters or {})
        try:
            self._cursor.execute(query)
            self._results = list(self._cursor)
            # XXX fix type
            self.description = self._cursor.getdescription() + (None,) * 5
        except apsw.SQLError as exc:
            message = exc.args[0]
            if not message.startswith(NO_SUCH_TABLE):
                raise exc

            # create the virtual table
            uri = message[len(NO_SUCH_TABLE) :]
            self._create_table(uri)

            # try again
            self._cursor.execute(query)
            self._results = list(self._cursor)
            # XXX fix type
            self.description = self._cursor.getdescription() + (None,) * 5

        return self

    def _create_table(self, uri: str) -> None:
        scheme = urllib.parse.urlparse(uri).scheme
        if scheme not in self._adapters:
            raise ProgrammingError(f"Invalid scheme: {scheme}")
        adapter = self._adapters[scheme]

        table_name = uri.replace("'", "''")
        args = ", ".join(adapter.parse_uri(uri))
        self._cursor.execute(
            f"CREATE VIRTUAL TABLE '{table_name}' USING {adapter.__name__}({args})"
        )

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

    def __init__(self, path: str, adapters: Dict[str, Adapter]):
        # create underlying APSW connection
        self._connection = apsw.Connection(path)

        # register adapters
        for name, adapter in adapters.items():
            self._connection.createmodule(name, VTModule(adapter))
        self._adapters = adapters

        self.closed = False
        self.cursors = []

    @check_closed
    def close(self) -> None:
        """Close the connection now."""
        self.closed = True
        for cursor in self.cursors:
            try:
                cursor.close()
            except Error:
                pass  # already closed

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
        self, operation: str, parameters: Optional[Dict[str, Any]] = None
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
    print(list(iter_entry_points("shillelagh.adapter")))
    enabled_adapters = {
        adapter.name: adapter.load()
        for adapter in iter_entry_points("shillelagh.adapter")
        if adapters is None or adapter.name in adapters
    }
    return Connection(path, enabled_adapters)


def apply_parameters(operation: str, parameters: Dict[str, Any]) -> str:
    escaped_parameters = {key: escape(value) for key, value in parameters.items()}
    return operation % escaped_parameters


def escape(value: Any) -> str:
    if value == "*":
        return value
    elif isinstance(value, str):
        return "'{}'".format(value.replace("'", "''"))
    elif isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, (list, tuple)):
        return "({0})".format(", ".join(escape(element) for element in value))
    else:
        raise ProgrammingError(f"Unable to escape value: {value!r}")
