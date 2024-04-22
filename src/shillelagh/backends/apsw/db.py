# pylint: disable=invalid-name, c-extension-no-member, unused-import
"""
A DB API 2.0 wrapper for APSW.
"""

import datetime
import itertools
import logging
import re
from functools import partial, wraps
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    cast,
)

import apsw

from shillelagh import functions
from shillelagh.adapters.base import Adapter
from shillelagh.adapters.registry import registry
from shillelagh.backends.apsw.vt import VTModule, type_map
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
from shillelagh.fields import Blob, Field
from shillelagh.lib import (
    best_index_object_available,
    combine_args_kwargs,
    escape_identifier,
    find_adapter,
    serialize,
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
from shillelagh.typing import Description, SQLiteValidType

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
]

apilevel = "2.0"
threadsafety = 2
paramstyle = "qmark"
sqlite_version_info = tuple(
    int(number) for number in apsw.sqlitelibversion().split(".")
)

NO_SUCH_TABLE = "SQLError: no such table: "
DEFAULT_SCHEMA = "main"

CURSOR_METHOD = TypeVar("CURSOR_METHOD", bound=Callable[..., Any])

_logger = logging.getLogger(__name__)


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


def get_type_code(type_name: str) -> Type[Field]:
    """
    Return a ``Field`` that corresponds to a type name.

    This is used to build the description of the cursor after a successful
    query.
    """
    return cast(Type[Field], type_map.get(type_name, Blob))


def convert_binding(binding: Any) -> SQLiteValidType:
    """
    Convert a binding to a SQLite type.

    Eg, if the user is filtering a timestamp column we need to convert the
    ``datetime.datetime`` object in the binding to a string.
    """
    if isinstance(binding, bool):
        return int(binding)
    if isinstance(binding, (int, float, str, bytes, type(None))):
        return binding
    if isinstance(binding, (datetime.datetime, datetime.date, datetime.time)):
        return binding.isoformat()
    return str(binding)


class Cursor:  # pylint: disable=too-many-instance-attributes
    """
    Connection cursor.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        cursor: "apsw.Cursor",
        adapters: List[Type[Adapter]],
        adapter_kwargs: Dict[str, Dict[str, Any]],
        isolation_level: Optional[str] = None,
        schema: str = DEFAULT_SCHEMA,
    ):
        self._cursor = cursor
        self._adapters = adapters
        self._adapter_kwargs = adapter_kwargs

        self.in_transaction = False
        self.isolation_level = isolation_level

        self.schema = schema

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

        self.operation: Optional[str] = None

        # Approach from: https://github.com/rogerbinns/apsw/issues/160#issuecomment-33927297
        # pylint: disable=unused-argument
        def exectrace(
            cursor: "apsw.Cursor",
            sql: str,
            bindings: Optional[Tuple[Any, ...]],
        ) -> bool:
            # In the case of an empty sequence, fall back to None,
            # meaning no rows returned.
            self.description = self._cursor.getdescription() or None
            return True

        self._cursor.setexectrace(exectrace)

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
        self._cursor.close()
        self.closed = True

    @check_closed
    def execute(
        self,
        operation: str,
        parameters: Optional[Tuple[Any, ...]] = None,
    ) -> "Cursor":
        """
        Execute a query using the cursor.
        """
        if not self.in_transaction and self.isolation_level:
            self._cursor.execute(f"BEGIN {self.isolation_level}")
            self.in_transaction = True

        self.description = None
        self._rowcount = -1

        # store current SQL in the cursor
        self.operation = operation

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
            except apsw.SQLError as ex:
                message = ex.args[0]
                if not message.startswith(NO_SUCH_TABLE):
                    raise ProgrammingError(message) from ex

                # create the virtual table
                uri = message[len(NO_SUCH_TABLE) :]
                self._create_table(uri)

        if uri := self._drop_table_uri(operation):
            adapter, args, kwargs = find_adapter(
                uri,
                self._adapter_kwargs,
                self._adapters,
            )
            instance = adapter(*args, **kwargs)
            instance.drop_table()

        return self

    def _drop_table_uri(self, operation: str) -> Optional[str]:
        """
        Build a ``DROP TABLE`` regexp.
        """
        # remove comments
        operation = "\n".join(
            line for line in operation.split("\n") if not line.strip().startswith("--")
        )
        regexp = re.compile(
            rf"^\s*DROP\s+TABLE\s+(IF\s+EXISTS\s+)?"
            rf'({self.schema}\.)?(?P<uri>(.*?)|(".*?"))\s*;?\s*$',
            re.IGNORECASE,
        )
        if match := regexp.match(operation):
            return match.groupdict()["uri"].strip('"')

        return None

    def _convert(self, cursor: "apsw.Cursor") -> Iterator[Tuple[Any, ...]]:
        """
        Convert row from SQLite types to native Python types.

        SQLite only supports 5 types. For booleans and time-related types
        we need to do the conversion here.
        """
        if not self.description:
            return  # pragma: no cover

        for row in cursor:
            yield tuple(
                # convert from SQLite types to native Python types
                type_map[desc[1].type]().parse(col)
                for col, desc in zip(row, self.description)
            )

    def _create_table(self, uri: str) -> None:
        """
        Create a virtual table.

        This method is called the first time a virtual table is accessed.
        """
        prefix = self.schema + "."
        if uri.startswith(prefix):
            uri = uri[len(prefix) :]

        adapter, args, kwargs = find_adapter(uri, self._adapter_kwargs, self._adapters)
        formatted_args = ", ".join(
            f"'{serialize(arg)}'"
            for arg in combine_args_kwargs(adapter, *args, **kwargs)
        )
        table_name = escape_identifier(uri)
        self._cursor.execute(
            f'CREATE VIRTUAL TABLE "{table_name}" USING {adapter.__name__}({formatted_args})',
        )

    def _get_description(self) -> Description:
        """
        Return the cursor description.

        We only return name and type, since that's what we get from APSW.
        """
        try:
            description = self._cursor.getdescription()
        except apsw.ExecutionCompleteError:
            return self.description

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
        seq_of_parameters: Optional[List[Tuple[Any, ...]]] = None,
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
    def fetchone(self) -> Optional[Tuple[Any, ...]]:
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
    def __iter__(self) -> Iterator[Tuple[Any, ...]]:
        for row in self._results:  # type: ignore
            self._rowcount = max(0, self._rowcount) + 1
            yield row

    @check_result
    @check_closed
    def __next__(self) -> Tuple[Any, ...]:
        return next(self._results)  # type: ignore

    next = __next__


def apsw_version() -> str:
    """
    Custom implementation of the ``VERSION`` function.

    This function shows the backend version::

        sql> SELECT VERSION();
        VERSION()
        ----------------------
        1.0.5 (apsw 3.36.0-r1)

    """
    return f"{functions.version()} (apsw {apsw.apswversion()})"


class Connection:
    """Connection."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        path: str,
        adapters: List[Type[Adapter]],
        adapter_kwargs: Dict[str, Dict[str, Any]],
        isolation_level: Optional[str] = None,
        apsw_connection_kwargs: Optional[Dict[str, Any]] = None,
        schema: str = DEFAULT_SCHEMA,
    ):
        # create underlying APSW connection
        apsw_connection_kwargs = apsw_connection_kwargs or {}
        self._connection = apsw.Connection(path, **apsw_connection_kwargs)
        self.isolation_level = isolation_level
        self.schema = schema

        # register adapters
        for adapter in adapters:
            if best_index_object_available():
                self._connection.createmodule(
                    adapter.__name__,
                    VTModule(adapter),
                    use_bestindex_object=adapter.supports_requested_columns,
                )
            else:
                self._connection.createmodule(adapter.__name__, VTModule(adapter))
        self._adapters = adapters
        self._adapter_kwargs = adapter_kwargs

        # register functions
        available_functions = {
            "sleep": functions.sleep,
            "version": apsw_version,
            "get_metadata": partial(
                functions.get_metadata,
                self._adapter_kwargs,
                adapters,
            ),
            "date_trunc": functions.date_trunc,
        }
        for name, function in available_functions.items():
            self._connection.create_scalar_function(name, function)

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
                cursor._cursor.execute("COMMIT")  # pylint: disable=protected-access
                cursor.in_transaction = False

    @check_closed
    def rollback(self) -> None:
        """Rollback any transactions."""
        for cursor in self.cursors:
            if cursor.in_transaction:
                cursor._cursor.execute("ROLLBACK")  # pylint: disable=protected-access
                cursor.in_transaction = False

    @check_closed
    def cursor(self) -> Cursor:
        """Return a new Cursor Object using the connection."""
        cursor = Cursor(
            self._connection.cursor(),
            self._adapters,
            self._adapter_kwargs,
            self.isolation_level,
            self.schema,
        )
        self.cursors.append(cursor)

        return cursor

    @check_closed
    def execute(
        self,
        operation: str,
        parameters: Optional[Tuple[Any, ...]] = None,
    ) -> Cursor:
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


def connect(  # pylint: disable=too-many-arguments
    path: str,
    adapters: Optional[List[str]] = None,
    adapter_kwargs: Optional[Dict[str, Dict[str, Any]]] = None,
    safe: bool = False,
    isolation_level: Optional[str] = None,
    apsw_connection_kwargs: Optional[Dict[str, Any]] = None,
    schema: str = DEFAULT_SCHEMA,
) -> Connection:
    """
    Constructor for creating a connection to the database.
    """
    adapter_kwargs = adapter_kwargs or {}
    enabled_adapters = registry.load_all(adapters, safe)

    # replace entry point names with class names
    mapping = {
        name: adapter.__name__.lower() for name, adapter in enabled_adapters.items()
    }
    adapter_kwargs = {mapping[k]: v for k, v in adapter_kwargs.items() if k in mapping}

    return Connection(
        path,
        list(enabled_adapters.values()),
        adapter_kwargs,
        isolation_level,
        apsw_connection_kwargs,
        schema,
    )
