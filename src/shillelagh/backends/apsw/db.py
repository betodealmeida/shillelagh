# pylint: disable=invalid-name, c-extension-no-member, unused-import
"""
A DB API 2.0 wrapper for APSW.
"""

import datetime
import logging
import re
from collections.abc import Iterator
from functools import partial
from typing import Any, Callable, Optional, TypeVar, cast

import apsw

from shillelagh import functions
from shillelagh.adapters.base import Adapter
from shillelagh.adapters.registry import registry
from shillelagh.backends.apsw.vt import VTModule, type_map
from shillelagh.db import (
    DEFAULT_SCHEMA,
    Connection,
    Cursor,
    apilevel,
    check_closed,
    paramstyle,
    threadsafety,
)
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
from shillelagh.typing import ColumnDescription, Description, SQLiteValidType

__all__ = [
    "DatabaseError",
    "DataError",
    "Error",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "OperationalError",
    "NotSupportedError",
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

sqlite_version_info = tuple(
    int(number) for number in apsw.sqlitelibversion().split(".")
)

NO_SUCH_TABLE = re.compile("no such table: (?P<uri>.*)")

CURSOR_METHOD = TypeVar("CURSOR_METHOD", bound=Callable[..., Any])

_logger = logging.getLogger(__name__)


def get_missing_table(message: str) -> Optional[str]:
    """
    Return the missing table from a message.

    This is used to extract the table name from an APSW error message.
    """
    if match := NO_SUCH_TABLE.search(message):
        return match.groupdict()["uri"]

    return None


def get_type_code(type_name: str) -> type[Field]:
    """
    Return a ``Field`` that corresponds to a type name.

    This is used to build the description of the cursor after a successful
    query.
    """
    return cast(type[Field], type_map.get(type_name, Blob))


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


class APSWCursor(Cursor):  # pylint: disable=too-many-instance-attributes
    """
    Connection cursor.
    """

    def __init__(  # pylint: disable=too-many-arguments, too-many-positional-arguments
        self,
        cursor: "apsw.Cursor",
        adapters: list[type[Adapter]],
        adapter_kwargs: dict[str, dict[str, Any]],
        isolation_level: Optional[str] = None,
        schema: str = DEFAULT_SCHEMA,
    ):
        super().__init__(adapters, adapter_kwargs, schema)

        self._cursor = cursor
        self.in_transaction = False
        self.isolation_level = isolation_level

        # Approach from: https://github.com/rogerbinns/apsw/issues/160#issuecomment-33927297
        # pylint: disable=unused-argument
        def exectrace(
            cursor: "apsw.Cursor",
            sql: str,
            bindings: Optional[tuple[Any, ...]],
        ) -> bool:
            # In the case of an empty sequence, fall back to None,
            # meaning no rows returned.
            self.description = self._cursor.getdescription() or None
            return True

        self._cursor.setexectrace(exectrace)

    @check_closed
    def execute(
        self,
        operation: str,
        parameters: Optional[tuple[Any, ...]] = None,
    ) -> "APSWCursor":
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
        created_tables = set()
        while True:
            try:
                self._cursor.execute(operation, parameters)
                self.description = self._get_description()
                self._results = self._convert(self._cursor)
                break
            except apsw.SQLError as ex:
                message = ex.args[0]
                if uri := get_missing_table(message):
                    if uri in created_tables:
                        raise ProgrammingError(message) from ex

                    # create the virtual table
                    self._create_table(uri)
                    created_tables.add(uri)
                    continue

                raise ProgrammingError(message) from ex

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
        schema = re.escape(self.schema)
        regexp = re.compile(
            r"^\s*DROP\s+TABLE\s+(IF\s+EXISTS\s+)?"
            rf'({schema}\.)?(?P<uri>(.*?)|(".*?"))\s*;?\s*$',
            re.IGNORECASE,
        )
        if match := regexp.match(operation):
            return match.groupdict()["uri"].strip('"')

        return None

    def _convert(self, cursor: "apsw.Cursor") -> Iterator[tuple[Any, ...]]:
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

        annotated_alias = re.compile(r"^(\w+)\s*\[(\w+)]$")

        out: list[ColumnDescription] = []
        for name, type_name in description:
            if match := annotated_alias.match(name):
                name = match.group(1)
                type_name = match.group(2)

            type_code = get_type_code(type_name)
            out.append((name, type_code, None, None, None, None, True))

        return out

    @check_closed
    def close(self) -> None:
        """
        Close the cursor.

        This will also close the underlying APSW cursor.
        """
        if self.in_transaction:
            self._cursor.execute("ROLLBACK")
            self.in_transaction = False

        self._cursor.close()
        super().close()


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


class APSWConnection(
    Connection[APSWCursor],
):  # pylint: disable=too-many-instance-attributes
    """Connection."""

    def __init__(  # pylint: disable=too-many-arguments, too-many-positional-arguments
        self,
        path: str,
        adapters: list[type[Adapter]],
        adapter_kwargs: dict[str, dict[str, Any]],
        isolation_level: Optional[str] = None,
        apsw_connection_kwargs: Optional[dict[str, Any]] = None,
        schema: str = DEFAULT_SCHEMA,
        safe: bool = False,
    ):
        super().__init__(adapters, adapter_kwargs, schema, safe)

        # create underlying APSW connection
        apsw_connection_kwargs = apsw_connection_kwargs or {}
        self._connection = apsw.Connection(path, **apsw_connection_kwargs)
        self.isolation_level = isolation_level

        # register adapters
        for adapter in self._adapters:
            if best_index_object_available():
                self._connection.createmodule(
                    adapter.__name__,
                    VTModule(adapter),
                    use_bestindex_object=adapter.supports_requested_columns,
                )
            else:
                self._connection.createmodule(adapter.__name__, VTModule(adapter))

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
        if not safe:
            available_functions["upgrade"] = functions.upgrade

        for name, function in available_functions.items():
            self._connection.create_scalar_function(name, function)

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
    def cursor(self) -> APSWCursor:
        """Return a new Cursor Object using the connection."""
        cursor = APSWCursor(
            self._connection.cursor(),
            self._adapters,
            self._adapter_kwargs,
            self.isolation_level,
            self.schema,
        )
        self.cursors.append(cursor)

        return cursor


def connect(  # pylint: disable=too-many-arguments, too-many-positional-arguments
    path: str,
    adapters: Optional[list[str]] = None,
    adapter_kwargs: Optional[dict[str, dict[str, Any]]] = None,
    safe: bool = False,
    isolation_level: Optional[str] = None,
    apsw_connection_kwargs: Optional[dict[str, Any]] = None,
    schema: str = DEFAULT_SCHEMA,
) -> APSWConnection:
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

    return APSWConnection(
        path,
        list(enabled_adapters.values()),
        adapter_kwargs,
        isolation_level,
        apsw_connection_kwargs,
        schema,
        safe,
    )
