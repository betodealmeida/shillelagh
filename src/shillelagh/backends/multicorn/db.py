# pylint: disable=invalid-name, c-extension-no-member, unused-import
"""
A DB API 2.0 wrapper.
"""
import logging
import re
from typing import Any, Dict, List, Optional, Tuple, Type, Union, cast
from uuid import uuid4

import psycopg2
from multicorn import ForeignDataWrapper, Qual, SortKey
from psycopg2 import extensions

from shillelagh.adapters.base import Adapter
from shillelagh.adapters.registry import registry
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
from shillelagh.lib import (
    combine_args_kwargs,
    deserialize,
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
from shillelagh.typing import Row

apilevel = "2.0"
threadsafety = 2
paramstyle = "pyformat"

NO_SUCH_TABLE = re.compile('relation "(.*?)" does not exist')
DEFAULT_SCHEMA = "main"

_logger = logging.getLogger(__name__)


class Cursor(extensions.cursor):  # pylint: disable=too-few-public-methods
    """
    A cursor that registers FDWs.
    """

    def __init__(
        self,
        *args: Any,
        adapters: Dict[str, Type[Adapter]],
        adapter_kwargs: Dict[str, Dict[str, Any]],
        schema: str,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)

        self._adapters = list(adapters.values())
        self._adapter_map = {v: k for k, v in adapters.items()}
        self._adapter_kwargs = adapter_kwargs
        self.schema = schema

    def execute(
        self,
        operation: str,
        parameters: Optional[Tuple[Any, ...]] = None,
    ) -> Union["Cursor", extensions.cursor]:
        """
        Execute a query, automatically registering FDWs if necessary.
        """
        # which cursor should be returned
        cursor: Union["Cursor", extensions.cursor] = self

        while True:
            savepoint = uuid4()
            super().execute(f'SAVEPOINT "{savepoint}"')

            try:
                cursor = cast(extensions.cursor, super().execute(operation, parameters))
                break
            except psycopg2.errors.UndefinedTable as ex:  # pylint: disable=no-member
                message = ex.args[0]
                match = NO_SUCH_TABLE.match(message)
                if not match:
                    raise ProgrammingError(message) from ex

                # Postgres truncates the table name in the error message, so we need to
                # find it in the original query
                fragment = match.group(1)
                uri = self._get_table_uri(fragment, operation)
                if not uri:
                    raise ProgrammingError("Could not determine table name") from ex

                super().execute(f'ROLLBACK TO SAVEPOINT "{savepoint}"')
                self._create_table(uri)

        if uri := self._drop_table_uri(operation):
            adapter, args, kwargs = find_adapter(
                uri,
                self._adapter_kwargs,
                self._adapters,
            )
            instance = adapter(*args, **kwargs)
            instance.drop_table()

        return cursor

    def _get_table_uri(self, fragment: str, operation: str) -> Optional[str]:
        """
        Extract the table name from a query.
        """
        schema = re.escape(self.schema)
        fragment = re.escape(fragment)
        regexp = re.compile(
            rf'\b(FROM|INTO)\s+({schema}\.)?(?P<uri>"{fragment}.*?")',
            re.IGNORECASE,
        )
        if match := regexp.search(operation):
            return match.groupdict()["uri"].strip('"')

        return None

    def _drop_table_uri(self, operation: str) -> Optional[str]:
        """
        Build a ``DROP TABLE`` regexp.
        """
        schema = re.escape(self.schema)
        regexp = re.compile(
            r"^\s*DROP\s+TABLE\s+(IF\s+EXISTS\s+)?"
            rf'({schema}\.)?(?P<uri>(.*?)|(".*?"))\s*;?\s*$',
            re.IGNORECASE,
        )
        if match := regexp.match(operation):
            return match.groupdict()["uri"].strip('"')

        return None

    def _create_table(self, uri: str) -> None:
        """
        Register a FDW.
        """
        adapter, args, kwargs = find_adapter(uri, self._adapter_kwargs, self._adapters)
        formatted_args = serialize(combine_args_kwargs(adapter, *args, **kwargs))
        entrypoint = self._adapter_map[adapter]

        table_name = escape_identifier(uri)

        columns = adapter(*args, **kwargs).get_columns()
        if not columns:
            raise ProgrammingError(f"Virtual table {table_name} has no columns")

        quoted_columns = {k.replace('"', '""'): v for k, v in columns.items()}
        formatted_columns = ", ".join(
            f'"{k}" {v.type}' for (k, v) in quoted_columns.items()
        )

        super().execute(
            """
CREATE SERVER shillelagh foreign data wrapper multicorn options (
    wrapper 'shillelagh.backends.multicorn.fdw.MulticornForeignDataWrapper'
);
    """,
        )
        super().execute(
            f"""
CREATE FOREIGN TABLE "{table_name}" (
    {formatted_columns}
) server shillelagh options (
    adapter '{entrypoint}',
    args '{formatted_args}'
);
        """,
        )


class CursorFactory:  # pylint: disable=too-few-public-methods
    """
    Custom cursor factory.

    This returns a custom cursor that will auto register FDWs for the user.
    """

    def __init__(
        self,
        adapters: Dict[str, Type[Adapter]],
        adapter_kwargs: Dict[str, Dict[str, Any]],
        schema: str,
    ):
        self.schema = schema
        self._adapters = adapters
        self._adapter_kwargs = adapter_kwargs

    def __call__(self, *args, **kwargs) -> Cursor:
        """
        Create a new cursor.
        """
        return Cursor(
            *args,
            adapters=self._adapters,
            adapter_kwargs=self._adapter_kwargs,
            schema=self.schema,
            **kwargs,
        )


def connect(  # pylint: disable=too-many-arguments
    dsn: Optional[str] = None,
    adapters: Optional[List[str]] = None,
    adapter_kwargs: Optional[Dict[str, Dict[str, Any]]] = None,
    schema: str = DEFAULT_SCHEMA,
    **psycopg2_connection_kwargs: Any,
) -> extensions.connection:
    """
    Constructor for creating a connection to the database.

    Only safe adapters can be loaded. If no adapters are specified, all safe adapters are
    loaded.
    """
    adapter_kwargs = adapter_kwargs or {}
    enabled_adapters = {
        name: adapter
        for name, adapter in registry.load_all(adapters, safe=False).items()
        if adapter.safe
    }

    # replace entry point names with class names
    mapping = {
        name: adapter.__name__.lower() for name, adapter in enabled_adapters.items()
    }
    adapter_kwargs = {mapping[k]: v for k, v in adapter_kwargs.items() if k in mapping}

    cursor_factory = CursorFactory(
        enabled_adapters,
        adapter_kwargs,
        schema,
    )
    return psycopg2.connect(
        dsn,
        cursor_factory=cursor_factory,
        **psycopg2_connection_kwargs,
    )
