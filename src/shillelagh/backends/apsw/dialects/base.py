# pylint: disable=protected-access, abstract-method
"""A SQLALchemy dialect."""
from typing import Any, Dict, List, Optional, Tuple, cast

import sqlalchemy.types
from sqlalchemy.dialects.sqlite.base import SQLiteDialect
from sqlalchemy.engine.url import URL
from sqlalchemy.pool.base import _ConnectionFairy
from sqlalchemy.sql.type_api import TypeEngine
from typing_extensions import TypedDict

from shillelagh.adapters.base import Adapter
from shillelagh.backends.apsw import db
from shillelagh.backends.apsw.vt import VTTable
from shillelagh.exceptions import ProgrammingError
from shillelagh.lib import find_adapter


class SQLAlchemyColumn(TypedDict):
    """
    A custom type for a SQLAlchemy column.
    """

    name: str
    type: TypeEngine
    nullable: bool
    default: Optional[str]
    autoincrement: str
    primary_key: int


class APSWDialect(SQLiteDialect):

    """
    A SQLAlchemy dialect for Shillelagh.

    The dialect is based on the ``SQLiteDialect``, since we're using APSW.
    """

    name = "shillelagh"
    driver = "apsw"

    # This is supported in ``SQLiteDialect``, and equally supported here. See
    # https://docs.sqlalchemy.org/en/14/core/connections.html#caching-for-third-party-dialects
    # for more context.
    supports_statement_cache = True

    # ``SQLiteDialect.colspecs`` has custom representations for objects that SQLite stores
    # as string (eg, timestamps). Since the Shillelagh DB API driver returns them as
    # proper objects the custom representations are not needed.
    colspecs: Dict[TypeEngine, TypeEngine] = {}

    supports_sane_rowcount = False

    @classmethod
    def dbapi(cls):  # pylint: disable=method-hidden
        """
        Return the DB API module.
        """
        return db

    def __init__(
        self,
        adapters: Optional[List[str]] = None,
        adapter_kwargs: Optional[Dict[str, Dict[str, Any]]] = None,
        safe: bool = False,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self._adapters = adapters
        self._adapter_kwargs = adapter_kwargs or {}
        self._safe = safe

    def create_connect_args(
        self,
        url: URL,
    ) -> Tuple[Tuple[()], Dict[str, Any]]:
        path = str(url.database) if url.database else ":memory:"
        return (), {
            "path": path,
            "adapters": self._adapters,
            "adapter_kwargs": self._adapter_kwargs,
            "safe": self._safe,
            "isolation_level": self.isolation_level,
        }

    def do_ping(self, dbapi_connection: _ConnectionFairy) -> bool:
        """
        Return true if the database is online.
        """
        return True

    def has_table(  # pylint: disable=unused-argument
        self,
        connection: _ConnectionFairy,
        table_name: str,
        schema: Optional[str] = None,
        info_cache: Optional[Dict[Any, Any]] = None,
        **kwargs: Any,
    ) -> bool:
        """
        Return true if a given table exists.
        """
        try:
            get_adapter_for_table_name(connection, table_name)
        except ProgrammingError:
            return False
        return True

    # needed for SQLAlchemy
    def _get_table_sql(  # pylint: disable=unused-argument
        self,
        connection: _ConnectionFairy,
        table_name: str,
        schema: Optional[str] = None,
        **kwargs: Any,
    ) -> str:
        adapter = get_adapter_for_table_name(connection, table_name)
        table = VTTable(adapter)
        return table.get_create_table(table_name)

    def get_columns(  # pylint: disable=unused-argument
        self,
        connection: _ConnectionFairy,
        table_name: str,
        schema: Optional[str] = None,
        **kwargs: Any,
    ) -> List[SQLAlchemyColumn]:
        adapter = get_adapter_for_table_name(connection, table_name)
        columns = adapter.get_columns()
        return [
            {
                "name": column_name,
                "type": getattr(sqlalchemy.types, field.type),
                "nullable": True,
                "default": None,
                "autoincrement": "auto",
                "primary_key": 0,
            }
            for column_name, field in columns.items()
        ]


def get_adapter_for_table_name(
    connection: _ConnectionFairy,
    table_name: str,
) -> Adapter:
    """
    Return an adapter associated with a connection.

    This function instantiates the adapter responsible for a given table name,
    using the connection to properly pass any adapter kwargs.
    """
    raw_connection = cast(db.Connection, connection.engine.raw_connection())
    adapter, args, kwargs = find_adapter(
        table_name,
        raw_connection._adapter_kwargs,
        raw_connection._adapters,
    )
    return adapter(*args, **kwargs)  # type: ignore
