# pylint: disable=protected-access, abstract-method
"""A SQLALchemy dialect."""
from typing import Any
from typing import cast
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import sqlalchemy.types
from sqlalchemy.dialects.sqlite.base import SQLiteDialect
from sqlalchemy.engine.url import URL
from sqlalchemy.pool.base import _ConnectionFairy
from sqlalchemy.sql.visitors import VisitableType
from typing_extensions import TypedDict

from shillelagh.adapters.base import Adapter
from shillelagh.backends.apsw import db
from shillelagh.backends.apsw.vt import VTTable
from shillelagh.exceptions import ProgrammingError


class SQLAlchemyColumn(TypedDict):
    """
    A custom type for a SQLAlchemy column.
    """

    name: str
    type: VisitableType
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
    for adapter in raw_connection._adapters:
        key = adapter.__name__.lower()
        kwargs = raw_connection._adapter_kwargs.get(key, {})
        if adapter.supports(table_name, **kwargs):
            key = adapter.__name__.lower()
            args = adapter.parse_uri(table_name)
            kwargs = raw_connection._adapter_kwargs.get(key, {})
            return adapter(*args, **kwargs)  # type: ignore

    raise ProgrammingError(f"Unsupported table: {table_name}")
