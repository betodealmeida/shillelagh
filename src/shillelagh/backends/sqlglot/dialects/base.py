"""
SQLAlchemy dialect.
"""

# pylint: disable=abstract-method, unused-argument

from types import ModuleType
from typing import Any, Optional, TypedDict, cast

import sqlalchemy.types
from sqlalchemy.engine.base import Connection as SqlaConnection
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.pool.base import _ConnectionFairy
from sqlalchemy.sql import compiler
from sqlalchemy.sql.type_api import TypeEngine

from shillelagh.adapters.base import Adapter
from shillelagh.backends.sqlglot import db
from shillelagh.backends.sqlglot.db import Connection
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


class SQLGlotDialect(DefaultDialect):
    """
    A SQLAlchemy dialect for Shillelagh based on the Python executor.
    """

    name = "shillelagh"
    driver = "sqlglot"

    statement_compiler = compiler.SQLCompiler
    ddl_compiler = compiler.DDLCompiler
    type_compiler = compiler.GenericTypeCompiler
    preparer = compiler.IdentifierPreparer

    supports_alter = False
    supports_comments = True
    inline_comments = True
    supports_statement_cache = True

    supports_schemas = False
    supports_views = False
    postfetch_lastrowid = False

    supports_native_boolean = True

    isolation_level = "AUTOCOMMIT"

    default_paramstyle = "qmark"

    supports_is_distinct_from = False

    @classmethod
    def dbapi(cls) -> ModuleType:  # pylint: disable=method-hidden
        """
        Return the DB API module.
        """
        return db

    @classmethod
    def import_dbapi(cls):  # pylint: disable=method-hidden
        """
        Return the DB API module.
        """
        return db

    def __init__(
        self,
        adapters: Optional[list[str]] = None,
        adapter_kwargs: Optional[dict[str, dict[str, Any]]] = None,
        safe: bool = False,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self._adapters = adapters
        self._adapter_kwargs = adapter_kwargs or {}
        self._safe = safe

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
        **kwargs: Any,
    ) -> bool:
        """
        Return true if a given table exists.
        """
        try:
            get_adapter_for_table_name(connection, table_name)
            return True
        except ProgrammingError:
            pass

        return False

    def get_table_names(
        self,
        connection: SqlaConnection,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> list[str]:
        """
        Return a list of table names.
        """
        return []

    def get_columns(  # pylint: disable=unused-argument
        self,
        connection: _ConnectionFairy,
        table_name: str,
        schema: Optional[str] = None,
        **kwargs: Any,
    ) -> list[SQLAlchemyColumn]:
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

    def do_rollback(self, dbapi_connection: Connection) -> None:
        """
        Executor doesn't support rollbacks.
        """

    # methods that are needed for integration with Apache Superset
    def get_schema_names(self, connection: SqlaConnection, **kw: Any):
        """
        Return the list of schemas.
        """
        return ["main"]

    def get_pk_constraint(
        self,
        connection: SqlaConnection,
        table_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ):
        return {"constrained_columns": [], "name": None}

    def get_foreign_keys(
        self,
        connection: SqlaConnection,
        table_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ):
        return []

    get_check_constraints = get_foreign_keys
    get_indexes = get_foreign_keys
    get_unique_constraints = get_foreign_keys

    def get_table_comment(self, connection, table_name, schema=None, **kwargs):
        return {"text": ""}


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
        raw_connection._adapter_kwargs,  # pylint: disable=protected-access
        raw_connection._adapters,  # pylint: disable=protected-access
    )
    return adapter(*args, **kwargs)  # type: ignore
