"""
A SQLAlchemy dialect based on psycopg2 and multicorn2.
"""

# pylint: disable=protected-access, abstract-method

from typing import Any, Optional, cast

from psycopg2 import extensions
from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from sqlalchemy.engine.url import URL
from sqlalchemy.pool.base import _ConnectionFairy

from shillelagh.adapters.base import Adapter
from shillelagh.backends.multicorn import db
from shillelagh.exceptions import ProgrammingError
from shillelagh.lib import find_adapter


class Multicorn2Dialect(PGDialect_psycopg2):
    """
    A SQLAlchemy dialect for Shillelagh based on psycopg2 and multicorn2.
    """

    name = "shillelagh"
    driver = "multicorn2"

    supports_statement_cache = True

    @classmethod
    def dbapi(cls):  # pylint: disable=method-hidden
        """
        Return the DB API module.
        """
        return db

    @classmethod
    def import_dbapi(cls):
        """
        New version of the ``dbapi`` method.
        """
        return db

    def __init__(
        self,
        adapters: Optional[list[str]] = None,
        adapter_kwargs: Optional[dict[str, dict[str, Any]]] = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self._adapters = adapters
        self._adapter_kwargs = adapter_kwargs or {}

    def create_connect_args(
        self,
        url: URL,
    ) -> tuple[list[Any], dict[str, Any]]:
        args, kwargs = super().create_connect_args(url)
        kwargs.update(
            {
                "adapters": self._adapters,
                "adapter_kwargs": self._adapter_kwargs,
            },
        )
        return args, kwargs

    def has_table(
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
        except ProgrammingError:
            return bool(
                super().has_table(
                    connection,
                    table_name,
                    schema,
                    **kwargs,
                ),
            )
        return True


def get_adapter_for_table_name(
    connection: _ConnectionFairy,
    table_name: str,
) -> Adapter:
    """
    Return an adapter associated with a connection.

    This function instantiates the adapter responsible for a given table name,
    using the connection to properly pass any adapter kwargs.
    """
    raw_connection = cast(extensions.connection, connection.engine.raw_connection())
    cursor = raw_connection.cursor()
    adapter, args, kwargs = find_adapter(
        table_name,
        cursor._adapter_kwargs,
        cursor._adapters,
    )
    return adapter(*args, **kwargs)
