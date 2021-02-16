import json
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import apsw

import sqlalchemy.types
from sqlalchemy import exc
from sqlalchemy.dialects import sqlite
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import NoSuchTableError

from shillelagh.backends.apsw import db
from shillelagh.exceptions import ProgrammingError


class APSWDialect(sqlite.dialect):
    name = "shillelagh"
    driver = "apsw"

    @classmethod
    def dbapi(cls):
        return db

    def __init__(
        self,
        adapters: Optional[List[str]] = None,
        adapter_args: Optional[Dict[str, Any]] = None,
        *args: Any,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)
        self._adapters = adapters
        self._adapter_args = adapter_args

    def create_connect_args(
        self,
        url: URL,
    ) -> Tuple[
        Tuple[str, Optional[List[str]], Optional[Dict[str, Any]]],
        Dict[str, Any],
    ]:
        path = url.database or ":memory:"
        return ([path, self._adapters, self._adapter_args, self.isolation_level], {})

    def do_ping(self, dbapi_connection) -> bool:
        return True

    def get_columns(self, connection, table_name, schema=None, **kw):
        raw_connection = connection.engine.raw_connection()
        for adapter in raw_connection._adapters:
            if adapter.supports(table_name):
                break
        else:
            raise ProgrammingError(f"Unsupported table: {table_name}")

        args = adapter.parse_uri(table_name) + raw_connection._adapter_args.get(
            adapter.__name__.lower(), ()
        )
        columns = adapter(*args).get_columns()
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

    def _get_table_sql(self, connection, table_name, schema=None, **kw):
        # XXX
        return ""


class APSWGSheetsDialect(APSWDialect):
    """Drop-in replacement for gsheetsdb."""

    name = "gsheets"

    def __init__(
        self,
        service_account_file: Optional[str] = None,
        service_account_info: Optional[Dict[str, Any]] = None,
        subject: Optional[str] = None,
        *args: Any,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)

        self.service_account_info = service_account_info
        if service_account_file:
            with open(service_account_file) as fp:
                self.service_account_info = json.load(fp)
        self.subject = subject

    def create_connect_args(
        self,
        url: URL,
    ) -> Tuple[
        Tuple[str, Optional[List[str]], Optional[Dict[str, Any]]],
        Dict[str, Any],
    ]:
        path = url.database or ":memory:"

        adapter_args: Dict[str, Any] = {}
        if self.service_account_info:
            adapter_args["gsheetsapi"] = (self.service_account_info, self.subject)

        return (path, ["gsheetsapi"], adapter_args), {}
