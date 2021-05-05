import json
from typing import Any
from typing import cast
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import apsw
import sqlalchemy.types
from google.auth.transport.requests import AuthorizedSession
from shillelagh.adapters.api.gsheets import get_credentials
from shillelagh.adapters.base import Adapter
from shillelagh.backends.apsw import db
from shillelagh.backends.apsw.vt import VTTable
from shillelagh.exceptions import ProgrammingError
from sqlalchemy import exc
from sqlalchemy.dialects.sqlite.base import SQLiteDialect
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.pool.base import _ConnectionFairy
from sqlalchemy.sql.visitors import VisitableType
from typing_extensions import TypedDict


class SQLAlchemyColumn(TypedDict):
    name: str
    type: VisitableType
    nullable: bool
    default: Optional[str]
    autoincrement: str
    primary_key: int


class APSWDialect(SQLiteDialect):
    name = "shillelagh"
    driver = "apsw"

    @classmethod
    def dbapi(cls):
        return db

    def __init__(
        self,
        adapters: Optional[List[str]] = None,
        adapter_args: Optional[Dict[str, Any]] = None,
        safe: bool = False,
        *args: Any,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)
        self._adapters = adapters
        self._adapter_args = adapter_args
        self._safe = safe

    def create_connect_args(
        self,
        url: URL,
    ) -> Tuple[
        Tuple[str, Optional[List[str]], Optional[Dict[str, Any]], bool, Optional[str]],
        Dict[str, Any],
    ]:
        path = str(url.database) if url.database else ":memory:"
        return (
            (
                path,
                self._adapters,
                self._adapter_args,
                self._safe,
                self.isolation_level,
            ),
            {},
        )

    def do_ping(self, dbapi_connection: _ConnectionFairy) -> bool:
        return True

    # needed for SQLAlchemy
    def _get_table_sql(
        self,
        connection: _ConnectionFairy,
        table_name: str,
        schema: Optional[str] = None,
        **kwargs: Any,
    ) -> str:
        adapter = self._get_adapter_for_table_name(connection, table_name)
        table = VTTable(adapter)
        return table.get_create_table(table_name)

    def get_columns(
        self,
        connection: _ConnectionFairy,
        table_name: str,
        schema: Optional[str] = None,
        **kwargs: Any,
    ) -> List[SQLAlchemyColumn]:
        adapter = self._get_adapter_for_table_name(connection, table_name)
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

    def _get_adapter_for_table_name(
        self,
        connection: _ConnectionFairy,
        table_name: str,
    ) -> Adapter:
        raw_connection = cast(db.Connection, connection.engine.raw_connection())
        for adapter in raw_connection._adapters:
            if adapter.supports(table_name):
                break
        else:
            raise ProgrammingError(f"Unsupported table: {table_name}")

        uri_args = adapter.parse_uri(table_name)
        adapter_args = raw_connection._adapter_args.get(adapter.__name__.lower(), ())
        return adapter(*uri_args, *adapter_args)


class APSWGSheetsDialect(APSWDialect):
    """Drop-in replacement for gsheetsdb."""

    name = "gsheets"

    def __init__(
        self,
        access_token: Optional[str] = None,
        service_account_file: Optional[str] = None,
        service_account_info: Optional[Dict[str, Any]] = None,
        subject: Optional[str] = None,
        *args: Any,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)

        self.access_token = access_token
        self.service_account_file = service_account_file
        self.service_account_info = service_account_info
        self.subject = subject

    def create_connect_args(
        self,
        url: URL,
    ) -> Tuple[
        Tuple[str, Optional[List[str]], Optional[Dict[str, Any]], bool, Optional[str]],
        Dict[str, Any],
    ]:
        adapter_args: Dict[str, Any] = {
            "gsheetsapi": (
                self.access_token,
                self.service_account_file,
                self.service_account_info,
                self.subject,
            ),
        }

        return (
            ":memory:",
            ["gsheetsapi"],
            adapter_args,
            True,
            self.isolation_level,
        ), {}

    def get_schema_names(
        self, connection: _ConnectionFairy, **kwargs: Any
    ) -> List[str]:
        return []

    def get_table_names(
        self, connection: _ConnectionFairy, schema: str = None, **kwargs: Any
    ) -> List[str]:
        credentials = get_credentials(
            self.access_token,
            self.service_account_file,
            self.service_account_info,
            self.subject,
        )
        if not credentials:
            return []

        session = AuthorizedSession(credentials)
        response = session.get(
            "https://spreadsheets.google.com/feeds/spreadsheets/private/full",
        )
        import xml.etree.ElementTree as ET
        import urllib.parse

        ns = "{http://www.w3.org/2005/Atom}"
        root = ET.fromstring(response.text)
        worksheet_feeds = [
            entry.attrib["href"]
            for entry in root.findall(
                f"*/{ns}link"
                "[@rel='http://schemas.google.com/spreadsheets/2006#worksheetsfeed']",
            )
        ]

        tables = []
        for worksheet_feed in worksheet_feeds:
            response = session.get(worksheet_feed)
            try:
                root = ET.fromstring(response.text)
            except ET.ParseError:
                continue
            links = root.findall(
                f"*/{ns}link"
                "[@rel='http://schemas.google.com/visualization/2008#visualizationApi']",
            )
            for link in links:
                href = link.attrib["href"]
                parsed = urllib.parse.urlparse(href)
                params = urllib.parse.parse_qs(parsed.query)
                path = parsed.path.replace("gviz/tq", "edit")
                gid = params["gid"][-1]
                fragment = f"gid={gid}"
                url = urllib.parse.urlunparse(
                    (
                        parsed.scheme,
                        parsed.netloc,
                        path,
                        "",
                        "",
                        fragment,
                    ),
                )
                tables.append(url)

        return tables


class APSWSafeDialect(APSWDialect):
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
        self._safe = True

    def create_connect_args(
        self,
        url: URL,
    ) -> Tuple[
        Tuple[str, Optional[List[str]], Optional[Dict[str, Any]], bool, Optional[str]],
        Dict[str, Any],
    ]:
        return (
            (
                ":memory:",
                self._adapters,
                self._adapter_args,
                True,
                self.isolation_level,
            ),
            {},
        )
