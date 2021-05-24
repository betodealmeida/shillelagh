import logging
import urllib.parse
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from google.auth.credentials import Credentials
from google.auth.transport.requests import AuthorizedSession
from shillelagh.adapters.api.gsheets import get_credentials
from shillelagh.backends.apsw.dialects.base import APSWDialect
from shillelagh.exceptions import ProgrammingError
from sqlalchemy.engine.url import URL
from sqlalchemy.pool.base import _ConnectionFairy

_logger = logging.getLogger(__name__)


def extract_query(url: URL) -> Dict[str, str]:
    """
    Extract the query from the SQLAlchemy URL.

    There's a bug in how SQLAlchemy handles URLs without hosts:

        >>> from sqlalchemy.engine.url import make_url
        >>> url = make_url("gsheets://")
        >>> url.query["subject"] = "user@example.com"
        >>> url
        gsheets://?subject=user%40example.com
        >>> make_url(str(url)).query
        {}
        >>> make_url(str(url)).host
        '?subject=user%40example.com'

    """
    if url.query:
        return dict(url.query)
    if url.host and url.host.startswith("?"):
        return dict(urllib.parse.parse_qsl(url.host[1:]))
    return {}


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
        query = extract_query(url)
        adapter_args: Dict[str, Any] = {
            "gsheetsapi": (
                query.get("access_token", self.access_token),
                query.get("service_account_file", self.service_account_file),
                query.get("service_account_info", self.service_account_info),
                query.get("subject", self.subject),
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
        query = extract_query(connection.url) if hasattr(connection, "url") else {}
        credentials = get_credentials(
            query.get("access_token", self.access_token),
            query.get("service_account_file", self.service_account_file),
            self.service_account_info,
            query.get("subject", self.subject),
        )
        if not credentials:
            return []

        session = AuthorizedSession(credentials)

        tables = []
        response = session.get(
            "https://www.googleapis.com/drive/v3/files?q=mimeType='application/vnd.google-apps.spreadsheet'",
        )
        payload = response.json()
        if "error" in payload:
            raise ProgrammingError(payload["error"]["message"])

        files = payload["files"]
        for file in files:
            spreadsheet_id = file["id"]
            response = session.get(
                f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}?includeGridData=false",
            )
            payload = response.json()
            if "error" in payload:
                _logger.warning(
                    "Error loading sheets from file: %s",
                    payload["error"]["message"],
                )
                continue

            sheets = payload["sheets"]
            for sheet in sheets:
                sheet_id = sheet["properties"]["sheetId"]
                tables.append(
                    f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_id}",
                )

        return tables
