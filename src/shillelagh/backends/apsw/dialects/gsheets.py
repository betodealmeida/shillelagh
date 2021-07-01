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
from shillelagh.adapters.api.gsheets import GSheetsAPI
from shillelagh.adapters.base import Adapter
from shillelagh.backends.apsw.dialects.base import APSWDialect
from shillelagh.exceptions import ProgrammingError
from sqlalchemy.engine.url import make_url
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
        catalog: Optional[Dict[str, str]] = None,
        list_all_sheets: bool = False,
        *args: Any,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)

        self.access_token = access_token
        self.service_account_file = service_account_file
        self.service_account_info = service_account_info
        self.subject = subject
        self.catalog = catalog or {}
        self.list_all_sheets = list_all_sheets

    def create_connect_args(
        self,
        url: URL,
    ) -> Tuple[Tuple[()], Dict[str, Any]]:
        adapter_kwargs: Dict[str, Any] = {
            "access_token": self.access_token,
            "service_account_file": self.service_account_file,
            "service_account_info": self.service_account_info,
            "subject": self.subject,
            "catalog": self.catalog,
        }
        # overtwrite parameters via query
        adapter_kwargs.update(extract_query(url))

        return (), {
            "path": ":memory:",
            "adapters": ["gsheetsapi"],
            "adapter_kwargs": {"gsheetsapi": adapter_kwargs},
            "safe": True,
            "isolation_level": self.isolation_level,
        }

    def get_table_names(
        self, connection: _ConnectionFairy, schema: str = None, **kwargs: Any
    ) -> List[str]:
        table_names = list(self.catalog.keys())

        query = extract_query(connection.url) if hasattr(connection, "url") else {}
        credentials = get_credentials(
            query.get("access_token", self.access_token),
            query.get("service_account_file", self.service_account_file),
            self.service_account_info,
            query.get("subject", self.subject),
        )
        if not (credentials and self.list_all_sheets):
            return table_names

        session = AuthorizedSession(credentials)

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
                table_names.append(
                    f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_id}",
                )

        return table_names
