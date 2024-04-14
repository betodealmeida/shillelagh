# pylint: disable=abstract-method
"""
A dialect that only connects to GSheets.

This dialect was implemented to replace the ``gsheetsdb`` library.
"""

import logging
import urllib.parse
from datetime import timedelta
from operator import itemgetter
from typing import Any, Dict, List, Optional, Tuple, cast

import requests
from google.auth.transport.requests import AuthorizedSession
from sqlalchemy.engine.url import URL
from sqlalchemy.pool.base import _ConnectionFairy
from typing_extensions import TypedDict

from shillelagh.adapters.api.gsheets.lib import get_credentials
from shillelagh.backends.apsw.dialects.base import APSWDialect
from shillelagh.exceptions import ProgrammingError

_logger = logging.getLogger(__name__)


DEFAULT_TIMEOUT = timedelta(minutes=3)


class QueryType(TypedDict, total=False):
    """
    Types for parameters in the SQLAlchemy URI query.
    """

    access_token: str
    service_account_file: str
    subject: str
    app_default_credentials: bool


def extract_query(url: URL) -> QueryType:
    """
    Extract the query from the SQLAlchemy URL.
    """
    if url.query:
        parameters = dict(url.query)
    # there's a bug in how SQLAlchemy <1.4 handles URLs without hosts,
    # putting the query string as the host; handle that case here
    elif url.host and url.host.startswith("?"):
        parameters = dict(urllib.parse.parse_qsl(url.host[1:]))  # pragma: no cover
    else:
        parameters = {}

    if "app_default_credentials" in parameters:
        parameters["app_default_credentials"] = parameters[
            "app_default_credentials"
        ].lower() in {"1", "true"}

    return cast(QueryType, parameters)


class APSWGSheetsDialect(APSWDialect):
    """
    Drop-in replacement for gsheetsdb.

    This dialect loads only the "gsheetsapi" adapter. To use it:

        >>> from sqlalchemy.engine import create_engine
        >>> engine = create_engine("gsheets://")

    """

    # This is supported in ``SQLiteDialect``, and equally supported here. See
    # https://docs.sqlalchemy.org/en/14/core/connections.html#caching-for-third-party-dialects
    # for more context.
    supports_statement_cache = True

    name = "gsheets"

    def __init__(  # pylint: disable=too-many-arguments
        self,
        access_token: Optional[str] = None,
        service_account_file: Optional[str] = None,
        service_account_info: Optional[Dict[str, Any]] = None,
        subject: Optional[str] = None,
        catalog: Optional[Dict[str, str]] = None,
        list_all_sheets: bool = False,
        app_default_credentials: bool = False,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)

        self.access_token = access_token
        self.service_account_file = service_account_file
        self.service_account_info = service_account_info
        self.subject = subject
        self.catalog = catalog or {}
        self.list_all_sheets = list_all_sheets
        self.app_default_credentials = app_default_credentials

    def create_connect_args(self, url: URL) -> Tuple[Tuple[()], Dict[str, Any]]:
        adapter_kwargs: Dict[str, Any] = {
            "access_token": self.access_token,
            "service_account_file": self.service_account_file,
            "service_account_info": self.service_account_info,
            "subject": self.subject,
            "catalog": self.catalog,
            "app_default_credentials": self.app_default_credentials,
        }
        # parameters can be overridden via the query in the URL
        adapter_kwargs.update(extract_query(url))

        return (), {
            "path": ":memory:",
            "adapters": ["gsheetsapi"],
            "adapter_kwargs": {"gsheetsapi": adapter_kwargs},
            "safe": True,
            "isolation_level": self.isolation_level,
        }

    def do_ping(self, dbapi_connection: _ConnectionFairy) -> bool:
        """
        Return Google Sheets API status.
        """
        response = requests.get(
            "https://www.google.com/appsstatus/dashboard/incidents.json",
            timeout=DEFAULT_TIMEOUT.total_seconds(),
        )
        payload = response.json()

        updates = [
            update for update in payload if update["service_name"] == "Google Sheets"
        ]
        if not updates:
            return True

        updates.sort(key=itemgetter("modified"), reverse=True)
        latest_update = updates[0]
        status: str = latest_update["most_recent_update"]["status"]

        if status in {"AVAILABLE", "SERVICE_DISRUPTION"}:
            return True

        # in case we don't understand the status, return True to be conservative
        return status != "SERVICE_OUTAGE"

    def get_table_names(  # pylint: disable=unused-argument
        self,
        connection: _ConnectionFairy,
        schema: Optional[str] = None,
        sqlite_include_internal: bool = False,
        **kwargs: Any,
    ) -> List[str]:
        """
        Return a list of table names.

        This will query for the authenticated user's spreadsheets, and return
        the URL of each sheet in all the spreadsheets. It's also possible to
        specify a "catalog" of URLs, which are also returned using their short
        names.
        """
        table_names = list(self.catalog.keys())

        query = extract_query(connection.url) if hasattr(connection, "url") else {}
        credentials = get_credentials(
            query.get("access_token", self.access_token),
            query.get("service_account_file", self.service_account_file),
            self.service_account_info,
            query.get("subject", self.subject),
            query.get("app_default_credentials", self.app_default_credentials),
        )
        if not (credentials and self.list_all_sheets):
            return table_names

        session = AuthorizedSession(credentials)

        spreadsheet_ids = get_spreadsheet_ids(session)
        for spreadsheet_id in spreadsheet_ids:
            table_names.extend(get_sheet_urls(spreadsheet_id, session))

        return table_names


def get_spreadsheet_ids(session: AuthorizedSession) -> List[str]:
    """
    Return the ID of all spreadsheets that the user has access to.
    """
    url = (
        "https://www.googleapis.com/drive/v3/files?"
        "q=mimeType='application/vnd.google-apps.spreadsheet'"
    )
    _logger.info("GET %s", url)
    response = session.get(url)
    payload = response.json()
    _logger.debug(payload)
    if "error" in payload:
        raise ProgrammingError(payload["error"]["message"])

    return [file["id"] for file in payload["files"]]


def get_sheet_urls(spreadsheet_id: str, session: AuthorizedSession) -> List[str]:
    """
    Return the URL for all sheets in a given spreadsheet.
    """
    response = session.get(
        "https://sheets.googleapis.com/v4/spreadsheets/"
        f"{spreadsheet_id}?includeGridData=false",
    )
    payload = response.json()
    if "error" in payload:
        _logger.warning(
            "Error loading sheets from file: %s",
            payload["error"]["message"],
        )
        return []

    sheet_urls: List[str] = []
    sheets = payload["sheets"]
    for sheet in sheets:
        sheet_id = sheet["properties"]["sheetId"]
        sheet_urls.append(
            "https://docs.google.com/spreadsheets/d/"
            f"{spreadsheet_id}/edit#gid={sheet_id}",
        )

    return sheet_urls
