# pylint: disable=abstract-method
"""
A dialect that only connects to GSheets.

This dialect was implemented to replace the ``gsheetsdb`` library.
"""
import logging
import urllib.parse
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from google.auth.transport.requests import AuthorizedSession
from sqlalchemy.engine.url import URL
from sqlalchemy.pool.base import _ConnectionFairy

from shillelagh.adapters.api.gsheets.lib import get_credentials
from shillelagh.backends.apsw.dialects.base import APSWDialect
from shillelagh.exceptions import ProgrammingError

_logger = logging.getLogger(__name__)


def extract_query(url: URL) -> Dict[str, str]:
    """
    Extract the query from the SQLAlchemy URL.
    """
    if url.query:
        return dict(url.query)

    # there's a bug in how SQLAlchemy <1.4 handles URLs without hosts,
    # putting the query string as the host; handle that case here
    if url.host and url.host.startswith("?"):
        return dict(urllib.parse.parse_qsl(url.host[1:]))  # pragma: no cover

    return {}


class APSWGSheetsDialect(APSWDialect):
    """
    Drop-in replacement for gsheetsdb.

    This dialect loads only the "gsheetsapi" adapter. To use it:

        >>> from sqlalchemy.engine import create_engine
        >>> engine = create_engine("gsheets://")

    """

    name = "gsheets"

    def __init__(  # pylint: disable=too-many-arguments
        self,
        access_token: Optional[str] = None,
        service_account_file: Optional[str] = None,
        service_account_info: Optional[Dict[str, Any]] = None,
        subject: Optional[str] = None,
        catalog: Optional[Dict[str, str]] = None,
        list_all_sheets: bool = False,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)

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
        # parameters can be overriden via the query in the URL
        adapter_kwargs.update(extract_query(url))

        return (), {
            "path": ":memory:",
            "adapters": ["gsheetsapi"],
            "adapter_kwargs": {"gsheetsapi": adapter_kwargs},
            "safe": True,
            "isolation_level": self.isolation_level,
        }

    def get_table_names(  # pylint: disable=unused-argument
        self, connection: _ConnectionFairy, schema: str = None, **kwargs: Any
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
