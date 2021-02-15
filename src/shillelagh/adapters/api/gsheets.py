import json
import urllib.parse
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Type
from typing import TypedDict

from google.auth.transport.requests import AuthorizedSession
from google.oauth2.service_account import Credentials
from requests import Session

from shillelagh.adapters.base import Adapter
from shillelagh.fields import Field
from shillelagh.fields import Float, String, DateTime, Boolean, Date, Time, Order
from shillelagh.filters import Equal
from shillelagh.filters import Filter
from shillelagh.filters import Range

# Google API scopes for authentication
# https://developers.google.com/chart/interactive/docs/spreadsheets
SCOPES = ["https://spreadsheets.google.com/feeds"]

JSON_PAYLOAD_PREFIX = ")]}'\n"


class UrlArgs(TypedDict, total=False):
    headers: int
    gid: int
    sheet: str


class QueryResultsColumn(TypedDict, total=False):
    id: str
    label: str
    type: str
    pattern: str  # optional


class QueryResultsCell(TypedDict, total=False):
    v: Any
    f: str  # optional


class QueryResultsRow(TypedDict):
    c: List[QueryResultsCell]


class QueryResultsTable(TypedDict):
    cols: List[QueryResultsColumn]
    rows: List[QueryResultsRow]
    parsedNumHeaders: int


class QueryResults(TypedDict):
    """
    Query results from the Google API.

    {
        "version": "0.6",
        "reqId": "0",
        "status": "ok",
        "sig": "1453301915",
        "table": {
            "cols": [
                {"id": "A", "label": "country", "type": "string"},
                {"id": "B", "label": "cnt", "type": "number", "pattern": "General"},
            ],
            "rows": [{"c": [{"v": "BR"}, {"v": 1.0, "f": "1"}]}],
            "parsedNumHeaders": 0,
        },
    }
    """

    version: str
    reqId: str
    status: str
    sig: str
    table: QueryResultsTable


type_map: Dict[str, Tuple[Type[Field], List[Type[Filter]]]] = {
    "string": (String, [Equal]),
    "number": (Float, [Range]),
    "boolean": (Boolean, [Equal]),
    "date": (Date, [Range]),
    "datetime": (DateTime, [Range]),
    "timeofday": (Time, [Range]),
}


def get_field(col: QueryResultsColumn) -> Field:
    class_, filters = type_map.get(col["type"], (String, [Equal]))
    return class_(
        filters=filters,
        order=Order.NONE,
        exact=True,
    )


def get_url(
    uri: str, headers: int = 0, gid: int = 0, sheet: Optional[str] = None
) -> str:
    """Return API URL given the spreadsheet URL."""
    parts = urllib.parse.urlparse(uri)

    # strip /edit
    path = parts.path[: -len("/edit")] if parts.path.endswith("/edit") else parts.path

    # add the gviz endpoint
    path = "/".join((path.rstrip("/"), "gviz/tq"))

    qs = urllib.parse.parse_qs(parts.query)
    if "headers" in qs:
        headers = int(qs["headers"][-1])
    if "gid" in qs:
        gid = int(qs["gid"][-1])
    if "sheet" in qs:
        sheet = qs["sheet"][-1]

    if parts.fragment.startswith("gid="):
        gid = int(parts.fragment[len("gid=") :])

    args: UrlArgs = {}
    if headers > 0:
        args["headers"] = headers
    if sheet is not None:
        args["sheet"] = sheet
    else:
        args["gid"] = gid
    params = urllib.parse.urlencode(args)

    return urllib.parse.urlunparse(
        (parts.scheme, parts.netloc, path, None, params, None)
    )


class GSheetsAPI(Adapter):

    name = "gsheets"

    @staticmethod
    def supports(uri: str) -> bool:
        parsed = urllib.parse.urlparse(uri)
        return parsed.netloc == "docs.google.com" and parsed.path.startswith(
            "/spreadsheets/"
        )

    @staticmethod
    def parse_uri(uri: str) -> Tuple[str]:
        return (uri,)

    def __init__(
        self,
        uri: str,
        service_account_info: Optional[Dict[str, Any]] = None,
        subject: Optional[str] = None,
    ):
        self.url = get_url(uri)
        self.credentials = (
            Credentials.from_service_account_info(
                service_account_info, scopes=SCOPES, subject=subject
            )
            if service_account_info
            else None
        )
        self._set_columns()

    def _run_query(self, sql: str) -> QueryResults:
        quoted_sql = urllib.parse.quote(sql, safe="/()")
        url = f"{self.url}&tq={quoted_sql}"
        headers = {"X-DataSource-Auth": "true"}

        session = AuthorizedSession(self.credentials) if self.credentials else Session()

        response = session.get(url, headers=headers)
        if response.encoding is None:
            response.encoding = "utf-8"

        if response.status_code != 200:
            raise ProgrammingError(response.text)

        if response.text.startswith(JSON_PAYLOAD_PREFIX):
            result = json.loads(response.text[len(JSON_PAYLOAD_PREFIX) :])
        else:
            result = response.json()

        return result

    def _set_columns(self) -> None:
        results = self._run_query("SELECT * LIMIT 0")

        # map between column letter (A, B, etc.) to column name
        self._column_map = {col["label"]: col["id"] for col in results["table"]["cols"]}

        self.columns = {col["id"]: get_fiels(col) for col in results["table"]["cols"]}

    def get_columns(self) -> Dict[str, Field]:
        return self.columns
