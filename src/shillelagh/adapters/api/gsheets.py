import datetime
import json
import urllib.parse
from typing import Any
from typing import cast
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple
from typing import Type

import google.oauth2.credentials
import google.oauth2.service_account
from google.auth.credentials import Credentials
from google.auth.transport.requests import AuthorizedSession
from requests import Session
from shillelagh.adapters.base import Adapter
from shillelagh.exceptions import ImpossibleFilterError
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Boolean
from shillelagh.fields import Date
from shillelagh.fields import DateTime
from shillelagh.fields import Field
from shillelagh.fields import Float
from shillelagh.fields import Order
from shillelagh.fields import String
from shillelagh.fields import Time
from shillelagh.filters import Equal
from shillelagh.filters import Filter
from shillelagh.filters import Range
from shillelagh.lib import build_sql
from shillelagh.types import RequestedOrder
from shillelagh.types import Row
from typing_extensions import Literal
from typing_extensions import TypedDict

# Google API scopes for authentication
SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
]

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


class QueryResultsError(TypedDict):
    reason: str
    message: str
    detailed_message: str


class QueryResults(TypedDict, total=False):
    """
    Query results from the Google API.

    Successful query:

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

    Failed:

    {
        "version": "0.6",
        "reqId": "0",
        "status": "error",
        "errors": [
            {
                "reason": "invalid_query",
                "message": "INVALID_QUERY",
                "detailed_message": "Invalid query: NO_COLUMN: C",
            }
        ],
    }
    """

    version: str
    reqId: str
    status: Literal["ok", "error"]
    sig: str
    table: QueryResultsTable
    errors: List[QueryResultsError]


class GSheetsDateTime(DateTime):
    @staticmethod
    def parse(value: Optional[str]) -> Optional[datetime.datetime]:
        if value is None:
            return None

        args = [int(number) for number in value[len("Date(") : -1].split(",")]
        args[1] += 1  # month is zero indexed in the response
        return datetime.datetime(*args, tzinfo=datetime.timezone.utc)  # type: ignore

    @staticmethod
    def quote(value: Any) -> str:
        return f"datetime '{value}'"


class GSheetsDate(Date):
    @staticmethod
    def parse(value: Optional[str]) -> Optional[datetime.date]:
        """Parse a string like 'Date(2018,0,1)'."""
        if value is None:
            return None

        args = [int(number) for number in value[len("Date(") : -1].split(",")]
        args[1] += 1  # month is zero indexed in the response
        return datetime.date(*args)

    @staticmethod
    def quote(value: Any) -> str:
        return f"date '{value}'"


class GSheetsTime(Time):
    @staticmethod
    def parse(values: Optional[List[int]]) -> Optional[datetime.time]:
        """Parse time of day as returned from the API."""
        if values is None:
            return None

        return datetime.time(*values, tzinfo=datetime.timezone.utc)  # type: ignore

    @staticmethod
    def quote(value: Any) -> str:
        return f"timeofday '{value}'"


class GSheetsBoolean(Boolean):
    @staticmethod
    def quote(value: bool) -> str:
        return "true" if value else "false"


type_map: Dict[str, Tuple[Type[Field], List[Type[Filter]]]] = {
    "string": (String, [Equal]),
    "number": (Float, [Range]),
    "boolean": (GSheetsBoolean, [Equal]),
    "date": (GSheetsDate, [Range]),
    "datetime": (GSheetsDateTime, [Range]),
    "timeofday": (GSheetsTime, [Range]),
}


def get_field(col: QueryResultsColumn) -> Field:
    class_, filters = type_map.get(col["type"], (String, [Equal]))
    return class_(
        filters=filters,
        order=Order.ANY,
        exact=True,
    )


def format_error_message(errors: List[QueryResultsError]) -> str:
    return "\n\n".join(error["detailed_message"] for error in errors)


def get_url(
    uri: str,
    headers: int = 0,
    gid: int = 0,
    sheet: Optional[str] = None,
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
        (parts.scheme, parts.netloc, path, None, params, None),
    )


def get_credentials(
    access_token: Optional[str],
    service_account_file: Optional[str],
    service_account_info: Optional[Dict[str, Any]],
    subject: Optional[str],
) -> Optional[Credentials]:
    if access_token:
        return google.oauth2.credentials.Credentials(access_token)

    if service_account_file:
        return google.oauth2.service_account.Credentials.from_service_account_file(
            service_account_file,
            scopes=SCOPES,
            subject=subject,
        )

    if service_account_info:
        return google.oauth2.service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=SCOPES,
            subject=subject,
        )

    return None


class GSheetsAPI(Adapter):

    safe = True

    @staticmethod
    def supports(uri: str) -> bool:
        parsed = urllib.parse.urlparse(uri)
        return parsed.netloc == "docs.google.com" and parsed.path.startswith(
            "/spreadsheets/",
        )

    @staticmethod
    def parse_uri(uri: str) -> Tuple[str]:
        return (uri,)

    def __init__(
        self,
        uri: str,
        access_token: Optional[str] = None,
        service_account_file: Optional[str] = None,
        service_account_info: Optional[Dict[str, Any]] = None,
        subject: Optional[str] = None,
    ):
        self.url = get_url(uri)
        self.credentials = get_credentials(
            access_token,
            service_account_file,
            service_account_info,
            subject,
        )

        self._offset = 0
        self._set_columns()

    def _get_session(self) -> Session:
        return cast(
            Session,
            AuthorizedSession(self.credentials) if self.credentials else Session(),
        )

    def _run_query(self, sql: str) -> QueryResults:
        quoted_sql = urllib.parse.quote(sql, safe="/()")
        url = f"{self.url}&tq={quoted_sql}"
        headers = {"X-DataSource-Auth": "true"}

        session = self._get_session()
        response = session.get(url, headers=headers)
        if response.encoding is None:
            response.encoding = "utf-8"

        if response.status_code != 200:
            raise ProgrammingError(response.text)

        if response.text.startswith(JSON_PAYLOAD_PREFIX):
            result = json.loads(response.text[len(JSON_PAYLOAD_PREFIX) :])
        else:
            try:
                result = response.json()
            except json.decoder.JSONDecodeError:
                raise ProgrammingError(
                    "Response from Google is not valid JSON. Please verify that you "
                    "have the proper credentials to access the spreadsheet.",
                )

        if result["status"] == "error":
            raise ProgrammingError(format_error_message(result["errors"]))

        return cast(QueryResults, result)

    def _set_columns(self) -> None:
        results = self._run_query("SELECT * LIMIT 1")
        cols = results["table"]["cols"]
        rows = results["table"]["rows"]

        # if the columns have no labels, use the first row as the labels if
        # it exists; otherwise use just the column letters
        if all(col["label"] == "" for col in cols):
            if rows:
                self._offset = 1
                for col, cell in zip(cols, rows[0]["c"]):
                    col["label"] = cell["v"]
            else:
                for col in cols:
                    col["label"] = col["id"]

        self._column_map = {col["label"].strip(): col["id"] for col in cols}
        self.columns = {
            col["label"].strip(): get_field(col) for col in cols if col["label"].strip()
        }

    def get_columns(self) -> Dict[str, Field]:
        return self.columns

    def get_data(
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
    ) -> Iterator[Row]:
        try:
            sql = build_sql(self.columns, bounds, order, self._column_map, self._offset)
        except ImpossibleFilterError:
            return

        payload = self._run_query(sql)
        rows = [
            {
                column_name: cell["v"] if cell else None
                for column_name, cell in zip(self.columns, row["c"])
            }
            for row in payload["table"]["rows"]
        ]
        for i, row in enumerate(rows):
            row["rowid"] = i
            yield row
