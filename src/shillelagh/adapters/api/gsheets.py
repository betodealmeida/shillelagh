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

from google.auth.transport.requests import AuthorizedSession
from google.oauth2.service_account import Credentials
from requests import Session
from typing_extensions import Literal
from typing_extensions import TypedDict
from shillelagh.adapters.base import Adapter
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
from shillelagh.filters import Impossible
from shillelagh.filters import Range
from shillelagh.types import Row

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


def quote(value: Any) -> str:
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        escaped_value = value.replace("'", "''")
        return f"'{escaped_value}'"
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return f"'{value.isoformat()}'"

    raise Exception(f"Can't quote value: {value}")


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


def parse_datetime(value: str) -> str:
    """Parse a string like 'Date(2018,0,1,0,0,0)'."""
    args = [int(number) for number in value[len("Date(") : -1].split(",")]
    args[1] += 1  # month is zero indexed in the response
    return datetime.datetime(*args, tzinfo=datetime.timezone.utc).isoformat()


def parse_date(value: str) -> str:
    """Parse a string like 'Date(2018,0,1)'."""
    args = [int(number) for number in value[len("Date(") : -1].split(",")]
    args[1] += 1  # month is zero indexed in the response
    return datetime.date(*args).isoformat()


def parse_timeofday(values: List[int]) -> str:
    """Parse time of day as returned from the API."""
    return datetime.time(*values, tzinfo=datetime.timezone.utc).isoformat()


converters = {
    "string": lambda v: v,
    "number": lambda v: v,
    "boolean": bool,
    "date": parse_date,
    "datetime": parse_datetime,
    "timeofday": parse_timeofday,
}


def convert_rows(
    cols: List[QueryResultsColumn], rows: List[QueryResultsRow]
) -> Iterator[List[Any]]:
    """Convert custom Google sheets types."""
    for row in rows:
        values = []
        for i, col in enumerate(row["c"]):
            if i < len(cols):
                converter = converters[cols[i]["type"]]
                values.append(converter(col["v"]) if col else None)
        yield values


class GSheetsAPI(Adapter):
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
        service_account_info: Optional[Dict[str, Any]] = None,
        subject: Optional[str] = None,
    ):
        self.url = get_url(uri)
        self.credentials = (
            Credentials.from_service_account_info(
                service_account_info,
                scopes=SCOPES,
                subject=subject,
            )
            if service_account_info
            else None
        )
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
        results = self._run_query("SELECT * LIMIT 0")

        # map between column letter (A, B, etc.) to column name
        self._column_map = {col["label"]: col["id"] for col in results["table"]["cols"]}

        self.columns = {
            col["label"]: get_field(col) for col in results["table"]["cols"]
        }

    def get_columns(self) -> Dict[str, Field]:
        return self.columns

    def get_data(self, bounds: Dict[str, Filter]) -> Iterator[Row]:
        sql = "SELECT *"

        conditions = []
        for column_name, filter_ in bounds.items():
            id_ = self._column_map[column_name]
            if isinstance(filter_, Impossible):
                conditions.append("1 = 0")
            elif isinstance(filter_, Equal):
                conditions.append(f"{id_} = {quote(filter_.value)}")
            elif isinstance(filter_, Range):
                if filter_.start:
                    op = ">=" if filter_.include_start else ">"
                    conditions.append(f"{id_} {op} {quote(filter_.start)}")
                if filter_.end:
                    op = "<=" if filter_.include_end else "<"
                    conditions.append(f"{id_} {op} {quote(filter_.end)}")
        if conditions:
            sql = f"{sql} WHERE {' AND '.join(conditions)}"

        results = self._run_query(sql)
        cols = results["table"]["cols"]
        rows = convert_rows(cols, results["table"]["rows"])

        column_names = [col["label"] for col in cols]
        for i, row in enumerate(rows):
            data = dict(zip(column_names, row))
            data["rowid"] = i
            yield data
