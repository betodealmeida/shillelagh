"""Helper functions for the GSheets adapter."""
import datetime
import itertools
import string
import urllib.parse
from typing import Any, Dict, Iterator, List, Optional, Tuple, Type

import google.auth
import google.oauth2.credentials
import google.oauth2.service_account
from google.auth.credentials import Credentials

from shillelagh.adapters.api.gsheets.fields import (
    GSheetsBoolean,
    GSheetsDate,
    GSheetsDateTime,
    GSheetsDuration,
    GSheetsField,
    GSheetsNumber,
    GSheetsString,
    GSheetsTime,
)
from shillelagh.adapters.api.gsheets.parsing.date import infer_column_type
from shillelagh.adapters.api.gsheets.types import SyncMode
from shillelagh.adapters.api.gsheets.typing import (
    QueryResultsCell,
    QueryResultsColumn,
    QueryResultsError,
    UrlArgs,
)
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Field, Order
from shillelagh.filters import Equal, Filter, IsNotNull, IsNull, Like, NotEqual, Range
from shillelagh.typing import Row

# Google API scopes for authentication
SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://spreadsheets.google.com/feeds",
]


def get_field(
    col: QueryResultsColumn,
    timezone: Optional[datetime.tzinfo] = None,
) -> Field:
    """
    Return a Shillelagh ``Field`` from a Google Chart API results column.
    """
    # GSheets returns type ``datetime`` for timestamps, but also for time of day and
    # durations. We need to tokenize the pattern in order to figure out the correct type.
    if col["type"] == "datetime" and "pattern" in col:
        col["type"] = infer_column_type(col["pattern"])

    type_map: Dict[str, Tuple[Type[GSheetsField], List[Type[Filter]]]] = {
        "string": (GSheetsString, [Range, Equal, NotEqual, Like, IsNull, IsNotNull]),
        "number": (GSheetsNumber, [Range, Equal, NotEqual, IsNull, IsNotNull]),
        "boolean": (GSheetsBoolean, [Equal, NotEqual, IsNull, IsNotNull]),
        "date": (GSheetsDate, [Range, Equal, NotEqual, IsNull, IsNotNull]),
        "datetime": (GSheetsDateTime, [Range, Equal, NotEqual, IsNull, IsNotNull]),
        "timeofday": (GSheetsTime, [Range, Equal, NotEqual, IsNull, IsNotNull]),
        "duration": (GSheetsDuration, [Range, Equal, NotEqual, IsNull, IsNotNull]),
    }
    class_, filters = type_map.get(
        col["type"],
        (GSheetsString, [Range, Equal, NotEqual, Like, IsNull, IsNotNull]),
    )
    return class_(
        filters=filters,
        order=Order.ANY,
        exact=True,
        pattern=col.get("pattern"),
        timezone=timezone,
    )


def format_error_message(errors: List[QueryResultsError]) -> str:
    """
    Return an error message from a Google Chart API error response.
    """
    return "\n\n".join(error["detailed_message"] for error in errors)


def get_url(
    uri: str,
    headers: int = 0,
    gid: int = 0,
    sheet: Optional[str] = None,
) -> str:
    """
    Return the Google Chart API URL given the spreadsheet URL.
    """
    parts = urllib.parse.urlparse(uri)

    # strip /edit
    path = parts.path[: -len("/edit")] if parts.path.endswith("/edit") else parts.path

    # add the gviz endpoint
    path = "/".join((path.rstrip("/"), "gviz/tq"))

    query_string = urllib.parse.parse_qs(parts.query)
    if "headers" in query_string:
        headers = int(query_string["headers"][-1])
    if "gid" in query_string:
        gid = int(query_string["gid"][-1])
    if "sheet" in query_string:
        sheet = query_string["sheet"][-1]

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


def get_sync_mode(uri: str) -> Optional[SyncMode]:
    """
    Extract the synchronization mode from the sheet URI.

    User can specify a custom synchronization mode by manually adding the
    ``sync_mode`` query argument. The mode can be specified using the
    enum names (lower or uppercase) or their corresponding numbers. These
    are all equivalent:

        ?sync_mode=BATCH
        ?sync_mode=batch
        ?sync_mode=3

    """
    parts = urllib.parse.urlparse(uri)
    query_string = urllib.parse.parse_qs(parts.query)
    if "sync_mode" not in query_string:
        return None

    parameter = query_string["sync_mode"][-1].upper()
    try:
        sync_mode = SyncMode[parameter]
    except KeyError:
        try:
            sync_mode = SyncMode(int(parameter))
        except ValueError as ex:
            raise ProgrammingError(f"Invalid sync mode: {parameter}") from ex

    return sync_mode


def gen_letters() -> Iterator[str]:
    """
    Generate column labels.

    This generator produces column labels for sheets: "A", "B", ..., "Z", "AA",
    "AB", etc.
    """
    letters = ["A"]
    index = 0
    while True:
        yield "".join(letters)

        index += 1
        if index == len(string.ascii_uppercase):
            letters[-1] = "A"
            letters.append("A")
            index = 0
        else:
            letters[-1] = string.ascii_uppercase[index]


def get_index_from_letters(letters: str) -> int:
    """
    Return the index of a given column label.

        >>> get_index_from_letters("A")
        0
        >>> get_index_from_letters("AA")
        26

    """
    base26 = reversed([string.ascii_uppercase.index(letter) + 1 for letter in letters])
    return int(
        sum(
            value * (len(string.ascii_uppercase) ** i) for i, value in enumerate(base26)
        )
        - 1,
    )


def get_values_from_row(row: Row, column_map: Dict[str, str]) -> List[Any]:
    """
    Convert a ``Row`` into a list of values.

    This takes into consideration empty columns. For example:

        >>> column_map = {"country": "A", "cnt": "C"}  # empty column B
        >>> row = {"country": "BR", "cnt": 10}
        >>> get_values_from_row(row, column_map)
        ['BR', '', 10]
    """
    n_cols = max(get_index_from_letters(val) for val in column_map.values()) + 1
    row = {column_map[k]: v for k, v in row.items() if k in column_map}
    return [row.get(column, "") for column in itertools.islice(gen_letters(), n_cols)]


def get_credentials(
    access_token: Optional[str] = None,
    service_account_file: Optional[str] = None,
    service_account_info: Optional[Dict[str, Any]] = None,
    subject: Optional[str] = None,
    app_default_credentials: Optional[bool] = False,
) -> Optional[Credentials]:
    """
    Return a set of credentials.

    The user can provide either an OAuth token directly, the location of a service
    account file, or the contents of the service account directly. When passing
    credentials from a service account the user can also specify a "subject", used
    to impersonate a given user. Application default credentials can also be used
    from the environment in lieu of directly specifying a token or service account
    information.
    """
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

    if app_default_credentials:
        return google.auth.default(scopes=SCOPES)[0]

    return None


def get_value_from_cell(cell: Optional[QueryResultsCell]) -> Any:
    """
    Return the value from cell.

    The Google Chart API returns many different values for cells, eg:

        {"v": "Date(2018,8,1,0,0,0)", "f": "9/1/2018 0:00:00"}
        {"v": "test"}
        {"v": 1.0, "f": "1"}
        {"v": True, "f": "TRUE"}
        None
        {"v": None}

    """
    if cell is None or cell.get("v") is None:
        return ""

    if "f" in cell:
        return cell["f"]

    return "" if cell.get("v") is None else cell["v"]
