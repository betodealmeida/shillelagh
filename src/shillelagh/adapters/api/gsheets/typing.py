"""Custom types for the GSheets adapter."""
from typing import Any, List

from typing_extensions import Literal, TypedDict


class UrlArgs(TypedDict, total=False):
    """
    URL arguments for a sheet.

    The sheet will have either a "gid" (sheet ID) or a sheet name
    under "sheet". Optionally, it can also have the number of
    header rows.
    """

    # optional
    headers: int

    # one of these will be present
    gid: int
    sheet: str


class QueryResultsColumn(TypedDict, total=False):
    """
    Metadata describing a column from the Google Chart API.

    An example::

        {"id": "A", "label": "country", "type": "string"}
        {"id": "B", "label": "cnt", "type": "number", "pattern": "General"}

    """

    id: str
    label: str
    type: str
    pattern: str  # optional


class QueryResultsCell(TypedDict, total=False):
    """
    A single cell from the Google Chart API.

    An example::

        {"v": 1.0, "f": 1 }
        {"v": "BR"}

    """

    v: Any
    f: str  # optional


class QueryResultsRow(TypedDict):
    """
    A row of results from the Google Chart API.

    An example::

        {
            "c": [{"v": "BR"}, {"v": 1.0, "f": "1"}]
        }

    """

    c: List[QueryResultsCell]


class QueryResultsTable(TypedDict):
    """
    A table with results from the Google Chart API.

    An example::

        {
            "cols": [
                {"id": "A", "label": "country", "type": "string"},
                {"id": "B", "label": "cnt", "type": "number", "pattern": "General"},
            ],
            "rows": [{"c": [{"v": "BR"}, {"v": 1.0, "f": "1"}]}],
            "parsedNumHeaders": 0,
        }

    """

    cols: List[QueryResultsColumn]
    rows: List[QueryResultsRow]
    parsedNumHeaders: int


class QueryResultsError(TypedDict):
    """
    Query errors from the Google Chart API.

    The API returns a list of errors like this::

        {
            "reason": "invalid_query",
            "message": "INVALID_QUERY",
            "detailed_message": "Invalid query: NO_COLUMN: C",
        }

    """

    reason: str
    message: str
    detailed_message: str


class QueryResults(TypedDict, total=False):
    """
    Query results from the Google Chart API.

    Successful query::

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

    Failed::

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
