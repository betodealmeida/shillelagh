# pylint: disable=fixme
"""
Google Sheets adapter.
"""
import datetime
import json
import logging
import urllib.parse
from typing import Any, Dict, Iterator, List, Optional, Tuple, cast

import dateutil.tz
from google.auth.transport.requests import AuthorizedSession
from requests import Session

from shillelagh.adapters.api.gsheets.lib import (
    format_error_message,
    gen_letters,
    get_credentials,
    get_field,
    get_index_from_letters,
    get_sync_mode,
    get_url,
    get_value_from_cell,
    get_values_from_row,
)
from shillelagh.adapters.api.gsheets.types import SyncMode
from shillelagh.adapters.api.gsheets.typing import QueryResults
from shillelagh.adapters.base import Adapter
from shillelagh.exceptions import (
    ImpossibleFilterError,
    InterfaceError,
    InternalError,
    ProgrammingError,
    UnauthenticatedError,
)
from shillelagh.fields import Field, Order
from shillelagh.filters import Filter
from shillelagh.lib import NetworkAPICostModel, apply_limit_and_offset, build_sql
from shillelagh.typing import RequestedOrder, Row

_logger = logging.getLogger(__name__)

JSON_PAYLOAD_PREFIX = ")]}'\n"

# based on very scientific tests ran in an RV using Starlink and 5G the cost for a query
# is 2882ms + 0.4212ms * number_of_rows :-P
AVERAGE_NUMBER_OF_ROWS = 1000
FIXED_COST = 2882
DOWNLOAD_COST = int(AVERAGE_NUMBER_OF_ROWS * 0.4212)


class GSheetsAPI(Adapter):  # pylint: disable=too-many-instance-attributes
    r"""
    A Google Sheets adapter.

    The adapter uses two different APIs. When only ``SELECT``\s are used the
    adapter uses the Google Chart API, which queries in a dialect of SQL.
    The Chart API is efficient because the data can be filterd and sorted
    on the backend before being retrieved.

    To handle DML -- ``INSERT``, ``UPDATE``, and ``DELETE`` -- the adapter
    will switch to the Google Sheets API. The Sheets API allows the
    spreadsheet to be modified, but requires the user to be authenticated,
    even when connecting to a public sheet.

    DML supports 3 different modes of synchronization. In ``BIDIRECTIONAL``
    mode the sheet is downloaded before every DML query, and changes are
    pushed immediately. This is very inneficient, since it requires
    downloading all the values before every modification, and should be
    used for small updates or when interactivity is required.

    In ``UNIDIRECTIONAL`` mode changes are still pushed immediately, but the
    sheet is downloaded only once. This mode is a good compromise, since
    the uploads are frequent but small, and the download that can be big
    (depending on the size of the sheet) happens only once.

    Finally, there's a ``BATCH`` mode, where the sheet is downloaded once,
    before the first DML operation, and all changes are uploaded at once
    when the adapter is closed. In this mode and in ``UNIDIRECTIONAL`` the
    data is stored locally, and filtered/sorted by the Shillelagh backend.
    """

    safe = True
    supports_limit = True
    supports_offset = True

    @staticmethod
    def supports(uri: str, fast: bool = True, **kwargs: Any) -> Optional[bool]:
        catalog = kwargs.get("catalog", {})
        if uri in catalog:
            uri = catalog[uri]

        parsed = urllib.parse.urlparse(uri)
        return parsed.netloc == "docs.google.com" and parsed.path.startswith(
            "/spreadsheets/",
        )

    @staticmethod
    def parse_uri(uri: str) -> Tuple[str]:
        return (uri,)

    def __init__(  # pylint: disable=too-many-arguments
        self,
        uri: str,
        access_token: Optional[str] = None,
        service_account_file: Optional[str] = None,
        service_account_info: Optional[Dict[str, Any]] = None,
        subject: Optional[str] = None,
        catalog: Optional[Dict[str, str]] = None,
        app_default_credentials: bool = False,
    ):
        super().__init__()
        if catalog and uri in catalog:
            uri = catalog[uri]

        self.url = get_url(uri)
        self.credentials = get_credentials(
            access_token,
            service_account_file,
            service_account_info,
            subject,
            app_default_credentials,
        )

        # Local data. When using DML we switch to the Google Sheets API,
        # keeping a local copy of the spreadsheets data so that we can
        # (1) find rows being updated/delete and (2) work on a local
        # dataset when using a mode other than ``BIDIRECTIONAL``.
        self._sync_mode = get_sync_mode(uri) or SyncMode.BIDIRECTIONAL
        self._values: Optional[List[List[Any]]] = None
        self._original_rows = 0
        self.modified = False

        # Extra metadata. Some of this metadata (sheet name and timezone)
        # can only be fetched if the user is authenticated -- that's OK,
        # since they're only used for DML, which requires authentication.
        self._spreadsheet_id: Optional[str] = None
        self._sheet_id: Optional[int] = None
        self._sheet_name: Optional[str] = None
        self._timezone: Optional[datetime.tzinfo] = None
        self._set_metadata(uri)

        # Determine columns in the sheet.
        self.columns: Dict[str, Field] = {}
        self._set_columns(uri)

        # Store row ids for DML. When the first DML command is issued
        # we switch from the Chart API (read-only) to the Sheets API
        # (read-write). The problem is that a ``SELECT`` is almost always
        # issued before a ``DELETE`` or ``UPDATE``, to get the IDs of the
        # rows. Because row IDs are generated on the fly, we need to
        # store the association between row ID and the values in the row
        # so we can ``DELETE`` or ``UPDATE`` them later.
        self._row_ids: Dict[int, Row] = {}

    def _set_metadata(self, uri: str) -> None:
        """
        Get spreadsheet ID, sheet ID, sheet name, and timezone.

        Sheet name and timezone can only be retrieved if the user is authenticated.
        """
        parts = urllib.parse.urlparse(uri)

        self._spreadsheet_id = parts.path.split("/")[3]

        query_string = urllib.parse.parse_qs(parts.query)
        if "gid" in query_string:
            sheet_id = int(query_string["gid"][-1])
        elif parts.fragment.startswith("gid="):
            sheet_id = int(parts.fragment[len("gid=") :])
        else:
            sheet_id = 0
        self._sheet_id = sheet_id

        if not self.credentials:
            return

        session = self._get_session()
        url = (
            f"https://sheets.googleapis.com/v4/spreadsheets/{self._spreadsheet_id}"
            "?includeGridData=false"
        )
        _logger.info("GET %s", url)
        response = session.get(url)
        payload = response.json()
        _logger.debug(payload)
        if "error" in payload:
            raise ProgrammingError(payload["error"]["message"])

        self._timezone = dateutil.tz.gettz(payload["properties"]["timeZone"])

        sheets = payload["sheets"]
        for sheet in sheets:
            if sheet["properties"]["sheetId"] == sheet_id:
                self._sheet_name = sheet["properties"]["title"]
                break
        else:
            _logger.warning("Could not determine sheet name!")

    def _get_session(self) -> Session:
        return cast(
            Session,
            AuthorizedSession(self.credentials) if self.credentials else Session(),
        )

    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata of a sheet.

        This currently returns the spreadsheet and sheet titles. We could also
        return number of rows and columns, since they're available in the response
        payload.
        """
        session = self._get_session()
        url = (
            f"https://sheets.googleapis.com/v4/spreadsheets/{self._spreadsheet_id}"
            "?includeGridData=false"
        )
        _logger.info("GET %s", url)
        response = session.get(url)
        payload = response.json()
        _logger.debug(payload)
        if "error" in payload:
            return {}

        sheets = [
            sheet
            for sheet in payload["sheets"]
            if sheet["properties"]["sheetId"] == self._sheet_id
        ]
        if len(sheets) != 1:
            return {}
        sheet = sheets[0]

        return {
            "Spreadsheet title": payload["properties"]["title"],
            "Sheet title": sheet["properties"]["title"],
        }

    def _run_query(self, sql: str) -> QueryResults:
        """
        Execute a query using the Google Chart API.
        """
        quoted_sql = urllib.parse.quote(sql, safe="/()")
        url = f"{self.url}&tq={quoted_sql}"
        headers = {"X-DataSource-Auth": "true"}

        session = self._get_session()
        _logger.info("GET %s", url)
        response = session.get(url, headers=headers)
        if response.encoding is None:
            response.encoding = "utf-8"

        try:
            response.raise_for_status()
        except Exception as ex:
            self._check_permissions(ex)
            raise ProgrammingError(response.text) from ex

        if response.text.startswith(JSON_PAYLOAD_PREFIX):
            result = json.loads(response.text[len(JSON_PAYLOAD_PREFIX) :])
        else:
            try:
                result = response.json()
            except Exception as ex:
                self._check_permissions(ex)
                raise ProgrammingError(
                    "Response from Google is not valid JSON.",
                ) from ex

        _logger.debug("Received payload: %s", result)
        if result["status"] == "error":
            raise ProgrammingError(format_error_message(result["errors"]))

        return cast(QueryResults, result)

    def _check_permissions(self, ex: Exception) -> None:
        """
        Check if we have permission to access a sheet.

        This is called when the response from an API is not valid JSON, trying to
        determine why the payload is not as expected.
        """
        session = self._get_session()
        url = (
            f"https://sheets.googleapis.com/v4/spreadsheets/{self._spreadsheet_id}"
            f"/developerMetadata/{self._sheet_id}"
        )
        response = session.get(url)
        if not response.ok:
            payload = response.json()
            error = payload["error"]

            # custom exception to trigger oauth
            if error["code"] == 401:
                raise UnauthenticatedError(error["message"]) from ex

            # something else happened, raise exception with the message
            raise InterfaceError(error["message"]) from ex

    def _set_columns(self, uri: str) -> None:
        """
        Download data and extract columns.

        We run a simple ``SELECT * LIMIT 1`` statement to get a small response
        so we can extract the column names and types.
        """
        results = self._run_query("SELECT * LIMIT 1")
        cols = results["table"]["cols"]
        rows = results["table"]["rows"]

        # if the columns have no labels, use the first row as the labels if
        # it exists; otherwise use just the column letters
        if all(col["label"] == "" for col in cols):
            _logger.warning("Couldn't extract column labels from sheet")
            if rows:
                # update URL with information to skip the first row
                self.url = get_url(uri, headers=1)
                for col, cell in zip(cols, rows[0]["c"]):
                    col["label"] = get_value_from_cell(cell)
            else:
                for col in cols:
                    col["label"] = col["id"]

        self._column_map = {
            col["label"].strip(): col["id"] for col in cols if col["label"].strip()
        }
        self.columns = {
            col["label"].strip(): get_field(col, self._timezone)
            for col in cols
            if col["label"].strip()
        }

    def get_columns(self) -> Dict[str, Field]:
        return self.columns

    get_cost = NetworkAPICostModel(DOWNLOAD_COST, FIXED_COST)

    def _clear_columns(self) -> None:
        """
        Clear filters and order from columns.

        This is called when we switch from the Chart API to the Sheets API. When
        that happens we use a local copy of the spreadsheet values. Clearing the
        columns ensure that filtering and sorting are performed by the backend,
        which is probably more efficient.
        """
        for field in self.columns.values():
            field.filters = []
            field.order = Order.NONE
            field.exact = False

    def _get_header_rows(self, values: List[List[Any]]) -> int:
        """
        Return the number of header rows.

        We can have column names in more than one row:

            ----------------------
            | this is  | this is |
            |----------|---------|
            | a string | a float |
            |----------|---------|
            | test     | 1.1     |
            ----------------------

        The Chart API returns the columns labels correctly, "this is a
        string" and "this is a float", but the Sheets API returns them
        as separate rows.

        In order to determine the number of header rows we pick the first
        column, and test how many rows are needed to make the column label
        returned by the Chart API (2, in this case).
        """
        first_column_name = list(self.columns)[0]
        letters = self._column_map[first_column_name]
        index = get_index_from_letters(letters)

        cells = []
        i = 0
        for i, row in enumerate(values):
            cells.append(str(row[index]))
            if " ".join(cells) == first_column_name:
                break
        else:
            raise InternalError("Could not determine number of header rows")

        return i + 1

    def get_data(  # pylint: disable=too-many-locals
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        **kwargs: Any,
    ) -> Iterator[Row]:
        """
        Fetch data.

        In ``BIDIRECTIONAL`` mode, or if we haven't done any DML, we use the Chart
        API to retrieve data, since it allows filtering/sorting the data. For
        other modes, once the sheet has been modified we read from a local copy
        of the data.
        """
        # build a reverse map so we know which columns are defined
        reverse_map = {v: k for k, v in self._column_map.items()}

        # For ``UNIDIRECTIONAL`` or ``BATCH`` mode we download the data once
        # by calling ``_get_values()``, and we use the local copy for
        # all further operations.
        if self.modified and self._sync_mode in {
            SyncMode.UNIDIRECTIONAL,
            SyncMode.BATCH,
        }:
            values = self._get_values()
            headers = self._get_header_rows(values)
            rows: Iterator[Row] = (
                {
                    reverse_map[letter]: cell
                    for letter, cell in zip(gen_letters(), row)
                    if letter in reverse_map
                }
                for row in values[headers:]
            )
            rows = apply_limit_and_offset(rows, limit, offset)

        # For ``BIDIRECTIONAL`` mode we continue using the Chart API to
        # retrieve data. This will happen before every DML query.
        else:
            try:
                sql = build_sql(
                    self.columns,
                    bounds,
                    order,
                    None,
                    self._column_map,
                    limit,
                    offset,
                )
            except ImpossibleFilterError:
                return

            payload = self._run_query(sql)
            cols = payload["table"]["cols"]
            rows = (
                {
                    reverse_map[col["id"]]: get_value_from_cell(cell)
                    for col, cell in zip(cols, row["c"])
                    if col["id"] in reverse_map
                }
                for row in payload["table"]["rows"]
            )

        for i, row in enumerate(rows):
            rowid = (offset or 0) + i
            self._row_ids[rowid] = row
            row["rowid"] = rowid
            _logger.debug(row)
            yield row

    def insert_data(self, row: Row) -> int:
        """
        Insert a row into a sheet.
        """
        row_id: Optional[int] = row.pop("rowid")
        if row_id is None:
            row_id = max(self._row_ids.keys()) + 1 if self._row_ids else 0
        self._row_ids[row_id] = row

        row_values = get_values_from_row(row, self._column_map)

        # In these modes we keep a local copy of the data, so we only have to
        # download the full sheet once.
        if self._sync_mode in {SyncMode.UNIDIRECTIONAL, SyncMode.BATCH}:
            values = self._get_values()
            values.append(row_values)
            self._clear_columns()

        # In these modes we push all changes immediately to the sheet.
        if self._sync_mode in {SyncMode.BIDIRECTIONAL, SyncMode.UNIDIRECTIONAL}:
            session = self._get_session()
            body = {
                "range": self._sheet_name,
                "majorDimension": "ROWS",
                "values": [row_values],
            }
            url = (
                "https://sheets.googleapis.com/v4/spreadsheets/"
                f"{self._spreadsheet_id}/values/{self._sheet_name}:append"
            )
            _logger.info("POST %s", url)
            _logger.debug(body)
            response = session.post(
                url,
                json=body,
                params={"valueInputOption": "USER_ENTERED"},
            )
            payload = response.json()
            _logger.debug(payload)
            if "error" in payload:
                raise ProgrammingError(payload["error"]["message"])

        self.modified = True

        return row_id

    def _get_values(self) -> List[List[Any]]:
        """
        Download all values from the spreadsheet.
        """
        if self._values is not None and self._sync_mode in {
            SyncMode.UNIDIRECTIONAL,
            SyncMode.BATCH,
        }:
            return self._values

        session = self._get_session()
        url = (
            f"https://sheets.googleapis.com/v4/spreadsheets/{self._spreadsheet_id}"
            f"/values/{self._sheet_name}"
        )
        params = {"valueRenderOption": "FORMATTED_VALUE"}

        # Log the URL. We can't use a prepared request here to extract the URL because
        # it doesn't work with ``AuthorizedSession``.
        query_string = urllib.parse.urlencode(params)
        _logger.info("GET %s?%s", url, query_string)

        response = session.get(url, params=params)
        payload = response.json()
        _logger.debug(payload)
        if "error" in payload:
            raise ProgrammingError(payload["error"]["message"])

        self._values = cast(List[List[Any]], payload["values"])

        # We store the number of original rows, so that when we replace the sheet
        # values later we can clear the bottom rows in case the number of rows is
        # reduced.
        self._original_rows = len(self._values)

        return self._values

    def _find_row_number(self, row: Row) -> int:
        """
        Return the 0-indexed number of a given row, defined by its values.
        """
        target_row_values = get_values_from_row(row, self._column_map)
        for i, row_values in enumerate(self._get_values()):
            # pad with empty strings to match size
            padding = [""] * (len(target_row_values) - len(row_values))
            if [*row_values, *padding] == target_row_values:
                return i

        raise ProgrammingError(f"Could not find row: {row}")

    def delete_data(self, row_id: int) -> None:
        """
        Delete a row from the sheet.
        """
        if row_id not in self._row_ids:
            raise ProgrammingError(f"Invalid row to delete: {row_id}")

        row = self._row_ids[row_id]
        row_number = self._find_row_number(row)

        # In these modes we keep a local copy of the data, so we only have to
        # download the full sheet once.
        if self._sync_mode in {SyncMode.UNIDIRECTIONAL, SyncMode.BATCH}:
            values = self._get_values()
            values.pop(row_number)
            self._clear_columns()

        # In these modes we push all changes immediately to the sheet.
        if self._sync_mode in {SyncMode.BIDIRECTIONAL, SyncMode.UNIDIRECTIONAL}:
            session = self._get_session()
            body = {
                "requests": [
                    {
                        "deleteDimension": {
                            "range": {
                                "sheetId": self._sheet_id,
                                "dimension": "ROWS",
                                "startIndex": row_number,
                                "endIndex": row_number + 1,
                            },
                        },
                    },
                ],
            }
            url = (
                "https://sheets.googleapis.com/v4/spreadsheets/"
                f"{self._spreadsheet_id}:batchUpdate"
            )
            _logger.info("POST %s", url)
            _logger.debug(body)
            response = session.post(
                url,
                json=body,
            )
            payload = response.json()
            _logger.debug(payload)
            if "error" in payload:
                raise ProgrammingError(payload["error"]["message"])

        # only delete row_id on a successful request
        del self._row_ids[row_id]

        self.modified = True

    def update_data(  # pylint: disable=too-many-locals
        self,
        row_id: int,
        row: Row,
    ) -> None:
        """
        Update a row in the sheet.
        """
        if row_id not in self._row_ids:
            raise ProgrammingError(f"Invalid row to update: {row_id}")

        current_row = self._row_ids[row_id]
        row_number = self._find_row_number(current_row)

        row_values = get_values_from_row(row, self._column_map)

        # In these modes we keep a local copy of the data, so we only have to
        # download the full sheet once.
        if self._sync_mode in {SyncMode.UNIDIRECTIONAL, SyncMode.BATCH}:
            values = self._get_values()
            values[row_number] = row_values
            self._clear_columns()

        # In these modes we push all changes immediately to the sheet.
        if self._sync_mode in {SyncMode.BIDIRECTIONAL, SyncMode.UNIDIRECTIONAL}:
            session = self._get_session()
            range_ = f"{self._sheet_name}!A{row_number + 1}"
            body = {
                "range": range_,
                "majorDimension": "ROWS",
                "values": [row_values],
            }
            url = (
                "https://sheets.googleapis.com/v4/spreadsheets/"
                f"{self._spreadsheet_id}/values/{range_}"
            )
            params = {"valueInputOption": "USER_ENTERED"}

            # Log the URL. We can't use a prepared request here to extract the URL because
            # it doesn't work with ``AuthorizedSession``.
            query_string = urllib.parse.urlencode(params)
            _logger.info("PUT %s?%s", url, query_string)
            _logger.debug(body)

            response = session.put(url, json=body, params=params)
            payload = response.json()
            _logger.debug(payload)
            if "error" in payload:
                raise ProgrammingError(payload["error"]["message"])

        # the row_id might change on an update
        new_row_id = row.pop("rowid")
        if new_row_id != row_id:
            del self._row_ids[row_id]
        self._row_ids[new_row_id] = row

        self.modified = True

    def close(self) -> None:
        """
        Push pending changes.

        This method is used only in ``BATCH`` mode to push pending modifications
        to the sheet.
        """
        if not self.modified or self._sync_mode != SyncMode.BATCH:
            return

        values = self._get_values()
        if not values:
            raise InternalError("An unexpected error happened")

        # Pad values. This ensures that rows are padded to the right with
        # empty strings, so they override any underlying cells when the
        # updated sheet is pushed. Similarly, append dummy rows so that if
        # the number of rows is smaller than the original the old data gets
        # erased by the new one.
        number_of_columns = max(len(row) for row in values)
        dummy_row = [""] * number_of_columns
        values = [[*row, *([""] * (number_of_columns - len(row)))] for row in values]
        values.extend([dummy_row] * (self._original_rows - len(values)))

        _logger.info("Pushing pending changes to the spreadsheet")
        session = self._get_session()
        range_ = f"{self._sheet_name}"
        body = {
            "range": range_,
            "majorDimension": "ROWS",
            "values": values,
        }
        url = (
            "https://sheets.googleapis.com/v4/spreadsheets/"
            f"{self._spreadsheet_id}/values/{range_}"
        )
        params = {"valueInputOption": "USER_ENTERED"}

        # Log the URL. We can't use a prepared request here to extract the URL because
        # it doesn't work with ``AuthorizedSession``.
        query_string = urllib.parse.urlencode(params)
        _logger.info("PUT %s?%s", url, query_string)
        _logger.debug(body)

        response = session.put(url, json=body, params=params)
        payload = response.json()
        _logger.debug(payload)
        if "error" in payload:
            message = payload["error"]["message"]
            _logger.warning("Unable to commit batch changes: %s", message)
            raise ProgrammingError(message)

        self.modified = False
        _logger.info("Success!")

    def drop_table(self) -> None:
        """
        Delete a sheet.
        """
        session = self._get_session()
        body = {
            "requests": [
                {
                    "deleteSheet": {
                        "sheetId": self._sheet_id,
                    },
                },
            ],
        }
        url = (
            "https://sheets.googleapis.com/v4/spreadsheets/"
            f"{self._spreadsheet_id}:batchUpdate"
        )
        _logger.info("POST %s", url)
        _logger.debug(body)
        response = session.post(
            url,
            json=body,
        )
        payload = response.json()
        _logger.debug(payload)
        if "error" in payload:
            raise ProgrammingError(payload["error"]["message"])
