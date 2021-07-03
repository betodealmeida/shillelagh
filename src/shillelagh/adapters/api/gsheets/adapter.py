import atexit
import datetime
import json
import logging
import urllib.parse
from typing import Any
from typing import cast
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple

import dateutil.tz
from google.auth.transport.requests import AuthorizedSession
from requests import Session
from shillelagh.adapters.api.gsheets.lib import format_error_message
from shillelagh.adapters.api.gsheets.lib import gen_letters
from shillelagh.adapters.api.gsheets.lib import get_credentials
from shillelagh.adapters.api.gsheets.lib import get_field
from shillelagh.adapters.api.gsheets.lib import get_index_from_letters
from shillelagh.adapters.api.gsheets.lib import get_sync_mode
from shillelagh.adapters.api.gsheets.lib import get_url
from shillelagh.adapters.api.gsheets.lib import get_values_from_row
from shillelagh.adapters.api.gsheets.types import SyncMode
from shillelagh.adapters.api.gsheets.typing import QueryResults
from shillelagh.adapters.base import Adapter
from shillelagh.exceptions import ImpossibleFilterError
from shillelagh.exceptions import InternalError
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Field
from shillelagh.filters import Filter
from shillelagh.lib import build_sql
from shillelagh.types import Order
from shillelagh.typing import RequestedOrder
from shillelagh.typing import Row

_logger = logging.getLogger(__name__)

JSON_PAYLOAD_PREFIX = ")]}'\n"


class GSheetsAPI(Adapter):

    safe = True

    @staticmethod
    def supports(uri: str, **kwargs: Any) -> bool:
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

    def __init__(
        self,
        uri: str,
        access_token: Optional[str] = None,
        service_account_file: Optional[str] = None,
        service_account_info: Optional[Dict[str, Any]] = None,
        subject: Optional[str] = None,
        catalog: Optional[Dict[str, str]] = None,
    ):
        if catalog and uri in catalog:
            uri = catalog[uri]

        self.modified = False

        # commit changes in batch when the connection is closed or when the
        # program terminates
        atexit.register(self.close)

        self.url = get_url(uri)
        self.credentials = get_credentials(
            access_token,
            service_account_file,
            service_account_info,
            subject,
        )

        self._sync_mode = get_sync_mode(uri) or SyncMode.BIDIRECTIONAL
        self._values: Optional[List[List[Any]]] = None
        self._original_rows = 0

        self._offset = 0

        # extra metadata
        self._spreadsheet_id: Optional[str] = None
        self._sheet_id: Optional[int] = None
        self._sheet_name: Optional[str] = None
        self._timezone: Optional[datetime.tzinfo] = None
        self._set_metadata(uri)

        # determine columns
        self.columns: Dict[str, Field] = {}
        self._set_columns()

        # store row ids for DML
        self._row_ids: Dict[int, Row] = {}

    def _set_metadata(self, uri: str) -> None:
        """
        Get spreadsheet ID, sheet ID, and sheet name.
        """
        parts = urllib.parse.urlparse(uri)

        self._spreadsheet_id = parts.path.split("/")[3]

        qs = urllib.parse.parse_qs(parts.query)
        if "gid" in qs:
            sheet_id = int(qs["gid"][-1])
        elif parts.fragment.startswith("gid="):
            sheet_id = int(parts.fragment[len("gid=") :])
        else:
            sheet_id = 0
        self._sheet_id = sheet_id

        if not self.credentials:
            return

        session = self._get_session()
        response = session.get(
            f"https://sheets.googleapis.com/v4/spreadsheets/{self._spreadsheet_id}"
            "?includeGridData=false",
        )
        payload = response.json()
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
        session = self._get_session()
        response = session.get(
            f"https://sheets.googleapis.com/v4/spreadsheets/{self._spreadsheet_id}"
            "?includeGridData=false",
        )
        payload = response.json()
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
            except Exception as ex:
                raise ProgrammingError(
                    "Response from Google is not valid JSON. Please verify that you "
                    "have the proper credentials to access the spreadsheet.",
                ) from ex

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

    def _clear_columns(self) -> None:
        # clear columns so that all the filtering happens in SQLite
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
        for i, row in enumerate(values):
            cells.append(str(row[index]))
            if " ".join(cells) == first_column_name:
                break
        else:
            raise InternalError("Could not determine number of header rows")

        return i + 1

    def get_data(
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
    ) -> Iterator[Row]:
        reverse_map = {v: k for k, v in self._column_map.items()}

        if self.modified and self._sync_mode in {
            SyncMode.UNIDIRECTIONAL,
            SyncMode.BATCH,
        }:
            values = self._get_values()
            headers = self._get_header_rows(values)
            rows = (
                {
                    reverse_map[letter]: cell
                    for letter, cell in zip(gen_letters(), row)
                    if letter in reverse_map
                }
                for row in values[headers:]
            )

        else:
            try:
                sql = build_sql(
                    self.columns,
                    bounds,
                    order,
                    self._column_map,
                    self._offset,
                )
            except ImpossibleFilterError:
                return

            payload = self._run_query(sql)
            cols = payload["table"]["cols"]
            rows = (
                {
                    reverse_map[col["id"]]: cell["v"] if cell else None
                    for col, cell in zip(cols, row["c"])
                    if col["id"] in reverse_map
                }
                for row in payload["table"]["rows"]
            )

        for i, row in enumerate(rows):
            self._row_ids[i] = row
            row["rowid"] = i
            yield row

    def insert_data(self, row: Row) -> int:
        row_id: Optional[int] = row.pop("rowid")
        if row_id is None:
            row_id = max(self._row_ids.keys()) + 1 if self._row_ids else 0
        self._row_ids[row_id] = row

        row_values = get_values_from_row(row, self._column_map)

        if self._sync_mode in {SyncMode.UNIDIRECTIONAL, SyncMode.BATCH}:
            values = self._get_values()
            values.append(row_values)

        if self._sync_mode in {SyncMode.BIDIRECTIONAL, SyncMode.UNIDIRECTIONAL}:
            session = self._get_session()
            body = {
                "range": self._sheet_name,
                "majorDimension": "ROWS",
                "values": [row_values],
            }
            response = session.post(
                (
                    "https://sheets.googleapis.com/v4/spreadsheets/"
                    f"{self._spreadsheet_id}/values/{self._sheet_name}:append"
                ),
                json=body,
                params={
                    # https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/append
                    "valueInputOption": "USER_ENTERED",
                },
            )
            payload = response.json()
            if "error" in payload:
                raise ProgrammingError(payload["error"]["message"])

        self._clear_columns()
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
        response = session.get(
            f"https://sheets.googleapis.com/v4/spreadsheets/{self._spreadsheet_id}"
            f"/values/{self._sheet_name}",
            params={"valueRenderOption": "UNFORMATTED_VALUE"},
        )
        payload = response.json()
        if "error" in payload:
            raise ProgrammingError(payload["error"]["message"])

        self._values = cast(List[List[Any]], payload["values"])
        self._original_rows = len(self._values)

        return self._values

    def _find_row_number(self, row: Row) -> int:
        """
        Return the 0-indexed number of a given row, defined by its values.
        """
        target_row_values = get_values_from_row(row, self._column_map)
        for i, row_values in enumerate(self._get_values()):
            if row_values == target_row_values:
                return i

        raise ProgrammingError(f"Could not find row: {row}")

    def delete_data(self, row_id: int) -> None:
        if row_id not in self._row_ids:
            raise ProgrammingError(f"Invalid row to delete: {row_id}")

        row = self._row_ids[row_id]
        row_number = self._find_row_number(row)

        if self._sync_mode in {SyncMode.UNIDIRECTIONAL, SyncMode.BATCH}:
            values = self._get_values()
            values.pop(row_number)

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
            response = session.post(
                (
                    "https://sheets.googleapis.com/v4/spreadsheets/"
                    f"{self._spreadsheet_id}:batchUpdate"
                ),
                json=body,
            )
            payload = response.json()
            if "error" in payload:
                raise ProgrammingError(payload["error"]["message"])

        # only delete row_id on a successful request
        del self._row_ids[row_id]

        self._clear_columns()
        self.modified = True

    def update_data(self, row_id: int, row: Row) -> None:
        if row_id not in self._row_ids:
            raise ProgrammingError(f"Invalid row to update: {row_id}")

        current_row = self._row_ids[row_id]
        row_number = self._find_row_number(current_row)

        row_values = get_values_from_row(row, self._column_map)

        if self._sync_mode in {SyncMode.UNIDIRECTIONAL, SyncMode.BATCH}:
            values = self._get_values()
            values[row_number] = row_values

        if self._sync_mode in {SyncMode.BIDIRECTIONAL, SyncMode.UNIDIRECTIONAL}:
            session = self._get_session()
            range_ = f"{self._sheet_name}!A{row_number + 1}"
            body = {
                "range": range_,
                "majorDimension": "ROWS",
                "values": [row_values],
            }
            response = session.put(
                (
                    "https://sheets.googleapis.com/v4/spreadsheets/"
                    f"{self._spreadsheet_id}/values/{range_}"
                ),
                json=body,
                params={
                    "valueInputOption": "USER_ENTERED",
                },
            )
            payload = response.json()
            if "error" in payload:
                raise ProgrammingError(payload["error"]["message"])

        # the row_id might change on an update
        new_row_id = row.pop("rowid")
        if new_row_id != row_id:
            del self._row_ids[row_id]
        self._row_ids[new_row_id] = row

        self._clear_columns()
        self.modified = True

    def close(self) -> None:
        if not self.modified or self._sync_mode != SyncMode.BATCH:
            return

        values = self._get_values()
        if not values:
            raise InternalError("An unexpected error happened")

        # pad values with empty rows if needed
        dummy_row = [""] * len(values[0])
        values.extend([dummy_row] * (self._original_rows - len(values)))

        _logger.info("Pushing pending changes to the spreadsheet")
        session = self._get_session()
        range_ = f"{self._sheet_name}"
        body = {
            "range": range_,
            "majorDimension": "ROWS",
            "values": values,
        }
        response = session.put(
            (
                "https://sheets.googleapis.com/v4/spreadsheets/"
                f"{self._spreadsheet_id}/values/{range_}"
            ),
            json=body,
            params={
                "valueInputOption": "USER_ENTERED",
            },
        )
        payload = response.json()
        if "error" in payload:
            message = payload["error"]["message"]
            _logger.warning("Unable to commit batch changes: %s", message)
            raise ProgrammingError(message)

        self.modified = False
        _logger.info("Success!")
