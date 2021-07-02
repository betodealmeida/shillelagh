import datetime
import json
from unittest import mock

import apsw
import dateutil.tz
import pytest
import requests
import requests_mock
from freezegun import freeze_time
from shillelagh.adapters.api.gsheets import format_error_message
from shillelagh.adapters.api.gsheets import get_credentials
from shillelagh.adapters.api.gsheets import get_sync_mode
from shillelagh.adapters.api.gsheets import get_url
from shillelagh.adapters.api.gsheets import GSheetsAPI
from shillelagh.adapters.api.gsheets import GSheetsBoolean
from shillelagh.adapters.api.gsheets import GSheetsDate
from shillelagh.adapters.api.gsheets import GSheetsDateTime
from shillelagh.adapters.api.gsheets import GSheetsTime
from shillelagh.adapters.api.gsheets import SyncMode
from shillelagh.adapters.base import Adapter
from shillelagh.backends.apsw.db import connect
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Order
from shillelagh.fields import String
from shillelagh.filters import Equal
from shillelagh.filters import Impossible
from shillelagh.filters import Range

from ...fakes import FakeAdapter
from ...fakes import FakeEntryPoint


@pytest.fixture
def simple_sheet_adapter():
    adapter = requests_mock.Adapter()
    adapter.register_uri(
        "GET",
        "https://docs.google.com/spreadsheets/d/1/gviz/tq?gid=0&tq=SELECT%20%2A%20LIMIT%201",
        json={
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
        },
    )
    adapter.register_uri(
        "GET",
        "https://docs.google.com/spreadsheets/d/1/gviz/tq?gid=0&tq=SELECT%20%2A",
        json={
            "version": "0.6",
            "reqId": "0",
            "status": "ok",
            "sig": "1642441872",
            "table": {
                "cols": [
                    {"id": "A", "label": "country", "type": "string"},
                    {"id": "B", "label": "cnt", "type": "number", "pattern": "General"},
                ],
                "rows": [
                    {"c": [{"v": "BR"}, {"v": 1.0, "f": "1"}]},
                    {"c": [{"v": "BR"}, {"v": 3.0, "f": "3"}]},
                    {"c": [{"v": "IN"}, {"v": 5.0, "f": "5"}]},
                    {"c": [{"v": "ZA"}, {"v": 6.0, "f": "6"}]},
                    {"c": [{"v": "CR"}, {"v": 10.0, "f": "10"}]},
                ],
                "parsedNumHeaders": 1,
            },
        },
    )
    adapter.register_uri(
        "GET",
        "https://sheets.googleapis.com/v4/spreadsheets/1?includeGridData=false",
        json={
            "spreadsheetId": "1",
            "properties": {
                "title": "Sheet1",
                "locale": "en_US",
                "autoRecalc": "ON_CHANGE",
                "timeZone": "America/Los_Angeles",
                "defaultFormat": {
                    "backgroundColor": {"red": 1, "green": 1, "blue": 1},
                    "padding": {"top": 2, "right": 3, "bottom": 2, "left": 3},
                    "verticalAlignment": "BOTTOM",
                    "wrapStrategy": "OVERFLOW_CELL",
                    "textFormat": {
                        "foregroundColor": {},
                        "fontFamily": "arial,sans,sans-serif",
                        "fontSize": 10,
                        "bold": False,
                        "italic": False,
                        "strikethrough": False,
                        "underline": False,
                        "foregroundColorStyle": {"rgbColor": {}},
                    },
                    "backgroundColorStyle": {
                        "rgbColor": {"red": 1, "green": 1, "blue": 1},
                    },
                },
            },
            "sheets": [
                {
                    "properties": {
                        "sheetId": 0,
                        "title": "Sheet1",
                        "index": 0,
                        "sheetType": "GRID",
                        "gridProperties": {"rowCount": 985, "columnCount": 26},
                    },
                },
            ],
            "spreadsheetUrl": "https://docs.google.com/spreadsheets/d/1/edit?ouid=111430789371895352716&urlBuilderDomain=dealmeida.net",
        },
    )
    adapter.register_uri(
        "GET",
        "https://sheets.googleapis.com/v4/spreadsheets/1/values/Sheet1?valueRenderOption=UNFORMATTED_VALUE",
        json={
            "range": "'Sheet1'!A1:Z983",
            "majorDimension": "ROWS",
            "values": [
                ["country", "cnt"],
                ["BR", 1],
                ["BR", 3],
                ["IN", 5],
                ["ZA", 6],
                ["CR", 10],
            ],
        },
    )
    yield adapter


def test_credentials(mocker):
    entry_points = [FakeEntryPoint("gsheetsapi", GSheetsAPI)]
    mocker.patch(
        "shillelagh.backends.apsw.db.iter_entry_points",
        return_value=entry_points,
    )

    connection = connect(
        ":memory:",
        ["gsheetsapi"],
        adapter_kwargs={
            "gsheetsapi": {
                "service_account_info": {"secret": "XXX"},
                "subject": "user@example.com",
            },
        },
        isolation_level="IMMEDIATE",
    )
    cursor = connection.cursor()
    cursor._cursor = mock.MagicMock()
    cursor._cursor.execute.side_effect = [
        "",
        apsw.SQLError(
            "SQLError: no such table: https://docs.google.com/spreadsheets/d/1",
        ),
        "",
        "",
    ]

    cursor.execute('SELECT 1 FROM "https://docs.google.com/spreadsheets/d/1"')
    cursor._cursor.execute.assert_has_calls(
        [
            mock.call("BEGIN IMMEDIATE"),
            mock.call('SELECT 1 FROM "https://docs.google.com/spreadsheets/d/1"', None),
            mock.call(
                """CREATE VIRTUAL TABLE "https://docs.google.com/spreadsheets/d/1" USING GSheetsAPI('"https://docs.google.com/spreadsheets/d/1"', 'null', 'null', '{"secret": "XXX"}', '"user@example.com"', 'null')""",
            ),
            mock.call('SELECT 1 FROM "https://docs.google.com/spreadsheets/d/1"', None),
        ],
    )


def test_get_credentials(mocker):
    service_account = mocker.patch(
        "shillelagh.adapters.api.gsheets.google.oauth2.service_account.Credentials",
    )
    credentials = mocker.patch(
        "shillelagh.adapters.api.gsheets.google.oauth2.credentials.Credentials",
    )

    # no credentials
    get_credentials(None, None, None, None)
    credentials.assert_not_called()
    service_account.assert_not_called()

    # access_token
    get_credentials("token", None, None, None)
    credentials.assert_called_with("token")
    credentials.reset_mock()
    service_account.assert_not_called()

    # service_account_file
    get_credentials(None, "credentials.json", None, None)
    credentials.assert_not_called()
    service_account.from_service_account_file.assert_called_with(
        "credentials.json",
        scopes=[
            "https://www.googleapis.com/auth/drive.readonly",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://spreadsheets.google.com/feeds",
        ],
        subject=None,
    )
    service_account.reset_mock()

    # service_account_info
    get_credentials(None, None, {"secret": "XXX"}, "user@example.com")
    credentials.assert_not_called()
    service_account.from_service_account_info.assert_called_with(
        {"secret": "XXX"},
        scopes=[
            "https://www.googleapis.com/auth/drive.readonly",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://spreadsheets.google.com/feeds",
        ],
        subject="user@example.com",
    )


def test_execute(mocker, simple_sheet_adapter):
    entry_points = [FakeEntryPoint("gsheetsapi", GSheetsAPI)]
    mocker.patch(
        "shillelagh.backends.apsw.db.iter_entry_points",
        return_value=entry_points,
    )

    session = requests.Session()
    session.mount("https://", simple_sheet_adapter)
    mocker.patch(
        "shillelagh.adapters.api.gsheets.GSheetsAPI._get_session",
        return_value=session,
    )

    connection = connect(":memory:", ["gsheetsapi"])
    cursor = connection.cursor()

    sql = '''SELECT * FROM "https://docs.google.com/spreadsheets/d/1/edit#gid=0"'''
    data = list(cursor.execute(sql))
    assert data == [
        ("BR", 1),
        ("BR", 3),
        ("IN", 5),
        ("ZA", 6),
        ("CR", 10),
    ]


def test_execute_with_catalog(mocker, simple_sheet_adapter):
    entry_points = [FakeEntryPoint("gsheetsapi", GSheetsAPI)]
    mocker.patch(
        "shillelagh.backends.apsw.db.iter_entry_points",
        return_value=entry_points,
    )

    session = requests.Session()
    session.mount("https://", simple_sheet_adapter)
    mocker.patch(
        "shillelagh.adapters.api.gsheets.GSheetsAPI._get_session",
        return_value=session,
    )

    connection = connect(
        ":memory:",
        ["gsheetsapi"],
        adapter_kwargs={
            "gsheetsapi": {
                "catalog": {
                    "sheet": "https://docs.google.com/spreadsheets/d/1/edit#gid=0",
                },
            },
        },
    )
    cursor = connection.cursor()

    sql = "SELECT * FROM sheet"
    data = list(cursor.execute(sql))
    assert data == [
        ("BR", 1),
        ("BR", 3),
        ("IN", 5),
        ("ZA", 6),
        ("CR", 10),
    ]


def test_execute_filter(mocker, simple_sheet_adapter):
    entry_points = [FakeEntryPoint("gsheetsapi", GSheetsAPI)]
    mocker.patch(
        "shillelagh.backends.apsw.db.iter_entry_points",
        return_value=entry_points,
    )

    session = requests.Session()
    session.mount("https://", simple_sheet_adapter)
    mocker.patch(
        "shillelagh.adapters.api.gsheets.GSheetsAPI._get_session",
        return_value=session,
    )
    simple_sheet_adapter.register_uri(
        "GET",
        "https://docs.google.com/spreadsheets/d/1/gviz/tq?gid=0&tq=SELECT%20%2A%20WHERE%20B%20%3C%205.0",
        json={
            "version": "0.6",
            "reqId": "0",
            "status": "ok",
            "sig": "11559839",
            "table": {
                "cols": [
                    {"id": "A", "label": "country", "type": "string"},
                    {"id": "B", "label": "cnt", "type": "number", "pattern": "General"},
                ],
                "rows": [
                    {"c": [{"v": "BR"}, {"v": 1.0, "f": "1"}]},
                    {"c": [{"v": "BR"}, {"v": 3.0, "f": "3"}]},
                ],
                "parsedNumHeaders": 1,
            },
        },
    )

    connection = connect(":memory:", ["gsheetsapi"])
    cursor = connection.cursor()
    sql = (
        "SELECT * FROM "
        '"https://docs.google.com/spreadsheets/d/1/edit#gid=0" '
        "WHERE cnt < 5"
    )
    data = list(cursor.execute(sql))
    assert data == [
        ("BR", 1),
        ("BR", 3),
    ]


def test_execute_impossible(mocker, simple_sheet_adapter):
    entry_points = [FakeEntryPoint("gsheetsapi", GSheetsAPI)]
    mocker.patch(
        "shillelagh.backends.apsw.db.iter_entry_points",
        return_value=entry_points,
    )

    session = requests.Session()
    session.mount("https://", simple_sheet_adapter)
    mocker.patch(
        "shillelagh.adapters.api.gsheets.GSheetsAPI._get_session",
        return_value=session,
    )

    connection = connect(":memory:", ["gsheetsapi"])
    cursor = connection.cursor()
    sql = (
        "SELECT * FROM "
        '"https://docs.google.com/spreadsheets/d/1/edit#gid=0" '
        "WHERE cnt < 5 AND cnt > 5"
    )
    data = list(cursor.execute(sql))
    assert data == []


def test_get_url():
    assert (
        get_url(
            "https://docs.google.com/spreadsheets/d/1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/edit#gid=0",
        )
        == "https://docs.google.com/spreadsheets/d/1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/gviz/tq?gid=0"
    )
    assert (
        get_url(
            "https://docs.google.com/spreadsheets/d/1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/edit#gid=0",
            headers=2,
            gid=3,
            sheet="some-sheet",
        )
        == "https://docs.google.com/spreadsheets/d/1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/gviz/tq?headers=2&sheet=some-sheet"
    )
    assert (
        get_url(
            "https://docs.google.com/spreadsheets/d/1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/edit?headers=2&gid=1",
        )
        == "https://docs.google.com/spreadsheets/d/1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/gviz/tq?headers=2&gid=1"
    )
    assert (
        get_url(
            "https://docs.google.com/spreadsheets/d/1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/edit?headers=2&sheet=some-sheet",
        )
        == "https://docs.google.com/spreadsheets/d/1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/gviz/tq?headers=2&sheet=some-sheet"
    )


def test_format_error_message():
    response = {
        "version": "0.6",
        "reqId": "0",
        "status": "error",
        "errors": [
            {
                "reason": "invalid_query",
                "message": "INVALID_QUERY",
                "detailed_message": "Invalid query: NO_COLUMN: C",
            },
        ],
    }
    assert format_error_message(response["errors"]) == "Invalid query: NO_COLUMN: C"


def test_convert_rows(mocker):
    entry_points = [FakeEntryPoint("gsheetsapi", GSheetsAPI)]
    mocker.patch(
        "shillelagh.backends.apsw.db.iter_entry_points",
        return_value=entry_points,
    )

    adapter = requests_mock.Adapter()
    session = requests.Session()
    session.mount("https://", adapter)
    mocker.patch(
        "shillelagh.adapters.api.gsheets.GSheetsAPI._get_session",
        return_value=session,
    )
    adapter.register_uri(
        "GET",
        "https://docs.google.com/spreadsheets/d/2/gviz/tq?gid=0&tq=SELECT%20%2A%20LIMIT%201",
        json={
            "version": "0.6",
            "reqId": "0",
            "status": "ok",
            "sig": "1819058448",
            "table": {
                "cols": [
                    {
                        "id": "A",
                        "label": "datetime",
                        "type": "datetime",
                        "pattern": "M/d/yyyy H:mm:ss",
                    },
                    {
                        "id": "B",
                        "label": "number",
                        "type": "number",
                        "pattern": "General",
                    },
                    {"id": "C", "label": "boolean", "type": "boolean"},
                    {"id": "D", "label": "date", "type": "date", "pattern": "M/d/yyyy"},
                    {
                        "id": "E",
                        "label": "timeofday",
                        "type": "timeofday",
                        "pattern": "h:mm:ss am/pm",
                    },
                    {"id": "F", "label": "string", "type": "string"},
                ],
                "rows": [
                    {
                        "c": [
                            {"v": "Date(2018,8,1,0,0,0)", "f": "9/1/2018 0:00:00"},
                            {"v": 1.0, "f": "1"},
                            {"v": True, "f": "TRUE"},
                            {"v": "Date(2018,0,1)", "f": "1/1/2018"},
                            {"v": [17, 0, 0, 0], "f": "5:00:00 PM"},
                            {"v": "test"},
                        ],
                    },
                ],
                "parsedNumHeaders": 0,
            },
        },
    )
    adapter.register_uri(
        "GET",
        "https://docs.google.com/spreadsheets/d/2/gviz/tq?gid=0&tq=SELECT%20%2A",
        json={
            "version": "0.6",
            "reqId": "0",
            "status": "ok",
            "sig": "1155522967",
            "table": {
                "cols": [
                    {
                        "id": "A",
                        "label": "datetime",
                        "type": "datetime",
                        "pattern": "M/d/yyyy H:mm:ss",
                    },
                    {
                        "id": "B",
                        "label": "number",
                        "type": "number",
                        "pattern": "General",
                    },
                    {"id": "C", "label": "boolean", "type": "boolean"},
                    {"id": "D", "label": "date", "type": "date", "pattern": "M/d/yyyy"},
                    {
                        "id": "E",
                        "label": "timeofday",
                        "type": "timeofday",
                        "pattern": "h:mm:ss am/pm",
                    },
                    {"id": "F", "label": "string", "type": "string"},
                ],
                "rows": [
                    {
                        "c": [
                            {"v": "Date(2018,8,1,0,0,0)", "f": "9/1/2018 0:00:00"},
                            {"v": 1.0, "f": "1"},
                            {"v": True, "f": "TRUE"},
                            {"v": "Date(2018,0,1)", "f": "1/1/2018"},
                            {"v": [17, 0, 0, 0], "f": "5:00:00 PM"},
                            {"v": "test"},
                        ],
                    },
                    {
                        "c": [
                            {"v": "Date(2018,8,2,0,0,0)", "f": "9/2/2018 0:00:00"},
                            {"v": 1.0, "f": "1"},
                            {"v": False, "f": "FALSE"},
                            None,
                            None,
                            {"v": "test"},
                        ],
                    },
                    {
                        "c": [
                            {"v": "Date(2018,8,3,0,0,0)", "f": "9/3/2018 0:00:00"},
                            {"v": 2.0, "f": "2"},
                            {"v": False, "f": "FALSE"},
                            None,
                            None,
                            {"v": "test"},
                        ],
                    },
                    {
                        "c": [
                            {"v": "Date(2018,8,4,0,0,0)", "f": "9/4/2018 0:00:00"},
                            {"v": 3.0, "f": "3"},
                            {"v": False, "f": "FALSE"},
                            None,
                            None,
                            {"v": "test"},
                        ],
                    },
                    {
                        "c": [
                            {"v": "Date(2018,8,5,0,0,0)", "f": "9/5/2018 0:00:00"},
                            {"v": 5.0, "f": "5"},
                            {"v": False, "f": "FALSE"},
                            None,
                            None,
                            {"v": "test"},
                        ],
                    },
                    {
                        "c": [
                            {"v": "Date(2018,8,6,0,0,0)", "f": "9/6/2018 0:00:00"},
                            {"v": 8.0, "f": "8"},
                            {"v": False, "f": "FALSE"},
                            None,
                            None,
                            {"v": "test"},
                        ],
                    },
                    {
                        "c": [
                            {"v": "Date(2018,8,7,0,0,0)", "f": "9/7/2018 0:00:00"},
                            {"v": 13.0, "f": "13"},
                            {"v": False, "f": "FALSE"},
                            None,
                            None,
                            {"v": None},
                        ],
                    },
                    {
                        "c": [
                            {"v": "Date(2018,8,8,0,0,0)", "f": "9/8/2018 0:00:00"},
                            None,
                            {"v": False, "f": "FALSE"},
                            None,
                            None,
                            {"v": "test"},
                        ],
                    },
                    {
                        "c": [
                            {"v": "Date(2018,8,9,0,0,0)", "f": "9/9/2018 0:00:00"},
                            {"v": 34.0, "f": "34"},
                            None,
                            None,
                            None,
                            {"v": "test"},
                        ],
                    },
                ],
                "parsedNumHeaders": 1,
            },
        },
    )

    connection = connect(":memory:", ["gsheetsapi"])
    cursor = connection.cursor()

    sql = '''SELECT * FROM "https://docs.google.com/spreadsheets/d/2/edit#gid=0"'''
    data = list(cursor.execute(sql))
    assert data == [
        (
            datetime.datetime(2018, 9, 1, 0, 0, tzinfo=datetime.timezone.utc),
            1.0,
            True,
            datetime.date(2018, 1, 1),
            datetime.time(17, 0, 0, tzinfo=datetime.timezone.utc),
            "test",
        ),
        (
            datetime.datetime(2018, 9, 2, 0, 0, tzinfo=datetime.timezone.utc),
            1.0,
            False,
            None,
            None,
            "test",
        ),
        (
            datetime.datetime(2018, 9, 3, 0, 0, tzinfo=datetime.timezone.utc),
            2.0,
            False,
            None,
            None,
            "test",
        ),
        (
            datetime.datetime(2018, 9, 4, 0, 0, tzinfo=datetime.timezone.utc),
            3.0,
            False,
            None,
            None,
            "test",
        ),
        (
            datetime.datetime(2018, 9, 5, 0, 0, tzinfo=datetime.timezone.utc),
            5.0,
            False,
            None,
            None,
            "test",
        ),
        (
            datetime.datetime(2018, 9, 6, 0, 0, tzinfo=datetime.timezone.utc),
            8.0,
            False,
            None,
            None,
            "test",
        ),
        (
            datetime.datetime(2018, 9, 7, 0, 0, tzinfo=datetime.timezone.utc),
            13.0,
            False,
            None,
            None,
            None,
        ),
        (
            datetime.datetime(2018, 9, 8, 0, 0, tzinfo=datetime.timezone.utc),
            None,
            False,
            None,
            None,
            "test",
        ),
        (
            datetime.datetime(2018, 9, 9, 0, 0, tzinfo=datetime.timezone.utc),
            34.0,
            None,
            None,
            None,
            "test",
        ),
    ]


def test_get_session(mocker):
    mock_authorized_session = mock.MagicMock()
    mocker.patch(
        "shillelagh.adapters.api.gsheets.AuthorizedSession",
        mock_authorized_session,
    )
    mock_session = mock.MagicMock()
    mocker.patch("shillelagh.adapters.api.gsheets.Session", mock_session)
    mocker.patch(
        "shillelagh.adapters.api.gsheets.get_credentials",
        return_value=None,
    )

    # prevent network calls
    mocker.patch(
        "shillelagh.adapters.api.gsheets.GSheetsAPI._set_columns",
        mock.MagicMock(),
    )
    mocker.patch(
        "shillelagh.adapters.api.gsheets.GSheetsAPI._set_metadata",
        mock.MagicMock(),
    )

    gsheets_adapter = GSheetsAPI("https://docs.google.com/spreadsheets/d/1")
    gsheets_adapter._get_session()
    mock_authorized_session.assert_not_called()
    mock_session.assert_called()

    mock_authorized_session.reset_mock()
    mock_session.reset_mock()

    mocker.patch(
        "shillelagh.adapters.api.gsheets.get_credentials",
        return_value="SECRET",
    )
    gsheets_adapter = GSheetsAPI(
        "https://docs.google.com/spreadsheets/d/1",
        service_account_info={"secret": "XXX"},
        subject="user@example.com",
    )
    assert gsheets_adapter.credentials == "SECRET"
    gsheets_adapter._get_session()
    mock_authorized_session.assert_called()
    mock_session.assert_not_called()


def test_api_bugs(mocker):
    entry_points = [FakeEntryPoint("gsheetsapi", GSheetsAPI)]
    mocker.patch(
        "shillelagh.backends.apsw.db.iter_entry_points",
        return_value=entry_points,
    )

    adapter = requests_mock.Adapter()
    session = requests.Session()
    session.mount("https://", adapter)
    mocker.patch(
        "shillelagh.adapters.api.gsheets.GSheetsAPI._get_session",
        return_value=session,
    )
    # use content= so that the response has no encoding
    adapter.register_uri(
        "GET",
        "https://docs.google.com/spreadsheets/d/3/gviz/tq?gid=0&tq=SELECT%20%2A%20LIMIT%201",
        content=json.dumps(
            {
                "version": "0.6",
                "reqId": "0",
                "status": "ok",
                "sig": "2050160589",
                "table": {
                    "cols": [
                        {"id": "A", "label": "country", "type": "string"},
                        {
                            "id": "B",
                            "label": "cnt",
                            "type": "number",
                            "pattern": "General",
                        },
                    ],
                    "rows": [{"c": [{"v": "BR"}, {"v": 1.0, "f": "1"}]}],
                    "parsedNumHeaders": 0,
                },
            },
        ).encode(),
    )
    # the API actually returns "201 OK" on errors, but let's assume for a second
    # that it uses HTTP status codes correctly...
    adapter.register_uri(
        "GET",
        "https://docs.google.com/spreadsheets/d/3/gviz/tq?gid=0&tq=SELECT%20%2A",
        content=json.dumps(
            {
                "version": "0.6",
                "reqId": "0",
                "status": "error",
                "errors": [
                    {
                        "reason": "invalid_query",
                        "message": "INVALID_QUERY",
                        "detailed_message": "Invalid query: NO_COLUMN: C",
                    },
                ],
            },
        ).encode(),
        status_code=400,
        headers={},
    )

    connection = connect(":memory:", ["gsheetsapi"])
    cursor = connection.cursor()

    sql = '''SELECT * FROM "https://docs.google.com/spreadsheets/d/3/edit#gid=0"'''
    with pytest.raises(ProgrammingError) as excinfo:
        cursor.execute(sql)

    assert (
        str(excinfo.value)
        == '{"version": "0.6", "reqId": "0", "status": "error", "errors": [{"reason": "invalid_query", "message": "INVALID_QUERY", "detailed_message": "Invalid query: NO_COLUMN: C"}]}'
    )


def test_execute_json_prefix(mocker, simple_sheet_adapter):
    entry_points = [FakeEntryPoint("gsheetsapi", GSheetsAPI)]
    mocker.patch(
        "shillelagh.backends.apsw.db.iter_entry_points",
        return_value=entry_points,
    )

    session = requests.Session()
    session.mount("https://", simple_sheet_adapter)
    mocker.patch(
        "shillelagh.adapters.api.gsheets.GSheetsAPI._get_session",
        return_value=session,
    )
    simple_sheet_adapter.register_uri(
        "GET",
        "https://docs.google.com/spreadsheets/d/1/gviz/tq?gid=0&tq=SELECT%20%2A",
        text=")]}'\n"
        + json.dumps(
            {
                "version": "0.6",
                "reqId": "0",
                "status": "ok",
                "sig": "1642441872",
                "table": {
                    "cols": [
                        {"id": "A", "label": "country", "type": "string"},
                        {
                            "id": "B",
                            "label": "cnt",
                            "type": "number",
                            "pattern": "General",
                        },
                    ],
                    "rows": [
                        {"c": [{"v": "BR"}, {"v": 1.0, "f": "1"}]},
                        {"c": [{"v": "BR"}, {"v": 3.0, "f": "3"}]},
                        {"c": [{"v": "IN"}, {"v": 5.0, "f": "5"}]},
                        {"c": [{"v": "ZA"}, {"v": 6.0, "f": "6"}]},
                        {"c": [{"v": "CR"}, {"v": 10.0, "f": "10"}]},
                    ],
                    "parsedNumHeaders": 1,
                },
            },
        ),
    )

    connection = connect(":memory:", ["gsheetsapi"])
    cursor = connection.cursor()

    sql = '''SELECT * FROM "https://docs.google.com/spreadsheets/d/1/edit#gid=0"'''
    data = list(cursor.execute(sql))
    assert data == [
        ("BR", 1),
        ("BR", 3),
        ("IN", 5),
        ("ZA", 6),
        ("CR", 10),
    ]


def test_execute_invalid_json(mocker):
    entry_points = [FakeEntryPoint("gsheetsapi", GSheetsAPI)]
    mocker.patch(
        "shillelagh.backends.apsw.db.iter_entry_points",
        return_value=entry_points,
    )

    adapter = requests_mock.Adapter()
    session = requests.Session()
    session.mount("https://", adapter)
    mocker.patch(
        "shillelagh.adapters.api.gsheets.GSheetsAPI._get_session",
        return_value=session,
    )
    adapter.register_uri(
        "GET",
        "https://docs.google.com/spreadsheets/d/5/gviz/tq?gid=0&tq=SELECT%20%2A%20LIMIT%201",
        text="NOT JSON",
    )

    connection = connect(":memory:", ["gsheetsapi"])
    cursor = connection.cursor()

    sql = '''SELECT * FROM "https://docs.google.com/spreadsheets/d/5/edit#gid=0"'''
    with pytest.raises(ProgrammingError) as excinfo:
        cursor.execute(sql)
    assert str(excinfo.value) == (
        "Response from Google is not valid JSON. Please verify that you have "
        "the proper credentials to access the spreadsheet."
    )


def test_execute_error_response(mocker):
    entry_points = [FakeEntryPoint("gsheetsapi", GSheetsAPI)]
    mocker.patch(
        "shillelagh.backends.apsw.db.iter_entry_points",
        return_value=entry_points,
    )

    adapter = requests_mock.Adapter()
    session = requests.Session()
    session.mount("https://", adapter)
    mocker.patch(
        "shillelagh.adapters.api.gsheets.GSheetsAPI._get_session",
        return_value=session,
    )
    adapter.register_uri(
        "GET",
        "https://docs.google.com/spreadsheets/d/6/gviz/tq?gid=0&tq=SELECT%20%2A%20LIMIT%201",
        json={
            "version": "0.6",
            "reqId": "0",
            "status": "error",
            "errors": [
                {
                    "reason": "invalid_query",
                    "message": "INVALID_QUERY",
                    "detailed_message": "Invalid query: NO_COLUMN: C",
                },
            ],
        },
    )

    connection = connect(":memory:", ["gsheetsapi"])
    cursor = connection.cursor()

    sql = '''SELECT * FROM "https://docs.google.com/spreadsheets/d/6/edit#gid=0"'''
    with pytest.raises(ProgrammingError) as excinfo:
        cursor.execute(sql)
    assert str(excinfo.value) == "Invalid query: NO_COLUMN: C"


def test_headers_not_detected(mocker):
    entry_points = [FakeEntryPoint("gsheetsapi", GSheetsAPI)]
    mocker.patch(
        "shillelagh.backends.apsw.db.iter_entry_points",
        return_value=entry_points,
    )

    adapter = requests_mock.Adapter()
    session = requests.Session()
    session.mount("https://", adapter)
    mocker.patch(
        "shillelagh.adapters.api.gsheets.GSheetsAPI._get_session",
        return_value=session,
    )
    adapter.register_uri(
        "GET",
        "https://docs.google.com/spreadsheets/d/7/gviz/tq?gid=0&tq=SELECT%20%2A%20LIMIT%201",
        json={
            "version": "0.6",
            "reqId": "0",
            "status": "ok",
            "sig": "1227631590",
            "table": {
                "cols": [
                    {"id": "A", "label": "", "type": "string"},
                    {"id": "B", "label": "", "type": "string"},
                    {"id": "C", "label": "", "type": "string"},
                ],
                "rows": [
                    {"c": [{"v": "Investor"}, {"v": "InvestorName"}, {"v": "Company"}]},
                ],
                "parsedNumHeaders": 0,
            },
        },
    )
    adapter.register_uri(
        "GET",
        "https://docs.google.com/spreadsheets/d/7/gviz/tq?gid=0&tq=SELECT%20%2A%20OFFSET%201",
        json={
            "version": "0.6",
            "reqId": "0",
            "status": "ok",
            "sig": "1227631590",
            "table": {
                "cols": [
                    {"id": "A", "label": "", "type": "string"},
                    {"id": "B", "label": "", "type": "string"},
                    {"id": "C", "label": "", "type": "string"},
                ],
                "rows": [
                    {
                        "c": [
                            {"v": "Bye Combinator"},
                            {"v": "John Doe"},
                            {"v": "Avocado & Hummus"},
                        ],
                    },
                ],
                "parsedNumHeaders": 0,
            },
        },
    )

    connection = connect(":memory:", ["gsheetsapi"])
    cursor = connection.cursor()

    sql = '''SELECT * FROM "https://docs.google.com/spreadsheets/d/7/edit#gid=0"'''
    data = list(cursor.execute(sql))
    assert data == [("Bye Combinator", "John Doe", "Avocado & Hummus")]


def test_headers_not_detected_no_rows(mocker):
    entry_points = [FakeEntryPoint("gsheetsapi", GSheetsAPI)]
    mocker.patch(
        "shillelagh.backends.apsw.db.iter_entry_points",
        return_value=entry_points,
    )

    adapter = requests_mock.Adapter()
    session = requests.Session()
    session.mount("https://", adapter)
    mocker.patch(
        "shillelagh.adapters.api.gsheets.GSheetsAPI._get_session",
        return_value=session,
    )
    adapter.register_uri(
        "GET",
        "https://docs.google.com/spreadsheets/d/8/gviz/tq?gid=0&tq=SELECT%20%2A%20LIMIT%201",
        json={
            "version": "0.6",
            "reqId": "0",
            "status": "ok",
            "sig": "1227631590",
            "table": {
                "cols": [
                    {"id": "A", "label": "", "type": "string"},
                    {"id": "B", "label": "", "type": "string"},
                    {"id": "C", "label": "", "type": "string"},
                ],
                "rows": [],
                "parsedNumHeaders": 0,
            },
        },
    )
    adapter.register_uri(
        "GET",
        "https://docs.google.com/spreadsheets/d/8/gviz/tq?gid=0&tq=SELECT%20%2A",
        json={
            "version": "0.6",
            "reqId": "0",
            "status": "ok",
            "sig": "1227631590",
            "table": {
                "cols": [
                    {"id": "A", "label": "", "type": "string"},
                    {"id": "B", "label": "", "type": "string"},
                    {"id": "C", "label": "", "type": "string"},
                ],
                "rows": [],
                "parsedNumHeaders": 0,
            },
        },
    )

    connection = connect(":memory:", ["gsheetsapi"])
    cursor = connection.cursor()

    sql = '''SELECT * FROM "https://docs.google.com/spreadsheets/d/8/edit#gid=0"'''
    data = list(cursor.execute(sql))
    assert data == []

    gsheets_adapter = GSheetsAPI("https://docs.google.com/spreadsheets/d/8/#gid=0")
    assert list(gsheets_adapter.columns) == ["A", "B", "C"]


def test_fields():
    assert GSheetsDateTime().parse(None) is None
    assert GSheetsDateTime().parse("Date(2018,8,9,0,0,0)") == datetime.datetime(
        2018,
        9,
        9,
        0,
        0,
    )
    assert (
        GSheetsDateTime().quote(
            datetime.datetime(2018, 9, 9, 0, 0, tzinfo=datetime.timezone.utc),
        )
        == "datetime '2018-09-09 00:00:00+00:00'"
    )

    assert GSheetsDate().parse(None) is None
    assert GSheetsDate().parse("Date(2018,0,1)") == datetime.date(2018, 1, 1)
    assert GSheetsDate().quote(datetime.date(2018, 1, 1)) == "date '2018-01-01'"

    assert GSheetsTime().parse(None) is None
    assert GSheetsTime().parse([17, 0, 0, 0]) == datetime.time(
        17,
        0,
    )
    assert (
        GSheetsTime().quote(datetime.time(17, 0, tzinfo=datetime.timezone.utc))
        == "timeofday '17:00:00+00:00'"
    )

    assert GSheetsBoolean().parse(None) is None
    assert GSheetsBoolean().parse("TRUE")
    assert not GSheetsBoolean().parse("FALSE")
    assert GSheetsBoolean().quote(True) == "true"
    assert GSheetsBoolean().quote(False) == "false"

    assert GSheetsDateTime().format(None) is None
    assert (
        GSheetsDateTime().format(
            datetime.datetime(2018, 1, 1, 0, 0, tzinfo=datetime.timezone.utc),
        )
        == "01/01/2018 00:00:00"
    )
    tz = dateutil.tz.gettz("America/Los_Angeles")
    assert (
        GSheetsDateTime(timezone=tz).format(
            datetime.datetime(2018, 1, 1, 0, 0, tzinfo=datetime.timezone.utc),
        )
        == "12/31/2017 16:00:00"
    )


def test_set_metadata(mocker, simple_sheet_adapter):
    mocker.patch(
        "shillelagh.adapters.api.gsheets.GSheetsAPI._set_columns",
    )
    mocker.patch(
        "shillelagh.adapters.api.gsheets.get_credentials",
        return_value="SECRET",
    )

    session = requests.Session()
    session.mount("https://", simple_sheet_adapter)
    mocker.patch(
        "shillelagh.adapters.api.gsheets.GSheetsAPI._get_session",
        return_value=session,
    )

    gsheets_adapter = GSheetsAPI(
        "https://docs.google.com/spreadsheets/d/1/edit#gid=0",
        "XXX",
    )
    assert gsheets_adapter._spreadsheet_id == "1"
    assert gsheets_adapter._sheet_id == 0
    assert gsheets_adapter._sheet_name == "Sheet1"
    assert gsheets_adapter._timezone == dateutil.tz.gettz("America/Los_Angeles")

    gsheets_adapter = GSheetsAPI(
        "https://docs.google.com/spreadsheets/d/1/edit?gid=0",
        "XXX",
    )
    assert gsheets_adapter._sheet_id == 0
    assert gsheets_adapter._sheet_name == "Sheet1"

    _logger = mocker.patch("shillelagh.adapters.api.gsheets._logger")
    gsheets_adapter = GSheetsAPI(
        "https://docs.google.com/spreadsheets/d/1/edit#gid=43",
        "XXX",
    )
    assert gsheets_adapter._sheet_name is None
    _logger.warning.assert_called_with("Could not determine sheet name!")


def test_set_metadata_error(mocker):
    mocker.patch(
        "shillelagh.adapters.api.gsheets.GSheetsAPI._set_columns",
    )
    mocker.patch(
        "shillelagh.adapters.api.gsheets.get_credentials",
        return_value="SECRET",
    )

    adapter = requests_mock.Adapter()
    session = requests.Session()
    session.mount("https://", adapter)
    mocker.patch(
        "shillelagh.adapters.api.gsheets.GSheetsAPI._get_session",
        return_value=session,
    )
    adapter.register_uri(
        "GET",
        "https://sheets.googleapis.com/v4/spreadsheets/1?includeGridData=false",
        json={
            "error": {
                "code": 404,
                "message": "Requested entity was not found.",
                "status": "NOT_FOUND",
            },
        },
    )

    with pytest.raises(ProgrammingError) as excinfo:
        GSheetsAPI(
            "https://docs.google.com/spreadsheets/d/1/edit#gid=42",
            "XXX",
        )
    assert str(excinfo.value) == "Requested entity was not found."


def test_insert_data(mocker, simple_sheet_adapter):
    mocker.patch(
        "shillelagh.adapters.api.gsheets.get_credentials",
        return_value="SECRET",
    )

    session = requests.Session()
    session.mount("https://", simple_sheet_adapter)
    mocker.patch(
        "shillelagh.adapters.api.gsheets.GSheetsAPI._get_session",
        return_value=session,
    )
    simple_sheet_adapter.register_uri(
        "POST",
        "https://sheets.googleapis.com/v4/spreadsheets/1/values/Sheet1:append?valueInputOption=USER_ENTERED",
        json={
            "spreadsheetId": "1",
            "tableRange": "'Sheet1'!A1:F10",
            "updates": {
                "spreadsheetId": "1",
                "updatedRange": "'Sheet1!A11",
                "updatedRows": 1,
                "updatedColumns": 1,
                "updatedCells": 1,
            },
        },
    )

    gsheets_adapter = GSheetsAPI("https://docs.google.com/spreadsheets/d/1/edit", "XXX")

    row_id = gsheets_adapter.insert_row({"country": "UK", "cnt": 10, "rowid": None})
    assert row_id == 0
    assert gsheets_adapter._row_ids == {0: {"cnt": 10.0, "country": "UK"}}
    assert simple_sheet_adapter.last_request.json() == {
        "range": "Sheet1",
        "majorDimension": "ROWS",
        "values": [["UK", 10.0]],
    }

    row_id = gsheets_adapter.insert_row({"country": "PY", "cnt": 11, "rowid": 3})
    assert row_id == 3
    assert gsheets_adapter._row_ids == {
        0: {"cnt": 10.0, "country": "UK"},
        3: {"cnt": 11.0, "country": "PY"},
    }

    simple_sheet_adapter.register_uri(
        "POST",
        "https://sheets.googleapis.com/v4/spreadsheets/1/values/Sheet1:append?valueInputOption=USER_ENTERED",
        json={
            "error": {
                "code": 400,
                "message": "Request range[WRONG] does not match value's range[Sheet1]",
                "status": "INVALID_ARGUMENT",
            },
        },
    )
    with pytest.raises(ProgrammingError) as excinfo:
        gsheets_adapter.insert_row({"country": "PY", "cnt": 11, "rowid": 3})
    assert (
        str(excinfo.value)
        == "Request range[WRONG] does not match value's range[Sheet1]"
    )


def test_delete_data(mocker, simple_sheet_adapter):
    mocker.patch(
        "shillelagh.adapters.api.gsheets.get_credentials",
        return_value="SECRET",
    )

    session = requests.Session()
    session.mount("https://", simple_sheet_adapter)
    mocker.patch(
        "shillelagh.adapters.api.gsheets.GSheetsAPI._get_session",
        return_value=session,
    )
    simple_sheet_adapter.register_uri(
        "POST",
        "https://sheets.googleapis.com/v4/spreadsheets/1:batchUpdate",
        json={"spreadsheetId": "1", "replies": [{}]},
    )

    gsheets_adapter = GSheetsAPI("https://docs.google.com/spreadsheets/d/1/edit", "XXX")
    gsheets_adapter._row_ids = {
        0: {"cnt": 10.0, "country": "CR"},
        3: {"cnt": 1.0, "country": "BR"},
        4: {"cnt": 12.0, "country": "PL"},
    }

    gsheets_adapter.delete_row(0)
    assert gsheets_adapter._row_ids == {
        3: {"cnt": 1.0, "country": "BR"},
        4: {"cnt": 12.0, "country": "PL"},
    }
    assert simple_sheet_adapter.last_request.json() == {
        "requests": [
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": 0,
                        "dimension": "ROWS",
                        "startIndex": 5,
                        "endIndex": 6,
                    },
                },
            },
        ],
    }

    with pytest.raises(ProgrammingError) as excinfo:
        gsheets_adapter.delete_row(4)
    assert str(excinfo.value) == "Could not find row: {'cnt': 12.0, 'country': 'PL'}"

    with pytest.raises(ProgrammingError) as excinfo:
        gsheets_adapter.delete_row(5)
    assert str(excinfo.value) == "Invalid row to delete: 5"

    simple_sheet_adapter.register_uri(
        "POST",
        "https://sheets.googleapis.com/v4/spreadsheets/1:batchUpdate",
        json={
            "error": {
                "code": 404,
                "message": "Requested entity was not found.",
                "status": "NOT_FOUND",
            },
        },
    )
    with pytest.raises(ProgrammingError) as excinfo:
        gsheets_adapter.delete_row(3)
    assert str(excinfo.value) == "Requested entity was not found."

    simple_sheet_adapter.register_uri(
        "GET",
        "https://sheets.googleapis.com/v4/spreadsheets/1/values/Sheet1?valueRenderOption=UNFORMATTED_VALUE",
        json={
            "error": {
                "code": 404,
                "message": "Requested entity was not found.",
                "status": "NOT_FOUND",
            },
        },
    )
    with pytest.raises(ProgrammingError) as excinfo:
        gsheets_adapter.delete_row(3)
    assert str(excinfo.value) == "Requested entity was not found."


def test_update_data(mocker, simple_sheet_adapter):
    mocker.patch(
        "shillelagh.adapters.api.gsheets.get_credentials",
        return_value="SECRET",
    )

    session = requests.Session()
    session.mount("https://", simple_sheet_adapter)
    mocker.patch(
        "shillelagh.adapters.api.gsheets.GSheetsAPI._get_session",
        return_value=session,
    )
    simple_sheet_adapter.register_uri(
        "PUT",
        "https://sheets.googleapis.com/v4/spreadsheets/1/values/Sheet1!A6?valueInputOption=USER_ENTERED",
        json={
            "spreadsheetId": "1",
            "tableRange": "'Sheet1'!A6:B6",
            "updates": {
                "spreadsheetId": "1",
                "updatedRange": "'Sheet1!A6:B6",
                "updatedRows": 1,
                "updatedColumns": 1,
                "updatedCells": 1,
            },
        },
    )

    gsheets_adapter = GSheetsAPI("https://docs.google.com/spreadsheets/d/1/edit", "XXX")
    gsheets_adapter._row_ids = {
        0: {"cnt": 10.0, "country": "CR"},
        3: {"cnt": 11.0, "country": "PY"},
        4: {"cnt": 12.0, "country": "PL"},
    }

    gsheets_adapter.update_row(0, {"cnt": 12.0, "country": "CR", "rowid": 0})
    assert gsheets_adapter._row_ids == {
        0: {"cnt": 12.0, "country": "CR"},
        3: {"cnt": 11.0, "country": "PY"},
        4: {"cnt": 12.0, "country": "PL"},
    }
    assert simple_sheet_adapter.last_request.json() == {
        "majorDimension": "ROWS",
        "range": "Sheet1!A6",
        "values": [["CR", 12.0]],
    }

    simple_sheet_adapter.register_uri(
        "GET",
        "https://sheets.googleapis.com/v4/spreadsheets/1/values/Sheet1?valueRenderOption=UNFORMATTED_VALUE",
        json={
            "range": "'Sheet1'!A1:Z983",
            "majorDimension": "ROWS",
            "values": [
                ["country", "cnt"],
                ["BR", 1],
                ["BR", 3],
                ["IN", 5],
                ["ZA", 6],
                ["CR", 12],
                ["PY", 11],
            ],
        },
    )
    gsheets_adapter.update_row(0, {"cnt": 12.0, "country": "UK", "rowid": 6})
    assert gsheets_adapter._row_ids == {
        6: {"cnt": 12.0, "country": "UK"},
        3: {"cnt": 11.0, "country": "PY"},
        4: {"cnt": 12.0, "country": "PL"},
    }

    with pytest.raises(ProgrammingError) as excinfo:
        gsheets_adapter.update_row(4, {"cnt": 13.0, "country": "PL"})
    assert str(excinfo.value) == "Could not find row: {'cnt': 12.0, 'country': 'PL'}"

    with pytest.raises(ProgrammingError) as excinfo:
        gsheets_adapter.update_row(5, {"cnt": 13.0, "country": "PL"})
    assert str(excinfo.value) == "Invalid row to update: 5"

    simple_sheet_adapter.register_uri(
        "PUT",
        "https://sheets.googleapis.com/v4/spreadsheets/1/values/Sheet1!A7?valueInputOption=USER_ENTERED",
        json={
            "error": {
                "code": 404,
                "message": "Requested entity was not found.",
                "status": "NOT_FOUND",
            },
        },
    )
    with pytest.raises(ProgrammingError) as excinfo:
        gsheets_adapter.update_row(3, {"cnt": 13.0, "country": "PL"})
    assert str(excinfo.value) == "Requested entity was not found."

    simple_sheet_adapter.register_uri(
        "GET",
        "https://sheets.googleapis.com/v4/spreadsheets/1/values/Sheet1?valueRenderOption=UNFORMATTED_VALUE",
        json={
            "error": {
                "code": 404,
                "message": "Requested entity was not found.",
                "status": "NOT_FOUND",
            },
        },
    )
    with pytest.raises(ProgrammingError) as excinfo:
        gsheets_adapter.update_row(3, {"cnt": 13.0, "country": "PL"})
    assert str(excinfo.value) == "Requested entity was not found."


def test_get_sync_mode():
    assert get_sync_mode("https://docs.google.com/spreadsheets/d/1/edit#gid=42") is None
    assert (
        get_sync_mode(
            "https://docs.google.com/spreadsheets/d/1/edit?sync_mode=BATCH#gid=42",
        )
        == SyncMode.BATCH
    )
    assert (
        get_sync_mode(
            "https://docs.google.com/spreadsheets/d/1/edit?sync_mode=batch#gid=42",
        )
        == SyncMode.BATCH
    )
    assert (
        get_sync_mode(
            "https://docs.google.com/spreadsheets/d/1/edit?sync_mode=1#gid=42",
        )
        == SyncMode.BIDIRECTIONAL
    )
    with pytest.raises(ProgrammingError) as excinfo:
        get_sync_mode(
            "https://docs.google.com/spreadsheets/d/1/edit?sync_mode=0#gid=42",
        )
    assert str(excinfo.value) == "Invalid sync mode: 0"
    with pytest.raises(ProgrammingError) as excinfo:
        get_sync_mode(
            "https://docs.google.com/spreadsheets/d/1/edit?sync_mode=INVALID#gid=42",
        )
    assert str(excinfo.value) == "Invalid sync mode: INVALID"


def test_batch_sync_mode(mocker, simple_sheet_adapter):
    mocker.patch(
        "shillelagh.adapters.api.gsheets.get_credentials",
        return_value="SECRET",
    )
    _logger = mocker.patch("shillelagh.adapters.api.gsheets._logger")

    session = requests.Session()
    session.mount("https://", simple_sheet_adapter)
    mocker.patch(
        "shillelagh.adapters.api.gsheets.GSheetsAPI._get_session",
        return_value=session,
    )
    update = simple_sheet_adapter.register_uri(
        "PUT",
        "https://sheets.googleapis.com/v4/spreadsheets/1/values/Sheet1?valueInputOption=USER_ENTERED",
        json={
            "spreadsheetId": "1",
            "tableRange": "'Sheet1'!A1:F10",
            "updates": {
                "spreadsheetId": "1",
                "updatedRange": "'Sheet1!A11",
                "updatedRows": 1,
                "updatedColumns": 1,
                "updatedCells": 1,
            },
        },
    )
    get_values = simple_sheet_adapter.register_uri(
        "GET",
        "https://sheets.googleapis.com/v4/spreadsheets/1/values/Sheet1?valueRenderOption=UNFORMATTED_VALUE",
        json={
            "range": "'Sheet1'!A1:Z983",
            "majorDimension": "ROWS",
            "values": [
                ["country", "cnt"],
                ["BR", 1],
                ["BR", 3],
                ["IN", 5],
                ["ZA", 6],
                ["UK", 10],
                ["PY", 11],
            ],
        },
    )

    gsheets_adapter = GSheetsAPI(
        "https://docs.google.com/spreadsheets/d/1/edit?sync_mode=BATCH",
        "XXX",
    )

    assert gsheets_adapter._values is None

    row_id = gsheets_adapter.insert_row({"country": "UK", "cnt": 10, "rowid": None})
    assert gsheets_adapter._values == [
        ["country", "cnt"],
        ["BR", 1],
        ["BR", 3],
        ["IN", 5],
        ["ZA", 6],
        ["UK", 10],
        ["PY", 11],
        ["UK", 10.0],
    ]

    # check that columns have no filters/order
    for column in {"country", "cnt"}:
        assert gsheets_adapter.columns[column].filters == []
        assert gsheets_adapter.columns[column].order == Order.NONE
        assert not gsheets_adapter.columns[column].exact

    # get_data should now return all data, since filtering is done by SQLite
    data = list(gsheets_adapter.get_data({"country": Equal("UK")}, []))
    assert data == [
        {"country": "BR", "cnt": 1, "rowid": 0},
        {"country": "BR", "cnt": 3, "rowid": 1},
        {"country": "IN", "cnt": 5, "rowid": 2},
        {"country": "ZA", "cnt": 6, "rowid": 3},
        {"country": "UK", "cnt": 10, "rowid": 4},
        {"country": "PY", "cnt": 11, "rowid": 5},
        {"country": "UK", "cnt": 10.0, "rowid": 6},
    ]

    row_id = 6
    gsheets_adapter.update_row(row_id, {"country": "UK", "cnt": 11, "rowid": row_id})
    assert gsheets_adapter._values == [
        ["country", "cnt"],
        ["BR", 1],
        ["BR", 3],
        ["IN", 5],
        ["ZA", 6],
        ["UK", 11.0],
        ["PY", 11],
        ["UK", 10.0],
    ]

    gsheets_adapter.delete_row(row_id)
    assert gsheets_adapter._values == [
        ["country", "cnt"],
        ["BR", 1],
        ["BR", 3],
        ["IN", 5],
        ["ZA", 6],
        ["PY", 11],
        ["UK", 10.0],
    ]

    # test that get_values was called only once
    assert get_values.call_count == 1

    # test that changes haven't been pushed yet
    assert update.call_count == 0
    assert update.last_request is None

    gsheets_adapter.close()

    # test that changes have been pushed
    assert update.call_count == 1
    assert update.last_request.json() == {
        "range": "Sheet1",
        "majorDimension": "ROWS",
        "values": [
            ["country", "cnt"],
            ["BR", 1],
            ["BR", 3],
            ["IN", 5],
            ["ZA", 6],
            ["PY", 11],
            ["UK", 10.0],
        ],
    }

    simple_sheet_adapter.register_uri(
        "PUT",
        "https://sheets.googleapis.com/v4/spreadsheets/1/values/Sheet1?valueInputOption=USER_ENTERED",
        json={
            "error": {
                "code": 404,
                "message": "Requested entity was not found.",
                "status": "NOT_FOUND",
            },
        },
    )

    gsheets_adapter = GSheetsAPI(
        "https://docs.google.com/spreadsheets/d/1/edit?sync_mode=BATCH",
        "XXX",
    )
    gsheets_adapter._values = []
    gsheets_adapter.modified = True
    with pytest.raises(ProgrammingError) as excinfo:
        gsheets_adapter.close()
    assert str(excinfo.value) == "Requested entity was not found."
    _logger.warning.assert_called_with(
        "Unable to commit batch changes: %s",
        "Requested entity was not found.",
    )
    # prevent atexit from running
    gsheets_adapter.modified = False


def test_batch_sync_mode_padding(mocker, simple_sheet_adapter):
    mocker.patch(
        "shillelagh.adapters.api.gsheets.get_credentials",
        return_value="SECRET",
    )

    session = requests.Session()
    session.mount("https://", simple_sheet_adapter)
    mocker.patch(
        "shillelagh.adapters.api.gsheets.GSheetsAPI._get_session",
        return_value=session,
    )
    update = simple_sheet_adapter.register_uri(
        "PUT",
        "https://sheets.googleapis.com/v4/spreadsheets/1/values/Sheet1?valueInputOption=USER_ENTERED",
        json={
            "spreadsheetId": "1",
            "tableRange": "'Sheet1'!A1:F10",
            "updates": {
                "spreadsheetId": "1",
                "updatedRange": "'Sheet1!A11",
                "updatedRows": 1,
                "updatedColumns": 1,
                "updatedCells": 1,
            },
        },
    )
    simple_sheet_adapter.register_uri(
        "GET",
        "https://sheets.googleapis.com/v4/spreadsheets/1/values/Sheet1?valueRenderOption=UNFORMATTED_VALUE",
        json={
            "range": "'Sheet1'!A1:Z983",
            "majorDimension": "ROWS",
            "values": [
                ["country", "cnt"],
                ["BR", 1],
                ["BR", 3],
                ["IN", 5],
                ["ZA", 6],
                ["UK", 10],
                ["PY", 11],
            ],
        },
    )

    gsheets_adapter = GSheetsAPI(
        "https://docs.google.com/spreadsheets/d/1/edit?sync_mode=BATCH",
        "XXX",
    )

    row_id = 0
    gsheets_adapter._row_ids = {row_id: {"cnt": 10.0, "country": "UK"}}
    gsheets_adapter.delete_row(row_id)
    assert gsheets_adapter._values == [
        ["country", "cnt"],
        ["BR", 1],
        ["BR", 3],
        ["IN", 5],
        ["ZA", 6],
        ["PY", 11],
    ]

    gsheets_adapter.close()

    assert update.last_request.json() == {
        "range": "Sheet1",
        "majorDimension": "ROWS",
        "values": [
            ["country", "cnt"],
            ["BR", 1],
            ["BR", 3],
            ["IN", 5],
            ["ZA", 6],
            ["PY", 11],
            ["", ""],
        ],
    }


def test_execute_batch(mocker, simple_sheet_adapter):
    entry_points = [FakeEntryPoint("gsheetsapi", GSheetsAPI)]
    mocker.patch(
        "shillelagh.backends.apsw.db.iter_entry_points",
        return_value=entry_points,
    )

    session = requests.Session()
    session.mount("https://", simple_sheet_adapter)
    mocker.patch(
        "shillelagh.adapters.api.gsheets.GSheetsAPI._get_session",
        return_value=session,
    )
    mocker.patch(
        "shillelagh.adapters.api.gsheets.get_credentials",
        return_value="SECRET",
    )
    simple_sheet_adapter.register_uri(
        "GET",
        "https://docs.google.com/spreadsheets/d/1/gviz/tq?gid=0&tq=SELECT%20%2A%20WHERE%20B%20%3C%205.0",
        json={
            "version": "0.6",
            "reqId": "0",
            "status": "ok",
            "sig": "1642441872",
            "table": {
                "cols": [
                    {"id": "A", "label": "country", "type": "string"},
                    {"id": "B", "label": "cnt", "type": "number", "pattern": "General"},
                ],
                "rows": [
                    {"c": [{"v": "BR"}, {"v": 1.0, "f": "1"}]},
                    {"c": [{"v": "BR"}, {"v": 3.0, "f": "3"}]},
                ],
                "parsedNumHeaders": 1,
            },
        },
    )
    simple_sheet_adapter.register_uri(
        "PUT",
        "https://sheets.googleapis.com/v4/spreadsheets/1/values/Sheet1?valueInputOption=USER_ENTERED",
        json={
            "spreadsheetId": "1",
            "tableRange": "'Sheet1'!A1:F10",
            "updates": {
                "spreadsheetId": "1",
                "updatedRange": "'Sheet1!A11",
                "updatedRows": 1,
                "updatedColumns": 1,
                "updatedCells": 1,
            },
        },
    )

    connection = connect(
        ":memory:",
        ["gsheetsapi"],
        adapter_kwargs={
            "gsheetsapi": {
                "service_account_info": {"secret": "XXX"},
                "subject": "user@example.com",
            },
        },
        isolation_level="IMMEDIATE",
    )
    cursor = connection.cursor()

    sql = '''SELECT * FROM "https://docs.google.com/spreadsheets/d/1/edit?sync_mode=BATCH#gid=0"'''
    data = list(cursor.execute(sql))
    assert data == [
        ("BR", 1),
        ("BR", 3),
        ("IN", 5),
        ("ZA", 6),
        ("CR", 10),
    ]

    sql = """
        DELETE FROM "https://docs.google.com/spreadsheets/d/1/edit?sync_mode=BATCH#gid=0"
        WHERE cnt < 5;
    """
    cursor.execute(sql)

    sql = '''SELECT * FROM "https://docs.google.com/spreadsheets/d/1/edit?sync_mode=BATCH#gid=0"'''
    data = list(cursor.execute(sql))
    assert data == [("IN", 5.0), ("ZA", 6.0), ("CR", 10.0)]


def test_unidirectional_sync_mode(mocker, simple_sheet_adapter):
    mocker.patch(
        "shillelagh.adapters.api.gsheets.get_credentials",
        return_value="SECRET",
    )

    session = requests.Session()
    session.mount("https://", simple_sheet_adapter)
    mocker.patch(
        "shillelagh.adapters.api.gsheets.GSheetsAPI._get_session",
        return_value=session,
    )
    insert = simple_sheet_adapter.register_uri(
        "POST",
        "https://sheets.googleapis.com/v4/spreadsheets/1/values/Sheet1:append?valueInputOption=USER_ENTERED",
        json={
            "spreadsheetId": "1",
            "tableRange": "'Sheet1'!A1:F10",
            "updates": {
                "spreadsheetId": "1",
                "updatedRange": "'Sheet1!A11",
                "updatedRows": 1,
                "updatedColumns": 1,
                "updatedCells": 1,
            },
        },
    )
    update = simple_sheet_adapter.register_uri(
        "PUT",
        "https://sheets.googleapis.com/v4/spreadsheets/1/values/Sheet1!A6?valueInputOption=USER_ENTERED",
        json={
            "spreadsheetId": "1",
            "tableRange": "'Sheet1'!A1:F10",
            "updates": {
                "spreadsheetId": "1",
                "updatedRange": "'Sheet1!A11",
                "updatedRows": 1,
                "updatedColumns": 1,
                "updatedCells": 1,
            },
        },
    )
    get_values = simple_sheet_adapter.register_uri(
        "GET",
        "https://sheets.googleapis.com/v4/spreadsheets/1/values/Sheet1?valueRenderOption=UNFORMATTED_VALUE",
        json={
            "range": "'Sheet1'!A1:Z983",
            "majorDimension": "ROWS",
            "values": [
                ["country", "cnt"],
                ["BR", 1],
                ["BR", 3],
                ["IN", 5],
                ["ZA", 6],
                ["UK", 10],
                ["PY", 11],
            ],
        },
    )
    delete = simple_sheet_adapter.register_uri(
        "POST",
        "https://sheets.googleapis.com/v4/spreadsheets/1:batchUpdate",
        json={"spreadsheetId": "1", "replies": [{}]},
    )

    gsheets_adapter = GSheetsAPI(
        "https://docs.google.com/spreadsheets/d/1/edit?sync_mode=2",
        "XXX",
    )

    assert gsheets_adapter._values is None

    row_id = gsheets_adapter.insert_row({"country": "UK", "cnt": 10, "rowid": None})
    assert gsheets_adapter._values == [
        ["country", "cnt"],
        ["BR", 1],
        ["BR", 3],
        ["IN", 5],
        ["ZA", 6],
        ["UK", 10],
        ["PY", 11],
        ["UK", 10.0],
    ]

    gsheets_adapter.update_row(row_id, {"country": "UK", "cnt": 11, "rowid": row_id})
    assert gsheets_adapter._values == [
        ["country", "cnt"],
        ["BR", 1],
        ["BR", 3],
        ["IN", 5],
        ["ZA", 6],
        ["UK", 11.0],
        ["PY", 11],
        ["UK", 10.0],
    ]

    gsheets_adapter.delete_row(row_id)
    assert gsheets_adapter._values == [
        ["country", "cnt"],
        ["BR", 1],
        ["BR", 3],
        ["IN", 5],
        ["ZA", 6],
        ["PY", 11],
        ["UK", 10.0],
    ]

    # test that get_values was called only once
    assert get_values.call_count == 1

    # test that changes were pushed
    assert insert.call_count == 1
    assert update.call_count == 1
    assert delete.call_count == 1

    gsheets_adapter.close()


def test_get_metadata(mocker, simple_sheet_adapter):
    mocker.patch(
        "shillelagh.adapters.api.gsheets.get_credentials",
        return_value="SECRET",
    )

    session = requests.Session()
    session.mount("https://", simple_sheet_adapter)
    mocker.patch(
        "shillelagh.adapters.api.gsheets.GSheetsAPI._get_session",
        return_value=session,
    )

    gsheets_adapter = GSheetsAPI(
        "https://docs.google.com/spreadsheets/d/1/edit",
        "XXX",
    )
    assert gsheets_adapter.get_metadata() == {
        "Sheet title": "Sheet1",
        "Spreadsheet title": "Sheet1",
    }

    simple_sheet_adapter.register_uri(
        "GET",
        "https://sheets.googleapis.com/v4/spreadsheets/1?includeGridData=false",
        json={
            "error": {
                "code": 404,
                "message": "Requested entity was not found.",
                "status": "NOT_FOUND",
            },
        },
    )

    assert gsheets_adapter.get_metadata() == {}

    simple_sheet_adapter.register_uri(
        "GET",
        "https://sheets.googleapis.com/v4/spreadsheets/1?includeGridData=false",
        json={
            "spreadsheetId": "1",
            "properties": {
                "title": "Sheet1",
                "locale": "en_US",
                "autoRecalc": "ON_CHANGE",
                "timeZone": "America/Los_Angeles",
                "defaultFormat": {
                    "backgroundColor": {"red": 1, "green": 1, "blue": 1},
                    "padding": {"top": 2, "right": 3, "bottom": 2, "left": 3},
                    "verticalAlignment": "BOTTOM",
                    "wrapStrategy": "OVERFLOW_CELL",
                    "textFormat": {
                        "foregroundColor": {},
                        "fontFamily": "arial,sans,sans-serif",
                        "fontSize": 10,
                        "bold": False,
                        "italic": False,
                        "strikethrough": False,
                        "underline": False,
                        "foregroundColorStyle": {"rgbColor": {}},
                    },
                    "backgroundColorStyle": {
                        "rgbColor": {"red": 1, "green": 1, "blue": 1},
                    },
                },
            },
            "sheets": [
                {
                    "properties": {
                        "sheetId": 1,
                        "title": "Sheet1",
                        "index": 0,
                        "sheetType": "GRID",
                        "gridProperties": {"rowCount": 985, "columnCount": 26},
                    },
                },
            ],
            "spreadsheetUrl": "https://docs.google.com/spreadsheets/d/1/edit?ouid=111430789371895352716&urlBuilderDomain=dealmeida.net",
        },
    )

    assert gsheets_adapter.get_metadata() == {}


def test_supports():
    assert GSheetsAPI.supports("https://docs.google.com/spreadsheets/d/1/edit")
    assert not GSheetsAPI.supports("https://github.com/betodealmeida/shillelagh/")

    assert not GSheetsAPI.supports("some_table")
    assert GSheetsAPI.supports(
        "some_table",
        catalog={"some_table": "https://docs.google.com/spreadsheets/d/1/edit"},
    )
    assert not GSheetsAPI.supports(
        "some_table",
        catalog={"some_table": "https://github.com/betodealmeida/shillelagh/"},
    )


def test_empty_middle_column(mocker):
    adapter = requests_mock.Adapter()
    adapter.register_uri(
        "GET",
        "https://docs.google.com/spreadsheets/d/1/gviz/tq?gid=0&tq=SELECT%20%2A%20LIMIT%201",
        json={
            "version": "0.6",
            "reqId": "0",
            "status": "ok",
            "sig": "1134071240",
            "table": {
                "cols": [
                    {"id": "A", "label": "one", "type": "string"},
                    {"id": "B", "label": "", "type": "string"},
                    {"id": "C", "label": "two", "type": "string"},
                    {
                        "id": "D",
                        "label": "three",
                        "type": "number",
                        "pattern": "General",
                    },
                    {
                        "id": "E",
                        "label": "four",
                        "type": "number",
                        "pattern": "General",
                    },
                ],
                "rows": [
                    {
                        "c": [
                            {"v": "test"},
                            None,
                            {"v": "test"},
                            {"v": 1.5, "f": "1.5"},
                            {"v": 10.1, "f": "10.1"},
                        ],
                    },
                ],
                "parsedNumHeaders": 0,
            },
        },
    )
    adapter.register_uri(
        "GET",
        "https://docs.google.com/spreadsheets/d/1/gviz/tq?gid=0&tq=SELECT%20%2A",
        json={
            "version": "0.6",
            "reqId": "0",
            "status": "ok",
            "sig": "1058539448",
            "table": {
                "cols": [
                    {"id": "A", "label": "one", "type": "string"},
                    {"id": "B", "label": "", "type": "string"},
                    {"id": "C", "label": "two", "type": "string"},
                    {
                        "id": "D",
                        "label": "three",
                        "type": "number",
                        "pattern": "General",
                    },
                    {
                        "id": "E",
                        "label": "four",
                        "type": "number",
                        "pattern": "General",
                    },
                ],
                "rows": [
                    {
                        "c": [
                            {"v": "test"},
                            None,
                            {"v": "test"},
                            {"v": 1.5, "f": "1.5"},
                            {"v": 10.1, "f": "10.1"},
                        ],
                    },
                    {
                        "c": [
                            {"v": "test2"},
                            None,
                            {"v": "test3"},
                            {"v": 0.1, "f": "0.1"},
                            {"v": 10.2, "f": "10.2"},
                        ],
                    },
                ],
                "parsedNumHeaders": 1,
            },
        },
    )

    entry_points = [FakeEntryPoint("gsheetsapi", GSheetsAPI)]
    mocker.patch(
        "shillelagh.backends.apsw.db.iter_entry_points",
        return_value=entry_points,
    )

    session = requests.Session()
    session.mount("https://", adapter)
    mocker.patch(
        "shillelagh.adapters.api.gsheets.GSheetsAPI._get_session",
        return_value=session,
    )

    connection = connect(":memory:", ["gsheetsapi"])
    cursor = connection.cursor()

    sql = '''SELECT * FROM "https://docs.google.com/spreadsheets/d/1/edit#gid=0"'''
    data = list(cursor.execute(sql))
    assert data == [("test", "test", 1.5, 10.1), ("test2", "test3", 0.1, 10.2)]
