import datetime
import json
from unittest import mock

import apsw
import pytest
import requests
import requests_mock
from freezegun import freeze_time
from shillelagh.adapters.api.gsheets import format_error_message
from shillelagh.adapters.api.gsheets import get_url
from shillelagh.adapters.api.gsheets import GSheetsAPI
from shillelagh.adapters.api.gsheets import quote
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


def test_credentials(mocker):
    entry_points = [FakeEntryPoint("gsheetsapi", GSheetsAPI)]
    mocker.patch(
        "shillelagh.backends.apsw.db.iter_entry_points",
        return_value=entry_points,
    )

    connection = connect(
        ":memory:",
        ["gsheetsapi"],
        adapter_args={
            "gsheetsapi": (
                {"secret": "XXX"},
                "user@example.com",
            ),
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
                """CREATE VIRTUAL TABLE "https://docs.google.com/spreadsheets/d/1" USING GSheetsAPI('"https://docs.google.com/spreadsheets/d/1"', '{"secret": "XXX"}', '"user@example.com"')""",
            ),
            mock.call('SELECT 1 FROM "https://docs.google.com/spreadsheets/d/1"', None),
        ],
    )


def test_execute(mocker):
    entry_points = [FakeEntryPoint("gsheetsapi", GSheetsAPI)]
    mocker.patch(
        "shillelagh.backends.apsw.db.iter_entry_points",
        return_value=entry_points,
    )

    adapter = requests_mock.Adapter()
    session = requests.Session()
    session.mount("https://docs.google.com/spreadsheets/d/1", adapter)
    mocker.patch(
        "shillelagh.adapters.api.gsheets.GSheetsAPI._get_session",
        return_value=session,
    )
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


def test_execute_filter(mocker):
    entry_points = [FakeEntryPoint("gsheetsapi", GSheetsAPI)]
    mocker.patch(
        "shillelagh.backends.apsw.db.iter_entry_points",
        return_value=entry_points,
    )

    adapter = requests_mock.Adapter()
    session = requests.Session()
    session.mount("https://docs.google.com/spreadsheets/d/1", adapter)
    mocker.patch(
        "shillelagh.adapters.api.gsheets.GSheetsAPI._get_session",
        return_value=session,
    )
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
        "https://docs.google.com/spreadsheets/d/1/gviz/tq?gid=0&tq=SELECT%20%2A%20WHERE%20B%20%3E%205.0",
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
                    {"c": [{"v": "ZA"}, {"v": 6.0, "f": "6"}]},
                    {"c": [{"v": "CR"}, {"v": 10.0, "f": "10"}]},
                ],
                "parsedNumHeaders": 1,
            },
        },
    )
    adapter.register_uri(
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


@freeze_time("2020-01-01")
def test_quote():
    assert quote("value") == "'value'"
    assert quote(True) == "true"
    assert quote(False) == "false"
    assert quote(1) == "1"
    assert quote(datetime.datetime.now()) == "datetime '2020-01-01 00:00:00'"
    assert quote(datetime.time(0, 0, 0)) == "timeofday '00:00:00'"
    assert quote(datetime.date.today()) == "date '2020-01-01'"

    with pytest.raises(Exception) as excinfo:
        quote([1])
    assert str(excinfo.value) == "Can't quote value: [1]"


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
    session.mount("https://docs.google.com/spreadsheets/d/2", adapter)
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
        "shillelagh.adapters.api.gsheets.Credentials.from_service_account_info",
        return_value="SECRET",
    )

    # prevent network call
    mocker.patch(
        "shillelagh.adapters.api.gsheets.GSheetsAPI._set_columns",
        mock.MagicMock(),
    )

    adapter = GSheetsAPI("https://docs.google.com/spreadsheets/d/1")
    adapter._get_session()
    mock_authorized_session.assert_not_called()
    mock_session.assert_called()

    mock_authorized_session.reset_mock()
    mock_session.reset_mock()

    adapter = GSheetsAPI(
        "https://docs.google.com/spreadsheets/d/1",
        service_account_info={"secret": "XXX"},
        subject="user@example.com",
    )
    assert adapter.credentials == "SECRET"
    adapter._get_session()
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
    session.mount("https://docs.google.com/spreadsheets/d/3", adapter)
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
    with pytest.raises(ProgrammingError):
        cursor.execute(sql)


def test_execute_json_prefix(mocker):
    entry_points = [FakeEntryPoint("gsheetsapi", GSheetsAPI)]
    mocker.patch(
        "shillelagh.backends.apsw.db.iter_entry_points",
        return_value=entry_points,
    )

    adapter = requests_mock.Adapter()
    session = requests.Session()
    session.mount("https://docs.google.com/spreadsheets/d/4", adapter)
    mocker.patch(
        "shillelagh.adapters.api.gsheets.GSheetsAPI._get_session",
        return_value=session,
    )
    adapter.register_uri(
        "GET",
        "https://docs.google.com/spreadsheets/d/4/gviz/tq?gid=0&tq=SELECT%20%2A%20LIMIT%201",
        json={
            "version": "0.6",
            "reqId": "0",
            "status": "ok",
            "sig": "2050160589",
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
        "https://docs.google.com/spreadsheets/d/4/gviz/tq?gid=0&tq=SELECT%20%2A",
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

    sql = '''SELECT * FROM "https://docs.google.com/spreadsheets/d/4/edit#gid=0"'''
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
    session.mount("https://docs.google.com/spreadsheets/d/5", adapter)
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
    session.mount("https://docs.google.com/spreadsheets/d/6", adapter)
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


def test_build_sql(mocker):
    # prevent network call
    mocker.patch(
        "shillelagh.adapters.api.gsheets.GSheetsAPI._set_columns",
        mock.MagicMock(),
    )

    adapter = GSheetsAPI("https://docs.google.com/spreadsheets/d/1")
    adapter._column_map = {f"col{i}_": letter for i, letter in enumerate("ABCDE")}

    bounds = {}
    order = []
    assert adapter._build_sql(bounds, order) == "SELECT *"

    bounds = {
        "col0_": Impossible(),
        "col1_": Equal(1),
        "col2_": Range(start=0, end=1, include_start=True, include_end=False),
        "col3_": Range(start=None, end=1, include_start=False, include_end=True),
        "col4_": Range(start=0, end=None, include_start=False, include_end=True),
    }
    order = [("col0_", Order.ASCENDING), ("col1_", Order.DESCENDING)]
    assert adapter._build_sql(bounds, order) == (
        "SELECT * WHERE 1 = 0 AND B = 1 AND C >= 0 AND C < 1 AND D <= 1 AND E > 0 "
        "ORDER BY col0_, col1_ DESC"
    )

    bounds = {"col0_": 1}
    order = []
    with pytest.raises(ProgrammingError) as excinfo:
        adapter._build_sql(bounds, order)
    assert str(excinfo.value) == "Invalid filter: 1"


def test_headers_not_detected(mocker):
    entry_points = [FakeEntryPoint("gsheetsapi", GSheetsAPI)]
    mocker.patch(
        "shillelagh.backends.apsw.db.iter_entry_points",
        return_value=entry_points,
    )

    adapter = requests_mock.Adapter()
    session = requests.Session()
    session.mount("https://docs.google.com/spreadsheets/d/7", adapter)
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
    session.mount("https://docs.google.com/spreadsheets/d/8", adapter)
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
