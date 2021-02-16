import datetime
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


class MockEntryPoint:
    def __init__(self, name: str, adapter: Adapter):
        self.name = name
        self.adapter = adapter

    def load(self) -> Adapter:
        return self.adapter


def test_credentials(mocker):
    entry_points = [MockEntryPoint("gsheetsapi", GSheetsAPI)]
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
    entry_points = [MockEntryPoint("gsheetsapi", GSheetsAPI)]
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
        "https://docs.google.com/spreadsheets/d/1/gviz/tq?gid=0&tq=SELECT%20%2A%20LIMIT%200",
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
                "rows": [],
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

    sql = "SELECT * FROM " '"https://docs.google.com/spreadsheets/d/1/edit#gid=0"'
    data = list(cursor.execute(sql))
    assert data == [
        ("BR", 1),
        ("BR", 3),
        ("IN", 5),
        ("ZA", 6),
        ("CR", 10),
    ]


def test_execute_filter(mocker):
    entry_points = [MockEntryPoint("gsheetsapi", GSheetsAPI)]
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
        "https://docs.google.com/spreadsheets/d/1/gviz/tq?gid=0&tq=SELECT%20%2A%20LIMIT%200",
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
                "rows": [],
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
    assert quote(1) == "1"
    assert quote(datetime.datetime.now()) == "'2020-01-01T00:00:00'"
    assert quote(datetime.time(0, 0, 0)) == "'00:00:00'"
    assert quote(datetime.date.today()) == "'2020-01-01'"

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
