import json
from unittest import mock

import requests
import requests_mock
from shillelagh.backends.apsw.dialects.gsheets import APSWGSheetsDialect
from shillelagh.backends.apsw.dialects.gsheets import extract_query
from sqlalchemy.engine import create_engine
from sqlalchemy.engine.url import make_url


def test_gsheets_dialect():
    dialect = APSWGSheetsDialect()
    assert dialect.create_connect_args(make_url("gsheets://")) == (
        (
            ":memory:",
            ["gsheetsapi"],
            {"gsheetsapi": (None, None, None, None)},
            True,
            None,
        ),
        {},
    )

    dialect = APSWGSheetsDialect(
        service_account_info={"secret": "XXX"},
        subject="user@example.com",
    )
    assert dialect.create_connect_args(make_url("gsheets://")) == (
        (
            ":memory:",
            ["gsheetsapi"],
            {"gsheetsapi": (None, None, {"secret": "XXX"}, "user@example.com")},
            True,
            None,
        ),
        {},
    )

    dialect = APSWGSheetsDialect(
        service_account_file="credentials.json",
        subject="user@example.com",
    )
    assert dialect.create_connect_args(make_url("gsheets://")) == (
        (
            ":memory:",
            ["gsheetsapi"],
            {"gsheetsapi": (None, "credentials.json", None, "user@example.com")},
            True,
            None,
        ),
        {},
    )

    mock_dbapi_connection = mock.MagicMock()
    assert dialect.get_schema_names(mock_dbapi_connection) == []


def test_get_table_names(mocker):
    get_credentials = mocker.patch(
        "shillelagh.backends.apsw.dialects.gsheets.get_credentials",
    )

    adapter = requests_mock.Adapter()
    session = requests.Session()
    session.mount("https://", adapter)
    mocker.patch(
        "shillelagh.backends.apsw.dialects.gsheets.AuthorizedSession",
        return_value=session,
    )
    adapter.register_uri(
        "GET",
        "https://www.googleapis.com/drive/v3/files?q=mimeType='application/vnd.google-apps.spreadsheet'",
        json={"files": [{"id": 1}, {"id": 2}]},
    )
    adapter.register_uri(
        "GET",
        "https://sheets.googleapis.com/v4/spreadsheets/1?includeGridData=false",
        json={
            "sheets": [
                {"properties": {"sheetId": 0}},
                {"properties": {"sheetId": 1}},
            ],
        },
    )
    adapter.register_uri(
        "GET",
        "https://sheets.googleapis.com/v4/spreadsheets/2?includeGridData=false",
        json={"sheets": [{"properties": {"sheetId": 0}}]},
    )

    engine = create_engine("gsheets://")

    get_credentials.return_value = None
    tables = engine.table_names()
    assert tables == []

    get_credentials.return_value = "SECRET"
    tables = engine.table_names()
    assert tables == [
        "https://docs.google.com/spreadsheets/d/1/edit#gid=0",
        "https://docs.google.com/spreadsheets/d/1/edit#gid=1",
        "https://docs.google.com/spreadsheets/d/2/edit#gid=0",
    ]


def test_extract_query():
    assert extract_query(make_url("gsheets://")) == {}
    assert extract_query(make_url("gsheets://?foo=bar")) == {"foo": "bar"}
    assert extract_query(make_url("gsheets:///?foo=bar")) == {"foo": "bar"}
