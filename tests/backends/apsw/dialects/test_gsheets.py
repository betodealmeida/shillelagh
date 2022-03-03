"""
Test for shillelagh.backends.apsw.dialects.gsheets.
"""
from unittest import mock

import pytest
import requests
from pytest_mock import MockerFixture
from requests_mock.mocker import Mocker
from sqlalchemy.engine import create_engine
from sqlalchemy.engine.url import make_url

from shillelagh.backends.apsw.dialects.gsheets import APSWGSheetsDialect, extract_query
from shillelagh.exceptions import ProgrammingError

from ....fakes import incidents


def test_gsheets_dialect() -> None:
    """
    Test the basic usage of the dialect
    """
    dialect = APSWGSheetsDialect()
    assert dialect.create_connect_args(make_url("gsheets://")) == (
        (),
        {
            "path": ":memory:",
            "adapters": ["gsheetsapi"],
            "adapter_kwargs": {
                "gsheetsapi": {
                    "access_token": None,
                    "service_account_file": None,
                    "service_account_info": None,
                    "subject": None,
                    "catalog": {},
                    "app_default_credentials": False,
                },
            },
            "safe": True,
            "isolation_level": None,
        },
    )

    dialect = APSWGSheetsDialect(
        service_account_info={"secret": "XXX"},
        subject="user@example.com",
    )
    assert dialect.create_connect_args(make_url("gsheets://")) == (
        (),
        {
            "path": ":memory:",
            "adapters": ["gsheetsapi"],
            "adapter_kwargs": {
                "gsheetsapi": {
                    "access_token": None,
                    "service_account_file": None,
                    "service_account_info": {"secret": "XXX"},
                    "subject": "user@example.com",
                    "catalog": {},
                    "app_default_credentials": False,
                },
            },
            "safe": True,
            "isolation_level": None,
        },
    )

    dialect = APSWGSheetsDialect(
        service_account_file="credentials.json",
        subject="user@example.com",
        catalog={"public_sheet": "https://example.com/"},
    )
    assert dialect.create_connect_args(make_url("gsheets://")) == (
        (),
        {
            "path": ":memory:",
            "adapters": ["gsheetsapi"],
            "adapter_kwargs": {
                "gsheetsapi": {
                    "access_token": None,
                    "service_account_file": "credentials.json",
                    "service_account_info": None,
                    "subject": "user@example.com",
                    "catalog": {"public_sheet": "https://example.com/"},
                    "app_default_credentials": False,
                },
            },
            "safe": True,
            "isolation_level": None,
        },
    )

    dialect = APSWGSheetsDialect(
        app_default_credentials=True,
    )
    assert dialect.create_connect_args(make_url("gsheets://")) == (
        (),
        {
            "path": ":memory:",
            "adapters": ["gsheetsapi"],
            "adapter_kwargs": {
                "gsheetsapi": {
                    "access_token": None,
                    "service_account_file": None,
                    "service_account_info": None,
                    "subject": None,
                    "catalog": {},
                    "app_default_credentials": True,
                },
            },
            "safe": True,
            "isolation_level": None,
        },
    )

    mock_dbapi_connection = mock.MagicMock()
    assert dialect.get_schema_names(mock_dbapi_connection) == []


def test_get_table_names(mocker: MockerFixture, requests_mock: Mocker) -> None:
    """
    Test ``get_table_names``.
    """
    get_credentials = mocker.patch(
        "shillelagh.backends.apsw.dialects.gsheets.get_credentials",
    )

    session = requests.Session()
    session.mount("https://", requests_mock)
    mocker.patch(
        "shillelagh.backends.apsw.dialects.gsheets.AuthorizedSession",
        return_value=session,
    )
    requests_mock.register_uri(
        "GET",
        (
            "https://www.googleapis.com/drive/v3/files?"
            "q=mimeType='application/vnd.google-apps.spreadsheet'"
        ),
        json={"files": [{"id": 1}, {"id": 2}, {"id": 3}]},
    )
    requests_mock.register_uri(
        "GET",
        "https://sheets.googleapis.com/v4/spreadsheets/1?includeGridData=false",
        json={
            "sheets": [
                {"properties": {"sheetId": 0}},
                {"properties": {"sheetId": 1}},
            ],
        },
    )
    requests_mock.register_uri(
        "GET",
        "https://sheets.googleapis.com/v4/spreadsheets/2?includeGridData=false",
        json={"sheets": [{"properties": {"sheetId": 0}}]},
    )
    requests_mock.register_uri(
        "GET",
        "https://sheets.googleapis.com/v4/spreadsheets/3?includeGridData=false",
        json={
            "error": {
                "code": 403,
                "message": (
                    "Google Sheets API has not been used in project 1034909279888 "
                    "before or it is disabled. Enable it by visiting "
                    "https://console.developers.google.com/apis/api/"
                    "sheets.googleapis.com/overview?project=1034909279888 then "
                    "retry. If you enabled this API recently, wait a few minutes "
                    "for the action to propagate to our systems and retry."
                ),
                "status": "PERMISSION_DENIED",
                "details": [
                    {
                        "@type": "type.googleapis.com/google.rpc.Help",
                        "links": [
                            {
                                "description": "Google developers console API activation",
                                "url": (
                                    "https://console.developers.google.com/apis/api/"
                                    "sheets.googleapis.com/overview?project=1034909279888"
                                ),
                            },
                        ],
                    },
                    {
                        "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                        "reason": "SERVICE_DISABLED",
                        "domain": "googleapis.com",
                        "metadata": {
                            "consumer": "projects/1034909279888",
                            "service": "sheets.googleapis.com",
                        },
                    },
                ],
            },
        },
    )
    _logger = mocker.patch("shillelagh.backends.apsw.dialects.gsheets._logger")

    engine = create_engine("gsheets://", list_all_sheets=True)

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

    _logger.warning.assert_called_with(
        "Error loading sheets from file: %s",
        (
            "Google Sheets API has not been used in project 1034909279888 before "
            "or it is disabled. Enable it by visiting "
            "https://console.developers.google.com/apis/api/"
            "sheets.googleapis.com/overview?project=1034909279888 then retry. "
            "If you enabled this API recently, wait a few minutes for the action "
            "to propagate to our systems and retry."
        ),
    )


def test_drive_api_disabled(mocker: MockerFixture, requests_mock: Mocker) -> None:
    """
    Test error message when the Drive API is disabled.
    """
    get_credentials = mocker.patch(
        "shillelagh.backends.apsw.dialects.gsheets.get_credentials",
    )

    session = requests.Session()
    session.mount("https://", requests_mock)
    mocker.patch(
        "shillelagh.backends.apsw.dialects.gsheets.AuthorizedSession",
        return_value=session,
    )
    requests_mock.register_uri(
        "GET",
        (
            "https://www.googleapis.com/drive/v3/files?"
            "q=mimeType='application/vnd.google-apps.spreadsheet'"
        ),
        json={
            "error": {
                "errors": [
                    {
                        "domain": "usageLimits",
                        "reason": "accessNotConfigured",
                        "message": (
                            "Access Not Configured. Drive API has not been used in "
                            "project 1034909279888 before or it is disabled. Enable "
                            "it by visiting https://console.developers.google.com/apis/api/"
                            "drive.googleapis.com/overview?project=1034909279888 then retry. "
                            "If you enabled this API recently, wait a few minutes for the "
                            "action to propagate to our systems and retry."
                        ),
                        "extendedHelp": (
                            "https://console.developers.google.com/apis/api/"
                            "drive.googleapis.com/overview?project=1034909279888"
                        ),
                    },
                ],
                "code": 403,
                "message": (
                    "Access Not Configured. Drive API has not been used in project "
                    "1034909279888 before or it is disabled. Enable it by visiting "
                    "https://console.developers.google.com/apis/api/"
                    "drive.googleapis.com/overview?project=1034909279888 then retry. "
                    "If you enabled this API recently, wait a few minutes for the "
                    "action to propagate to our systems and retry."
                ),
            },
        },
    )

    engine = create_engine("gsheets://", list_all_sheets=True)

    get_credentials.return_value = "SECRET"
    with pytest.raises(ProgrammingError) as excinfo:
        engine.table_names()

    assert str(excinfo.value) == (
        "Access Not Configured. Drive API has not been used in project 1034909279888 "
        "before or it is disabled. Enable it by visiting "
        "https://console.developers.google.com/apis/api/"
        "drive.googleapis.com/overview?project=1034909279888 then retry. If you "
        "enabled this API recently, wait a few minutes for the action to propagate "
        "to our systems and retry."
    )


def test_extract_query() -> None:
    """
    Test ``extract_query``.

    Older version of SQLAlchemy (<1.4) have a bug in ``make_url``.
    """
    assert extract_query(make_url("gsheets://")) == {}
    assert extract_query(make_url("gsheets://host")) == {}
    assert extract_query(make_url("gsheets://?foo=bar")) == {"foo": "bar"}
    assert extract_query(make_url("gsheets:///?foo=bar")) == {"foo": "bar"}


def test_do_ping(mocker: MockerFixture, requests_mock: Mocker) -> None:
    """
    Test ``do_ping``.
    """
    connection = mocker.MagicMock()

    dialect = APSWGSheetsDialect()

    requests_mock.get(
        "https://www.google.com/appsstatus/dashboard/incidents.json",
        json=incidents,
    )
    assert dialect.do_ping(connection)

    outage = {
        "service_name": "Google Sheets",
        "modified": "2021-04-12T17:00:00+00:00",
        "most_recent_update": {"status": "SERVICE_OUTAGE"},
    }
    requests_mock.get(
        "https://www.google.com/appsstatus/dashboard/incidents.json",
        json=[outage],
    )
    assert dialect.do_ping(connection) is False
