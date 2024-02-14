"""
Test the Preset adapter.
"""

import re

import pytest
from pytest_mock import MockerFixture
from requests_mock.mocker import Mocker

from shillelagh.adapters.api.preset import PresetAPI, PresetWorkspaceAPI, get_urls
from shillelagh.backends.apsw.db import connect
from shillelagh.exceptions import ProgrammingError


def test_preset(requests_mock: Mocker) -> None:
    """
    Test a simple query.
    """
    # for datassette
    requests_mock.get(re.compile(".*-/versions.json.*"), status_code=404)

    requests_mock.post(
        "https://api.app.preset.io/v1/auth/",
        json={"payload": {"access_token": "SECRET"}},
    )
    requests_mock.get(
        "https://api.app.preset.io/v1/teams/",
        json={"payload": [{"id": 1, "name": "Team 1"}]},
    )

    connection = connect(
        ":memory:",
        adapter_kwargs={
            "presetapi": {
                "access_token": "XXX",
                "access_secret": "YYY",
                "cache_expiration": -1,
            },
        },
    )
    cursor = connection.cursor()

    sql = 'SELECT * FROM "https://api.app.preset.io/v1/teams/"'
    rows = list(cursor.execute(sql))
    assert rows == [(1, "Team 1")]


def test_preset_missing_token() -> None:
    """
    Test a simple query.
    """
    connection = connect(
        ":memory:",
        adapter_kwargs={"presetapi": {"cache_expiration": -1}},
    )
    cursor = connection.cursor()

    sql = 'SELECT * FROM "https://api.app.preset.io/v1/teams/"'
    with pytest.raises(ValueError) as exc_info:
        cursor.execute(sql)
    assert str(exc_info.value) == "access_token and access_secret must be provided"


def test_supports() -> None:
    """
    Test the ``supports`` method.
    """
    assert PresetAPI.supports("/etc/password") is False
    assert PresetAPI.supports("https://example.org/data.html") is False
    assert PresetAPI.supports("https://api.app.preset.io/v1/teams/") is True
    assert PresetAPI.supports("https://api.appxpreset.io/v1/teams/") is False
    assert PresetAPI.supports("https://api.app-sdx.preset.io/v1/teams/") is True
    assert (
        PresetAPI.supports(
            "https://abcdef01.us1a.app.preset.io/sqllab/?savedQueryId=1",
        )
        is False
    )

    assert PresetWorkspaceAPI.supports("https://api.app.preset.io/v1/teams/") is False
    assert (
        PresetWorkspaceAPI.supports(
            "https://abcdef01.us1a.app.preset.io/sqllab/?savedQueryId=1",
        )
        is True
    )
    assert (
        PresetWorkspaceAPI.supports(
            "https://abcdef01.us1a.app-sdx.preset.io/sqllab/?savedQueryId=1",
        )
        is True
    )


def test_get_urls() -> None:
    """
    Test the ``get_urls`` function.
    """
    gen = get_urls(
        "https://abcdef01.us1a.app-sdx.preset.io/api/v1/chart/",
        offset=45,
        limit=50,
        page_size=42,
    )

    url, slice_ = next(gen)
    assert (
        url
        == "https://abcdef01.us1a.app-sdx.preset.io/api/v1/chart/?q=(page:1,page_size:42)"
    )
    assert slice_.start == 3
    url, slice_ = next(gen)
    assert (
        url
        == "https://abcdef01.us1a.app-sdx.preset.io/api/v1/chart/?q=(page:2,page_size:11)"
    )
    assert slice_.start == 0
    with pytest.raises(StopIteration):
        next(gen)


def test_get_urls_unable_to_parse() -> None:
    """
    Test the ``get_urls`` function when the URL query can't be parsed.
    """

    gen = get_urls("https://example.org/?q=(((")
    assert next(gen)[0] == "https://example.org/?q=((("
    with pytest.raises(StopIteration):
        next(gen)


def test_get_urls_with_page_parameters() -> None:
    """
    Test the ``get_urls`` function when the URL already has page parameters.
    """

    gen = get_urls("https://example.org/?q=(page:0,page_size:42)")
    assert next(gen)[0] == "https://example.org/?q=(page:0,page_size:42)"
    with pytest.raises(StopIteration):
        next(gen)


def test_preset_workspace(requests_mock: Mocker) -> None:
    """
    Test a simple query to a Preset workspace.
    """
    # for datassette
    requests_mock.get(re.compile(".*-/versions.json.*"), status_code=404)

    requests_mock.post(
        "https://api.app.preset.io/v1/auth/",
        json={"payload": {"access_token": "SECRET"}},
    )
    requests_mock.get(
        "https://abcdef01.us1a.app.preset.io/api/v1/chart/?q=(page:0,page_size:100)",
        json={"result": [{"id": 1, "slice_name": "Team 1"}]},
    )
    requests_mock.get(
        "https://abcdef01.us1a.app.preset.io/api/v1/chart/?q=(page:1,page_size:100)",
        json={"result": []},
    )

    connection = connect(
        ":memory:",
        adapter_kwargs={
            "presetworkspaceapi": {
                "access_token": "XXX",
                "access_secret": "YYY",
                "cache_expiration": -1,
            },
        },
    )
    cursor = connection.cursor()

    sql = 'SELECT * FROM "https://abcdef01.us1a.app.preset.io/api/v1/chart/"'
    rows = list(cursor.execute(sql))
    assert rows == [(1, "Team 1")]


def test_preset_workspace_pagination(requests_mock: Mocker) -> None:
    """
    Test pagination in a query to a Preset workspace.
    """
    # for datassette
    requests_mock.get(re.compile(".*-/versions.json.*"), status_code=404)

    requests_mock.post(
        "https://api.app.preset.io/v1/auth/",
        json={"payload": {"access_token": "SECRET"}},
    )
    requests_mock.get(
        "https://abcdef01.us1a.app.preset.io/api/v1/chart/?q=(page:0,page_size:100)",
        json={
            "result": [{"id": i + 1, "slice_name": f"Team {i+1}"} for i in range(100)],
        },
    )
    requests_mock.get(
        "https://abcdef01.us1a.app.preset.io/api/v1/chart/?q=(page:1,page_size:3)",
        json={
            "result": [
                {"id": i + 101, "slice_name": f"Team {i+101}"} for i in range(3)
            ],
        },
    )

    connection = connect(
        ":memory:",
        adapter_kwargs={
            "presetworkspaceapi": {
                "access_token": "XXX",
                "access_secret": "YYY",
                "cache_expiration": -1,
            },
        },
    )
    cursor = connection.cursor()

    sql = 'SELECT * FROM "https://abcdef01.us1a.app.preset.io/api/v1/chart/" LIMIT 5 OFFSET 98'
    rows = list(cursor.execute(sql))
    assert rows == [
        (99, "Team 99"),
        (100, "Team 100"),
        (101, "Team 101"),
        (102, "Team 102"),
        (103, "Team 103"),
    ]


def test_preset_workspace_error(requests_mock: Mocker) -> None:
    """
    Test error handling when accessing a workspace API.
    """
    # for datassette
    requests_mock.get(re.compile(".*-/versions.json.*"), status_code=404)

    requests_mock.post(
        "https://api.app.preset.io/v1/auth/",
        json={"payload": {"access_token": "SECRET"}},
    )
    requests_mock.get(
        "https://abcdef01.us1a.app.preset.io/api/v1/chart/?q=(page:0,page_size:100)",
        json={
            "errors": [
                {
                    "message": "Your session has expired. Please refresh the page to sign in.",
                    "error_type": "GENERIC_BACKEND_ERROR",
                    "level": "error",
                    "extra": {
                        "issue_codes": [
                            {
                                "code": 1011,
                                "message": "Issue 1011 - Superset encountered an unexpected error.",
                            },
                        ],
                    },
                },
            ],
        },
        status_code=500,
    )

    connection = connect(
        ":memory:",
        adapter_kwargs={
            "presetworkspaceapi": {
                "access_token": "XXX",
                "access_secret": "YYY",
                "cache_expiration": -1,
            },
        },
    )
    cursor = connection.cursor()

    sql = 'SELECT * FROM "https://abcdef01.us1a.app.preset.io/api/v1/chart/"'
    with pytest.raises(ProgrammingError) as excinfo:
        cursor.execute(sql)
    assert (
        str(excinfo.value)
        == "Error: Your session has expired. Please refresh the page to sign in."
    )


def test_preset_workspace_no_urls(mocker: MockerFixture, requests_mock: Mocker) -> None:
    """
    Test when no URLs are returned.
    """
    mocker.patch("shillelagh.adapters.api.preset.get_urls", return_value=[])

    requests_mock.post(
        "https://api.app.preset.io/v1/auth/",
        json={"payload": {"access_token": "SECRET"}},
    )

    adapter = PresetWorkspaceAPI(
        "https://abcdef01.us1a.app.preset.io/api/v1/chart/",
        access_token="XXX",
        access_secret="YYY",
        cache_expiration=-1,
    )
    assert list(adapter.get_data({}, [])) == []
