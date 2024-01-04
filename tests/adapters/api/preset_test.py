"""
Test the Preset adapter.
"""

import re
from datetime import timedelta

import pytest
from pytest_mock import MockerFixture
from requests_mock.mocker import Mocker

from shillelagh.adapters.api.preset import PresetAPI
from shillelagh.backends.apsw.db import connect

DO_NOT_CACHE = timedelta(seconds=-1)


def test_preset(mocker: MockerFixture, requests_mock: Mocker) -> None:
    """
    Test a simple query.
    """
    mocker.patch("shillelagh.adapters.api.generic_json.CACHE_EXPIRATION", DO_NOT_CACHE)

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
            },
        },
    )
    cursor = connection.cursor()

    sql = 'SELECT * FROM "https://api.app.preset.io/v1/teams/"'
    rows = list(cursor.execute(sql))
    assert rows == [(1, "Team 1")]


def test_preset_missing_token(mocker: MockerFixture) -> None:
    """
    Test a simple query.
    """
    mocker.patch("shillelagh.adapters.api.generic_json.CACHE_EXPIRATION", DO_NOT_CACHE)

    connection = connect(":memory:")
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
