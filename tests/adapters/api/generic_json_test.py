"""
Test the generic JSON adapter.
"""

import pytest
from pytest_mock import MockerFixture
from requests import Session
from requests_mock.mocker import Mocker
from yarl import URL

from shillelagh.adapters.api.generic_json import GenericJSONAPI
from shillelagh.backends.apsw.db import connect
from shillelagh.exceptions import ProgrammingError
from shillelagh.typing import Maybe

baseurl = URL("https://api.stlouisfed.org/fred/series")


def test_generic_json(mocker: MockerFixture, requests_mock: Mocker) -> None:
    """
    Test a simple query.
    """
    mocker.patch(
        "shillelagh.adapters.api.generic_json.requests_cache.CachedSession",
        return_value=Session(),
    )

    # for datassette and other probing adapters
    requests_mock.head(
        "https://api.stlouisfed.org/-/versions.json?"
        "series_id=GNPCA&"
        "api_key=abcdefghijklmnopqrstuvwxyz123456&"
        "file_type=json#$.seriess%5B*%5D",
        status_code=404,
    )

    params = {
        "series_id": "GNPCA",
        "api_key": "abcdefghijklmnopqrstuvwxyz123456",
        "file_type": "json",
    }
    url = (baseurl % params).with_fragment("$.seriess[*]")
    requests_mock.head(str(url), headers={"content-type": "application/json"})
    requests_mock.get(
        str(url),
        json={
            "realtime_start": "2022-11-01",
            "realtime_end": "2022-11-01",
            "seriess": [
                {
                    "id": "GNPCA",
                    "realtime_start": "2022-11-01",
                    "realtime_end": "2022-11-01",
                    "title": "Real Gross National Product",
                    "observation_start": "1929-01-01",
                    "observation_end": "2021-01-01",
                    "frequency": "Annual",
                    "frequency_short": "A",
                    "units": "Billions of Chained 2012 Dollars",
                    "units_short": "Bil. of Chn. 2012 $",
                    "seasonal_adjustment": "Not Seasonally Adjusted",
                    "seasonal_adjustment_short": "NSA",
                    "last_updated": "2022-09-29 07:45:54-05",
                    "popularity": 16,
                    "notes": "BEA Account Code: A001RX\n\n",
                },
            ],
        },
    )

    connection = connect(":memory:")
    cursor = connection.cursor()

    sql = f'SELECT * FROM "{url}"'
    data = list(cursor.execute(sql))
    assert data == [
        (
            "GNPCA",
            "2022-11-01",
            "2022-11-01",
            "Real Gross National Product",
            "1929-01-01",
            "2021-01-01",
            "Annual",
            "A",
            "Billions of Chained 2012 Dollars",
            "Bil. of Chn. 2012 $",
            "Not Seasonally Adjusted",
            "NSA",
            "2022-09-29 07:45:54-05",
            16,
            "BEA Account Code: A001RX\n\n",
        ),
    ]

    requests_mock.head(
        "https://example.org/data.json",
        headers={"content-type": "application/json"},
    )
    requests_mock.get(
        "https://example.org/data.json",
        json=[{"a": 1, "b": [10, 20]}, {"a": 2, "b": [11]}],
    )
    sql = 'SELECT a, b FROM "https://example.org/data.json"'
    data = list(cursor.execute(sql))
    assert data == [(1, "[10, 20]"), (2, "[11]")]

    requests_mock.get(
        "https://example.org/data.json",
        json={"message": "An error occurred"},
        status_code=500,
    )
    with pytest.raises(ProgrammingError) as excinfo:
        list(cursor.execute(sql))
    assert str(excinfo.value) == "Error: An error occurred"


def test_generic_json_complex_type(
    mocker: MockerFixture,
    requests_mock: Mocker,
) -> None:
    """
    Test a query where columns are complex.
    """
    mocker.patch(
        "shillelagh.adapters.api.generic_json.requests_cache.CachedSession",
        return_value=Session(),
    )

    # for datassette and other probing adapters
    requests_mock.head("https://exmaple.org/-/versions.json", status_code=404)

    url = URL("https://example.org/")
    requests_mock.head(str(url), headers={"content-type": "application/json"})
    requests_mock.get(
        str(url),
        json=[
            {
                "foo": "bar",
                "baz": ["one", "two"],
            },
        ],
    )

    connection = connect(":memory:")
    cursor = connection.cursor()

    sql = f'SELECT * FROM "{url}"'
    data = list(cursor.execute(sql))
    assert data == [("bar", '["one", "two"]')]


def test_supports(requests_mock: Mocker) -> None:
    """
    Test the ``supports`` method.
    """
    requests_mock.head(
        "https://example.org/data.html",
        headers={"content-type": "text/html"},
    )
    requests_mock.head(
        "https://example.org/data.json",
        headers={"content-type": "application/json"},
    )

    assert GenericJSONAPI.supports("/etc/password") is False
    assert GenericJSONAPI.supports("https://example.org/data.html") is Maybe
    assert GenericJSONAPI.supports("https://example.org/data.html", fast=False) is False
    assert GenericJSONAPI.supports("https://example.org/data.json", fast=False) is True
