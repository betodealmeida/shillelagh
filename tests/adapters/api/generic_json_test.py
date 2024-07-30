"""
Test the generic JSON adapter.
"""

import re

import pytest
from requests_mock.mocker import Mocker
from yarl import URL

from shillelagh.adapters.api.generic_json import GenericJSONAPI
from shillelagh.backends.apsw.db import connect
from shillelagh.exceptions import ProgrammingError
from shillelagh.typing import Maybe

baseurl = URL("https://api.stlouisfed.org/fred/series")


def test_generic_json(requests_mock: Mocker) -> None:
    """
    Test a simple query.
    """
    # for datassette
    requests_mock.get(re.compile(".*-/versions.json.*"), status_code=404)

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

    connection = connect(
        ":memory:",
        adapter_kwargs={"genericjsonapi": {"cache_expiration": -1}},
    )
    cursor = connection.cursor()

    sql = f'SELECT * FROM "{url}"'
    rows = list(cursor.execute(sql))
    assert rows == [
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
    rows = list(cursor.execute(sql))
    assert rows == [(1, "[10, 20]"), (2, "[11]")]

    requests_mock.get(
        "https://example.org/data.json",
        json={
            "error": {"code": 1002, "message": "API key is invalid or not provided."},
        },
        status_code=500,
    )
    with pytest.raises(ProgrammingError) as excinfo:
        list(cursor.execute(sql))
    assert str(excinfo.value) == "Error: API key is invalid or not provided."


def test_generic_json_complex_type(requests_mock: Mocker) -> None:
    """
    Test a query where columns are complex.
    """
    # for datassette and other probing adapters
    requests_mock.head("https://example.org/-/versions.json", status_code=404)

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

    connection = connect(
        ":memory:",
        adapter_kwargs={"genericjsonapi": {"cache_expiration": -1}},
    )
    cursor = connection.cursor()

    sql = f'SELECT * FROM "{url}"'
    rows = list(cursor.execute(sql))
    assert rows == [("bar", '["one", "two"]')]


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


def test_request_headers(requests_mock: Mocker) -> None:
    """
    Test passing requests headers.
    """
    supports = requests_mock.head(
        "https://example.org/data.json",
        headers={"content-type": "application/json"},
    )

    # for datassette and other probing adapters
    requests_mock.head("https://example.org/-/versions.json", status_code=404)

    url = URL("https://example.org/")
    data = requests_mock.head(str(url), headers={"content-type": "application/json"})
    requests_mock.get(
        str(url),
        json=[
            {
                "foo": "bar",
                "baz": ["one", "two"],
            },
        ],
    )

    # test the supports method
    GenericJSONAPI.supports(
        "https://example.org/data.json",
        fast=False,
        request_headers={"foo": "bar"},
        cache_expiration=-1,
    )
    assert supports.last_request.headers["foo"] == "bar"

    connection = connect(
        ":memory:",
        adapter_kwargs={
            "genericjsonapi": {
                "request_headers": {"foo": "bar"},
                "cache_expiration": -1,
            },
        },
    )
    cursor = connection.cursor()

    sql = f'SELECT * FROM "{url}"'
    rows = list(cursor.execute(sql))
    assert rows == [("bar", '["one", "two"]')]
    assert data.last_request.headers["foo"] == "bar"


def test_request_headers_in_url(requests_mock: Mocker) -> None:
    """
    Test passing requests headers.
    """
    supports = requests_mock.head(
        "https://example.org/data.json",
        headers={"content-type": "application/json"},
    )

    # for datassette and other probing adapters
    requests_mock.head("https://example.org/-/versions.json", status_code=404)

    url = URL("https://example.org/")
    data = requests_mock.head(str(url), headers={"content-type": "application/json"})
    requests_mock.get(
        str(url),
        json=[
            {
                "foo": "bar",
                "baz": ["one", "two"],
            },
        ],
    )

    # test the supports method
    GenericJSONAPI.supports(
        "https://example.org/data.json?_s_headers=(foo:bar)",
        fast=False,
        cache_expiration=-1,
    )
    assert supports.last_request.headers["foo"] == "bar"

    connection = connect(
        ":memory:",
        adapter_kwargs={"genericjsonapi": {"cache_expiration": -1}},
    )
    cursor = connection.cursor()

    sql = 'SELECT * FROM "https://example.org/?_s_headers=(foo:bar)"'
    rows = list(cursor.execute(sql))
    assert rows == [("bar", '["one", "two"]')]
    assert data.last_request.headers["foo"] == "bar"


def test_single_row(requests_mock: Mocker) -> None:
    """
    Test a query where the response is a single row as a dictionary.
    """
    # for datassette
    requests_mock.get(re.compile(".*-/versions.json.*"), status_code=404)

    url = "https://www.boredapi.com/api/activity?participants=1#$"
    requests_mock.head(str(url), headers={"content-type": "application/json"})
    requests_mock.get(
        str(url),
        json={
            "activity": "Solve a Rubik's cube",
            "type": "recreational",
            "participants": 1,
            "price": 0,
            "link": "",
            "key": "4151544",
            "accessibility": 0.1,
        },
    )

    connection = connect(
        ":memory:",
        adapter_kwargs={"genericjsonapi": {"cache_expiration": -1}},
    )
    cursor = connection.cursor()

    sql = f'SELECT * FROM "{url}"'
    rows = list(cursor.execute(sql))
    assert rows == [("Solve a Rubik's cube", "recreational", 1, 0, "", "4151544", 0.1)]


def test_generic_json_array(requests_mock: Mocker) -> None:
    """
    Test a query where the response has only arrays.
    """
    # for datassette and other probing adapters
    requests_mock.get(
        "https://api.github.com/repos/apache/superset/-/versions.json",
        status_code=404,
    )

    url = URL("https://api.github.com/repos/apache/superset/stats/punch_card")
    requests_mock.head(str(url), headers={"content-type": "application/json"})
    requests_mock.get(
        str(url),
        json=[
            [0, 0, 15],
            [0, 1, 8],
            [0, 2, 6],
            [0, 3, 3],
            [0, 4, 2],
            None,
        ],
    )

    connection = connect(
        ":memory:",
        adapter_kwargs={"genericjsonapi": {"cache_expiration": -1}},
    )
    cursor = connection.cursor()

    sql = f'SELECT * FROM "{url}"'
    rows = list(cursor.execute(sql))
    assert rows == [
        (0, 0, 15),
        (0, 1, 8),
        (0, 2, 6),
        (0, 3, 3),
        (0, 4, 2),
        (None, None, None),
    ]
    assert cursor.description
    assert {t[0] for t in cursor.description} == {"col_0", "col_1", "col_2"}
