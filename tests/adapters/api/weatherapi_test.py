# pylint: disable=c-extension-no-member
"""
Tests for shillelagh.adapters.api.weatherapi.
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict

import pytest
from pytest_mock import MockerFixture
from requests import Session
from requests_mock.mocker import Mocker

from shillelagh.adapters.api.weatherapi import WeatherAPI, combine_time_filters
from shillelagh.backends.apsw.db import connect
from shillelagh.exceptions import ImpossibleFilterError
from shillelagh.fields import Order
from shillelagh.filters import Equal, Filter, Impossible, Operator, Range

from ...fakes import weatherapi_response


def test_weatherapi(mocker: MockerFixture, requests_mock: Mocker) -> None:
    """
    Test the adapter.
    """
    mocker.patch(
        "shillelagh.adapters.api.weatherapi.requests_cache.CachedSession",
        return_value=Session(),
    )

    url = "https://api.weatherapi.com/v1/history.json?key=XXX&q=iceland&dt=2021-03-17"
    requests_mock.get(url, json=weatherapi_response)

    connection = connect(":memory:")
    cursor = connection.cursor()
    sql = """
SELECT *
FROM "https://api.weatherapi.com/v1/history.json?key=XXX&q=iceland"
WHERE time = '2021-03-17T12:00:00+00:00'
    """
    data = list(cursor.execute(sql))
    assert data == [
        (
            "0",
            "0",
            90,
            6.1,
            43.0,
            3.3,
            37.9,
            34.6,
            21.5,
            6.7,
            44.1,
            96,
            True,
            0.01,
            0.13,
            30.7,
            1023.0,
            6.7,
            44.1,
            datetime(2021, 3, 17, 12, 0, tzinfo=timezone.utc),
            1615982400.0,
            2.0,
            1.0,
            False,
            False,
            139,
            "SE",
            19.4,
            12.1,
            3.3,
            37.9,
        ),
    ]


def test_weatherapi_impossible(requests_mock: Mocker) -> None:
    """
    Test the adapter with impossible filters.
    """
    url = "https://api.weatherapi.com/v1/history.json?key=XXX&q=iceland&dt=2021-03-17"
    requests_mock.get(url, json=weatherapi_response)

    connection = connect(":memory:")
    cursor = connection.cursor()
    sql = """
SELECT *
FROM "https://api.weatherapi.com/v1/history.json?key=XXX&q=iceland"
WHERE time = '2021-03-17T12:00:00+00:00' AND time = '2021-03-18T12:00:00+00:00'
    """
    data = list(cursor.execute(sql))
    assert data == []

    sql = """
SELECT *
FROM "https://api.weatherapi.com/v1/history.json?key=XXX&q=iceland"
WHERE time_epoch = 0 AND time_epoch = 1
    """
    data = list(cursor.execute(sql))
    assert data == []


def test_weatherapi_api_error(mocker: MockerFixture, requests_mock: Mocker) -> None:
    """
    Test handling errors in the API.
    """
    mocker.patch(
        "shillelagh.adapters.api.weatherapi.requests_cache.CachedSession",
        return_value=Session(),
    )

    url1 = "https://api.weatherapi.com/v1/history.json?key=XXX&q=iceland&dt=2021-03-17"
    requests_mock.get(url1, json=weatherapi_response)

    url2 = "https://api.weatherapi.com/v1/history.json?key=XXX&q=iceland&dt=2021-03-18"
    requests_mock.get(url2, status_code=404)

    connection = connect(":memory:")
    cursor = connection.cursor()
    sql = """
SELECT *
FROM "https://api.weatherapi.com/v1/history.json?key=XXX&q=iceland"
WHERE time >= '2021-03-17T12:00:00+00:00' AND time <= '2021-03-18T12:00:00+00:00'
    """
    data = list(cursor.execute(sql))
    print(data)
    assert data == [
        (
            "0",
            "0",
            90,
            6.1,
            43.0,
            3.3,
            37.9,
            34.6,
            21.5,
            6.7,
            44.1,
            96,
            True,
            0.01,
            0.13,
            30.7,
            1023.0,
            6.7,
            44.1,
            datetime(2021, 3, 17, 12, 0, tzinfo=timezone.utc),
            1615982400.0,
            2.0,
            1.0,
            False,
            False,
            139,
            "SE",
            19.4,
            12.1,
            3.3,
            37.9,
        ),
        (
            "51",
            "0",
            85,
            6.2,
            43.2,
            3.5,
            38.2,
            36.7,
            22.8,
            6.9,
            44.5,
            95,
            True,
            0.0,
            0.07,
            30.7,
            1023.0,
            6.9,
            44.5,
            datetime(2021, 3, 17, 13, 0, tzinfo=timezone.utc),
            1615986000.0,
            4.7,
            2.0,
            False,
            False,
            137,
            "SE",
            20.8,
            12.9,
            3.5,
            38.2,
        ),
        (
            "2",
            "0",
            81,
            6.3,
            43.3,
            3.6,
            38.5,
            38.9,
            24.2,
            7.2,
            44.9,
            94,
            True,
            0.0,
            0.0,
            30.7,
            1024.0,
            7.2,
            44.9,
            datetime(2021, 3, 17, 14, 0, tzinfo=timezone.utc),
            1615989600.0,
            7.3,
            4.0,
            False,
            False,
            136,
            "SE",
            22.1,
            13.7,
            3.6,
            38.5,
        ),
        (
            "0",
            "0",
            76,
            6.4,
            43.5,
            3.8,
            38.8,
            41.0,
            25.5,
            7.4,
            45.3,
            93,
            True,
            0.0,
            0.0,
            30.7,
            1024.0,
            7.4,
            45.3,
            datetime(2021, 3, 17, 15, 0, tzinfo=timezone.utc),
            1615993200.0,
            10.0,
            6.0,
            False,
            False,
            135,
            "SE",
            23.4,
            14.5,
            3.8,
            38.8,
        ),
        (
            "2",
            "0",
            80,
            6.2,
            43.2,
            3.6,
            38.4,
            42.6,
            26.5,
            7.3,
            45.1,
            93,
            True,
            0.0,
            0.0,
            30.7,
            1024.0,
            7.3,
            45.1,
            datetime(2021, 3, 17, 16, 0, tzinfo=timezone.utc),
            1615996800.0,
            10.0,
            6.0,
            False,
            False,
            136,
            "SE",
            24.4,
            15.1,
            3.6,
            38.4,
        ),
        (
            "21",
            "0",
            84,
            6.1,
            42.9,
            3.3,
            38.0,
            44.2,
            27.4,
            7.2,
            45.0,
            92,
            True,
            0.0,
            0.03,
            30.7,
            1024.0,
            7.2,
            45.0,
            datetime(2021, 3, 17, 17, 0, tzinfo=timezone.utc),
            1616000400.0,
            10.0,
            6.0,
            False,
            False,
            137,
            "SE",
            25.3,
            15.7,
            3.3,
            38.0,
        ),
        (
            "0",
            "0",
            88,
            5.9,
            42.6,
            3.1,
            37.6,
            45.7,
            28.4,
            7.1,
            44.8,
            92,
            True,
            0.0,
            0.04,
            30.7,
            1024.0,
            7.1,
            44.8,
            datetime(2021, 3, 17, 18, 0, tzinfo=timezone.utc),
            1616004000.0,
            10.0,
            6.0,
            False,
            False,
            138,
            "SE",
            26.3,
            16.3,
            3.1,
            37.6,
        ),
        (
            "21",
            "0",
            90,
            5.9,
            42.6,
            3.1,
            37.6,
            44.8,
            27.8,
            7.0,
            44.7,
            93,
            True,
            0.0,
            0.02,
            30.7,
            1024.0,
            7.0,
            44.7,
            datetime(2021, 3, 17, 19, 0, tzinfo=timezone.utc),
            1616007600.0,
            10.0,
            6.0,
            False,
            False,
            138,
            "SE",
            25.4,
            15.8,
            3.1,
            37.6,
        ),
        (
            "3",
            "0",
            92,
            5.9,
            42.6,
            3.1,
            37.6,
            43.8,
            27.2,
            7.0,
            44.5,
            93,
            False,
            0.0,
            0.0,
            30.7,
            1025.0,
            7.0,
            44.5,
            datetime(2021, 3, 17, 20, 0, tzinfo=timezone.utc),
            1616011200.0,
            10.0,
            6.0,
            False,
            False,
            138,
            "SE",
            24.6,
            15.3,
            3.1,
            37.6,
        ),
        (
            "0",
            "0",
            94,
            5.9,
            42.6,
            3.1,
            37.6,
            42.8,
            26.6,
            6.9,
            44.4,
            93,
            False,
            0.0,
            0.0,
            30.8,
            1025.0,
            6.9,
            44.4,
            datetime(2021, 3, 17, 21, 0, tzinfo=timezone.utc),
            1616014800.0,
            10.0,
            6.0,
            False,
            False,
            138,
            "SE",
            23.8,
            14.8,
            3.1,
            37.6,
        ),
        (
            "3",
            "0",
            94,
            5.8,
            42.5,
            3.1,
            37.5,
            42.0,
            26.1,
            6.8,
            44.3,
            93,
            False,
            0.0,
            0.0,
            30.8,
            1026.0,
            6.8,
            44.3,
            datetime(2021, 3, 17, 22, 0, tzinfo=timezone.utc),
            1616018400.0,
            10.0,
            6.0,
            False,
            False,
            140,
            "SE",
            23.3,
            14.5,
            3.1,
            37.5,
        ),
        (
            "3",
            "0",
            94,
            5.8,
            42.4,
            3.0,
            37.5,
            41.2,
            25.6,
            6.8,
            44.2,
            93,
            False,
            0.0,
            0.0,
            30.8,
            1026.0,
            6.8,
            44.2,
            datetime(2021, 3, 17, 23, 0, tzinfo=timezone.utc),
            1616022000.0,
            10.0,
            6.0,
            False,
            False,
            142,
            "SE",
            22.8,
            14.2,
            3.0,
            37.5,
        ),
    ]


def test_dispatch(mocker: MockerFixture, requests_mock: Mocker) -> None:
    """
    Test the dispatcher.
    """
    mocker.patch(
        "shillelagh.adapters.api.weatherapi.requests_cache.CachedSession",
        return_value=Session(),
    )

    url = "https://api.weatherapi.com/v1/history.json?key=XXX&q=iceland&dt=2021-03-17"
    requests_mock.get(url, json=weatherapi_response)

    connection = connect(":memory:", ["weatherapi"])
    cursor = connection.cursor()

    sql = (
        "SELECT * FROM "
        '"https://api.weatherapi.com/v1/history.json?key=XXX&q=iceland" '
        "WHERE time = '2021-03-17T12:00:00+00:00'"
    )
    data = list(cursor.execute(sql))
    assert data == [
        (
            "0",
            "0",
            90,
            6.1,
            43.0,
            3.3,
            37.9,
            34.6,
            21.5,
            6.7,
            44.1,
            96,
            True,
            0.01,
            0.13,
            30.7,
            1023.0,
            6.7,
            44.1,
            datetime(2021, 3, 17, 12, 0, tzinfo=timezone.utc),
            1615982400.0,
            2.0,
            1.0,
            False,
            False,
            139,
            "SE",
            19.4,
            12.1,
            3.3,
            37.9,
        ),
    ]


def test_dispatch_api_key_connection(
    mocker: MockerFixture,
    requests_mock: Mocker,
) -> None:
    """
    Test passing the key via the adapter kwargs.
    """
    mocker.patch(
        "shillelagh.adapters.api.weatherapi.requests_cache.CachedSession",
        return_value=Session(),
    )

    url = "https://api.weatherapi.com/v1/history.json?key=YYY&q=iceland&dt=2021-03-17"
    data = requests_mock.get(url, json=weatherapi_response)

    connection = connect(
        ":memory:",
        ["weatherapi"],
        adapter_kwargs={"weatherapi": {"api_key": "YYY"}},
    )
    cursor = connection.cursor()

    sql = (
        "SELECT * FROM "
        '"https://api.weatherapi.com/v1/history.json?q=iceland" '
        "WHERE time = '2021-03-17T12:00:00+00:00'"
    )
    cursor.execute(sql)

    assert data.call_count == 1


def test_dispatch_impossible(mocker: MockerFixture) -> None:
    """
    Test that no data is returned on an impossible constraint.

    Shillelagh should identify the impossible constraint and not do
    any network requests.
    """
    session = mocker.patch(
        "shillelagh.adapters.api.weatherapi.requests_cache.CachedSession",
    )

    connection = connect(
        ":memory:",
        ["weatherapi"],
        adapter_kwargs={"weatherapi": {"api_key": "YYY"}},
    )
    cursor = connection.cursor()

    sql = (
        "SELECT * FROM "
        '"https://api.weatherapi.com/v1/history.json?q=iceland" '
        "WHERE time = '2021-03-17T12:00:00+00:00' "
        "AND time_epoch < 0"
    )
    data = list(cursor.execute(sql))
    assert data == []

    assert session.return_value.get.call_count == 0


def test_combine_time_filters() -> None:
    """
    Test queries with both ``time`` and ``time_epoch``.
    """
    bounds: Dict[str, Filter] = {
        "time": Range(datetime(2021, 1, 1, tzinfo=timezone.utc)),
        "time_epoch": Range(
            None,
            datetime(2022, 1, 1, tzinfo=timezone.utc).timestamp(),
        ),
    }
    assert combine_time_filters(bounds) == Range(
        datetime(2021, 1, 1, tzinfo=timezone.utc),
        datetime(2022, 1, 1, tzinfo=timezone.utc),
    )

    bounds = {
        "time": Range(datetime(2021, 1, 1, tzinfo=timezone.utc)),
        "time_epoch": Range(
            None,
            datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp(),
        ),
    }
    with pytest.raises(ImpossibleFilterError):
        combine_time_filters(bounds)

    bounds = {
        "time": Range(datetime(2021, 1, 1, tzinfo=timezone.utc)),
        "time_epoch": Impossible(),
    }
    with pytest.raises(ImpossibleFilterError):
        combine_time_filters(bounds)

    bounds = {
        "time": Equal(datetime(2021, 1, 1, tzinfo=timezone.utc)),
        "time_epoch": Range(
            None,
            datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp(),
        ),
    }
    with pytest.raises(Exception) as excinfo:
        combine_time_filters(bounds)  # type: ignore
    assert str(excinfo.value) == "Invalid filter"


@pytest.mark.integration_test
def test_integration(adapter_kwargs: Dict[str, Any]) -> None:
    """
    Full integration test reading from the API.
    """
    connection = connect(":memory:", adapter_kwargs=adapter_kwargs)
    cursor = connection.cursor()

    sql = """
        SELECT *
        FROM "https://api.weatherapi.com/v1/history.json?q=London"
        WHERE time = ?
    """
    cursor.execute(
        sql,
        (
            datetime.now().replace(
                hour=12,
                minute=0,
                second=0,
                microsecond=0,
                tzinfo=timezone.utc,
            )
            - timedelta(days=3),
        ),
    )
    data = cursor.fetchall()
    assert len(data) == 1
    assert len(data[0]) == 31


def test_get_cost(mocker: MockerFixture) -> None:
    """
    Test ``get_cost``.
    """
    mocker.patch(
        "shillelagh.adapters.api.weatherapi.requests_cache.CachedSession",
        return_value=Session(),
    )

    adapter = WeatherAPI("location", "XXX")
    assert adapter.get_cost([], []) == 0
    assert adapter.get_cost([("one", Operator.GT)], []) == 7000
    assert adapter.get_cost([("one", Operator.GT), ("two", Operator.EQ)], []) == 8000
    assert (
        adapter.get_cost(
            [("one", Operator.GT), ("two", Operator.EQ)],
            [("one", Order.ASCENDING)],
        )
        == 8000
    )

    adapter = WeatherAPI("location", "XXX", 14)
    assert adapter.get_cost([], []) == 0
    assert adapter.get_cost([("one", Operator.GT)], []) == 14000
    assert adapter.get_cost([("one", Operator.GT), ("two", Operator.EQ)], []) == 15000
    assert (
        adapter.get_cost(
            [("one", Operator.GT), ("two", Operator.EQ)],
            [("one", Order.ASCENDING)],
        )
        == 15000
    )


def test_window(mocker: MockerFixture) -> None:
    """
    Test the default window size of days to fetch data.
    """
    session = mocker.patch(
        "shillelagh.adapters.api.weatherapi.requests_cache.CachedSession",
    )
    session.return_value.get.return_value.json.return_value = weatherapi_response

    adapter = WeatherAPI("location", "XXX")
    list(adapter.get_data({}, []))
    assert session.return_value.get.call_count == 7

    session.reset_mock()
    adapter = WeatherAPI("location", "XXX", 14)
    list(adapter.get_data({}, []))
    assert session.return_value.get.call_count == 14
