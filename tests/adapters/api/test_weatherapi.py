import os
from datetime import datetime
from datetime import timezone

import apsw
import pytest
from shillelagh.adapters.api.weatherapi import WeatherAPI
from shillelagh.adapters.base import Adapter
from shillelagh.backends.apsw.db import connect
from shillelagh.backends.apsw.vt import VTModule

from ...fakes import FakeEntryPoint


def test_weatherapi(requests_mock):
    url = "https://api.weatherapi.com/v1/history.json?key=f426b51ea9aa4e4ab68190907202309&q=94923&dt=2020-10-20"
    payload = {
        "forecast": {
            "forecastday": [
                {
                    "hour": [
                        {"time": "2020-10-20 11:00", "temp_c": 20.0},
                        {"time": "2020-10-20 12:00", "temp_c": 20.1},
                        {"time": "2020-10-20 13:00", "temp_c": 20.2},
                    ],
                },
            ],
        },
    }
    requests_mock.get(url, json=payload)

    connection = apsw.Connection(":memory:")
    cursor = connection.cursor()
    connection.createmodule("weatherapi", VTModule(WeatherAPI))
    cursor.execute(
        f"""CREATE VIRTUAL TABLE bodega_bay USING weatherapi('94923', '"f426b51ea9aa4e4ab68190907202309"')""",
    )

    sql = "SELECT * FROM bodega_bay WHERE ts = '2020-10-20T12:00:00'"
    data = list(cursor.execute(sql))
    assert data == [(20.1, "2020-10-20T12:00:00")]

    try:
        os.unlink("weatherapi_cache.sqlite")
    except FileNotFoundError:
        pass


def test_weatherapi_impossible(requests_mock):
    url = "https://api.weatherapi.com/v1/history.json?key=f426b51ea9aa4e4ab68190907202309&q=94923&dt=2020-10-20"
    payload = {
        "forecast": {
            "forecastday": [
                {
                    "hour": [
                        {"time": "2020-10-20 11:00", "temp_c": 20.0},
                        {"time": "2020-10-20 12:00", "temp_c": 20.1},
                        {"time": "2020-10-20 13:00", "temp_c": 20.2},
                    ],
                },
            ],
        },
    }
    requests_mock.get(url, json=payload)

    connection = apsw.Connection(":memory:")
    cursor = connection.cursor()
    connection.createmodule("weatherapi", VTModule(WeatherAPI))
    cursor.execute(
        f"""CREATE VIRTUAL TABLE bodega_bay USING weatherapi('94923', '"f426b51ea9aa4e4ab68190907202309"')""",
    )

    sql = "SELECT * FROM bodega_bay WHERE ts = '2020-10-20T12:00:00' AND ts = '2020-10-21T12:00:00'"
    with pytest.raises(Exception) as excinfo:
        cursor.execute(sql)

    assert str(excinfo.value) == "Invalid filter"


def test_weatherapi_api_error(requests_mock):
    url1 = "https://api.weatherapi.com/v1/history.json?key=f426b51ea9aa4e4ab68190907202309&q=94923&dt=2020-10-20"
    payload = {
        "forecast": {
            "forecastday": [
                {
                    "hour": [
                        {"time": "2020-10-20 11:00", "temp_c": 20.0},
                        {"time": "2020-10-20 12:00", "temp_c": 20.1},
                        {"time": "2020-10-20 13:00", "temp_c": 20.2},
                    ],
                },
            ],
        },
    }
    requests_mock.get(url1, json=payload)

    url2 = "https://api.weatherapi.com/v1/history.json?key=f426b51ea9aa4e4ab68190907202309&q=94923&dt=2020-10-21"
    requests_mock.get(url2, status_code=404)

    connection = apsw.Connection(":memory:")
    cursor = connection.cursor()
    connection.createmodule("weatherapi", VTModule(WeatherAPI))
    cursor.execute(
        f"""CREATE VIRTUAL TABLE bodega_bay USING weatherapi('94923', '"f426b51ea9aa4e4ab68190907202309"')""",
    )

    sql = "SELECT * FROM bodega_bay WHERE ts >= '2020-10-20T12:00:00' AND ts <= '2020-10-21T12:00:00'"
    data = list(cursor.execute(sql))
    assert data == [
        (20.1, "2020-10-20T12:00:00"),
        (20.2, "2020-10-20T13:00:00"),
    ]

    try:
        os.unlink("weatherapi_cache.sqlite")
    except FileNotFoundError:
        pass


def test_dispatch(mocker, requests_mock):
    entry_points = [FakeEntryPoint("weatherapi", WeatherAPI)]
    mocker.patch(
        "shillelagh.backends.apsw.db.iter_entry_points",
        return_value=entry_points,
    )

    url = "https://api.weatherapi.com/v1/history.json?key=f426b51ea9aa4e4ab68190907202309&q=94923&dt=2020-10-20"
    payload = {
        "forecast": {
            "forecastday": [
                {
                    "hour": [
                        {"time": "2020-10-20 11:00", "temp_c": 20.0},
                        {"time": "2020-10-20 12:00", "temp_c": 20.1},
                        {"time": "2020-10-20 13:00", "temp_c": 20.2},
                    ],
                },
            ],
        },
    }
    requests_mock.get(url, json=payload)

    connection = connect(":memory:", ["weatherapi"])
    cursor = connection.cursor()

    sql = (
        "SELECT * FROM "
        '"https://api.weatherapi.com/v1/history.json?key=f426b51ea9aa4e4ab68190907202309&q=94923" '
        "WHERE ts = '2020-10-20T12:00:00'"
    )
    data = list(cursor.execute(sql))
    assert data == [(20.1, datetime(2020, 10, 20, 19, 0, tzinfo=timezone.utc))]

    try:
        os.unlink("weatherapi_cache.sqlite")
    except FileNotFoundError:
        pass
