"""
Tests for shillelagh.functions.
"""

import json
import sys

import apsw
import pytest
from pytest_mock import MockerFixture

from shillelagh.adapters.registry import AdapterLoader
from shillelagh.backends.apsw.db import connect
from shillelagh.exceptions import ProgrammingError
from shillelagh.functions import date_trunc, get_metadata

from .fakes import FakeAdapter

if sys.version_info < (3, 10):
    from importlib_metadata import distribution
else:
    from importlib.metadata import distribution


def test_sleep_from_sql(mocker: MockerFixture, registry: AdapterLoader) -> None:
    """
    Test ``sleep``.
    """
    sleep = mocker.patch("time.sleep")
    registry.add("dummy", FakeAdapter)
    connection = connect(":memory:", ["dummy"], isolation_level="IMMEDIATE")
    cursor = connection.cursor()
    cursor.execute("SELECT sleep(5)")

    sleep.assert_called_with(5)


def test_get_metadata() -> None:
    """
    Test ``get_metadata``.
    """
    assert (
        get_metadata(
            {"dummy": {"key": "value"}, "other": {"one": "two"}},
            [FakeAdapter],
            "dummy://",
        )
        == '{"extra": {}, "adapter": "FakeAdapter"}'
    )

    with pytest.raises(ProgrammingError) as excinfo:
        get_metadata({}, [], "dummy://")
    assert str(excinfo.value) == "Unsupported table: dummy://"

    with pytest.raises(ProgrammingError) as excinfo:
        get_metadata({}, [FakeAdapter], "invalid://")
    assert str(excinfo.value) == "Unsupported table: invalid://"


def test_get_metadata_from_sql(mocker: MockerFixture, registry: AdapterLoader) -> None:
    """
    Test calling ``get_metadata`` from SQL.
    """
    mocker.patch(
        "shillelagh.functions.get_metadata",
        return_value=json.dumps({"hello": "world"}),
    )
    registry.add("dummy", FakeAdapter)
    connection = connect(":memory:", ["dummy"], isolation_level="IMMEDIATE")
    cursor = connection.cursor()
    cursor.execute('SELECT get_metadata("dummy://")')
    assert cursor.fetchall() == [('{"hello": "world"}',)]


def test_version_from_sql() -> None:
    """
    Test calling ``version`` from SQL.
    """
    connection = connect(":memory:")
    cursor = connection.cursor()
    cursor.execute("SELECT version()")
    shillelagh_version = distribution("shillelagh").version
    apsw_version = apsw.apswversion()  # pylint: disable=c-extension-no-member
    version = f"{shillelagh_version} (apsw {apsw_version})"
    assert cursor.fetchall() == [(version,)]


def test_date_trunc() -> None:
    """
    Test the ``date_trunc`` function.
    """
    assert date_trunc(None, "YEAR") is None

    assert date_trunc("2024-02-03T04:05:06.700000", "YEAR") == "2024-01-01T00:00:00"
    assert date_trunc("2024-02-03T04:05:06.700000", "QUARTER") == "2024-01-01T00:00:00"
    assert date_trunc("2024-02-03T04:05:06.700000", "MONTH") == "2024-02-01T00:00:00"
    assert date_trunc("2024-02-03T04:05:06.700000", "WEEK") == "2024-01-29T00:00:00"
    assert date_trunc("2024-02-03T04:05:06.700000", "DAY") == "2024-02-03T00:00:00"
    assert date_trunc("2024-02-03T04:05:06.700000", "HOUR") == "2024-02-03T04:00:00"
    assert date_trunc("2024-02-03T04:05:06.700000", "MINUTE") == "2024-02-03T04:05:00"
    assert date_trunc("2024-02-03T04:05:06.700000", "SECOND") == "2024-02-03T04:05:06"

    with pytest.raises(ValueError) as excinfo:
        date_trunc("2024-02-03T04:05:06.700000", "INVALID")
    assert str(excinfo.value) == "Unsupported truncation unit: invalid"
