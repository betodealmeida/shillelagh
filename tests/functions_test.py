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
from shillelagh.functions import get_metadata

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
