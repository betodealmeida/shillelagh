"""
Tests for shillelagh.functions.
"""
import json

import apsw
import pkg_resources
import pytest
from pytest_mock import MockerFixture

from shillelagh.backends.apsw.db import connect
from shillelagh.exceptions import ProgrammingError
from shillelagh.functions import get_metadata

from .fakes import FakeAdapter, FakeEntryPoint


def test_sleep_from_sql(mocker: MockerFixture) -> None:
    """
    Test ``sleep``.
    """
    sleep = mocker.patch("time.sleep")
    entry_points = [FakeEntryPoint("dummy", FakeAdapter)]
    mocker.patch(
        "shillelagh.backends.apsw.db.iter_entry_points",
        return_value=entry_points,
    )
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


def test_get_metadata_from_sql(mocker: MockerFixture) -> None:
    """
    Test calling ``get_metadata`` from SQL.
    """
    mocker.patch(
        "shillelagh.functions.get_metadata",
        return_value=json.dumps({"hello": "world"}),
    )
    entry_points = [FakeEntryPoint("dummy", FakeAdapter)]
    mocker.patch(
        "shillelagh.backends.apsw.db.iter_entry_points",
        return_value=entry_points,
    )
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
    shillelagh_version = pkg_resources.get_distribution("shillelagh").version
    apsw_version = apsw.apswversion()  # pylint: disable=c-extension-no-member
    version = f"{shillelagh_version} (apsw {apsw_version})"
    assert cursor.fetchall() == [(version,)]
