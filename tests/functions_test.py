"""
Tests for shillelagh.functions.
"""

import json
import subprocess
import sys

import apsw
import pytest
from pytest_mock import MockerFixture

from shillelagh.adapters.registry import AdapterLoader
from shillelagh.backends.apsw.db import connect
from shillelagh.exceptions import ProgrammingError
from shillelagh.functions import date_trunc, get_metadata, upgrade

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


def test_upgrade(mocker: MockerFixture) -> None:
    """
    Test ``upgrade`` with uv available.
    """
    subprocess_run = mocker.patch("shillelagh.functions.subprocess.run")
    subprocess_run.return_value = mocker.MagicMock(returncode=0)
    importlib = mocker.patch("shillelagh.functions.importlib")

    output = upgrade("1.2.3")

    assert output == "Upgrade to 1.2.3 successful using uv."
    subprocess_run.assert_called_once_with(
        ["uv", "pip", "install", "shillelagh==1.2.3"],
        capture_output=True,
        text=True,
        check=True,
        timeout=300,
    )
    importlib.reload.assert_called_with(sys.modules["shillelagh"])


def test_upgrade_invalid_version() -> None:
    """
    Test ``upgrade`` with an invalid version.
    """
    output = upgrade("1.2.3 malicious_package=0.1")
    assert output == "Invalid version: 1.2.3 malicious_package=0.1"


def test_upgrade_fallback_to_pip(mocker: MockerFixture) -> None:
    """
    Test ``upgrade`` falling back to pip when uv is not available.
    """
    subprocess_run = mocker.patch("shillelagh.functions.subprocess.run")
    # First call (uv) fails with FileNotFoundError
    # Second call (pip) succeeds
    subprocess_run.side_effect = [
        FileNotFoundError("uv not found"),
        mocker.MagicMock(returncode=0),
    ]
    importlib = mocker.patch("shillelagh.functions.importlib")

    output = upgrade("1.2.3")

    assert output == "Upgrade to 1.2.3 successful using pip."
    assert subprocess_run.call_count == 2
    # Check that pip was called with correct arguments
    subprocess_run.assert_called_with(
        [sys.executable, "-m", "pip", "install", "shillelagh==1.2.3"],
        capture_output=True,
        text=True,
        check=True,
        timeout=300,
    )
    importlib.reload.assert_called_with(sys.modules["shillelagh"])


def test_upgrade_all_fail(mocker: MockerFixture) -> None:
    """
    Test ``upgrade`` when all package managers fail.
    """
    subprocess_run = mocker.patch("shillelagh.functions.subprocess.run")
    subprocess_run.side_effect = [
        FileNotFoundError("uv not found"),
        FileNotFoundError("pip not found"),
        FileNotFoundError("pipx not found"),
    ]

    output = upgrade("1.2.3")

    assert "Upgrade failed. Tried:" in output
    assert "uv: Not found" in output
    assert "pip: Not found" in output
    assert "pipx: Not found" in output
    assert subprocess_run.call_count == 3


def test_upgrade_subprocess_error(mocker: MockerFixture) -> None:
    """
    Test ``upgrade`` with subprocess errors.
    """
    subprocess_run = mocker.patch("shillelagh.functions.subprocess.run")

    # Create a mock CalledProcessError
    error = subprocess.CalledProcessError(
        1,
        ["uv", "pip", "install", "shillelagh==1.2.3"],
        stderr="Error: Package not found",
    )
    subprocess_run.side_effect = [
        error,
        FileNotFoundError("pip not found"),
        FileNotFoundError("pipx not found"),
    ]

    output = upgrade("1.2.3")

    assert "Upgrade failed. Tried:" in output
    assert "uv: Error: Package not found" in output
    assert subprocess_run.call_count == 3


def test_upgrade_timeout(mocker: MockerFixture) -> None:
    """
    Test ``upgrade`` with subprocess timeout.
    """
    subprocess_run = mocker.patch("shillelagh.functions.subprocess.run")
    subprocess_run.side_effect = [
        subprocess.TimeoutExpired(["uv", "pip", "install", "shillelagh==1.2.3"], 300),
        FileNotFoundError("pip not found"),
        FileNotFoundError("pipx not found"),
    ]

    output = upgrade("1.2.3")

    assert "Upgrade failed. Tried:" in output
    assert "uv: Timeout after 5 minutes" in output
    assert subprocess_run.call_count == 3


def test_upgrade_generic_exception(mocker: MockerFixture) -> None:
    """
    Test ``upgrade`` with generic exception.
    """
    subprocess_run = mocker.patch("shillelagh.functions.subprocess.run")
    subprocess_run.side_effect = [
        RuntimeError("Unexpected error"),
        FileNotFoundError("pip not found"),
        FileNotFoundError("pipx not found"),
    ]

    output = upgrade("1.2.3")

    assert "Upgrade failed. Tried:" in output
    assert "uv: Unexpected error" in output
    assert subprocess_run.call_count == 3
