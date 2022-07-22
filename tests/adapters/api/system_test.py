"""
Tests for the system adapter.
"""
from datetime import datetime, timezone
from unittest import mock

import pytest
from freezegun import freeze_time
from pytest_mock import MockerFixture

from shillelagh.backends.apsw.db import connect
from shillelagh.exceptions import ProgrammingError


def test_system(mocker: MockerFixture) -> None:
    """
    Test a simple query.
    """
    psutil = mocker.patch("shillelagh.adapters.api.system.psutil")
    psutil.cpu_count.return_value = 4
    psutil.cpu_percent.side_effect = [
        [1, 2, 3, 4],
        [1, 2, 3, 4],
        [1, 2, 3, 4],
        [1, 2, 3, 4],
    ]

    connection = connect(":memory:")
    cursor = connection.cursor()
    sql = """
        SELECT * FROM "system://cpu"
        LIMIT 2
    """
    with freeze_time("2021-01-01T00:00:00Z"):
        data = list(cursor.execute(sql))
    assert data == [
        (datetime(2021, 1, 1, tzinfo=timezone.utc), 0.01, 0.02, 0.03, 0.04),
        (datetime(2021, 1, 1, tzinfo=timezone.utc), 0.01, 0.02, 0.03, 0.04),
    ]

    psutil.cpu_percent.assert_has_calls(
        [
            mock.call(interval=1, percpu=True),
            mock.call(interval=1, percpu=True),
        ],
    )


def test_system_different_interval(mocker: MockerFixture) -> None:
    """
    Test a simple query with a custom interval.
    """
    psutil = mocker.patch("shillelagh.adapters.api.system.psutil")
    psutil.cpu_count.return_value = 4
    psutil.cpu_percent.side_effect = [
        [1, 2, 3, 4],
        [1, 2, 3, 4],
        [1, 2, 3, 4],
        [1, 2, 3, 4],
    ]

    connection = connect(":memory:")
    cursor = connection.cursor()
    sql = """
        SELECT * FROM "system://cpu?interval=2"
        LIMIT 2
    """
    with freeze_time("2021-01-01T00:00:00Z"):
        data = list(cursor.execute(sql))
    assert data == [
        (datetime(2021, 1, 1, tzinfo=timezone.utc), 0.01, 0.02, 0.03, 0.04),
        (datetime(2021, 1, 1, tzinfo=timezone.utc), 0.01, 0.02, 0.03, 0.04),
    ]

    psutil.cpu_percent.assert_has_calls(
        [
            mock.call(interval=2, percpu=True),
            mock.call(interval=2, percpu=True),
        ],
    )


def test_system_invalid_resource() -> None:
    """
    Test a query referencing an invalid resource.
    """
    connection = connect(":memory:")
    cursor = connection.cursor()
    sql = """
        SELECT * FROM "system://memory"
        LIMIT 2
    """
    with pytest.raises(ProgrammingError) as excinfo:
        list(cursor.execute(sql))
    assert str(excinfo.value) == "Unknown resource: memory"


def test_system_interrupt(mocker: MockerFixture) -> None:
    """
    Test query interrupt.
    """
    psutil = mocker.patch("shillelagh.adapters.api.system.psutil")
    psutil.cpu_count.return_value = 4
    psutil.cpu_percent.side_effect = [
        [1, 2, 3, 4],
        [1, 2, 3, 4],
        KeyboardInterrupt(),
    ]

    connection = connect(":memory:")
    cursor = connection.cursor()
    sql = """
        SELECT * FROM "system://cpu"
    """
    with freeze_time("2021-01-01T00:00:00Z"):
        data = list(cursor.execute(sql))
    assert data == [
        (datetime(2021, 1, 1, tzinfo=timezone.utc), 0.01, 0.02, 0.03, 0.04),
        (datetime(2021, 1, 1, tzinfo=timezone.utc), 0.01, 0.02, 0.03, 0.04),
    ]
