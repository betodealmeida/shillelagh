"""
Tests for the system adapter.
"""

from datetime import datetime, timezone
from unittest import mock

from freezegun import freeze_time
from pytest_mock import MockerFixture

from shillelagh.adapters.api.system import SystemAPI
from shillelagh.backends.apsw.db import connect


def test_system_cpu(mocker: MockerFixture) -> None:
    """
    Test a simple CPU query.
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


def test_system_memory(mocker: MockerFixture) -> None:
    """
    Test a simple memory query.
    """
    mocker.patch("shillelagh.adapters.api.system.time")
    psutil = mocker.patch("shillelagh.adapters.api.system.psutil")
    psutil.virtual_memory()._asdict.return_value = {
        "total": 34359738368,
        "available": 15130095616,
        "percent": 56.0,
        "used": 18285113344,
        "free": 1579941888,
        "active": 13551853568,
        "inactive": 13460545536,
        "wired": 4733259776,
    }

    connection = connect(":memory:")
    cursor = connection.cursor()
    sql = """
        SELECT * FROM "system://memory"
        LIMIT 1
    """
    with freeze_time("2021-01-01T00:00:00Z"):
        data = list(cursor.execute(sql))
    assert data == [
        (
            datetime(2021, 1, 1, 0, 0, tzinfo=timezone.utc),
            34359738368,
            15130095616,
            56.0,
            18285113344,
            1579941888,
            13551853568,
            13460545536,
            4733259776,
        ),
    ]


def test_system_swap(mocker: MockerFixture) -> None:
    """
    Test a simple swap memory query.
    """
    mocker.patch("shillelagh.adapters.api.system.time")
    psutil = mocker.patch("shillelagh.adapters.api.system.psutil")
    psutil.swap_memory()._asdict.return_value = {
        "total": 18253611008,
        "used": 16865034240,
        "free": 1388576768,
        "percent": 92.4,
        "sin": 1010873262080,
        "sout": 4259106816,
    }

    connection = connect(":memory:")
    cursor = connection.cursor()
    sql = """
        SELECT * FROM "system://swap"
        LIMIT 1
    """
    with freeze_time("2021-01-01T00:00:00Z"):
        data = list(cursor.execute(sql))
    assert data == [
        (
            datetime(2021, 1, 1, 0, 0, tzinfo=timezone.utc),
            18253611008,
            16865034240,
            1388576768,
            92.4,
            1010873262080,
            4259106816,
        ),
    ]


def test_system_different_interval(mocker: MockerFixture) -> None:
    """
    Test a simple CPU query with a custom interval.
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


def test_system_interrupt(mocker: MockerFixture) -> None:
    """
    Test CPU query interrupt.
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


def test_get_data_cpu(mocker: MockerFixture) -> None:
    """
    Test ``get_data`` with the ``CPU`` resource.
    """
    psutil = mocker.patch("shillelagh.adapters.api.system.psutil")
    psutil.cpu_count.return_value = 4
    psutil.cpu_percent.side_effect = [
        [1, 2, 3, 4],
        [1, 2, 3, 4],
        [1, 2, 3, 4],
        [1, 2, 3, 4],
    ]
    time = mocker.patch("shillelagh.adapters.api.system.time")

    adapter = SystemAPI("cpu")

    with freeze_time("2021-01-01T00:00:00Z"):
        data = list(adapter.get_data({}, [], limit=2, offset=1))
    assert data == [
        {
            "rowid": 0,
            "timestamp": datetime(2021, 1, 1, 0, 0, tzinfo=timezone.utc),
            "cpu0": 0.01,
            "cpu1": 0.02,
            "cpu2": 0.03,
            "cpu3": 0.04,
        },
        {
            "rowid": 1,
            "timestamp": datetime(2021, 1, 1, 0, 0, tzinfo=timezone.utc),
            "cpu0": 0.01,
            "cpu1": 0.02,
            "cpu2": 0.03,
            "cpu3": 0.04,
        },
    ]

    time.sleep.assert_called_with(1.0)

    data = list(adapter.get_data({}, [], limit=2, offset=1, requested_columns={"cpu1"}))
    assert data == [
        {"rowid": 0, "cpu1": 0.02},
        {"rowid": 1, "cpu1": 0.02},
    ]


def test_get_data_all(mocker: MockerFixture) -> None:
    """
    Test ``get_data`` with the ``ALL`` resource.
    """
    psutil = mocker.patch("shillelagh.adapters.api.system.psutil")
    psutil.cpu_count.return_value = 4
    psutil.cpu_percent.return_value = [1, 2, 3, 4]
    psutil.virtual_memory()._asdict.return_value = {
        "total": 34359738368,
        "available": 15130095616,
        "percent": 56.0,
        "used": 18285113344,
        "free": 1579941888,
        "active": 13551853568,
        "inactive": 13460545536,
        "wired": 4733259776,
    }
    psutil.swap_memory()._asdict.return_value = {
        "total": 18253611008,
        "used": 16865034240,
        "free": 1388576768,
        "percent": 92.4,
        "sin": 1010873262080,
        "sout": 4259106816,
    }
    time = mocker.patch("shillelagh.adapters.api.system.time")

    adapter = SystemAPI("all")

    with freeze_time("2021-01-01T00:00:00Z"):
        data = list(adapter.get_data({}, [], limit=1))
    assert data == [
        {
            "rowid": 0,
            "timestamp": datetime(2021, 1, 1, 0, 0, tzinfo=timezone.utc),
            "cpu0": 0.01,
            "cpu1": 0.02,
            "cpu2": 0.03,
            "cpu3": 0.04,
            "virtual_total": 34359738368,
            "virtual_available": 15130095616,
            "virtual_percent": 56.0,
            "virtual_used": 18285113344,
            "virtual_free": 1579941888,
            "virtual_active": 13551853568,
            "virtual_inactive": 13460545536,
            "virtual_wired": 4733259776,
            "swap_total": 18253611008,
            "swap_used": 16865034240,
            "swap_free": 1388576768,
            "swap_percent": 92.4,
            "swap_sin": 1010873262080,
            "swap_sout": 4259106816,
        },
    ]

    time.sleep.assert_not_called()

    psutil.cpu_percent.reset_mock()
    psutil.virtual_memory.reset_mock()
    psutil.swap_memory.reset_mock()

    with freeze_time("2021-01-01T00:00:00Z"):
        data = list(
            adapter.get_data(
                {},
                [],
                limit=1,
                requested_columns={"virtual_percent"},
            ),
        )
    assert data == [{"rowid": 0, "virtual_percent": 56.0}]

    psutil.cpu_percent.assert_not_called()
    psutil.virtual_memory.assert_called()
    psutil.swap_memory.assert_not_called()

    time.sleep.assert_called_with(1.0)


def test_get_data_bogus(mocker: MockerFixture) -> None:
    """
    Test ``get_data`` with a bogus ``requested_columns``.
    """
    mocker.patch("shillelagh.adapters.api.system.time")

    with freeze_time("2021-01-01T00:00:00Z"):
        data = list(
            SystemAPI("all").get_data({}, [], limit=1, requested_columns={"bogus"}),
        )
    assert data == [{"rowid": 0}]

    with freeze_time("2021-01-01T00:00:00Z"):
        data = list(
            SystemAPI("cpu").get_data({}, [], limit=1, requested_columns={"bogus"}),
        )
    assert data == [{"rowid": 0}]

    with freeze_time("2021-01-01T00:00:00Z"):
        data = list(
            SystemAPI("memory").get_data({}, [], limit=1, requested_columns={"bogus"}),
        )
    assert data == [{"rowid": 0}]

    with freeze_time("2021-01-01T00:00:00Z"):
        data = list(
            SystemAPI("swap").get_data({}, [], limit=1, requested_columns={"bogus"}),
        )
    assert data == [{"rowid": 0}]
