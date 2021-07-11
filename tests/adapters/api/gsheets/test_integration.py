"""
Integration tests for GSheets.

These tests operate on a spreadsheet, fetching and changing data.
"""
import datetime

import pytest
from dateutil.tz import tzoffset

from shillelagh.adapters.api.gsheets.types import SyncMode
from shillelagh.backends.apsw.db import connect


@pytest.mark.slow_integration_test
@pytest.mark.parametrize("sync_mode", [SyncMode.BIDIRECTIONAL, SyncMode.BATCH])
def test_simple_sheet(sync_mode, adapter_kwargs):
    """
    Test queries against the simple sheet.

        -----------------
        | country | cnt |
        |---------|-----|
        | BR      |   1 |
        | BR      |   3 |
        | IN      |   5 |
        | ZA      |   6 |
        | CR      |  10 |
        -----------------

    """
    table = (
        '"https://docs.google.com/spreadsheets/d/'
        "1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/edit"
        f'?sync_mode={sync_mode.value}#gid=0"'
    )

    connection = connect(":memory:", adapter_kwargs=adapter_kwargs)
    cursor = connection.cursor()

    sql = f"SELECT * FROM {table}"
    cursor.execute(sql)
    assert cursor.fetchall() == [
        ("BR", 1.0),
        ("BR", 3.0),
        ("IN", 5.0),
        ("ZA", 6.0),
        ("CR", 10.0),
    ]

    sql = f"INSERT INTO {table} (country, cnt) VALUES (?, ?)"
    cursor.execute(sql, ("US", 14))
    sql = f"SELECT * FROM {table}"
    cursor.execute(sql)
    assert cursor.fetchall() == [
        ("BR", 1.0),
        ("BR", 3.0),
        ("IN", 5.0),
        ("ZA", 6.0),
        ("CR", 10.0),
        ("US", 14.0),
    ]

    sql = f"UPDATE {table} SET cnt = cnt + 1"
    cursor.execute(sql)
    sql = f"SELECT * FROM {table}"
    cursor.execute(sql)
    assert cursor.fetchall() == [
        ("BR", 2.0),
        ("BR", 4.0),
        ("IN", 6.0),
        ("ZA", 7.0),
        ("CR", 11.0),
        ("US", 15.0),
    ]

    sql = f"DELETE FROM {table} WHERE country = ?"
    cursor.execute(sql, ("US",))
    sql = f"SELECT * FROM {table}"
    cursor.execute(sql)
    assert cursor.fetchall() == [
        ("BR", 2.0),
        ("BR", 4.0),
        ("IN", 6.0),
        ("ZA", 7.0),
        ("CR", 11.0),
    ]

    sql = f"UPDATE {table} SET cnt = cnt - 1"
    cursor.execute(sql)
    sql = f"SELECT * FROM {table}"
    cursor.execute(sql)
    assert cursor.fetchall() == [
        ("BR", 1.0),
        ("BR", 3.0),
        ("IN", 5.0),
        ("ZA", 6.0),
        ("CR", 10.0),
    ]


@pytest.mark.slow_integration_test
@pytest.mark.parametrize("sync_mode", [SyncMode.BIDIRECTIONAL, SyncMode.BATCH])
def test_2_header_sheet(sync_mode, adapter_kwargs):
    """
    Test a sheet where the column names occupy the first 2 rows.
    """
    table = (
        '"https://docs.google.com/spreadsheets/d/'
        "1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/edit"
        f'?sync_mode={sync_mode.value}#gid=1077884006"'
    )

    connection = connect(":memory:", adapter_kwargs=adapter_kwargs)
    cursor = connection.cursor()

    sql = f"SELECT * FROM {table}"
    cursor.execute(sql)
    assert cursor.fetchall() == [
        ("BR", 1.0),
        ("BR", 3.0),
        ("IN", 5.0),
        ("ZA", 6.0),
        ("CR", 10.0),
    ]
    assert [(t[0], t[1].type) for t in cursor.description] == [
        ("country string", "TEXT"),
        ("cnt number", "REAL"),
    ]

    sql = f'INSERT INTO {table} ("country string", "cnt number") VALUES (?, ?)'
    cursor.execute(sql, ("US", 14))
    sql = f"SELECT * FROM {table}"
    cursor.execute(sql)
    assert cursor.fetchall() == [
        ("BR", 1.0),
        ("BR", 3.0),
        ("IN", 5.0),
        ("ZA", 6.0),
        ("CR", 10.0),
        ("US", 14.0),
    ]

    sql = f"""UPDATE {table} SET "cnt number" = "cnt number" + 1"""
    cursor.execute(sql)
    sql = f"SELECT * FROM {table}"
    cursor.execute(sql)
    assert cursor.fetchall() == [
        ("BR", 2.0),
        ("BR", 4.0),
        ("IN", 6.0),
        ("ZA", 7.0),
        ("CR", 11.0),
        ("US", 15.0),
    ]

    sql = f'DELETE FROM {table} WHERE "country string" = ?'
    cursor.execute(sql, ("US",))
    sql = f"SELECT * FROM {table}"
    cursor.execute(sql)
    assert cursor.fetchall() == [
        ("BR", 2.0),
        ("BR", 4.0),
        ("IN", 6.0),
        ("ZA", 7.0),
        ("CR", 11.0),
    ]

    sql = f"""UPDATE {table} SET "cnt number" = "cnt number" - 1"""
    cursor.execute(sql)
    sql = f"SELECT * FROM {table}"
    cursor.execute(sql)
    assert cursor.fetchall() == [
        ("BR", 1.0),
        ("BR", 3.0),
        ("IN", 5.0),
        ("ZA", 6.0),
        ("CR", 10.0),
    ]


@pytest.mark.slow_integration_test
@pytest.mark.parametrize("sync_mode", [SyncMode.BIDIRECTIONAL, SyncMode.BATCH])
def test_types_and_nulls(sync_mode, adapter_kwargs):
    """
    Test a sheet with all the supported types, including NULLs.
    """
    table = (
        '"https://docs.google.com/spreadsheets/d/'
        "1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/edit"
        f'?sync_mode={sync_mode.value}#gid=1648320094"'
    )

    connection = connect(":memory:", adapter_kwargs=adapter_kwargs)
    cursor = connection.cursor()

    sql = f"SELECT * FROM {table}"
    cursor.execute(sql)
    assert cursor.fetchall() == [
        (
            datetime.datetime(2018, 9, 1, 0, 0, tzinfo=tzoffset(None, -25200)),
            1.0,
            True,
            datetime.date(2018, 1, 1),
            datetime.time(17, 0),
            "test",
        ),
        (
            datetime.datetime(2018, 9, 2, 0, 0, tzinfo=tzoffset(None, -25200)),
            1.0,
            False,
            None,
            None,
            "test",
        ),
        (
            datetime.datetime(2018, 9, 3, 0, 0, tzinfo=tzoffset(None, -25200)),
            2.0,
            False,
            None,
            None,
            "test",
        ),
        (
            datetime.datetime(2018, 9, 4, 0, 0, tzinfo=tzoffset(None, -25200)),
            3.0,
            False,
            None,
            None,
            "test",
        ),
        (
            datetime.datetime(2018, 9, 5, 0, 0, tzinfo=tzoffset(None, -25200)),
            5.0,
            False,
            None,
            None,
            "test",
        ),
        (
            datetime.datetime(2018, 9, 6, 0, 0, tzinfo=tzoffset(None, -25200)),
            8.0,
            False,
            None,
            None,
            "test",
        ),
        (
            datetime.datetime(2018, 9, 7, 0, 0, tzinfo=tzoffset(None, -25200)),
            13.0,
            False,
            None,
            None,
            None,
        ),
        (
            datetime.datetime(2018, 9, 8, 0, 0, tzinfo=tzoffset(None, -25200)),
            None,
            False,
            None,
            None,
            "test",
        ),
        (
            datetime.datetime(2018, 9, 9, 0, 0, tzinfo=tzoffset(None, -25200)),
            34.0,
            None,
            None,
            None,
            "test",
        ),
    ]

    sql = (
        f"INSERT INTO {table} "
        "(datetime, number, boolean, date, timeofday, string) "
        "VALUES (?, ?, ?, ?, ?, ?)"
    )
    cursor.execute(
        sql,
        (
            datetime.datetime(2021, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
            10,
            True,
            None,
            None,
            "temporary",
        ),
    )
    cursor.execute(
        sql,
        (
            None,
            None,
            None,
            datetime.date(2021, 7, 10),
            datetime.time(12, 0),
            "temporary",
        ),
    )
    sql = f"SELECT * FROM {table}"
    cursor.execute(sql)
    assert cursor.fetchall() == [
        (
            datetime.datetime(2018, 9, 1, 0, 0, tzinfo=tzoffset(None, -25200)),
            1.0,
            True,
            datetime.date(2018, 1, 1),
            datetime.time(17, 0),
            "test",
        ),
        (
            datetime.datetime(2018, 9, 2, 0, 0, tzinfo=tzoffset(None, -25200)),
            1.0,
            False,
            None,
            None,
            "test",
        ),
        (
            datetime.datetime(2018, 9, 3, 0, 0, tzinfo=tzoffset(None, -25200)),
            2.0,
            False,
            None,
            None,
            "test",
        ),
        (
            datetime.datetime(2018, 9, 4, 0, 0, tzinfo=tzoffset(None, -25200)),
            3.0,
            False,
            None,
            None,
            "test",
        ),
        (
            datetime.datetime(2018, 9, 5, 0, 0, tzinfo=tzoffset(None, -25200)),
            5.0,
            False,
            None,
            None,
            "test",
        ),
        (
            datetime.datetime(2018, 9, 6, 0, 0, tzinfo=tzoffset(None, -25200)),
            8.0,
            False,
            None,
            None,
            "test",
        ),
        (
            datetime.datetime(2018, 9, 7, 0, 0, tzinfo=tzoffset(None, -25200)),
            13.0,
            False,
            None,
            None,
            None,
        ),
        (
            datetime.datetime(2018, 9, 8, 0, 0, tzinfo=tzoffset(None, -25200)),
            None,
            False,
            None,
            None,
            "test",
        ),
        (
            datetime.datetime(2018, 9, 9, 0, 0, tzinfo=tzoffset(None, -25200)),
            34.0,
            None,
            None,
            None,
            "test",
        ),
        (
            datetime.datetime(2020, 12, 31, 16, 0, tzinfo=tzoffset(None, -28800)),
            10.0,
            True,
            None,
            None,
            "temporary",
        ),
        (
            None,
            None,
            None,
            datetime.date(2021, 7, 10),
            datetime.time(12, 0),
            "temporary",
        ),
    ]

    sql = f'DELETE FROM {table} WHERE "string" = ?'
    cursor.execute(sql, ("temporary",))
    sql = f"SELECT * FROM {table}"
    cursor.execute(sql)
    assert cursor.fetchall() == [
        (
            datetime.datetime(2018, 9, 1, 0, 0, tzinfo=tzoffset(None, -25200)),
            1.0,
            True,
            datetime.date(2018, 1, 1),
            datetime.time(17, 0),
            "test",
        ),
        (
            datetime.datetime(2018, 9, 2, 0, 0, tzinfo=tzoffset(None, -25200)),
            1.0,
            False,
            None,
            None,
            "test",
        ),
        (
            datetime.datetime(2018, 9, 3, 0, 0, tzinfo=tzoffset(None, -25200)),
            2.0,
            False,
            None,
            None,
            "test",
        ),
        (
            datetime.datetime(2018, 9, 4, 0, 0, tzinfo=tzoffset(None, -25200)),
            3.0,
            False,
            None,
            None,
            "test",
        ),
        (
            datetime.datetime(2018, 9, 5, 0, 0, tzinfo=tzoffset(None, -25200)),
            5.0,
            False,
            None,
            None,
            "test",
        ),
        (
            datetime.datetime(2018, 9, 6, 0, 0, tzinfo=tzoffset(None, -25200)),
            8.0,
            False,
            None,
            None,
            "test",
        ),
        (
            datetime.datetime(2018, 9, 7, 0, 0, tzinfo=tzoffset(None, -25200)),
            13.0,
            False,
            None,
            None,
            None,
        ),
        (
            datetime.datetime(2018, 9, 8, 0, 0, tzinfo=tzoffset(None, -25200)),
            None,
            False,
            None,
            None,
            "test",
        ),
        (
            datetime.datetime(2018, 9, 9, 0, 0, tzinfo=tzoffset(None, -25200)),
            34.0,
            None,
            None,
            None,
            "test",
        ),
    ]


@pytest.mark.slow_integration_test
@pytest.mark.parametrize("sync_mode", [SyncMode.BIDIRECTIONAL, SyncMode.BATCH])
def test_empty_column(sync_mode, adapter_kwargs):
    """
    Test queries against a sheet with an empty column in the middle.

        ---------------------------------------------------------
        | one   |                        | two   | three | four |
        |-------|------------------------|-------|-------|------|
        | test  |                        | test  |   1.5 | 10.1 |
        | test2 | this should not appear | test3 |   0.1 | 10.2 |
        ---------------------------------------------------------

    """
    table = (
        '"https://docs.google.com/spreadsheets/d/'
        "1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/edit"
        f'?sync_mode={sync_mode.value}#gid=486670973"'
    )

    connection = connect(":memory:", adapter_kwargs=adapter_kwargs)
    cursor = connection.cursor()

    sql = f"SELECT * FROM {table}"
    cursor.execute(sql)
    assert cursor.fetchall() == [
        ("test", "test", 1.5, 10.1),
        ("test2", "test3", 0.1, 10.2),
    ]
    assert [(t[0], t[1].type) for t in cursor.description] == [
        ("one", "TEXT"),
        ("two", "TEXT"),
        ("three", "REAL"),
        ("four", "REAL"),
    ]

    sql = f"INSERT INTO {table} (two) VALUES (?)"
    cursor.execute(sql, ("test4",))
    sql = f"SELECT * FROM {table}"
    cursor.execute(sql)
    assert cursor.fetchall() == [
        ("test", "test", 1.5, 10.1),
        ("test2", "test3", 0.1, 10.2),
        (None, "test4", None, None),
    ]

    sql = f"DELETE FROM {table} WHERE two = ?"
    cursor.execute(sql, ("test4",))
    sql = f"SELECT * FROM {table}"
    cursor.execute(sql)
    assert cursor.fetchall() == [
        ("test", "test", 1.5, 10.1),
        ("test2", "test3", 0.1, 10.2),
    ]


@pytest.mark.slow_integration_test
def test_order_by(adapter_kwargs):
    """
    Test that ORDER BY works on multiple columns.
    """
    table = (
        '"https://docs.google.com/spreadsheets/d/'
        '1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/edit#gid=624495018"'
    )

    connection = connect(":memory:", adapter_kwargs=adapter_kwargs)
    cursor = connection.cursor()

    sql = f"SELECT * FROM {table} ORDER BY bar, foo, baz"
    cursor.execute(sql)
    assert cursor.fetchall() == [
        (1.0, 1.0, 2.0),
        (2.0, 1.0, 5.0),
        (3.0, 1.0, 8.0),
        (1.0, 2.0, 3.0),
        (2.0, 2.0, 6.0),
        (3.0, 2.0, 9.0),
        (1.0, 3.0, 1.0),
        (2.0, 3.0, 4.0),
        (3.0, 3.0, 7.0),
    ]
