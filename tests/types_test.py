"""
Tests for shillelagh.types.
"""

from datetime import date, datetime, time, timezone

from shillelagh.backends.apsw.db import connect
from shillelagh.fields import String
from shillelagh.types import (
    STRING,
    Binary,
    Date,
    DateFromTicks,
    Time,
    TimeFromTicks,
    Timestamp,
    TimestampFromTicks,
)


def test_types() -> None:
    """
    Test that native Python types can be used in queries.
    """
    connection = connect(":memory:")
    cursor = connection.cursor()

    cursor.execute(
        """
        CREATE TABLE test_types (
            type_date DATE,
            type_time TIME,
            type_timestamp TIMESTAMP,
            type_binary BLOB
        )
    """,
    )
    cursor.execute(
        "INSERT INTO test_types VALUES (?, ?, ?, ?)",
        (
            Date(2020, 1, 1),
            Time(0, 0, 0),
            Timestamp(2020, 1, 1, 0, 0, 0),
            Binary("🦥"),
        ),
    )
    cursor.execute("SELECT * FROM test_types")
    row = cursor.fetchone()
    assert row == (
        date(2020, 1, 1),
        time(0, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 1, 0, 0, tzinfo=timezone.utc),
        b"\xf0\x9f\xa6\xa5",
    )

    cursor.execute(
        "INSERT INTO test_types VALUES (?, ?, ?, ?)",
        (
            DateFromTicks(1),
            TimeFromTicks(2),
            TimestampFromTicks(3),
            Binary("🦥"),
        ),
    )
    cursor.execute("SELECT * FROM test_types")
    rows = cursor.fetchall()
    assert rows == [
        (
            date(2020, 1, 1),
            time(0, 0, tzinfo=timezone.utc),
            datetime(2020, 1, 1, 0, 0, tzinfo=timezone.utc),
            b"\xf0\x9f\xa6\xa5",
        ),
        (
            date(1970, 1, 1),
            time(0, 0, 2, tzinfo=timezone.utc),
            datetime(1970, 1, 1, 0, 0, 3, tzinfo=timezone.utc),
            b"\xf0\x9f\xa6\xa5",
        ),
    ]


def test_comparison() -> None:
    """
    Test type comparison.
    """
    assert STRING == String
    assert (STRING == 1) is False
