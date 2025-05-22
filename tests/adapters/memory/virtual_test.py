"""
Test the virtual table adapter.
"""

from datetime import datetime, timezone

from freezegun import freeze_time

from shillelagh.adapters.memory.virtual import int_to_base26
from shillelagh.backends.apsw.db import connect
from shillelagh.fields import FastISODateTime, IntBoolean, String, StringInteger


def test_int_to_base26() -> None:
    """
    Test the ``int_to_base26`` function.
    """
    assert int_to_base26(0) == "a"
    assert int_to_base26(1) == "b"
    assert int_to_base26(25) == "z"
    assert int_to_base26(26) == "aa"
    assert int_to_base26(-1) == ""


def test_virtual_start_end() -> None:
    """
    Test a query with start/end.
    """
    connection = connect(":memory:")
    cursor = connection.cursor()

    sql = """
SELECT *
FROM "virtual://?cols=a:int,b:str,c:bool,t1:day,t2:second&start=2024-01-01&end=2024-01-07"
"""
    cursor.execute(sql)

    assert cursor.description == [
        ("a", StringInteger, None, None, None, None, True),
        ("b", String, None, None, None, None, True),
        ("c", IntBoolean, None, None, None, None, True),
        ("t1", FastISODateTime, None, None, None, None, True),
        ("t2", FastISODateTime, None, None, None, None, True),
    ]
    assert cursor.fetchall() == [
        (
            0,
            "a",
            True,
            datetime(2024, 1, 1, 0, 0),
            datetime(2024, 1, 1, 0, 0),
        ),
        (
            1,
            "b",
            False,
            datetime(2024, 1, 2, 0, 0),
            datetime(2024, 1, 1, 0, 0, 1),
        ),
        (
            2,
            "c",
            True,
            datetime(2024, 1, 3, 0, 0),
            datetime(2024, 1, 1, 0, 0, 2),
        ),
        (
            3,
            "d",
            False,
            datetime(2024, 1, 4, 0, 0),
            datetime(2024, 1, 1, 0, 0, 3),
        ),
        (
            4,
            "e",
            True,
            datetime(2024, 1, 5, 0, 0),
            datetime(2024, 1, 1, 0, 0, 4),
        ),
        (
            5,
            "f",
            False,
            datetime(2024, 1, 6, 0, 0),
            datetime(2024, 1, 1, 0, 0, 5),
        ),
    ]


def test_virtual_start_rows() -> None:
    """
    Test a query with start/rows.
    """
    connection = connect(":memory:")
    cursor = connection.cursor()

    sql = """
SELECT *
FROM "virtual://?cols=a:int,b:str,c:bool,t1:day,t2:second&start=2024-01-01&rows=2"
"""
    cursor.execute(sql)

    assert cursor.description == [
        ("a", StringInteger, None, None, None, None, True),
        ("b", String, None, None, None, None, True),
        ("c", IntBoolean, None, None, None, None, True),
        ("t1", FastISODateTime, None, None, None, None, True),
        ("t2", FastISODateTime, None, None, None, None, True),
    ]
    assert cursor.fetchall() == [
        (
            6,
            "g",
            True,
            datetime(2024, 1, 7, 0, 0),
            datetime(2024, 1, 1, 0, 0, 6),
        ),
        (
            7,
            "h",
            False,
            datetime(2024, 1, 8, 0, 0),
            datetime(2024, 1, 1, 0, 0, 7),
        ),
    ]


@freeze_time("2024-02-03T12:34:56.789012")
def test_virtual() -> None:
    """
    Test a query with just columns.
    """
    connection = connect(":memory:")
    cursor = connection.cursor()

    sql = """
SELECT *
FROM "virtual://?cols=a:int,b:str,c:bool,t1:day,t2:second"
"""
    cursor.execute(sql)

    assert cursor.fetchall() == [
        (
            8,
            "i",
            True,
            datetime(2024, 2, 11, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 2, 3, 0, 0, 8, tzinfo=timezone.utc),
        ),
        (
            9,
            "j",
            False,
            datetime(2024, 2, 12, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 2, 3, 0, 0, 9, tzinfo=timezone.utc),
        ),
        (
            10,
            "k",
            True,
            datetime(2024, 2, 13, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 2, 3, 0, 0, 10, tzinfo=timezone.utc),
        ),
        (
            11,
            "l",
            False,
            datetime(2024, 2, 14, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 2, 3, 0, 0, 11, tzinfo=timezone.utc),
        ),
        (
            12,
            "m",
            True,
            datetime(2024, 2, 15, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 2, 3, 0, 0, 12, tzinfo=timezone.utc),
        ),
        (
            13,
            "n",
            False,
            datetime(2024, 2, 16, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 2, 3, 0, 0, 13, tzinfo=timezone.utc),
        ),
        (
            14,
            "o",
            True,
            datetime(2024, 2, 17, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 2, 3, 0, 0, 14, tzinfo=timezone.utc),
        ),
        (
            15,
            "p",
            False,
            datetime(2024, 2, 18, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 2, 3, 0, 0, 15, tzinfo=timezone.utc),
        ),
        (
            16,
            "q",
            True,
            datetime(2024, 2, 19, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 2, 3, 0, 0, 16, tzinfo=timezone.utc),
        ),
        (
            17,
            "r",
            False,
            datetime(2024, 2, 20, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 2, 3, 0, 0, 17, tzinfo=timezone.utc),
        ),
    ]
