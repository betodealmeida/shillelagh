"""
Test the holidays in-memory adapter.
"""

import datetime

from shillelagh.backends.apsw.db import connect


def test_holidays() -> None:
    """
    Test basic queries.
    """
    connection = connect(":memory:")
    cursor = connection.cursor()

    sql = """
SELECT *
    FROM holidays
WHERE
    country='US' AND
    "date" > '2020-01-01' AND
    "date" < '2022-01-01';
"""
    cursor.execute(sql)
    assert cursor.fetchall() == [
        ("US", datetime.date(2020, 1, 20), "Martin Luther King Jr. Day"),
        ("US", datetime.date(2020, 2, 17), "Washington's Birthday"),
        ("US", datetime.date(2020, 5, 25), "Memorial Day"),
        ("US", datetime.date(2020, 7, 3), "Independence Day (Observed)"),
        ("US", datetime.date(2020, 7, 4), "Independence Day"),
        ("US", datetime.date(2020, 9, 7), "Labor Day"),
        ("US", datetime.date(2020, 10, 12), "Columbus Day"),
        ("US", datetime.date(2020, 11, 11), "Veterans Day"),
        ("US", datetime.date(2020, 11, 26), "Thanksgiving"),
        ("US", datetime.date(2020, 12, 25), "Christmas Day"),
        ("US", datetime.date(2021, 1, 1), "New Year's Day"),
        ("US", datetime.date(2021, 1, 18), "Martin Luther King Jr. Day"),
        ("US", datetime.date(2021, 2, 15), "Washington's Birthday"),
        ("US", datetime.date(2021, 5, 31), "Memorial Day"),
        (
            "US",
            datetime.date(2021, 6, 18),
            "Juneteenth National Independence Day (Observed)",
        ),
        ("US", datetime.date(2021, 6, 19), "Juneteenth National Independence Day"),
        ("US", datetime.date(2021, 7, 4), "Independence Day"),
        ("US", datetime.date(2021, 7, 5), "Independence Day (Observed)"),
        ("US", datetime.date(2021, 9, 6), "Labor Day"),
        ("US", datetime.date(2021, 10, 11), "Columbus Day"),
        ("US", datetime.date(2021, 11, 11), "Veterans Day"),
        ("US", datetime.date(2021, 11, 25), "Thanksgiving"),
        ("US", datetime.date(2021, 12, 24), "Christmas Day (Observed)"),
        ("US", datetime.date(2021, 12, 25), "Christmas Day"),
        ("US", datetime.date(2021, 12, 31), "New Year's Day (Observed)"),
    ]
