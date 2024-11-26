"""
Test the holidays in-memory adapter.
"""

from holidays import country_holidays

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
    "date" >= '2020-01-01' AND
    "date" < '2022-01-01';
"""
    cursor.execute(sql)

    holidays = country_holidays("US", years=[2020, 2021])
    assert cursor.fetchall() == [
        ("US", date, name) for date, name in sorted(holidays.items())
    ]
