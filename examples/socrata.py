"""
A simple example showing the Socrata adapter.
"""

from shillelagh.backends.apsw.db import connect

if __name__ == "__main__":
    connection = connect(":memory:")
    cursor = connection.cursor()

    SQL = """
    SELECT date, administered_dose1_recip_4
    FROM "https://data.cdc.gov/resource/unsk-b7fc.json"
    WHERE location = 'US'
    ORDER BY date DESC
    LIMIT 10
    """
    for row in cursor.execute(SQL):
        print(row)
