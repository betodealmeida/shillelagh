"""
A simple example showing the WeatherAPI adapter.
"""

import os
import sys
from datetime import datetime, timedelta

from shillelagh.backends.apsw.db import connect

if __name__ == "__main__":
    three_days_ago = datetime.now() - timedelta(days=3)

    # sign up for an API key at https://www.weatherapi.com/my/
    api_key = sys.argv[1]  # pylint: disable=invalid-name

    connection = connect(":memory:")
    cursor = connection.cursor()

    SQL = f"""
    SELECT *
    FROM "https://api.weatherapi.com/v1/history.json?key={api_key}&q=94923" AS bodega_bay
    WHERE time >= ?
    """
    for row in cursor.execute(SQL, (three_days_ago,)):
        print(row)

    SQL = f"""
    SELECT bodega_bay.time, bodega_bay.time_epoch, bodega_bay.temp_c, san_mateo.temp_c
    FROM "https://api.weatherapi.com/v1/history.json?key={api_key}&q=94923" AS bodega_bay
    JOIN "https://api.weatherapi.com/v1/history.json?key={api_key}&q=94401" AS san_mateo
    ON bodega_bay.time = san_mateo.time
    WHERE bodega_bay.time >= '{three_days_ago}'
    AND san_mateo.time >= '{three_days_ago}'
    AND san_mateo.temp_c > bodega_bay.temp_c
    """
    for row in cursor.execute(SQL):
        print(row)

    os.unlink("weatherapi_cache.SQLite")
