import os
import sys
from datetime import datetime
from datetime import timedelta

from shillelagh.backends.apsw.db import connect


if __name__ == "__main__":
    three_days_ago = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%dT12:00:00")

    # sign up for an API key at https://www.weatherapi.com/my/
    api_key = sys.argv[1]

    connection = connect(":memory:")
    cursor = connection.cursor()

    # TODO: use datetime functions?
    sql = f"""
    SELECT *
    FROM "https://api.weatherapi.com/v1/history.json?key={api_key}&q=94923" AS bodega_bay
    WHERE ts >= '{three_days_ago}'
    """
    for row in cursor.execute(sql):
        print(row)

    sql = f"""
    SELECT *
    FROM "https://api.weatherapi.com/v1/history.json?key={api_key}&q=94923" AS bodega_bay
    JOIN "https://api.weatherapi.com/v1/history.json?key={api_key}&q=94401" AS san_mateo
    ON bodega_bay.ts = san_mateo.ts
    WHERE bodega_bay.ts >= '{three_days_ago}'
    AND san_mateo.ts >= '{three_days_ago}'
    AND san_mateo.temperature < bodega_bay.temperature
    """
    for row in cursor.execute(sql):
        print(row)

    os.unlink("weatherapi_cache.sqlite")
