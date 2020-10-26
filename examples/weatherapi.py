import os
import sys
from datetime import datetime
from datetime import timedelta

import apsw
from shillelagh.adapters.api.weatherapi import WeatherAPI
from shillelagh.backends.apsw.vt import VTModule


if __name__ == "__main__":
    three_days_ago = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%dT12:00:00")

    # sign up for an API key at https://www.weatherapi.com/my/
    api_key = sys.argv[1]

    connection = apsw.Connection(":memory:")
    cursor = connection.cursor()
    connection.createmodule("weatherapi", VTModule(WeatherAPI))

    cursor.execute(
        f"CREATE VIRTUAL TABLE bodega_bay USING weatherapi(94923, {api_key})",
    )
    cursor.execute(
        f"CREATE VIRTUAL TABLE san_mateo USING weatherapi(94401, {api_key})",
    )

    # TODO: use datetime functions?
    sql = f"SELECT * FROM bodega_bay WHERE ts >= '{three_days_ago}'"
    for row in cursor.execute(sql):
        print(row)

    sql = f"""
    SELECT * FROM bodega_bay
    JOIN san_mateo
    ON bodega_bay.ts = san_mateo.ts
    WHERE bodega_bay.ts >= '{three_days_ago}'
    AND san_mateo.ts >= '{three_days_ago}'
    AND san_mateo.temperature < bodega_bay.temperature
    """
    for row in cursor.execute(sql):
        print(row)

    os.unlink("weatherapi_cache.sqlite")
