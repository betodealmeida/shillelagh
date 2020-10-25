import sys

import apsw
from shillelagh.adapters.api.weatherapi import WeatherAPI


if __name__ == "__main__":
    api_key = sys.argv[0]

    connection = apsw.Connection("weatherapi.db")
    cursor = connection.cursor()
    connection.createmodule("weatherapi", WeatherAPI)

    cursor.execute(
        f"CREATE VIRTUAL TABLE IF NOT EXISTS bodega_bay USING weatherapi(94923, {api_key})",
    )
    cursor.execute(
        f"CREATE VIRTUAL TABLE IF NOT EXISTS san_mateo USING weatherapi(94401, {api_key})",
    )

    # TODO: use datetime functions?
    sql = "SELECT * FROM bodega_bay WHERE ts >= '2020-10-20T12:00:00'"
    for row in cursor.execute(sql):
        print(row)

    sql = """
    SELECT * FROM bodega_bay
    JOIN san_mateo
    ON bodega_bay.ts = san_mateo.ts
    WHERE bodega_bay.ts > '2020-10-20T12:00:00'
    AND san_mateo.ts > '2020-10-20T12:00:00'
    AND san_mateo.temperature < bodega_bay.temperature
    """
    for row in cursor.execute(sql):
        print(row)
