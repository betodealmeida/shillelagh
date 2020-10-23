import apsw

from shillelagh.adapters.weatherapi import WeatherAPI


if __name__ == "__main__":
    connection = apsw.Connection("weatherapi.db")
    cursor = connection.cursor()
    connection.createmodule("weatherapi", WeatherAPI)

    cursor.execute(
        "CREATE VIRTUAL TABLE bodega_bay USING weatherapi(94923, f426b51ea9aa4e4ab68190907202309)"
    )
    cursor.execute(
        "CREATE VIRTUAL TABLE san_mateo USING weatherapi(94401, f426b51ea9aa4e4ab68190907202309)"
    )

    sql = "SELECT * FROM bodega_bay WHERE ts >= '2020-09-23T12:00:00'"
    for row in cursor.execute(sql):
        print(row)

    sql = """
    SELECT * FROM bodega_bay
    JOIN san_mateo
    ON bodega_bay.ts = san_mateo.ts
    WHERE bodega_bay.ts > '2020-09-20T12:00:00'
    AND san_mateo.ts > '2020-09-20T12:00:00'
    AND san_mateo.temperature < bodega_bay.temperature
    """
    for row in cursor.execute(sql):
        print(row)
