import logging
from shillelagh.backends.apsw.db import connect
logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    connection = connect(":memory:")
    cursor = connection.cursor()

    sql = """
    SELECT *
    FROM "https://api.opensea.io/api/v1/events"
    """
    for row in cursor.execute(sql):
        print(row)
