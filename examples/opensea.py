from shillelagh.backends.apsw.db import connect


if __name__ == "__main__":
    connection = connect(":memory:")
    cursor = connection.cursor()

    sql = """
    SELECT *
    FROM "https://api.opensea.io/api/v1/events?only_opensea=false&offset=0&limit=20"
    """
    for row in cursor.execute(sql):
        print(row)
