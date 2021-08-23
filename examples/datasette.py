from shillelagh.backends.apsw.db import connect


if __name__ == "__main__":
    connection = connect(":memory:")
    cursor = connection.cursor()

    sql = """
    SELECT * FROM "https://latest.datasette.io/fixtures/facetable"
    """
    for row in cursor.execute(sql):
        print(row)
