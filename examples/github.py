from shillelagh.backends.apsw.db import connect


if __name__ == "__main__":
    connection = connect(":memory:")
    cursor = connection.cursor()

    sql = """
SELECT * FROM "https://api.github.com/repos/apache/superset/pulls"
    """
    for row in cursor.execute(sql):
        print(row)
