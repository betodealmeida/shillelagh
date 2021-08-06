import pandas as pd

from shillelagh.backends.apsw.db import connect


if __name__ == "__main__":
    connection = connect(":memory:")
    cursor = connection.cursor()

    mydf = pd.DataFrame({"a": [1, 2, 3]})

    sql = "SELECT SUM(a) FROM mydf"
    for row in cursor.execute(sql):
        print(row)

    sql = "UPDATE mydf SET a = a + 1"
    cursor.execute(sql)

    sql = "SELECT SUM(a) FROM mydf"
    for row in cursor.execute(sql):
        print(row)
