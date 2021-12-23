"""
A simple example showing the Pandas adapter.
"""
import pandas as pd

from shillelagh.backends.apsw.db import connect

if __name__ == "__main__":
    connection = connect(":memory:")
    cursor = connection.cursor()

    mydf = pd.DataFrame({"a": [1, 2, 3]})

    SQL = "SELECT SUM(a) FROM mydf"
    for row in cursor.execute(SQL):
        print(row)

    SQL = "UPDATE mydf SET a = a + 1"
    cursor.execute(SQL)

    SQL = "SELECT SUM(a) FROM mydf"
    for row in cursor.execute(SQL):
        print(row)
