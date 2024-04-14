"""
A simple example showing the CSV adapter.
"""

from shillelagh.backends.apsw.db import connect

if __name__ == "__main__":
    connection = connect(":memory:")
    cursor = connection.cursor()

    SQL = '''SELECT * FROM "test.csv"'''
    print(SQL)
    for row in cursor.execute(SQL):
        print(row)
    print("==")

    SQL = """SELECT * FROM "test.csv" WHERE "index" > 11"""
    print(SQL)
    for row in cursor.execute(SQL):
        print(row)
    print("==")

    SQL = """INSERT INTO "test.csv" ("index", temperature, site) VALUES (14, 10.1, 'New_Site')"""
    print(SQL)
    cursor.execute(SQL)

    SQL = """SELECT * FROM "test.csv" WHERE "index" > 11"""
    print(SQL)
    for row in cursor.execute(SQL):
        print(row)
    print("==")

    SQL = """DELETE FROM "test.csv" WHERE site = 'New_Site'"""
    print(SQL)
    cursor.execute(SQL)
    SQL = '''SELECT * FROM "test.csv"'''
    print(SQL)
    for row in cursor.execute(SQL):
        print(row)
    print("==")
