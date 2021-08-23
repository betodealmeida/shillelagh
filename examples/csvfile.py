from shillelagh.backends.apsw.db import connect

if __name__ == "__main__":
    connection = connect(":memory:")
    cursor = connection.cursor()

    sql = '''SELECT * FROM "test.csv"'''
    print(sql)
    for row in cursor.execute(sql):
        print(row)
    print("==")

    sql = """SELECT * FROM "test.csv" WHERE "index" > 11"""
    print(sql)
    for row in cursor.execute(sql):
        print(row)
    print("==")

    sql = """INSERT INTO "test.csv" ("index", temperature, site) VALUES (14, 10.1, 'New_Site')"""
    print(sql)
    cursor.execute(sql)

    sql = """SELECT * FROM "test.csv" WHERE "index" > 11"""
    print(sql)
    for row in cursor.execute(sql):
        print(row)
    print("==")

    sql = """DELETE FROM "test.csv" WHERE site = 'New_Site'"""
    print(sql)
    cursor.execute(sql)
    sql = '''SELECT * FROM "test.csv"'''
    print(sql)
    for row in cursor.execute(sql):
        print(row)
    print("==")
