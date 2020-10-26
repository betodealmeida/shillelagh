import apsw
from shillelagh.adapters.file.csvfile import CSVFile
from shillelagh.backends.apsw.vt import VTModule

if __name__ == "__main__":
    connection = apsw.Connection(":memory:")
    cursor = connection.cursor()
    connection.createmodule("csvfile", VTModule(CSVFile))
    cursor.execute(f"CREATE VIRTUAL TABLE test USING csvfile(test.csv)")

    sql = 'SELECT * FROM test WHERE "index" > 11'
    print(sql)
    for row in cursor.execute(sql):
        print(row)
    print("==")

    sql = """INSERT INTO test ("index", temperature, site) VALUES (14, 10.1, 'New_Site')"""
    print(sql)
    cursor.execute(sql)

    sql = 'SELECT * FROM test WHERE "index" > 11'
    print(sql)
    for row in cursor.execute(sql):
        print(row)
    print("==")

    sql = "DELETE FROM test WHERE site = 'New_Site'"
    print(sql)
    cursor.execute(sql)
    sql = "SELECT * FROM test"
    print(sql)
    for row in cursor.execute(sql):
        print(row)
    print("==")
