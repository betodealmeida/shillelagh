"""
A simple example querying a virtual table.
"""

from shillelagh.backends.apsw.db import connect

if __name__ == "__main__":
    connection = connect(":memory:")
    cursor = connection.cursor()

    SQL = """
        SELECT * FROM
        "virtual://?cols=a:int,b:str,c:bool,t1:day,t2:second&start=2024-01-01&end=2024-02-15";
    """
    for row in cursor.execute(SQL):
        print(row)
