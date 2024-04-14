"""
A simple example showing the Datasette adapter.
"""

from shillelagh.backends.apsw.db import connect

if __name__ == "__main__":
    connection = connect(":memory:")
    cursor = connection.cursor()

    SQL = """
SELECT "Latitude" AS "Latitude",
       "Longitude" AS "Longitude",
       COUNT(*) AS count
FROM "https://san-francisco.datasettes.com/sf-trees/Street_Tree_List"
WHERE "Latitude" IS NOT NULL
  AND "Longitude" IS NOT NULL
  AND "qSpecies" = 6
GROUP BY "Latitude",
         "Longitude"
ORDER BY count DESC
LIMIT 1000
OFFSET 0;
    """
    for row in cursor.execute(SQL):
        print(row)
