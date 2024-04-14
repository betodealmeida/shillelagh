"""
A simple example showing the generic XML.
"""

import sys

from shillelagh.backends.apsw.db import connect

if __name__ == "__main__":
    API_KEY = sys.argv[1]

    connection = connect(":memory:")
    cursor = connection.cursor()

    SQL = f"""
    SELECT congress, type, latestAction FROM
    "https://api.congress.gov/v3/bill/118?format=xml&offset=0&limit=2&api_key={API_KEY}#.//bill"
    """
    for row in cursor.execute(SQL):
        print(row)
