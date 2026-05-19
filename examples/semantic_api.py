"""Querying a Semantic Layer REST API server.

This example uses the reference Pandas implementation from
``pandas-semantic-layer``; start it with ``semantic-api`` (port 8000 by
default) before running this script.
"""

from shillelagh.backends.apsw.db import connect

if __name__ == "__main__":
    connection = connect(":memory:")
    cursor = connection.cursor()

    SALES = "semantic-api+http://localhost:8000/views/sales"

    SQL = f"""
        SELECT product_category, region, total_revenue
        FROM "{SALES}"
        WHERE region IN ('North', 'East')
        GROUP BY product_category, region
        ORDER BY total_revenue DESC
        LIMIT 5
    """
    for row in cursor.execute(SQL):
        print(row)
