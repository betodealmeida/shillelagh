"""
A simple example querying the dbt MetricFlow.
"""

from shillelagh.backends.apsw.db import connect

if __name__ == "__main__":
    connection = connect(
        ":memory:",
        adapter_kwargs={
            "dbtmetricflowapi": {
                "service_token": "dbtc_XXX",
                "environment_id": 123456,
            },
        },
    )
    cursor = connection.cursor()

    SQL = """
    SELECT orders, order_id__is_food_order
    FROM "https://semantic-layer.cloud.getdbt.com/"
    LIMIT 10
    """
    for row in cursor.execute(SQL):
        print(row)
