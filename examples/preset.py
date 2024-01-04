"""
A simple example querying the Preset API.
"""

from shillelagh.backends.apsw.db import connect

if __name__ == "__main__":
    connection = connect(
        ":memory:",
        adapter_kwargs={
            "presetapi": {
                # create a token/secret at https://manage.app.preset.io/app/user
                "access_token": "XXX",
                "access_secret": "YYY",
            },
        },
    )
    cursor = connection.cursor()

    SQL = """
    SELECT * FROM
    "https://api.app.preset.io/v1/teams/"
    LIMIT 1
    """
    for row in cursor.execute(SQL):
        print(row)
