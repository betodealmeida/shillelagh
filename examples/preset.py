"""
A simple example querying the Preset API.
"""

from shillelagh.backends.apsw.db import connect

if __name__ == "__main__":
    connection = connect(
        ":memory:",
        # create tokens/secrets at https://manage.app.preset.io/app/user
        adapter_kwargs={
            "presetapi": {
                "access_token": "",
                "access_secret": "",
            },
            "presetworkspaceapi": {
                "access_token": "",
                "access_secret": "",
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

    SQL = """
    SELECT * FROM
    "https://d90230ca.us1a.app-sdx.preset.io/api/v1/chart/"
    LIMIT 12
    """
    for row in cursor.execute(SQL):
        print(row)
