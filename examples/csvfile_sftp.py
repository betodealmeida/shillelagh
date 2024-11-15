"""
An example showing the CSV adapter with SFTP connection.

If you want to play with it Shillelagh has a `docker-compose.yml` file that will run
SFTP server. Just run:

    $ docker compose -f docker/docker-compose.yml up

Then you can run this script.
"""

from shillelagh.backends.apsw.db import connect

if __name__ == "__main__":
    connection = connect(":memory:")
    cursor = connection.cursor()

    # Credentials from docker/docker-compose.yml
    USERNAME = "shillelagh"
    PASSWORD = "shillelagh123"
    HOST = "localhost"
    PORT = 2222
    PATH = "test.csv"

    URL = f"sftp://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{PATH}"

    SQL = f'''SELECT * FROM "{URL}"'''
    print(SQL)
    for row in cursor.execute(SQL):
        print(row)
