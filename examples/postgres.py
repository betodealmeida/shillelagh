"""
Simple multicorn2 test.

Multicorn2 is an extension for PostgreSQL that allows you to create foreign data wrappers
in Python. To use it, you need to install on the machine running Postgres the extension,
the multicorn2 package (not on (PyPI), and the shillelagh package.

If you want to play with it Shillelagh has a `docker-compose.yml` file that will run
Postgres with the extension and the Python packages. Just run:

    $ docker compose -f postgres/docker-compose.yml up

Then you can run this script.
"""

from sqlalchemy import create_engine, text

# the backend uses psycopg2 under the hood, so any valid connection string for it will
# work; just replace the scheme with `shillelagh+multicorn2`
engine = create_engine(
    "shillelagh+multicorn2://shillelagh:shillelagh123@localhost:5432/shillelagh",
)
connection = engine.connect()

SQL = text(
    'SELECT * FROM "https://docs.google.com/spreadsheets/d/'
    '1LcWZMsdCl92g7nA-D6qGRqg1T5TiHyuKJUY1u9XAnsk/edit#gid=0"',
)
for row in connection.execute(SQL):
    print(row)
