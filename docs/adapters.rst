.. _adapters:

========
Adapters
========

Adapters are plugins that make it possible for Shillelagh to query APIs and other non-SQL resources.

.. _gsheets:

Google Sheets
=============

The adapter allows users to run queries against Google Sheets, treating them like tables. Use the URI of the sheet as the table name (a "sheet" is a tab inside a Google Spreadsheet):

.. code-block:: sql

    INSERT INTO "https://docs.google.com/spreadsheets/d/1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/edit#gid=0"
    (country, cnt)
    VALUES ('US', 14);

The query above will insert a new row into `this sheet <https://docs.google.com/spreadsheets/d/1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/edit#gid=0>`_.

You can see a simple `example <https://github.com/betodealmeida/shillelagh/blob/main/examples/gsheets.py>`__.

Credentials
~~~~~~~~~~~

The Google Sheets adapter requires authorization for most queries. Without credentials you can only run ``SELECT`` queries against public sheets. To run ``SELECT`` queries against private sheets that you have access, or to run ``INSERT``/``DELETE``/``UPDATE`` queries against **any** sheets you need a set of credentials.

For a single user, you can configure an OAuth token that has access to the following scopes:

- ``https://www.googleapis.com/auth/drive.readonly``
- ``https://www.googleapis.com/auth/spreadsheets``
- ``https://spreadsheets.google.com/feeds``

Then, pass the token when creating a connection:

.. code-block:: python

    from shillelagh.backends.apsw.db import connect

    connection = connect(":memory:", adapter_kwargs={"gsheetsapi": {"access_token": "XXX"}})

For domain wide access you need to create a service account. Make sure that the account has domain delegation enabled, and access to the 3 scopes above. Also make sure that "Google Sheets" and "Google Drive" are enabled in the project. You can then download the credentials as JSON, and pass them either as a file location or as a Python dictionary:

.. code-block:: python

    from shillelagh.backends.apsw.db import connect

    connection = connect(
        ":memory:",
        adapter_kwargs={
            "gsheetaspi": {
                # "service_account_file": "/path/to/credentials.json",
                "service_account_info": {
                    "type": "service_account",
                    ...
                },
                "subject": "user@example.com",
            },
        },
    )

You also need to pass a "subject" if you want to impersonate users. If not present the connection will have full access to all spreadsheets in a given project, so be careful.

If running in an environment where Application Default Credentials are available, you can use them by configuring the connection as:

.. code-block:: python

    from shillelagh.backends.apsw.db import connect

    connection = connect(":memory:", adapter_kwargs={"gsheetsapi": {"app_default_credentials": True}})


Sync modes
~~~~~~~~~~

By default, when interacting with a Google sheet every query will issue at least one network request. A ``SELECT`` will fetch data using the Chart API, which allows filtering on the server-side. Manipulating data with ``DELETE`` and ``UPDATE``, on the other hand, is very expensive. On those operations the whole sheet is download for every row manipulated, and for each row a ``PUT`` request is made.

The standard mode of operation is called "bidirectional", since the sheet is download in every modification to ensure the adapter has the latest version, and changes are pushed immediately to the sheet. There are other, more efficient modes od synchronization between the adapter and the sheet:

Bidirectional (default)
        The whole sheet is downloaded before every DML query, and changes are pushed for each row immediately.
Unidirectional
        The whole sheet is downloaded only once, before the first DML query. Changes are pushed immediately.
Batch
        The whole sheet is downloaded only once, before the first DML query. Changes are pushed only when the adapter is closed (usually when the connection is closed).

To specify a different mode other than "bidirectional" you need to append ``sync_mode=${mode}`` to the URI when accessing the sheet:

.. code-block:: sql

    DELETE FROM "https://docs.google.com/spreadsheets/d/1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/edit?sync_mode=BATCH#gid=0";

Note that ``sync_mode`` should go between ``edit`` and ``#gid=``, since it's a query string argument. You can use either the mode names ("BIDIRECTIONAL", "UNIDIRECTIONAL", "BATCH") or their numbers (1, 2, and 3, respectively).

Custom dialect
~~~~~~~~~~~~~~

The Google Sheets adapter has a custom SQLAlchemy dialect, ``gsheets://``. When using this dialect only the Google Sheets adapter is enabled. The connection can be configured with the contents from ``adapter_kwargs['gsheetsapi']`` directly, ie:

.. code-block:: python

    from sqlalchemy.engine import create_engine

    engine = create_engine("gsheets://", service_account_file="/path/to/credentials.json")

The dialect also exposes the list of sheets that the user has via the ``get_table_names``

.. code-block:: python

    from sqlalchemy.engine import create_engine
    from sqlalchemy import inspect

    engine = create_engine("gsheets://", service_account_file="/path/to/credentials.json")
    inspector = inspect(engine)
    print(inspector.get_table_names())

The code above will print the URI of every sheet (every tab inside every spreadsheet) that the user owns. The URIs can then be opened using Shillelagh.

The dialect also allows users to specify a "catalog" of sheets, so they can be referenced by an alias:

.. code-block:: python

    from sqlalchemy.engine import create_engine

    engine = create_engine(
        "gsheets://",
        "catalog": {
            "simple_sheet": "https://docs.google.com/spreadsheets/d/1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/edit#gid=0",
        },
    )
    connection = engine.connect()
    connection.execute("SELECT * FROM simple_sheet")

Header rows
~~~~~~~~~~~

The Google Chart API (which is used when fetching data) will try to guess how many rows are headers in the Google sheet. If all your columns are string data, the spreadsheet might have difficulty determining which rows are header rows, requiring it to be `passed manually <https://developers.google.com/chart/interactive/docs/spreadsheets#creating-a-chart-from-a-separate-spreadsheet>`_.

You can specify a fixed number of header rows by adding ``headers=N`` to the sheet URI, eg:

.. code-block:: python

    from sqlalchemy.engine import create_engine

    engine = create_engine(
        "gsheets://",
        "catalog": {
            "simple_sheet": (
                "https://docs.google.com/spreadsheets/d/1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/edit?"
                "headers=1"  # <= here
                "#gid=0"
            ),
        },
    )
    connection = engine.connect()
    connection.execute("SELECT * FROM simple_sheet")

CSV files
=========

CSV (comma separated values) are supported (`an example <https://github.com/betodealmeida/shillelagh/blob/main/examples/csvfile.py>`__):

.. code-block:: sql

    SELECT * FROM "/path/to/file.csv";

The adapter supports full DML, so you can also ``INSERT``, ``UPDATE``, or ``DELETE`` rows from the CSV file. Deleted rows are marked for deletion, modified and inserted rows are appended at the end of the file, and garbage collection is applied when the connection is closed.


Socrata
=======

The `Socrata Open Data API <https://dev.socrata.com/>`_ is a simple API used by many governments, non-profits, and NGOs around the world, including the `CDC <https://www.cdc.gov/>`_. Similarly to the Google Spreadsheets adapter, with the Socrata adapter you can query any API URL directly (`an example <https://github.com/betodealmeida/shillelagh/blob/main/examples/socrata.py>`__):

.. code-block:: sql

    SELECT date, administered_dose1_recip_4
    FROM "https://data.cdc.gov/resource/unsk-b7fc.json"
    WHERE location = 'US'
    ORDER BY date DESC
    LIMIT 10

The adapter is currently read-only.

WeatherAPI
==========

The `WeatherAPI <https://www.weatherapi.com/>`_ adapter was the first one to be written, and provides access to historical weather data. You need an API key in order to use it (`an example <https://github.com/betodealmeida/shillelagh/blob/main/examples/weatherapi.py>`__):

.. code-block:: python

    from datetime import datetime, timedelta
    from shillelagh.backends.apsw.db import connect

    three_days_ago = datetime.now() - timedelta(days=3)

    # sign up for an API key at https://www.weatherapi.com/my/
    api_key = "XXX"

    connection = connect(":memory:", adapter_kwargs={"weatherapi": {"api_key": api_key}})
    cursor = connection.cursor()

    sql = """
    SELECT *
    FROM "https://api.weatherapi.com/v1/history.json?q=London"
    WHERE time >= ?
    """
    for row in cursor.execute(sql, (three_days_ago,)):
        print(row)

By default the adapter will only look at the last 7 days of data, since that's what's available for free accounts. You can specify a larger time window:

.. code-block:: python

    from datetime import datetime, timedelta
    from shillelagh.backends.apsw.db import connect

    three_days_ago = datetime.now() - timedelta(days=3)

    # sign up for an API key at https://www.weatherapi.com/my/
    api_key = "XXX"

    # query 30 days of data
    connection = connect(":memory:", adapter_kwargs={"weatherapi": {"api_key": api_key, "window": 30}})

Pandas
======

Shillelagh has support for Pandas dataframes, inspired by `DuckDB <https://duckdb.org/2021/05/14/sql-on-pandas.html>`_:

.. code-block:: python

    import pandas as pd
    from shillelagh.backends.apsw.db import connect

    connection = connect(":memory:")
    cursor = connection.cursor()

    mydf = pd.DataFrame({"a": [1, 2, 3]})

    sql = "SELECT SUM(a) FROM mydf"
    for row in cursor.execute(sql):
        print(row)

Datasette
=========

You can select data from any `Datasette <https://datasette.io/>`_ table, by using the full URL with the database and the table:

.. code-block:: sql

    SELECT * FROM "https://fivethirtyeight.datasettes.com/polls/president_polls"

GitHub
======

The GitHub adapter currently allows pull requests to be queried (other endpoints can be easily added):

.. code-block:: sql

    SELECT *
    FROM "https://api.github.com/repos/apache/superset/pulls"
    WHERE
        state = 'open' AND
        username = 'betodealmeida'

System resources
================

Shilellagh comes with a simple adapter that can query system resources. It's based on `psutil <https://github.com/giampaolo/psutil>`_, and currently displays CPU usage per processor:

.. code-block:: sql

    SELECT cpu0 FROM "system://cpu" LIMIT 1

An important thing to know is that the adapter streams the data. If the query doesn't specify a ``LIMIT`` it might hang if the client expects all data to be returned before displaying the results. This is true for the ``shillelagh`` CLI, but not for Python cursors. For example, the following code will print a new line every 1 second until it's interrupted:

.. code-block:: python

    from shillelagh.backends.apsw.db import connect

    connection = connect(":memory:")
    cursor = connection.cursor()

    query = 'SELECT * FROM "system://cpu"'
    for row in cursor.execute(query):
        print(row)

It's possible to specify a different polling interval by passing the ``interval`` parameter to the URL:

.. code-block:: sql

    SELECT cpu0 FROM "system://cpu?interval=0.1" -- 0.1 seconds
