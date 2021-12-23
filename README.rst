==========
Shillelagh
==========

.. image:: https://coveralls.io/repos/github/betodealmeida/shillelagh/badge.svg?branch=master
   :target: https://coveralls.io/github/betodealmeida/shillelagh?branch=master
.. image:: https://img.shields.io/cirrus/github/betodealmeida/shillelagh
   :target: https://cirrus-ci.com/github/betodealmeida/shillelagh
   :alt: Cirrus CI - Base Branch Build Status
.. image:: https://readthedocs.org/projects/shillelagh/badge/?version=latest
   :target: https://shillelagh.readthedocs.io/en/latest/?badge=latest
   :alt: Documentation Status
.. image:: https://badge.fury.io/py/shillelagh.svg
   :target: https://badge.fury.io/py/shillelagh
.. image:: https://img.shields.io/pypi/pyversions/shillelagh
   :alt: PyPI - Python Version

Shillelagh (ʃɪˈleɪlɪ) is an implementation of the `Python DB API 2.0 <https://www.python.org/dev/peps/pep-0249/>`_ based on `SQLite <https://sqlite.org/index.html>`_ (using the `APSW <https://rogerbinns.github.io/apsw/>`_ library):

.. code-block:: python

    from shillelagh.backends.apsw.db import connect

    connection = connect(":memory:")
    cursor = connection.cursor()

    query = "SELECT * FROM a_table"
    for row in cursor.execute(query):
        print(row)

There is also a `SQLAlchemy <https://www.sqlalchemy.org/>`_ dialect:

.. code-block:: python

    from sqlalchemy.engine import create_engine

    engine = create_engine("shillelagh://")
    connection = engine.connect()

    query = "SELECT * FROM a_table"
    for row in connection.execute(query):
        print(row)

And a command-line utility:

.. code-block:: bash

    $ shillelagh
    sql> SELECT * FROM a_table

Installation
============

Install Shillelagh with ``pip``:

.. code-block:: bash

    $ pip install 'shillelagh'

This will install an unofficial APSW package from the `Python package index <https://pypi.org/project/apsw/>`_. It's highly recommend to install a newer version:

.. code-block:: bash

    $ pip install https://github.com/rogerbinns/apsw/releases/download/3.36.0-r1/apsw-3.36.0-r1.zip \
    --global-option=fetch --global-option=--version --global-option=3.36.0 --global-option=--all \
    --global-option=build --global-option=--enable-all-extensions

How is it different?
====================

Shillelagh allows you to easily query non-SQL resources. For example, if you have a `Google Spreadsheet <https://docs.google.com/spreadsheets/d/1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/edit#gid=0>`_ you can query it directly as if it were a table in a database:

.. code-block:: sql

    SELECT country, SUM(cnt)
    FROM "https://docs.google.com/spreadsheets/d/1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/edit#gid=0"
    WHERE cnt > 0
    GROUP BY country

You can even run ``INSERT``/``DELETE``/``UPDATE`` queries against the spreadsheet:

.. code-block:: sql

    UPDATE "https://docs.google.com/spreadsheets/d/1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/edit#gid=0"
    SET cnt = cnt + 1
    WHERE country != 'BR'

Queries like this are supported by `adapters <https://shillelagh.readthedocs.io/en/latest/adapters.html>`_. Currently Shillelagh has the following adapters:

- Google Spreadsheets
- `WeatherAPI <https://www.weatherapi.com/>`_
- `Socrata Open Data API <https://dev.socrata.com/>`_
- CSV files
- Pandas dataframes
- `Datasette tables <https://datasette.io/>`_
- GitHub (currently only pull requests, but other endpoints can be easily added)
- System information (currently only CPU usage, but other resources can be easily added)

A query can combine data from multiple adapters:

.. code-block:: sql

    INSERT INTO "/tmp/file.csv"
    SELECT time, chance_of_rain
    FROM "https://api.weatherapi.com/v1/history.json?q=London"
    WHERE time IN (
      SELECT datetime
      FROM "https://docs.google.com/spreadsheets/d/1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/edit#gid=1648320094"
    )

The query above reads timestamps from a Google sheet, uses them to filter weather data from `WeatherAPI <https://www.weatherapi.com/>`_, and writes the chance of rain into a (pre-existing) CSV file.

New adapters are relatively easy to implement. There's a `step-by-step tutorial <https://shillelagh.readthedocs.io/en/latest/development.html>`_ that explains how to create a new adapter to an API or filetype.
