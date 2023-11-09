==========
Shillelagh
==========

.. image:: https://coveralls.io/repos/github/betodealmeida/shillelagh/badge.svg?branch=master
   :target: https://coveralls.io/github/betodealmeida/shillelagh?branch=master
.. image:: https://readthedocs.org/projects/shillelagh/badge/?version=latest
   :target: https://shillelagh.readthedocs.io/en/latest/?badge=latest
   :alt: Documentation Status
.. image:: https://badge.fury.io/py/shillelagh.svg
   :target: https://badge.fury.io/py/shillelagh
.. image:: https://img.shields.io/pypi/pyversions/shillelagh
   :alt: PyPI - Python Version

.. image:: docs/logo.png
   :width: 25 %

Shillelagh (ʃɪˈleɪlɪ) is a Python library and CLI that allows you to query many resources (APIs, files, in memory objects) using SQL. It's both user and developer friendly, making it trivial to access resources and easy to add support for new ones.

Learn more on the `documentation <https://shillelagh.readthedocs.io/en/latest/>`_.

The library is an implementation of the `Python DB API 2.0 <https://www.python.org/dev/peps/pep-0249/>`_ based on `SQLite <https://sqlite.org/index.html>`_ (using the `APSW <https://rogerbinns.github.io/apsw/>`_ library):

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

Why SQL?
========

Sharks have been around for a long time. They're older than trees and the rings of Saturn, actually! The reason they haven't changed that much in hundreds of millions of years is because they're really good at what they do.

SQL has been around for some 50 years for the same reason: it's really good at what it does.

Why "Shillelagh"?
=================

Picture a leprechaun hitting APIs with a big stick so that they accept SQL.

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

============= ============ ========================================================================== =====================================================================================================
 Name          Type         URI pattern                                                                Example URI
============= ============ ========================================================================== =====================================================================================================
 CSV           File/API     ``/path/to/file.csv``; ``http(s)://*``                                     ``/home/user/sample_data.csv``
 Datasette     API          ``http(s)://*``                                                            ``https://global-power-plants.datasettes.com/global-power-plants/global-power-plants``
 Generic JSON  API          ``http(s)://*``                                                            ``https://api.stlouisfed.org/fred/series?series_id=GNPCA&api_key=XXX&file_type=json#$.seriess[*]``
 Generic XML   API          ``http(s)://*``                                                            ``https://api.congress.gov/v3/bill/118?format=xml&offset=0&limit=2&api_key=XXX#.//bill``
 GitHub        API          ``https://api.github.com/repos/${owner}/{$repo}/pulls``                    ``https://api.github.com/repos/apache/superset/pulls``
 GSheets       API          ``https://docs.google.com/spreadsheets/d/${id}/edit#gid=${sheet_id}``      ``https://docs.google.com/spreadsheets/d/1LcWZMsdCl92g7nA-D6qGRqg1T5TiHyuKJUY1u9XAnsk/edit#gid=0``
 HTML table    API          ``http(s)://*``                                                            ``https://en.wikipedia.org/wiki/List_of_countries_and_dependencies_by_population``
 Pandas        In memory    Any variable name (local or global)                                        ``my_df``
 S3            API          ``s3://bucket/path/to/file``                                               ``s3://shillelagh/sample_data.csv``
 Socrata       API          ``https://${domain}/resource/${dataset-id}.json``                          ``https://data.cdc.gov/resource/unsk-b7fc.json``
 System        API          ``system://${resource}``                                                   ``system://cpu?interval=2``
 WeatherAPI    API          ``https://api.weatherapi.com/v1/history.json?key=${key}&q=${location}``    ``https://api.weatherapi.com/v1/history.json?key=XXX&q=London``
============= ============ ========================================================================== =====================================================================================================

There are also 3rd-party adapters:

- `Airtable <https://github.com/cancan101/airtable-db-api>`_
- `GraphQL <https://github.com/cancan101/graphql-db-api>`_

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

Installation
============

Install Shillelagh with ``pip``:

.. code-block:: bash

    $ pip install 'shillelagh'

You also need to install optional dependencies, depending on the adapter you want to use:

.. code-block:: bash

    $ pip install 'shillelagh[console]'        # to use the CLI
    $ pip install 'shillelagh[genericjsonapi]' # for Generic JSON
    $ pip install 'shillelagh[genericxmlapi]'  # for Generic XML
    $ pip install 'shillelagh[githubapi]'      # for GitHub
    $ pip install 'shillelagh[gsheetsapi]'     # for GSheets
    $ pip install 'shillelagh[htmltableapi]'   # for HTML tables
    $ pip install 'shillelagh[pandasmemory]'   # for Pandas in memory
    $ pip install 'shillelagh[s3selectapi]'    # for S3 files
    $ pip install 'shillelagh[systemapi]'      # for CPU information

Alternatively, you can install everything with:

.. code-block:: bash

    $ pip install 'shillelagh[all]'
