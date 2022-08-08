.. _usage:

=====
Usage
=====

Shillelagh implements the `Python DB API 2.0 <https://www.python.org/dev/peps/pep-0249/>`_ specification and a custom `SQLAlchemy <https://www.sqlalchemy.org/>`_ dialect. It also comes with a simple command-line utility to run queries from the console.

.. _dbapi2:

DB API 2.0
==========

The DB API 2.0 specification defines standard mechanisms to create database connections and to work with cursors. Here's a simple example that creates a table, inserts some data, and queries it:

.. code-block:: python

    from shillelagh.backends.apsw.db import connect

    connection = connect(":memory:")
    cursor = connection.cursor()

    query = "CREATE TABLE a_table (A int, B string)"
    cursor.execute(query)

    query = "INSERT INTO a_table VALUES (?, ?)"
    cursor.execute(query, (1, "one"))
    cursor.execute(query, (2, "two"))

    query = "SELECT * FROM a_table"
    for row in cursor.execute(query):
        print(row)

You can use a file instead of ``:memory:``:

.. code-block:: python

    from shillelagh.backends.apsw.db import connect

    connection = connect("/path/to/file.sqlite")

Note that using a file is not recommended for security reasons. Shillelagh works by creating `virtual tables <https://sqlite.org/vtab.html>`_, and if a given resource requires credentials for access they will be stored in the table name.

Configuration
~~~~~~~~~~~~~

By default all available adapters are loaded by Shillelagh. It's possible to limit the adapters that you want to load by passing a list of strings to the ``adapters`` argument when creating the connection:

.. code-block:: python

    from shillelagh.adapters.registry import registry
    from shillelagh.backends.apsw.db import connect

    # show names of available adapters
    print(registry.loaders.keys())

    # enable on the CSV and the WeatherAPI adapters
    connection = connect(":memory:", adapters=["csvfile", "weatherapi"])

Some adapters allow optional configuration to be passed via keyword arguments. For example, we can specify an API key for WeatherAPI:

.. code-block:: python

    from shillelagh.backends.apsw.db import connect

    connection = connect(":memory:", adapter_kwargs={"weatherapi": {"api_key": "XXX"}})

You can find the accepted arguments in the documentation of each one the :ref:`adapters`.

When loading adapters, you can also specify the ``safe`` keyword. When set to true, this will ensure that only "safe" adapters are loaded — ie, adapters that have no access to the filesystem. The keyword also ensures that an exception is raised whenever there are repeated adapters with the same name, to prevent a malicious party from introducing an adapter with the same name as an authorized one:

.. code-block:: python

    from shillelagh.adapters.registry import registry
    from shillelagh.adapters.file.csvfile import CSVFile
    from shillelagh.backends.apsw.db import connect

    registry.add('csvfile', CSVFile)
    registry.add('csvfile', FakeAdapter)
    connect(':memory:', adapters=['csvfile'], safe=True)

The code above will raise an exception saying "Multiple adapters found with name csvfile". This is needed because adapters can be loaded from third-party libraries via `entry points <https://packaging.python.org/specifications/entry-points/>`_, and not just from the Shillelagh library.

Registering new adapters
~~~~~~~~~~~~~~~~~~~~~~~~

Shillelagh uses a plugin registry similar to SQLAlchemy's. Adapters that are registered via entry points are registered automatically, but you can manually register adapter classes:

.. code-block:: Python

    from shillelagh.adapters.base import Adapter
    from shillelagh.adapters.registry import registry

    class CustomAdapter(Adapter):
        ...

    # add an adapter class directly
    registry.add('customadapter', CustomAdapter)

    # add an adapter class by passing the module path and class name
    registry.register('someotheradapter', 'path.to.module', 'ClassName')

SQLAlchemy
==========

Shillelagh implements a SQLAlchemy dialect called ``shillelagh``:

.. code-block:: python

    from sqlalchemy.engine import create_engine

    engine = create_engine("shillelagh://")
    connection = engine.connect()

    query = "SELECT * FROM a_table"
    for row in connection.execute(query):
        print(row)

Configuration
~~~~~~~~~~~~~

The SQLAlchemy engine can be configured in the same way as the :ref:`dbapi2` ``connect`` function, defining the adapters to be loaded, passing custom keyword arguments to the adapters, or loading only safe adapters. For example, if you want to connect only to Google Spreadsheets, using credentials from a service account:

.. code-block:: python

    from sqlalchemy.engine import create_engine

    engine = create_engine(
        "shillelagh://",
         adapters=["gsheetsapi"],
         adapter_kwargs={
             "gsheetsapi": {
                 "service_account_file": "/path/to/credentials.json",
                 "subject": "user@example.com",
             },
         },
    )

Alternatively, Shillelagh also comes with a custom Google Sheets dialect for SQLAlchemy. See :ref:`gsheets` for more details.


Command-line utility
====================

Shillelagh comes with a simple command-line utility aptly named ``shillelagh``:

.. code-block:: bash

    $ shillelagh
    sql> SELECT * FROM "https://docs.google.com/spreadsheets/d/1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/edit#gid=0";
    country      cnt
    ---------  -----
    BR             1
    BR             3
    IN             5
    ZA             6
    CR            10
    sql>

The ``shillelagh`` application has very simple autocomplete, and runs the query on :kbd:`return`.

Configuration
~~~~~~~~~~~~~

The command-line utility can be configured through a YAML file stored in ``~/.config/shillelagh/shillelagh.yaml``. The contents of the file correspond to the ``adapter_kwargs`` argument, eg:

.. code-block:: YAML

    gsheetsapi:
      service_account_file: /path/to/credentials.json
      subject: user@example.com
    weatherapi:
      api_key: XXX

Custom functions
================

Shillelagh exposes a few custom functions that can be called via SQL.

Sleep
~~~~~

The ``sleep`` function is useful to create long-running queries, in order to test time outs in your application.

.. code-block:: sql

    sql> SELECT sleep(120);

This will return ``None`` after 2 minutes.

Retrieving metadata
~~~~~~~~~~~~~~~~~~~

The ``get_metadata`` function returns metadata about a special table as a JSON string, including which adapter handles it:

.. code-block::

    sql> SELECT GET_METADATA("https://docs.google.com/spreadsheets/d/1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/edit#gid=0");
    GET_METADATA("https://docs.google.com/spreadsheets/d/1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/edit#gid=0")
    --------------------------------------------------------------------------------------------------------------
    {
        "extra": {
            "Spreadsheet title": "Shillelagh GSheets",
            "Sheet title": "Simple sheet"
        },
        "adapter": "GSheetsAPI"
    }

Finding out the version
~~~~~~~~~~~~~~~~~~~~~~~

Sometimes it's useful to identify the version of Shillelagh that's running on a system, in order to troubleshoot it. You can do that with ``version()``:

.. code-block:: sql

    sql> SELECT VERSION();
    VERSION()
    -----------
    1.0.0
