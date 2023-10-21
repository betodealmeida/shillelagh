============
Architecture
============

SQLite supports a powerful concept called "virtual tables" (also called "foreign data wrappers" in Postgres). A virtual table is a table that, instead of being stored on disk, is implemented in code. Here's a quick example using Python:

.. code-block:: python

    connection = apsw.Connection(":memory:")

    # register the module responsible for the virtual table
    some_module = SomeModule()
    connection.createmodule("somemodule", some_module)

    # create the virtual table
    cursor = connection.cursor()
    cursor.execute('CREATE VIRTUAL TABLE sometable USING somemodule("arg1", 2)')

    # query it
    cursor.execute("SELECT * FROM sometable")

In the example above, whenever the ``sometable`` table is queried SQLite will call methods in the instance of ``SomeModule``, either to retrieve, insert, update, or delete rows. `Many different virtual tables exist for SQLite <https://www.sqlite.org/vtablist.html>`_, including modules for accessing CSV files or implementing spatial indexes.

The SQLite library that comes in the Python standard library does not expose virtual tables, but there's a library called ``apsw`` (another Python wrapper for SQLite) that does. Using ``apsw`` it's possible to implement modules like ``SomeModule``, and register them as virtual tables. Unfortunately the API exposed in ``apsw`` is not Pythonic, making it non-trivial to implement new virtual tables.

Shillelagh builds on top of ``apsw`` to simplify the life of both users and developers. When using Shillelagh a user doesn't need to register module for virtual tables. And the API for implementing new types of virtual tables is relatively simple and easy to understand.

Automatically registering modules
=================================

In Shillelagh, each virtual table is implemented by an "adapter". Each adapter has a different pattern for the table names it supports. For example, the ``s3select`` adapter supports table names with the pattern ``s3://bucket/path/to/file``, and provides access to CSV/JSON/Parquet files stored in S3.

To use the ``s3select`` a user simply needs to query a table with the corresponding pattern:

.. code-block:: sql

    SELECT * FROM "s3://bucket/path/to/file";

When Shillelagh first runs that query, it will fail, because the table ``s3://bucket/path/to/file`` doesn't exist. Instead of raising an exception, the Shillelagh `DB API 2.0 driver <https://peps.python.org/pep-0249/>`_ will parse the error message, and detect that a table is missing. It will then look at all registered adapters, trying to find one that supports the ``s3://bucket/path/to/file`` table name.

If it can find an adapter that handles the table name it will register the virtual table module, create the virtual table, and re-run the query. This way, to the user everything just works.

Behind the scenes
=================

Finding existing adapters
~~~~~~~~~~~~~~~~~~~~~~~~~

To find existing adapters, Shillelagh relies on `entry points <https://packaging.python.org/en/latest/specifications/entry-points/>`_. The library comes with its own adapters, but 3rd party Python packages can register their own adapters, and Shillelagh will find them. This is how the `airtable-db-api <https://github.com/cancan101/airtable-db-api/blob/218713cf70b026b731f9dc27a4a3a9ed659291cc/setup.py#L108-L110>`_ library does, for example.

It's also possible to register adapters on runtime, using a registry similar to the one used by SQLAlchemy:

.. code-block:: python

    from shillelagh.adapters.registry import registry

    registry.add("myadapter", MyAdapter)
    registry.register("myadapter", "my.module", "MyAdapter")

Finding the right adapter
~~~~~~~~~~~~~~~~~~~~~~~~~

Whenever Shillelagh encounters an error message saying "SQLError: no such table" while running a query, it will extract the table name and iterate over all adapters, calling the ``supports`` class method, trying to find one that can manage that virtual table. This is done in two phases.

In the first phase Shillelagh calls the ``supports`` method of each adapter, passing the table name (eg, ``file.csv``) and ``fast=True``. In this first pass adapters should implement only cheap methods, avoiding network calls and other expensive operations. If an adapter unsure whether they support a given URL they return ``None``, instead of a boolean.

For example, the ``datasette`` adapter can support potentially any http/https URL. On the first pass, if the domain is in the know list of datasette domains (``datasette.io``, eg) the adapter returns "true". For other domains it needs to do a ``HEAD`` request on a special endpoint, so on the first pass it returns ``None`` if it doesn't recognize the domain.

The second phase happens only if (1) no plugins returned ``True``, and at least one plugin returned ``None``. When that happens Shillelagh will call the ``supports`` method of the adapters that returned ``None``, but this time passing ``fast=False``. Now adapters can perform more expensive operations to determine if they support a given table name or not.

One easy way to understand the process is thinking of ``None`` as "maybe".

Creating the virtual table
~~~~~~~~~~~~~~~~~~~~~~~~~~

Once an adapter has been found for a given table name, Shillelagh then calls the ``parse_uri`` class method of the adapter. This method takes the table name and returns the arguments that are needed to initialize the adapter. These arguments are then passed in the ``CREATE VIRTUAL TABLE`` statement.

For example, the GitHub adapter:

.. code-block:: python

   >>> from shillelagh.adapters.api.github import GitHubAPI
   >>> GitHubAPI.parse_uri("https://api.github.com/repos/apache/superset/pulls")
   ('repos', 'apache', 'superset', 'pulls')

These arguments will be used by ``apsw`` to instantiate the adapter later, when Shillelagh runs the following query:

.. code-block:: sql

    CREATE VIRTUAL TABLE "https://api.github.com/repos/apache/superset/pulls"
      USING githubapi('repos', 'apache', 'superset', 'pulls');

Except that the parameters are not passed as strings, since they can be of any type. Instead, they are first marshalled and then encoded as ``base64``. This is all happens behind the scenes, both for adapter developers as for users.

Once the table has been created, Shillelagh will re-execute the query. The whole flow looks like this:

.. code-block:: sql

    -- (1) user:
    SELECT * FROM "https://api.github.com/repos/apache/superset/pulls";
    -- (2) raises: NO SUCH TABLE: "https://api.github.com/repos/apache/superset/pulls"
    -- (3) Shillelagh captures the exception, registers the "githubapi" module, and runs:
    CREATE VIRTUAL TABLE "https://api.github.com/repos/apache/superset/pulls"
      USING githubapi('repos', 'apache', 'superset', 'pulls');
    SELECT * FROM "https://api.github.com/repos/apache/superset/pulls";

From that point on the virtual table is registered in the connection. Additional queries won't require the module to be registered nor the virtual table to be created, and will simply succeed.

Columns names and types
~~~~~~~~~~~~~~~~~~~~~~~

Before the virtual table can be created SQLite needs a ``CREATE TABLE`` statement, so it knows the column names and types. This is done after the adapter is instantiated. For some adapters, the column names and types are static; this is usually true for adapters that talk to APIs. For other adapters the columns are dynamic: for a CSV file it will depend on the actual contents of the file.

Adapters have a method called ``get_columns`` that returns a dictionary with the column name as keys, and "fields" as values. The fields describe the column types, but also additional information on which columns can be filtered.

The implementation of ``get_columns`` in the base adapter class reads all class attributes that are fields:

.. code-block:: python

    def get_columns(self) -> Dict[str, Field]:
        return dict(
            inspect.getmembers(self, lambda attribute: isinstance(attribute, Field)),
        )

This way, adapters with static columns can simply declare them as class attributes:

.. code-block:: python

    class MyAdapter(Adapter):

        name = String()
        age = Float()

Fields and filters
==================

Fields are how Shillelagh represents columns. They store a lot of information:

- The column type;
- If the column can be filtered by the adapter, or if it should be filtered by SQLite instead;
- If the filtering is exact or inexact (in which case SQLite will do post-filtering);
- If the column can be sorted by the adapter, or by SQLite.

Here's a complete example:

.. code-block:: python

    event_time = ISODateTime(
        filters=[Range, Equal, NotEqual, IsNull, IsNotNull],
        exact=True,
        order=Order.ANY,
    )

First, for the type: in this example the column ``event_time`` is a date time (timestamp), stored internally as an ISO string. This means that the adapter will return values as strings, and Shillelagh will parse them to the proper Python type (a ``datetime.datetime``). Shillelagh will also convert values from a ``datetime.datetime`` to an ISO string when a query filters the column. The important thing is that all the adapter ever sees for that column are strings, but externally the user will receive ``datetime.datetime`` when querying it.

There's another layer of type conversion. SQLite has limited support for types, so values are converted to SQLite safe types whenever they enter or leave SQLite. For example, if the user runs a query like this:

.. code-block:: python

    cursor.execute(
        "SELECT event_time FROM sometable WHERE event_time > %s",
        (datetime.datetime.now() - datetime.timdelta(days=7),),
    )

Shillelagh will convert ``datetime.datetime.now() - datetime.timdelta(days=7)`` to a string and pass it to SQLite. SQLite will then pass the value as a string to the Shillelagh virtual table module, which converts it back to a ``datetime.datetime``. Then it gets converted back to a string, and passed to the adapter. The inverse process then happens for the data returned by the adapter. (The process could be optimized preventing unnecessary data conversions.)

Second, the filters. The column ``event_time`` is declaring that it can be filtered using a list of filter types. When SQLite sees the following query:

.. code-block:: sql

    SELECT * FROM sometable WHERE event_time IS NOT NULL;

It knows that the adapter will handle the predicate ``event_time IS NOT NULL``, since the field has ``IsNotNull`` in its filters. Shillelagh will collect all the predicates that should be handled by the adapter, and pass them in a dictionary called ``bounds`` to the ``get_data`` method of the adapter, eg:

.. code-block:: python

    bounds = {"event_time": IsNotNull(), ...}

The adapter is then responsible for filtering the data accordingly.

Sometimes, it's useful to do only partial filtering on the adapter. For example, the WeatherAPI adapter returns hourly data, but the API endpoint can only be filtered at the day level. For example, imagine the following query:

.. code-block:: sql

    SELECT * FROM weatherapi WHERE hour >= '2022-01-01 12:00' AND hour <= '2022-01-02 12:00';

This requires the adapter to fetch data for 2 full days, 2022-01-01 and 2022-01-02. It would then need to narrow down the data returned from the API endpoint to only those values between noon on both days. There is an easier way, though: the adapter can simply return all the data for those 2 days, and declare the filtering as "inexact", using the ``exact=False`` argument. This way the adapter does an initial coarse filtering to the day level, greatly reducing the amount of data that needs to be fetched, but the fine filtering is done by SQLite after the data is returned by the adapter.

Finally, we have the order. In this example the field ``event_time`` has ``order=Order.ANY``, which means that SQLite can request the data in any order and the adapter will fulfill the request, ie, the adapter is responsible for sorting the data.

If the data is presorted the column can be declared with a static sorting, eg, ``order=Order.ASCENDING`` or ``order=Order.DESCENDING``. When that happens, SQLite won't sort the data if it matches the requests sorting order. Finally, fields can also have ``order=Order.NONE``, which means that SQLite will always be responsible for sorting the data.

Limit and offset
================

There are 2 additional filters that are not tied to specific columns: ``LIMIT`` and ``OFFSET``. Adapters declare support for limit/offset via class attributes that default to ``False`` in the base class:

.. code-block:: python

    class MyAdapter(Adapter):

        supports_limit = True
        supports_offset = True

When an adapter supports them, any relevant values of limit and offset are passed as optional integers to the adapter's ``get_data`` method. The adapter is then responsible for applying the offset and limit before returning the data. Note that SQLite will still ensure that the limit is met; if an adapter returns with support for ``LIMIT`` returns more data than it should SQLite will drop the excess data.

Safe adapters
=============

Adapters have a boolean class attribute ``safe``. Adapters that have access to the filesystem should be marked as unsafe. This allows users to use Shillelagh in shared environments safely. Shillelagh also comes with a special SQLAlchemy dialect ``shillelagh+safe://`` that will only load safe plugins.

Writing adapters
================

The simplest adapter for Shillelagh has no filterable nor sortable columns, and doesn't support limit nor offset. This means that the adapter returns all the data for every request, leaving the filtering, sorting, limit, and offset to SQLite. On the other hand the most efficient adapter for Shillelagh implements all the data processing, returning the data exactly as it is needed.
