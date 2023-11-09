.. _development:

======================
Creating a new adapter
======================

Creating a new adapter for Shillelagh is relatively easy, specially if the adapter is read-only.

A read-only adapter
===================

Let's create an adapter for historical meteorological data from `WeatherAPI <https://www.weatherapi.com/>`_ step-by-step to understand the process. Their API is as simple as it gets. To get historical data for a given date (say, **2021-01-01**) and a given place (say, **London**) all we need to do is an HTTP request to::

    https://api.weatherapi.com/v1/history.json?key=XXX&q=London&dt=2021-01-01

Here, ``XXX`` is an API key that authorizes the request. The response for this request is a JSON payload, with **hourly values** for the requested day

How can we expose this API endpoint via SQL? A simple way to do it is to treat **each location as a table**, allowing users to filter by the hourly timestamps. We can imagine something like this (let's ignore the API key for now):

.. code-block:: sql

    SELECT * FROM "https://api.weatherapi.com/v1/history.json?q=London"
    WHERE time = '2021-01-01T12:00:00+00:00'

The query above would then map to an HTTP request to::

    https://api.weatherapi.com/v1/history.json?key=XXX&q=London&dt=2021-01-01

Once we have the JSON payload we need to filter the hourly values, returning only the data that matches the requested timestamp ("2021-01-01T12:00:00+00:00").

Let's create an adapter that does that.

The adapter class
~~~~~~~~~~~~~~~~~

The first step is to create a class based on ``shillelagh.adapter.base.Adapter``:

.. code-block:: python

    from shillelagh.adapters.base import Adapter

    class WeatherAPI(Adapter):

        """
        An adapter to historical data from https://www.weatherapi.com/.
        """

        safe = True

Since our adapter doesn't read or write from the filesystem we can mark it as safe.

Informing Shillelagh of our class
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When a user writes a query like this one:

.. code-block:: sql

    SELECT * FROM "https://api.weatherapi.com/v1/history.json?q=London"
    WHERE time = '2021-01-01T12:00:00+00:00'

We want Shillelagh to know that it should be handled by our adapter. The first thing needed for that is to register our class using a Python `entry point <https://packaging.python.org/specifications/entry-points/>`_. We do this by adding it to the library's ``setup.cfg`` file:

.. code-block:: ini

    shillelagh.adapter =
        weatherapi = shillelagh.adapters.api.weatherapi:WeatherAPI

Second, we need to implement a method called ``supports`` in our class, that returns true when it knows how to handle a given table (or URI, in this case). In our case, it should return true if these 2 conditions are met:

1. The URI matches "https://api.weatherapi.com/v1/history.json?q=${location}[&key=${api_key}]"
2. The user has provided an API key, either via the URI (``&key=XXX``) or via the configuration arguments.

This means that these are two valid ways of querying the API using Shillelagh:

.. code-block:: python

    # specify the API key on the connection arguments
    connection = connect(":memory:", adapter_kwargs={"weatherapi": {"api_key": "XXX"}})
    connection.execute('SELECT * FROM "https://api.weatherapi.com/v1/history.json?q=London"')

    # specify the API key on the URI directly
    connection = connect(":memory:")
    connection.execute('SELECT * FROM "https://api.weatherapi.com/v1/history.json?q=London&key=XXX"')

So our method should look like this:

.. code-block:: python

    @staticmethod
    def supports(uri: str, fast: bool = True, **kwargs: Any) -> Optional[bool]:
        parsed = urllib.parse.urlparse(uri)
        query_string = urllib.parse.parse_qs(parsed.query)
        return (
            parsed.netloc == "api.weatherapi.com"
            and parsed.path == "/v1/history.json"
            and "q" in query_string
            and ("key" in query_string or "api_key" in kwargs)
        )

Note that the ``supports`` method takes a parameter called ``fast``. Adapter discovery is done in 2 phases: first all adapters have their ``supports`` method called with ``fast=True``. When this happens, adapter should return an optional boolean quickly. If your adapter needs to perform costy operations to determine if it supports a given URI it should return ``None`` in this first pass, to indicate that it **may** support the URI.

If no adapters return ``True`` on the first pass, a second pass is performed with ``fast=False``. On this second pass adapters can perform expensive operations, performing network requests to instrospect the URI and gather more information.

Instantiating the class
~~~~~~~~~~~~~~~~~~~~~~~

The next step is instructing Shillelagh how to instantiate our class from the URI. The easiest way to do that is by defining a dummy method ``parse_uri`` that simply returns the URI to our class' ``__init__`` method:

.. code-block:: python

    @staticmethod
    def parse_uri(uri: str) -> Tuple[str]:
        return uri

    def __init__(self, uri: str, api_key: Optional[str] = None):
        """
        Instantiate the adapter.

        Here ``uri`` will be passed from the ``parse_uri`` method, while
        ``api_key`` will come from the connection arguments.
        """
        super().__init__()

        parsed = urllib.parse.urlparse(uri)
        query_string = urllib.parse.parse_qs(parsed.query)

        # store the location, eg, "London"
        self.location = query_string["q"][0]

        # store the API key
        if not api_key:
            api_key = query_string["key"][0]
        self.api_key = api_key

Alternatively, we might want to do more work in the ``parse_uri`` method:

.. code-block:: python

    @staticmethod
    def parse_uri(uri: str) -> Union[Tuple[str], Tuple[str, str]]:
        parsed = urllib.parse.urlparse(uri)
        query_string = urllib.parse.parse_qs(parsed.query)
        location = query_string["q"][0]

        # key can be passed in the URI or via connection arguments
        if "key" in query_string:
            return (location, query_string["key"][0])
        return (location,)

    def __init__(self, location: str, api_key: str):
        super().__init__()

        self.location = location
        self.api_key = api_key

In the block above, the ``parse_uri`` method returns either ``(location,)`` or ``(location, key)``, if the key is present in the URI. Those two arguments are then passed in that order to the class' ``__init__`` method.

Note that the ``api_key`` argument **is not optional**, so if it's not passed either from the URI or from the connection arguments an exception will be raised. Though in theory that should never happen, since our ``supports`` method ensure that they key is set in at least one of those places.

Now, when we instantiate our adapter we have an object that represents **a virtual table**, containing weather data for a particular location.

The table columns
~~~~~~~~~~~~~~~~~

Next, we need to inform Shillelagh of the columns available in a given table. This is done by implementing a ``get_columns`` method.

For this particular example the columns are always the same, since we will return the same weather variables regardless of the location. Because of that, we can simply define the columns as class attributes. The original ``get_columns`` method in the base class will then find these columns and return them.

With more complex APIs the columns might change from instance to instance of the adapter — eg, for Google Sheets the number, names, and types of columns will vary from spreadsheet to spreadsheet. In that case we would need to implement a method that instrospects the spreadsheet in order to return the columns.

The Weather API returns many variables, but for simplicity let's imagine we want to return only two variables from the API: time and temperature in Celsius. We add these class attributes to our adapter:

.. code-block:: python

    from shillelagh.fields import DateTime
    from shillelagh.fields import Float
    from shillelagh.fields import Order
    from shillelagh.filters import Range

    time = DateTime(filters=[Range], exact=False, order=Order.ASCENDING)
    temp_c = Float()

Here we're using ``Field``\s to declare the columns available. The types of our ``time`` and ``temp_c`` columns are ``DateTime`` (a timestamp) and ``Float``, respectively.

More important, we also declare that **we can filter data** based on the ``time`` column. When the query has a predicate on the ``time`` column we can use it to request less data from the API. For example, if we have this query:

.. code-block:: sql

    SELECT time, temp_c FROM "https://api.weatherapi.com/v1/history.json?q=London"
    WHERE time > '2021-01-01T12:00:00+00:00'

We want our adapter to call the API by passing ``dt=2021-01-01``. The resulting payload will have hourly data, and we only have to filter those values that don't match ``12:00:00+00:00``.

It's actually easier than that! We can declare the results coming back from a filtered column as "inexact", by passing ``exact=False`` as in the code above. When a column is inexact Shillelagh will filter the returned data to ensure that it matches the predicate. So our adapter only needs to filter data down to the daily granularity, and Shillelagh will filter it further.

Finally, we also know that the resulting payload from the API is sorted by time, so we add ``order=Order.ASCENDING``. This means that any query that has ``ORDER BY time`` won't need any additional post-processing. Other allowed values for order are ``Order.NONE`` (the default), when no order is guaranteed; ``Order.DESCENDING``, when the data is sorted in descending order; and ``Order.ANY``, when the adapter will handle any requested order.

As for temperature, we can't filter any data based on a predicate that involves ``temp_c``, because that's not supported by the API. If a query has a predicate involving ``temp_c`` we need to download data from the API for all days, and pass that data to Shillelagh so it can do the filtering.

Returning data
~~~~~~~~~~~~~~

The last step is defining a method called ``get_rows`` to return rows:

.. code-block:: python

    from datetime import date
    from datetime import timedelta
    from typing import Any
    from typing import Dict
    from typing import Iterator
    from typing import List
    from typing import Tuple

    import dateutil.parser
    import requests

    from shillelagh.filters import Filter
    from shillelagh.filters import Range
    from shillelagh.typing import RequestedOrder

    def get_rows(
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
        **kwargs: Any,
    ) -> Iterator[Dict[str, Any]]:
        """
        Yield rows.

        The ``get_rows`` method should yield rows as dictionaries. Python native
        types should be used: int, float, str, bytes, bool, ``datetime.datetime``,
        ``datetime.date``, ``datetime.time``.
        """
        # get the time predicate
        time_range = bounds.get("time", Range())

        # the free version of the API offers only 7 days of data; default to that
        today = date.today()
        a_week_ago = today - timedelta(days=7)
        start = time_range.start.date() if time_range.start else a_week_ago
        end = time_range.end.date() if time_range.end else today

        while start <= end:
            url = "https://api.weatherapi.com/v1/history.json"
            params = {"key": self.api_key, "q": self.location, "dt": start}
            response = requests.get(url, params=params)
            if not response.ok:
                continue

            payload = response.json()
            for record in payload["forecast"]["forecastday"][0]["hour"]:
                yield {
                    "rowid": int(record["time_epoch"]),
                    "time": dateutil.parser.parse(record["time"]),
                    "temp_c": record["temp_c"],
                }

            start += timedelta(days=1)

The ``get_rows`` method receives two arguments. The first one, ``bounds``, is a dictionary containing optional filters that should be applied to the data. Since our adapter defines only ``time`` as a filterable column, ``bounds`` will contain at most one value, and it will be for the ``time`` column. For queries without time predicates the dictionary will be empty.

There's one more detail. We declared that the ``time`` column supports only ``Range`` filters (``filters=[Range]``), so if ``bounds['time']`` is present it will contain a ``Range``. A ``Range`` has optional start and end values, as well as the boolean attributes ``include_start`` and ``include_end``.

In the code above we use the range to determine the start and end **days** that we should query the API, defaulting to the last week. The code then fetches **all data** for those days, yielding dictionaries for each row. Because the ``time`` column was declared as inexact it's ok to return hourly data that doesn't match the range perfectly.

Each row is represented as a dictionary with column names for keys. The rows have a special column called "rowid". This should be a unique number for each row, and they can vary from call to call. The row ID is only important for adapters that support ``DELETE`` and ``UPDATE``, since those commands reference the rows by their ID.

Take a look at the `WeatherAPI adapter <https://github.com/betodealmeida/shillelagh/blob/main/src/shillelagh/adapters/api/weatherapi.py>`_ to see how everything looks like together.

Supporting limit and offset
~~~~~~~~~~~~~~~~~~~~~~~~~~~

We might want to implement support for ``LIMIT`` and ``OFFSET`` in our adapter, to improve performance; otherwise the adapter might return more data than is needed. To implement the support for ``LIMIT`` and ``OFFSET`` first the adapter must declare it:

.. code-block:: python

    class WeatherAPI(Adapter):

        supports_limit = True
        supports_offset = True

If an adapter declares support for ``LIMIT`` and ``OFFSET`` a corresponding parameter will be passed to ``get_rows`` (or ``get_data``, as described below), so that the signature should look like this:

.. code-block:: python

    def get_rows(
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        **kwargs: Any,
    ) -> Iterator[Dict[str, Any]]:

Now the adapter can handle ``limit`` and ``offset``, reducing the amount of data that is returned. Note that even if the adapter declares supporting ``LIMIT``, SQLite will still enforce the limit, ie, if for any reason the adapter returns more rows than the limit SQLite will fix the problem. The same is not true for the offset.

Returning only the requested columns
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default adapters should return all the columns available, since they have no information on which columns are actually needed. Starting with apsw `apsw 3.41.0.0 <https://github.com/rogerbinns/apsw/releases/tag/3.41.0.0>`_ adapters can optionally receive only the requested columns in their ``get_rows`` and ``get_data`` methods. The adapter must declare support for it by setting the attribute ``supports_requested_columns`` to true:

.. code-block:: python

    class WeatherAPI(Adapter):

        supports_requested_columns = True

Then the ``requested_columns: Optional[Set[str]]`` argument will be passed to ``get_rows`` and ``get_data``:

.. code-block:: python

    def get_rows(
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
        requested_columns: Optional[Set[str]] = None,
        **kwargs: Any,
    ) -> Iterator[Dict[str, Any]]:

A read-write adapter
====================

For a read-write adapter we need to implement at least 2 additional methods:

- ``insert_row(self, row: Dict[str, Any]) -> int``
- ``delete_row(self, row_id: int) -> None``

We also might want to implement a method for updating rows:

- ``update_row(self, row_id: int, row: Dict[str, Any]) -> None``

If ``update_row`` is not defined Shillelagh will update rows by calling ``delete_row`` followed by an ``insert_row`` with the updated values.

Note that ``DELETE`` and ``UPDATE`` operations use row IDs. When a user runs a query like this one:

.. code-block:: sql

    sql> DELETE FROM a_table WHERE foo = 'bar';

Shillelagh will run the following query:

.. code-block:: sql

    sql> SELECT rowid FROM a_table WHERE foo = 'bar';

It will then run a series for ``DELETE`` statements, one for each row ID returned. The same happens for ``UPDATE`` queries. This means that the adapter needs to keep track of the association between row IDs and rows, at least within a transaction. Since adapters have no awareness of transactions this means they need to preserve that mapping until they are closed.

Here's a simple example that supports these methods:

.. code-block:: python

    class SimpleAdapter(Adapter):

        safe = True

        # store people's age, name, and number of pets they have
        age = Float()
        name = String()
        pets = Integer()

        @staticmethod
        def supports(uri: str, fast: bool = True, **kwargs: Any) -> Optional[bool]:
            """
            Supports tables with the ``simple://`` scheme.

            Eg::

                SELECT * FROM "simple://a_table"

            """
            parsed = urllib.parse.urlparse(uri)
            return parsed.scheme == "simple"

        @staticmethod
        def parse_uri(uri: str) -> Tuple[()]:
            return ()

        def __init__(self):
            self.data = []

        def get_row(
            self,
            bounds: Dict[str, Filter],
            order: List[Tuple[str, RequestedOrder]],
            **kwargs: Any,
        ) -> Iterator[Dict[str, Any]]:
            yield from iter(self.data)

        def insert_row(self, row: Dict[str, Any]) -> int:
            row_id: Optional[int] = row["rowid"]

            # add a row ID if none was specified
            if row_id is None:
                max_rowid = max(row["rowid"] for row in self.data) if self.data else 0
                row["rowid"] = max_rowid + 1

            self.data.append(row)
            return row["rowid"]

        def delete_row(self, row_id: int) -> None:
            self.data = [row for row in self.data if row["rowid"] != row_id]

        def update_row(self, row_id: int, row: Dict[str, Any]) -> None:
            old_row = [row for row in self.data if row["rowid"] == row_id][0]
            old_row.update(row)

The `CSV <https://github.com/betodealmeida/shillelagh/blob/main/src/shillelagh/adapters/file/csvfile.py>`_ and the `Google Sheets <https://github.com/betodealmeida/shillelagh/blob/main/src/shillelagh/adapters/api/gsheets/adapter.py>`_ adapters are two examples of adapters that support DML (data modification language).

Custom fields
=============

In the examples above both adapters return data as native Python objects, eg, ``datetime.datetime`` object for timestamps. Some APIs might return timestamps as ISO strings, forcing the adapter to handle the conversion in the ``get_rows`` data before the rows are returned.

There's a different way of handling data conversion. The adapter can specify a custom ``Field`` for a given column. ``Field`` objects have two methods called ``parse`` and ``format``, responsible for the conversion between the format used by the adapter and native Python types. When using a custom field, **the adapter can return the original format before conversion** by defining the ``get_data`` method instead of ``get_rows``.

For example, if we have timestamps returned by an API as ISO strings we can define an adapter like this:

.. code-block:: python

    from shillelagh.fields import ISODateTime

    class ISOAdapter(Adapter):

        # time will be represented internally in the adapter as an ISO string
        time = ISODateTime()

        def get_data(
            self,
            bounds: Dict[str, Filter],
            order: List[Tuple[str, RequestedOrder]],
            **kwargs: Any,
        ) -> Iterator[Dict[str, Any]]:
            yield {
                "rowid": 1,
                "time": "2021-01-01T12:00:00+00:00",
            }

Shillelagh will then call ``get_data`` instead of ``get_rows``, and call ``ISODateTime.parse(row['time'])`` to convert the ISO string into a proper ``datetime.datetime`` object. Similarly, when inserting data it will call ``ISODateTime.format(row['time'])`` on the ``datetime.datetime`` object, and pass an ISO string to the ``insert_data`` method of the adapter.

When writing an adapter, you have then two options. You can produce and consume native Python types, and define these methods:

- ``get_rows``
- ``insert_row``
- ``delete_row``
- ``update_row``

Or define custom fields for your columns, produce and consume the internal format, and define these methods:

- ``get_data``
- ``insert_data``
- ``delete_data``
- ``update_data``

The ``shillelagh.fields`` module has implementation of common representations. For example, SQLite stores booleans as integers. This is how the custom field looks like:

.. code-block:: python

    class IntBoolean(Field[int, bool]):
        """
        A boolean.

        This field is used in adapters that represent booleans as an
        integer. SQLite, eg, has no boolean type, using 1 and 0 to
        represent true and false, respectively.
        """

        # the SQLite text (see https://www.sqlite.org/datatype3.html)
        type = "BOOLEAN"

        # one of the 5 types in https://peps.python.org/pep-0249/#type-objects
        db_api_type = "NUMBER"

        def parse(self, value: Optional[int]) -> Optional[bool]:
            if value is None:
                return None
            return bool(value)

        def format(self, value: Optional[bool]) -> Optional[int]:
            if value is None:
                return None
            return 1 if value else 0

        # only needed if the adapter uses the ``build_sql`` helper function.
        def quote(self, value: Optional[int]) -> str:
            if value is None:
                return "NULL"
            return str(value)

Note that the base class for ``IntBoolean`` is ``Field[int, bool]`` — that means that the internal representation of the value is an integer, and the external is a boolean.

Estimating query cost
=====================

You can define a method ``get_cost`` on your adapter to help the query planner to optimize queries. The method receives two lists, one with the column names and operations applied to filter them, and the other with column names and the requested sort order:

.. code-block:: python

    class MyAdapter:

        def get_cost(
            self,
            filtered_columns: List[Tuple[str, Operator]],
            order: List[Tuple[str, RequestedOrder]],
            **kwargs: Any,
        ) -> float:
            return (
                100
                + 1000 * len(filtered_columns)
                + 10000 * len(order)
            )

In the example above, we have an initial cost of 100. Each filtering operation costs an additional 1000 units, and each sorting costs 10000. This is a simple representation of filtering 1000 points in O(n), and sorting them in O(n log n) (note that the numbers are unitless). These numbers can be improved if you know the size of the data.

If you want to use the model above you can do this in your adapter:

.. code-block:: python

    from shillelagh.lib import SimpleCostModel

    class MyAdapter:

        get_cost = SimpleCostModel(rows=1000, fixed_cost=100)

====================================
Creating a custom SQLAlchemy dialect
====================================

There are cases when you might want to write a new SQLAlchemy dialect, instead of (or in addition to) an adapter. This is the case of the `GSheets dialect <https://github.com/betodealmeida/shillelagh/blob/main/src/shillelagh/backends/apsw/dialects/gsheets.py>`_, which implements a ``gsheets://`` dialect, meant as a drop-in replacement for `gsheetsdb <https://pypi.org/project/gsheetsdb/>`_.

As an example, let's create a custom dialect to query S3 files, based on the ``s3select`` adapter. To use the ``s3select`` adapter the user must first create an engine using the ``shillelagh://`` SQLAlchemy URI, and then they can query files using the ``s3://bucket/path/to/file`` pattern, eg:

.. code-block:: python

    from sqlalchemy import create_engine

    engine = create_engine("shillelagh://")
    connection = engine.connect()
    cursor = connection.cursor()

    cursor.execute('SELECT * FROM "s3://shillelagh/files/sample_data.parquet"')

Imagine instead that we want the user to create an engine passing a bucket name and a default prefix, as well as querying the file without having to specify the suffix, since we only want to support Parquet files:

.. code-block:: python

    from sqlalchemy import create_engine

    engine = create_engine("s3://shillelagh/files")
    connection = engine.connect()
    cursor = connection.cursor()

    cursor.execute("SELECT * FROM sample_data")

The first thing to do is implement our dialect:

.. code-block:: python

    from sqlalchemy.engine.url import URL

    from shillelagh.backends.apsw.dialects.base import APSWDialect

    class S3Dialect(APSWDialect):

        # scheme of the SQLAlchemy URI (s3://)
        name = "s3"

        # this is supported in the base class, but needs to be explicitly set in children
        supports_statement_cache = True

        def create_connect_args(self, url: URL) -> Tuple[Tuple[()], Dict[str, Any]]:
            parsed = urllib.parse.urlparse(url)
            bucket = parsed.netloc
            prefix = parsed.path.strip("/") + "/"

            return (), {
                "path": ":memory:",
                "adapters": ["custom_s3select"],
                "adapter_kwargs": {
                    "custom_s3select": {
                        "bucket": bucket,
                        "prefix": prefix,
                    },
                },
                "safe": True,
                "isolation_level": self.isolation_level,
            }

The ``create_connect_args`` method will parse the engine URI, ``s3://shillelagh/files``, and pass the bucket name ("shillelagh") and the key prefix ("files") to a custom adapter ("custom_s3select") that we're going to implement. The dialect will use only a single Shillelagh adapter.

The adapter is based on the ``s3select`` adapter:

.. code-block:: python

    from shillelagh.adapters.api.s3select import InputSerializationType, S3SelectAPI

    class CustomS3AdapterAPI(S3SelectAPI):

        def __init__(self, table: str, bucket: str, prefix: str, **kwargs: Any):
            # build the key based on the prefix/suffix
            key = f"{prefix}{table}.parquet"

            # the dialect will only support uncompressed Parquet files
            input_serialization = {"CompressionType": "NONE", "Parquet": {}}

            return super().__init__(bucket, key, input_serialization, **kwargs)

        @staticmethod
        def supports(uri: str, fast: bool = True, **kwargs: Any) -> Optional[bool]:
            # since there's only one adapter, support all table names
            return True

        @staticmethod
        def parse_uri(uri: str) -> Tuple[str]:
            # simple return the table name
            return (uri,)

With the adapter above, when the user writes a query like ``SELECT * FROM sample_data`` Shillelagh will iterate over all the registered adapters, which is only ``custom_s3select``. It will then call the ``supports`` method to see if the adapter can handle ``sample_data``; since there's only a single adapter it can simply return true.

Shillelagh will then call ``parse_uri("sample_data")``, which returns the table name unmodified. It will then instantiate the adapter with the response from ``parse_uri``, together with any additional keyword arguments present in ``adapter_kwargs`` (populated in the dialect's ``create_connect_args``). In this case:

.. code-block:: python

    CustomS3AdapterAPI("sample_data", bucket="shillelagh", prefix="files/")

Then ``CustomS3AdapterAPI`` combines the prefix and the table name into a single key with ``files/sample_data.parquet``, and calls the base class:

.. code-block:: python

    S3SelectAPI(
        "shilellagh",
        "files/sample_data.parquet",
        {"CompressionType": "NONE", "Parquet": {}},
    )

Everything else is handled by the original ``s3select`` adapter.

In order for this to work we need to register the SQLAlchemy dialect and the Shillelagh adapter. An easy way to do that is by adding entry points in ``setup.py``:

.. code-block:: python

    setup(
        ...,
        entry_points={
            "shillelagh.adapter": ["custom_s3select = path.to:CustomS3AdapterAPI"],
            "sqlalchemy.dialects": ["s3 = path.to:S3Dialect"],
        },
    )

Customizing the dialect
=======================

Finally, to make our dialect more useful, we can implement a few methods. It's useful to start with ``do_ping``, which is used to determine if the database is online. For our dialect we can simply do a ``HEAD`` request on a file that is known to exist.

Second, we want to implement ``has_table`` and ``get_table_names``. The first is used to determine if a given table name exists. The dialect will have to build the full key based on the table name and do an S3 request to determine if the corresponding file exists. The second is used to retrieve the list of existing tables. The dialect will fetch all the keys for the given bucket/prefix, and format them by stripping the prefix and suffix.

.. code-block:: python

    import boto3
    from botocore.exceptions import ClientError
    from sqlalchemy.pool.base import _ConnectionFairy

    HEALTH_BUCKET = "bucket-name"
    HEALTH_KEY = "health-file"

    class CustomS3AdapterAPI(S3SelectAPI):

        def __init__(self, *args: Any, **kwargs: Any):
            super().__init__(*args, **kwargs)

            self.s3_client = boto3.client("s3")

            ...

        def do_ping(self, dbapi_connection: _ConnectionFairy) -> bool:
            """
            Return true if the database is online.

            To check if S3 is accessible the method will do a ``HEAD`` request on a known file
            """
            try:
                s3_client.head_object(Bucket=HEALTH_BUCKET, Key=HEALTH_KEY)
                return True
            except ClientError:
                return False

        def has_table(
            self,
            connection: _ConnectionFairy,
            table_name: str,
            schema: Optional[str] = None,
            info_cache: Optional[Dict[Any, Any]] = None,
            **kwargs: Any,
        ) -> bool:
            """
            Return true if a given table exists.

            In order to determine if a table exists the method will build the full key
            and do a ``HEAD`` request on the resource.
            """
            raw_connection = connection.engine.raw_connection()
            bucket = raw_connection._adapter_kwargs["custom_s3select"]["bucket"]
            prefix = raw_connection._adapter_kwargs["custom_s3select"]["prefix"]
            key = f"{prefix}{table_name}.parquet"

            try:
                s3_client.head_object(Bucket=bucket, Key=key)
                return True
            except ClientError:
                return False

        def get_table_names(  # pylint: disable=unused-argument
            self,
            connection: _ConnectionFairy,
            schema: str = None,
            sqlite_include_internal: bool = False,
            **kwargs: Any,
        ) -> List[str]:
            """
            Return a list of table names.

            To build the list of table names the method will retrieve all objects from the
            prefix, and strip out the prefix and suffix from the key name:

                files/sample_data.parquet => sample_data

            """
            raw_connection = connection.engine.raw_connection()
            bucket = raw_connection._adapter_kwargs["custom_s3select"]["bucket"]
            prefix = raw_connection._adapter_kwargs["custom_s3select"]["prefix"]
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

            # strip the prefix and the suffix from the key to get the table name
            start = len(prefix)
            end = -len('.parquet')
            return [
                obj["Key"][start:end]
                for obj in response.get("Contents", [])
                if obj["Key"].startswith(prefix) and obj["Key"].endswith(SUFFIX)
            ]

Cookiecutter template
=====================

Shillelagh has a `cookiecutter <https://github.com/cookiecutter/cookiecutter>`_ template that scaffolds the creation of new adapters and their tests. Just run:

.. code-block:: bash

    $ pip install cookiecutter
    $ cookiecutter templates/adapter/

And answer the questions.
