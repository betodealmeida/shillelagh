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

Ssecond, we need to implement a method called ``supports`` in our class, that returns true when it knows how to handle a given table (or URI, in this case). In our case, it should return true if these 2 conditions are met:

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
    def supports(uri: str, **kwargs: Any) -> bool:
        parsed = urllib.parse.urlparse(uri)
        query_string = urllib.parse.parse_qs(parsed.query)
        return (
            parsed.netloc == "api.weatherapi.com"
            and parsed.path == "/v1/history.json"
            and "q" in query_string
            and ("key" in query_string or "api_key" in kwargs)
        )

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

With more complex APIs the columns might change from instance to instance of the adapter — eg, for Google Sheets the number, names, and types of columns will vary from spreadsheet to spreadsheet. In that case we would need to implement a method that instrospects the spreadsheet in order to return the collumns.

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

A read-write adapter
====================

For a read-write adapter we need to implement at least 2 additional methods:

- ``insert_row(self, row: Dict[str, Any]) -> int``
- ``delete_row(self, row_id: int) -> None``

We also might want to implement a method for updating rows:

- ``update_row(self, row_id: int, row: Dict[str, Any]) -> None``

If ``update_row`` is not defined Shillelagh will udpate rows by calling ``delete_row`` followed by an ``insert_row`` with the updated values.

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
        def supports(uri: str, **kwargs: Any) -> bool:
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
        ) -> Iterator[Dict[str, Any]]:
            yield from iter(self.data)

        def insert_rows(self, row: Dict[str, Any]) -> int:
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

There's a different way of handling data conversion. The adapter can specify a custom ``Field`` for a given column. ``Field`` objects have two methods called ``parse`` and ``format``, responsible for the conversion between the format used by the adapter and native Python types. When using a custom field **the adapter can return the original format before conversion**, but defining the ``get_data`` method instead of ``get_rows``.

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

The ``shilellagh.fields`` module has implementation of common representations. For example, SQLite stores booleans as integers. This is how the custom field looks like:

.. code-block:: python

    class IntBoolean(Field[int, bool]):
        """
        A boolean.
    
        This field is used in adapters that represent booleans as an
        integer. SQLite, eg, has no boolean type, using 1 and 0 to
        represent true and false, respectively.
        """
    
        type = "BOOLEAN"
        db_api_type = "NUMBER"
    
        def parse(self, value: Optional[int]) -> Optional[bool]:
            if value is None:
                return None
            return bool(value)
    
        def format(self, value: Optional[bool]) -> Optional[int]:
            if value is None:
                return None
            return 1 if value else 0
    
        def quote(self, value: Optional[int]) -> str:
            if value is None:
                return "NULL"
            return str(value)

Note that the base class for ``IntBoolean`` is ``Field[int, bool]`` — that means that the internal representation of the value is an integer, and the external is a boolean.