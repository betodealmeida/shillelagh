==========
shillelagh
==========

Shillelagh is a library that makes it easy to write adapters to APIs so that they can be queried via SQL.

Quick example
=============

Let's say we want to fetch data from `WeatherAPI <https://www.weatherapi.com/docs/>`_ using SQL. Their API is pretty straightforward — to fetch data for a given day in a given location all we need is an HTTP request:

.. code-block::

    https://api.weatherapi.com/v1/history.json?key=XXX&q=94158&dt=2020-01-01

This will return data for 2020-01-01 in the ZIP code 94158 as a JSON payload.

The response contains many different variables, but let's assume we're only interested in ``timestamp`` and ``temperature`` for the sake of this example. Of those two, ``timestamp`` is special because it can be used to filter data coming from the API, reducing the amount that needs to be downloaded.

We start by defining an "adapter" class, with the columns we're interested in:

.. code-block:: python

    from shillelagh.adapters.base import Adapter

    class WeatherAPI(Adapter):

        ts = DateTime(filters=[Range], order=Order.ASCENDING, exact=False)
        temperature = Float()

The ``ts`` (timestamp) column has the type ``DateTime``, and can be filtered with a desired range (for example, ``WHERE ts >= '2020-01-01' AND ts <= '2020-01-07'``). We know that the values will be returned in ascending order by the API, so we annotate that to help the SQL engine. If a query has ``ORDER BY ts ASC`` we know that we don't need to sort the payload.

In addition, we declare that the results from filtering ``ts`` are not exact. This is because the API returns data for every hour of a given day. To make our lives easier we're going to filter the data down to the daily granularity, and let the SQL engine filter the rest. For example, imagine this query:

.. code-block:: sql

    SELECT * FROM weatherapi WHERE ts > '2020-01-01T12:00:00' AND ts < '2020-01-02T12:00:00'

In this case, the adapter is going to download **all data** for the days 2020-01-01 and 2020-01-02, and pass them to the SQL engine to narrow it down to between noon in each day. We could do that filtering ourselves in the adapter, but since we're not discarding a lot of data it's ok.

For ``temperature`` we simply declare it as float, since we can't use temperature values to pre-filter data in the API.

Now we define our ``__init__`` method, which initializes the adapter with the location and API key:

.. code-block:: python

        def __init__(self, location: str, api_key: str):
            self.location = location
            self.api_key = api_key

Finally, we define a method to download data from the API:

.. code-block:: python

        def get_data(self, bounds: Dict[str, Filter], order: List[Tuple[str, RequestedOrder]]) -> Iterator[Row]:
            ts_range: Range = bounds["ts"]
            today = date.today()
            start = ts_range.start.date() if ts_range.start else today - timedelta(days=7)
            end = ts_range.end.date() if ts_range.end else today

            while start <= end:
                url = (
                    f"https://api.weatherapi.com/v1/history.json?key={self.api_key}"
                    f"&q={self.location}&dt={start}"
                )
                response = requests.get(url)
                if response.ok:
                    payload = response.json()
                    hourly_data = payload["forecast"]["forecastday"][0]["hour"]
                    for record in hourly_data:
                        dt = dateutil.parser.parse(record["time"])
                        yield {
                            "rowid": int(dt.timestamp()),
                            "ts": dt.isoformat(),
                            "temperature": record["temp_c"],
                        }

                start += timedelta(days=1)

The important thing to know here is that since we defined ``ts`` as being filtered through a ``Range``, a corresponding range will be passed to the ``get_data`` method specifying how ``ts`` should be filtered. The range has optional start and end values, which when not present are defaulted to 7 days ago and today, respectively.

Note also that the method yields rows as dictionaries. In addition to values for ``ts`` and ``temperature`` it also returns a row ID. This should be a unique value for each row.

We also need to define some dispatching methods, so our adapter can be found:

.. code-block:: python

        @staticmethod
        def supports(uri: str) -> bool:
            """https://api.weatherapi.com/v1/history.json?key=XXX&q=94158"""
            parsed = urllib.parse.urlparse(uri)
            query_string = urllib.parse.parse_qs(parsed.query)
            return (
                parsed.netloc == "api.weatherapi.com"
                and parsed.path == "/v1/history.json"
                and "key" in query_string
                and "q" in query_string
            )

        @staticmethod
        def parse_uri(uri: str) -> Tuple[str, str]:
            parsed = urllib.parse.urlparse(uri)
            query_string = urllib.parse.parse_qs(parsed.query)
            location = query_string["q"][0]
            api_key = query_string["key"][0]
    
            return (location, api_key)

Now we can use our class to query the API using Sqlite:

.. code-block:: python

    from shillelagh.backends.apsw.db import connect

    connection = connect(":memory:")
    cursor = connection.cursor()

    api_key = "XXX"
    query = f"""
        SELECT *
        FROM "https://api.weatherapi.com/v1/history.json?key={api_key}&q=94923" AS bodega_bay
        WHERE ts >= '2020-01-01T12:00:00'
    """
    for row in cursor.execute(query):
        print(row)
