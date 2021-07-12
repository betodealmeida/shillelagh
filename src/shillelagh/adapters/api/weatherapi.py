"""
An adapter to WeatherAPI (https://www.weatherapi.com/).
"""
import logging
import urllib.parse
from datetime import date
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any
from typing import cast
from typing import Dict
from typing import Iterator
from typing import List
from typing import Tuple
from typing import Union

import dateutil.parser
import dateutil.tz
import requests_cache

from shillelagh.adapters.base import Adapter
from shillelagh.exceptions import ImpossibleFilterError
from shillelagh.fields import DateTime
from shillelagh.fields import Float
from shillelagh.fields import IntBoolean
from shillelagh.fields import Integer
from shillelagh.fields import Order
from shillelagh.fields import String
from shillelagh.filters import Filter
from shillelagh.filters import Impossible
from shillelagh.filters import Range
from shillelagh.typing import RequestedOrder
from shillelagh.typing import Row

_logger = logging.getLogger(__name__)


def combine_time_filters(bounds: Dict[str, Filter]) -> Range:
    """
    Combine both time filters together.

    The adapter has two time columns that can be used to filter the data, "time" as
    a timestamp and "time_epoch" as a float. We conver the latter to a timestamp and
    combine the two filters into a single ``Range``.
    """
    time_range = bounds.get("time", Range())
    time_epoch_range = bounds.get("time_epoch", Range())
    if not isinstance(time_range, Range) or not isinstance(time_epoch_range, Range):
        raise Exception("Invalid filter")

    # convert time_epoch range to datetime so we can combine it
    # with the time range
    time_epoch_range.start = (
        datetime.fromtimestamp(time_epoch_range.start, tz=timezone.utc)
        if time_epoch_range.start is not None
        else None
    )
    time_epoch_range.end = (
        datetime.fromtimestamp(time_epoch_range.end, tz=timezone.utc)
        if time_epoch_range.end is not None
        else None
    )

    # combine time ranges together and check if the result is a valid range
    time_range += time_epoch_range
    if isinstance(time_range, Impossible):
        raise ImpossibleFilterError()

    return cast(Range, time_range)


class WeatherAPI(Adapter):

    """
    An adapter for WeatherAPI (https://www.weatherapi.com/).

    The adapter expects an URL like::

        https://api.weatherapi.com/v1/history.json?key=$key&q=$location

    Where ``$key`` is an API key (available for free), and ``$location`` is a
    freeform value that can be a US Zipcode, UK Postcode, Canada Postalcode,
    IP address, Latitude/Longitude (decimal degree) or city name.
    """

    safe = True

    # These two columns can be used to filter the results from the API. We
    # define them as inexact since we will retrieve data for the whole day,
    # even if specific hours are requested. The post-filtering will be done
    # by the backend.
    time = DateTime(filters=[Range], order=Order.ASCENDING, exact=False)
    time_epoch = Float(filters=[Range], order=Order.ASCENDING, exact=False)

    temp_c = Float()
    temp_f = Float()
    is_day = IntBoolean()
    wind_mph = Float()
    wind_kph = Float()
    wind_degree = Integer()
    wind_dir = String()
    pressure_mb = Float()
    pressure_in = Float()
    precip_mm = Float()
    precip_in = Float()
    humidity = Integer()
    cloud = Integer()
    feelslike_c = Float()
    feelslike_f = Float()
    windchill_c = Float()
    windchill_f = Float()
    heatindex_c = Float()
    heatindex_f = Float()
    dewpoint_c = Float()
    dewpoint_f = Float()
    will_it_rain = IntBoolean()
    chance_of_rain = String()
    will_it_snow = IntBoolean()
    chance_of_snow = String()
    vis_km = Float()
    vis_miles = Float()
    gust_mph = Float()
    gust_kph = Float()

    @staticmethod
    def supports(uri: str, **kwargs: Any) -> bool:
        """https://api.weatherapi.com/v1/history.json?key=XXX&q=94158"""
        parsed = urllib.parse.urlparse(uri)
        query_string = urllib.parse.parse_qs(parsed.query)
        return (
            parsed.netloc == "api.weatherapi.com"
            and parsed.path == "/v1/history.json"
            and "q" in query_string
            and ("key" in query_string or "api_key" in kwargs)
        )

    @staticmethod
    def parse_uri(uri: str) -> Union[Tuple[str], Tuple[str, str]]:
        parsed = urllib.parse.urlparse(uri)
        query_string = urllib.parse.parse_qs(parsed.query)
        location = query_string["q"][0]

        # key can be passed in the URL or via connection arguments
        if "key" in query_string:
            return (location, query_string["key"][0])
        return (location,)

    def __init__(self, location: str, api_key: str):
        super().__init__()

        self.location = location
        self.api_key = api_key

        # use a cache, since the adapter does a lot of similar API requests,
        # and the data should rarely (never?) change
        self._session = requests_cache.CachedSession(
            cache_name="weatherapi_cache",
            backend="sqlite",
            expire_after=180,
        )

    def get_data(  # pylint: disable=too-many-locals
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
    ) -> Iterator[Row]:
        # combine filters from the two time columns
        try:
            time_range = combine_time_filters(bounds)
        except ImpossibleFilterError:
            return

        # the free version of the API offers only 7 days of data; default to that
        today = date.today()
        a_week_ago = today - timedelta(days=7)
        start = time_range.start.date() if time_range.start else a_week_ago
        end = time_range.end.date() if time_range.end else today
        _logger.debug("Range is %s to %s", start, end)

        # download data from every today from [start, end]
        while start <= end:
            url = "https://api.weatherapi.com/v1/history.json"
            params = {"key": self.api_key, "q": self.location, "dt": start}

            query_string = urllib.parse.urlencode(params)
            _logger.info("GET %s?%s", url, query_string)

            response = self._session.get(url, params=params)
            if response.ok:
                payload = response.json()
                local_timezone = dateutil.tz.gettz(payload["location"]["tz_id"])
                for record in payload["forecast"]["forecastday"][0]["hour"]:
                    row = {column: record[column] for column in self.get_columns()}
                    row["time"] = dateutil.parser.parse(record["time"]).replace(
                        tzinfo=local_timezone,
                    )
                    row["rowid"] = int(row["time_epoch"])
                    yield row
                    _logger.debug(row)

            start += timedelta(days=1)
