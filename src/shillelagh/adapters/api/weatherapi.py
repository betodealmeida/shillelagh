import urllib.parse
from datetime import date
from datetime import datetime
from datetime import timedelta
from typing import Dict
from typing import Iterator
from typing import List
from typing import Tuple

import dateutil.parser
import requests
import requests_cache
from shillelagh.adapters.base import Adapter
from shillelagh.fields import Boolean
from shillelagh.fields import DateTime
from shillelagh.fields import Float
from shillelagh.fields import Integer
from shillelagh.fields import Order
from shillelagh.fields import String
from shillelagh.filters import Filter
from shillelagh.filters import Range
from shillelagh.types import RequestedOrder
from shillelagh.types import Row


requests_cache.install_cache(
    cache_name="weatherapi_cache",
    backend="sqlite",
    expire_after=180,
)


class WeatherAPI(Adapter):

    safe = True

    time = DateTime(filters=[Range], order=Order.ASCENDING, exact=False)
    time_epoch = Float(filters=[Range], order=Order.ASCENDING, exact=False)
    temp_c = Float()
    temp_f = Float()
    is_day = Boolean()
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
    will_it_rain = Boolean()
    chance_of_rain = String()
    will_it_snow = Boolean()
    chance_of_snow = String()
    vis_km = Float()
    vis_miles = Float()
    gust_mph = Float()
    gust_kph = Float()

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

    def __init__(self, location: str, api_key: str):
        self.location = location
        self.api_key = api_key

    def get_data(
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
    ) -> Iterator[Row]:
        time_range = bounds.get("time", Range(None, None, False, False))
        if not isinstance(time_range, Range):
            raise Exception("Invalid filter")

        #  convert time_epoch range to datetime so we can combine
        #  with the time range
        time_epoch_range = bounds.get("time_epoch", Range(None, None, False, False))
        if not isinstance(time_epoch_range, Range):
            raise Exception("Invalid filter")
        time_epoch_range.start = (
            datetime.utcfromtimestamp(time_epoch_range.start)
            if time_epoch_range.start is not None
            else None
        )
        time_epoch_range.end = (
            datetime.utcfromtimestamp(time_epoch_range.end)
            if time_epoch_range.end is not None
            else None
        )
        time_range += time_epoch_range

        today = date.today()
        start = (
            time_range.start.date() if time_range.start else today - timedelta(days=7)
        )
        end = time_range.end.date() if time_range.end else today

        while start <= end:
            url = (
                f"https://api.weatherapi.com/v1/history.json?key={self.api_key}"
                f"&q={self.location}&dt={start}"
            )
            response = requests.get(url)
            if response.ok:
                payload = response.json()
                hourly_data = payload["forecast"]["forecastday"][0]["hour"]
                columns = self.get_columns()
                for record in hourly_data:
                    row = {column: record[column] for column in columns}
                    row["time"] = dateutil.parser.parse(record["time"])
                    row["rowid"] = int(row["time_epoch"])
                    yield row

            start += timedelta(days=1)
