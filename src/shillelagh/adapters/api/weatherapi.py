import urllib.parse
from datetime import date
from datetime import datetime
from datetime import timedelta
from typing import Dict
from typing import Iterator
from typing import Tuple

import dateutil.parser
import requests
import requests_cache
from shillelagh.adapters.base import Adapter
from shillelagh.fields import DateTime
from shillelagh.fields import Float
from shillelagh.fields import Order
from shillelagh.filters import Filter
from shillelagh.filters import Range
from shillelagh.types import Row


requests_cache.install_cache(
    cache_name="weatherapi_cache",
    backend="sqlite",
    expire_after=180,
)


class WeatherAPI(Adapter):

    safe = True

    ts = DateTime(filters=[Range], order=Order.ASCENDING, exact=False)
    temperature = Float()

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

    def get_data(self, bounds: Dict[str, Filter]) -> Iterator[Row]:
        ts_range = bounds["ts"]
        if not isinstance(ts_range, Range):
            raise Exception("Invalid filter")

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
