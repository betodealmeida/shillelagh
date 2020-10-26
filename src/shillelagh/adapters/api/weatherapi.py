from datetime import date
from datetime import datetime
from datetime import timedelta
from typing import Dict
from typing import Iterator

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
    cache_name="weatherapi_cache", backend="sqlite", expire_after=180,
)


class WeatherAPI(Adapter):

    ts = DateTime(filters=[Range], order=Order.ASCENDING, exact=False)
    temperature = Float()

    def __init__(self, location: str, api_key: str):
        self.location = location
        self.api_key = api_key

    def get_data(self, bounds: Dict[str, Filter]) -> Iterator[Row]:
        ts_range = bounds["ts"]
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
