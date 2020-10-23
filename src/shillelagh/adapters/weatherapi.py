from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterator

import dateutil.parser
import requests
import requests_cache

from shillelagh.table import VirtualTable
from shillelagh.types import DateTime, Float, Order
from shillelagh.filters import Filter, Range


requests_cache.install_cache(
    cache_name="weatherapi_cache", backend="sqlite", expire_after=180
)


class WeatherAPI(VirtualTable):

    ts = DateTime(filters=[Range], order=Order.ASCENDING, exact=False)
    temperature = Float()

    def __init__(self, location: str, api_key: str):
        self.location = location
        self.api_key = api_key

    def get_data(self, bounds: Dict[str, Filter]) -> Iterator[Dict[str, Any]]:
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
