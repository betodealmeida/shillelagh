"""
An adapter to Datasette instances.

See https://datasette.io/ for more information.
"""
import logging
import urllib.parse
from typing import Any
from typing import Dict
from typing import Iterator
from typing import List
from typing import Tuple

import requests_cache

from shillelagh.adapters.base import Adapter
from shillelagh.fields import Field
from shillelagh.fields import Float
from shillelagh.fields import Integer
from shillelagh.fields import Order
from shillelagh.fields import String
from shillelagh.filters import Filter
from shillelagh.filters import Range
from shillelagh.lib import build_sql
from shillelagh.lib import SimpleCostModel
from shillelagh.typing import RequestedOrder
from shillelagh.typing import Row

_logger = logging.getLogger(__name__)

# this is just a wild guess; used to estimate query cost
AVERAGE_NUMBER_OF_ROWS = 1000


def get_field(value: Any) -> Field:
    """
    Return a Shillelagh ``Field`` based on the value type.
    """
    if isinstance(value, int):
        return Integer(filters=[Range], order=Order.ANY, exact=True)
    if isinstance(value, float):
        return Float(filters=[Range], order=Order.ANY, exact=True)
    return String(filters=[Range], order=Order.ANY, exact=True)


class DatasetteAPI(Adapter):

    """
    An adapter to Datasette instances (https://datasette.io/).
    """

    safe = True

    @staticmethod
    def supports(uri: str, **kwargs: Any) -> bool:
        parsed = urllib.parse.urlparse(uri)
        return parsed.scheme in {"datasette+http", "datasette+https"}

    @staticmethod
    def parse_uri(uri: str) -> Tuple[str, str]:
        parts = list(urllib.parse.urlparse(uri))
        parts[0] = parts[0].split("+")[1]
        uri = urllib.parse.urlunparse(parts)
        baseurl, table = uri.rsplit("/", 1)
        return baseurl, table

    def __init__(self, baseurl: str, table: str):
        super().__init__()

        self.baseurl = baseurl
        self.table = table

        # use a cache for the API requests
        self._session = requests_cache.CachedSession(
            cache_name="datasette_cache",
            backend="sqlite",
            expire_after=180,
        )

        self._set_columns()

    def _run_query(self, sql: str) -> List[Dict[str, Any]]:
        url = f"{self.baseurl}.json"

        _logger.info("GET %s", url)
        response = self._session.get(url, params={"sql": sql})
        payload = response.json()

        rows = payload["rows"]
        column_names = payload["columns"]

        return [dict(zip(column_names, row)) for row in rows]

    def _set_columns(self) -> None:
        rows = self._run_query(f'SELECT * FROM "{self.table}" LIMIT 1')
        self.columns = {key: get_field(value) for key, value in rows[0].items()}

    def get_columns(self) -> Dict[str, Field]:
        return self.columns

    get_cost = SimpleCostModel(AVERAGE_NUMBER_OF_ROWS)

    def get_data(
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
    ) -> Iterator[Row]:
        sql = build_sql(self.columns, bounds, order, f'"{self.table}"')
        for i, row in enumerate(self._run_query(sql)):
            row["rowid"] = i
            _logger.debug(row)
            yield row
