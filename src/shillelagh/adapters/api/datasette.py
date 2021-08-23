"""
An adapter to Datasette instances.

See https://datasette.io/ for more information.
"""
import logging
import urllib.parse
from typing import Any
from typing import cast
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple
from typing import Type

import dateutil.parser
import requests_cache

from shillelagh.adapters.base import Adapter
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Field
from shillelagh.fields import Float
from shillelagh.fields import Integer
from shillelagh.fields import ISODate
from shillelagh.fields import ISODateTime
from shillelagh.fields import Order
from shillelagh.fields import String
from shillelagh.filters import Filter
from shillelagh.filters import Range
from shillelagh.lib import build_sql
from shillelagh.lib import SimpleCostModel
from shillelagh.typing import RequestedOrder
from shillelagh.typing import Row

_logger = logging.getLogger(__name__)

KNOWN_DOMAINS = {"datasette.io", "datasettes.com"}

# this is just a wild guess; used to estimate query cost
AVERAGE_NUMBER_OF_ROWS = 1000


def is_known_domain(netloc: str) -> bool:
    """
    Identify well known Datasette domains.
    """
    for domain in KNOWN_DOMAINS:
        if netloc == domain or netloc.endswith("." + domain):
            return True
    return False


def is_datasette(uri: str) -> bool:
    """
    Identify Datasette servers via a HEAD request.
    """
    parts = list(urllib.parse.urlparse(uri))
    try:
        # pylint: disable=unused-variable
        mountpoint, database, table = parts[2].rsplit("/", 2)
    except ValueError:
        return False

    parts[2] = f"{mountpoint}/-/versions.json"
    uri = urllib.parse.urlunparse(parts)

    session = requests_cache.CachedSession(
        cache_name="datasette_cache",
        backend="sqlite",
        expire_after=180,
    )

    response = session.head(uri)
    return cast(bool, response.ok)


def get_field(value: Any) -> Field:
    """
    Return a Shillelagh ``Field`` based on the value type.
    """
    class_: Type[Field] = String

    if isinstance(value, int):
        class_ = Integer
    elif isinstance(value, float):
        class_ = Float
    elif isinstance(value, str):
        try:
            dateutil.parser.isoparse(value)
        except Exception:  # pylint: disable=broad-except
            pass
        else:
            class_ = ISODate if len(value) == 10 else ISODateTime  # type: ignore

    return class_(filters=[Range], order=Order.ANY, exact=True)


class DatasetteAPI(Adapter):

    """
    An adapter to Datasette instances (https://datasette.io/).
    """

    safe = True

    @staticmethod
    def supports(uri: str, fast: bool = True, **kwargs: Any) -> Optional[bool]:
        parsed = urllib.parse.urlparse(uri)

        if parsed.scheme in {"http", "https"} and is_known_domain(parsed.netloc):
            return True

        if fast:
            return None

        return is_datasette(uri)

    @staticmethod
    def parse_uri(uri: str) -> Tuple[str, str, str]:
        server_url, database, table = uri.rsplit("/", 2)
        return server_url, database, table

    def __init__(self, server_url: str, database: str, table: str):
        super().__init__()

        self.server_url = server_url
        self.database = database
        self.table = table

        # use a cache for the API requests
        self._session = requests_cache.CachedSession(
            cache_name="datasette_cache",
            backend="sqlite",
            expire_after=180,
        )

        self._set_columns()

    def _run_query(self, sql: str) -> Dict[str, Any]:
        """
        Run a query and return the JSON payload.
        """
        url = f"{self.server_url}/{self.database}.json"
        _logger.info("GET %s", url)
        response = self._session.get(url, params={"sql": sql})
        payload = response.json()
        return cast(Dict[str, Any], payload)

    def _set_columns(self) -> None:
        # get column names first
        payload = self._run_query(f'SELECT * FROM "{self.table}" LIMIT 0')
        columns = payload["columns"]

        # now try to get non-null values for all columns
        select = ", ".join(f'MAX("{column}")' for column in columns)
        payload = self._run_query(f'SELECT {select} FROM "{self.table}" LIMIT 1')
        rows = payload["rows"]

        if not rows:
            raise ProgrammingError(f'Table "{self.table}" has no data')

        self.columns = {key: get_field(value) for key, value in zip(columns, rows[0])}

    def get_columns(self) -> Dict[str, Field]:
        return self.columns

    get_cost = SimpleCostModel(AVERAGE_NUMBER_OF_ROWS)

    def get_metadata(self) -> Dict[str, Any]:
        url = f"{self.server_url}/-/metadata.json"
        response = self._session.get(url)
        payload = response.json()
        metadata = payload["databases"][self.database]["tables"][self.table]
        return cast(Dict[str, Any], metadata)

    def get_data(
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
    ) -> Iterator[Row]:
        offset = 0
        while True:
            sql = build_sql(
                self.columns,
                bounds,
                order,
                f'"{self.table}"',
                offset=offset,
            )
            payload = self._run_query(sql)
            columns = payload["columns"]

            i = -1
            for i, values in enumerate(payload["rows"]):
                row = dict(zip(columns, values))
                row["rowid"] = i
                _logger.debug(row)
                yield row

            if not payload["truncated"]:
                break
            offset += i + 1
