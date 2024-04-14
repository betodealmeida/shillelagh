"""
An adapter to Datasette instances.

See https://datasette.io/ for more information.
"""

import logging
import urllib.parse
from datetime import timedelta
from typing import Any, Dict, Iterator, List, Optional, Tuple, Type, cast

import dateutil.parser

from shillelagh.adapters.base import Adapter
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Field, Float, Integer, ISODate, ISODateTime, Order, String
from shillelagh.filters import Equal, Filter, IsNotNull, IsNull, Like, NotEqual, Range
from shillelagh.lib import SimpleCostModel, build_sql, get_session
from shillelagh.typing import RequestedOrder, Row

_logger = logging.getLogger(__name__)

KNOWN_DOMAINS = {"datasette.io", "datasettes.com"}

# this is just a wild guess; used to estimate query cost
AVERAGE_NUMBER_OF_ROWS = 1000

# how many rows to get when performing our own pagination
DEFAULT_LIMIT = 1000

CACHE_EXPIRATION = timedelta(minutes=3)


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
    parsed = urllib.parse.urlparse(uri)
    try:
        # pylint: disable=unused-variable
        mountpoint, database, table = parsed.path.rsplit("/", 2)
    except ValueError:
        return False

    parsed = parsed._replace(path=f"{mountpoint}/-/versions.json")
    uri = urllib.parse.urlunparse(parsed)

    session = get_session({}, "datasette_cache", CACHE_EXPIRATION)
    response = session.get(uri)
    try:
        payload = response.json()
    except Exception:  # pylint: disable=broad-exception-caught
        return False

    return "datasette" in payload


def get_field(value: Any) -> Field:
    """
    Return a Shillelagh ``Field`` based on the value type.
    """
    class_: Type[Field] = String
    filters = [Range, Equal, NotEqual, IsNull, IsNotNull]

    if isinstance(value, int):
        class_ = Integer
    elif isinstance(value, float):
        class_ = Float
    elif isinstance(value, str):
        try:
            dateutil.parser.isoparse(value)
        except Exception:  # pylint: disable=broad-except
            # regular string
            filters.append(Like)
        else:
            class_ = ISODate if len(value) == 10 else ISODateTime  # type: ignore

    return class_(filters=filters, order=Order.ANY, exact=True)


class DatasetteAPI(Adapter):
    """
    An adapter to Datasette instances (https://datasette.io/).
    """

    safe = True

    supports_limit = True
    supports_offset = True

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
        self._session = get_session({}, "datasette_cache", CACHE_EXPIRATION)

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
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        **kwargs: Any,
    ) -> Iterator[Row]:
        offset = offset or 0
        while True:
            if limit is None:
                # request 1 more, so we know if there are more pages to be fetched
                end = DEFAULT_LIMIT + 1
            else:
                end = min(limit, DEFAULT_LIMIT + 1)

            sql = build_sql(
                self.columns,
                bounds,
                order,
                f'"{self.table}"',
                limit=end,
                offset=offset,
            )
            payload = self._run_query(sql)

            if payload.get("error"):
                raise ProgrammingError(
                    f'Error ({payload["title"]}): {payload["error"]}',
                )

            columns = payload["columns"]
            rows = payload["rows"]

            i = -1
            for i, values in enumerate(rows[:DEFAULT_LIMIT]):
                row = dict(zip(columns, values))
                row["rowid"] = i
                _logger.debug(row)
                yield row

            if not payload["truncated"] and len(rows) <= DEFAULT_LIMIT:
                break

            offset += i + 1
            if limit is not None:
                limit -= i + 1
