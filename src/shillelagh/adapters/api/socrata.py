"""
An adapter to the Socrata Open Data API.

See https://dev.socrata.com/ for more information.
"""

import logging
import re
import urllib.parse
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple, Type, Union

import requests_cache
from requests import Request
from typing_extensions import TypedDict

from shillelagh.adapters.base import Adapter
from shillelagh.exceptions import ImpossibleFilterError, ProgrammingError
from shillelagh.fields import Field, Order, String, StringDate
from shillelagh.filters import Equal, Filter, IsNotNull, IsNull, Like, NotEqual, Range
from shillelagh.lib import SimpleCostModel, build_sql, flatten
from shillelagh.typing import RequestedOrder, Row

_logger = logging.getLogger(__name__)

# regex used to determine if the URI is supported by the adapter
path_regex = re.compile(r"/resource/\w{4}-\w{4}.json")

# this is just a wild guess; used to estimate query cost
AVERAGE_NUMBER_OF_ROWS = 1000


class MetadataColumn(TypedDict):
    """
    A dictionary with metadata about a Socrata API column.
    """

    id: int
    name: str
    dataTypeName: str
    description: str
    fieldName: str
    position: int
    renderTypeName: str
    tableColumnId: int
    cachedContents: Dict[str, Any]
    format: Dict[str, Any]


class Number(Field[str, float]):
    """
    A type for numbers stored as strings.

    The Socrata API will return numbers as strings. This custom field
    will convert between them and floats.
    """

    type = "REAL"
    db_api_type = "NUMBER"

    def parse(self, value: Optional[str]) -> Optional[float]:
        if value is None:
            return None
        return float(value)

    def format(self, value: Optional[float]) -> Optional[str]:
        if value is None:
            return None
        return str(value)


type_map: Dict[str, Tuple[Type[Field], List[Type[Filter]]]] = {
    "calendar_date": (StringDate, [Range, Equal, NotEqual, IsNull, IsNotNull]),
    "number": (Number, [Range, Equal, NotEqual, IsNull, IsNotNull]),
    "text": (String, [Range, Equal, NotEqual, Like, IsNull, IsNotNull]),
}


def get_field(col: MetadataColumn) -> Field:
    """
    Return a Shillelagh ``Field`` from a Socrata column.
    """
    class_, filters = type_map.get(col["dataTypeName"], (String, [Equal]))
    return class_(
        filters=filters,
        order=Order.ANY,
        exact=True,
    )


class SocrataAPI(Adapter):
    """
    An adapter to the Socrata Open Data API (https://dev.socrata.com/).

    The API is used in many governmental websites, including the CDC. Queries
    can be sent in the "Socrata Query Language", a small dialect of SQL.
    """

    safe = True

    supports_limit = True
    supports_offset = True

    @staticmethod
    def supports(uri: str, fast: bool = True, **kwargs: Any) -> Optional[bool]:
        """https://data.cdc.gov/resource/unsk-b7fc.json"""
        parsed = urllib.parse.urlparse(uri)
        return bool(path_regex.match(parsed.path))

    @staticmethod
    def parse_uri(uri: str) -> Union[Tuple[str, str], Tuple[str, str, str]]:
        parsed = urllib.parse.urlparse(uri)
        dataset_id = Path(parsed.path).stem
        query_string = urllib.parse.parse_qs(parsed.query)

        # app_token can be passed in the URL or via connection arguments
        if "$$app_token" in query_string:
            return (parsed.netloc, dataset_id, query_string["$$app_token"][0])
        return (parsed.netloc, dataset_id)

    def __init__(self, netloc: str, dataset_id: str, app_token: Optional[str] = None):
        super().__init__()

        self.netloc = netloc
        self.dataset_id = dataset_id
        self.app_token = app_token

        # use a cache for the API requests
        self._session = requests_cache.CachedSession(
            cache_name="socrata_cache",
            backend="sqlite",
            expire_after=180,
        )

        self._set_columns()

    def _set_columns(self) -> None:
        url = f"https://{self.netloc}/api/views/{self.dataset_id}"
        _logger.info("GET %s", url)
        response = self._session.get(url)
        payload = response.json()
        self.columns = {col["fieldName"]: get_field(col) for col in payload["columns"]}

    def get_columns(self) -> Dict[str, Field]:
        return self.columns

    get_cost = SimpleCostModel(AVERAGE_NUMBER_OF_ROWS)

    def get_data(
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        **kwargs: Any,
    ) -> Iterator[Row]:
        try:
            sql = build_sql(self.columns, bounds, order, limit=limit, offset=offset)
        except ImpossibleFilterError:
            return

        url = f"https://{self.netloc}/resource/{self.dataset_id}.json"
        headers = {"X-App-Token": self.app_token} if self.app_token else {}
        prepared = Request(
            "GET",
            url,
            params={"$query": sql},
            headers=headers,
        ).prepare()
        _logger.info("GET %s", prepared.url)
        response = self._session.send(prepared)
        payload = response.json()

        # {'message': 'Invalid SoQL query', 'errorCode': 'query.soql.invalid', 'data': {}}
        if "errorCode" in payload:
            raise ProgrammingError(payload["message"])

        for i, row in enumerate(payload):
            row["rowid"] = i
            _logger.debug(row)
            yield flatten(row)
