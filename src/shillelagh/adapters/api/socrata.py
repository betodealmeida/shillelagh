"""
An adapter to the Socrata Open Data API.

See https://dev.socrata.com/ for more information.
"""
import logging
import re
import urllib.parse
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple
from typing import Type
from typing import Union

import requests_cache
from requests import Request
from typing_extensions import TypedDict

from shillelagh.adapters.base import Adapter
from shillelagh.exceptions import ImpossibleFilterError
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Field
from shillelagh.fields import ISODate
from shillelagh.fields import Order
from shillelagh.fields import String
from shillelagh.filters import Equal
from shillelagh.filters import Filter
from shillelagh.filters import Range
from shillelagh.lib import build_sql
from shillelagh.typing import RequestedOrder
from shillelagh.typing import Row

_logger = logging.getLogger(__name__)

# regex used to determien if the URI is supported by the adapter
path_regex = re.compile(r"/resource/\w{4}-\w{4}.json")


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
    "calendar_date": (ISODate, [Range]),
    "number": (Number, [Range]),
    "text": (String, [Equal]),
}


def get_field(col: MetadataColumn) -> Field:
    """Return a Shillelagh `Field` from a Socrata column."""
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

    @staticmethod
    def supports(uri: str, **kwargs: Any) -> bool:
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

    def get_data(
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
    ) -> Iterator[Row]:
        try:
            sql = build_sql(self.columns, bounds, order)
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
        print(prepared.url)
        response = self._session.send(prepared)
        payload = response.json()

        # {'message': 'Invalid SoQL query', 'errorCode': 'query.soql.invalid', 'data': {}}
        if "errorCode" in payload:
            raise ProgrammingError(payload["message"])

        for i, row in enumerate(payload):
            row["rowid"] = i
            yield row
            _logger.debug(row)
