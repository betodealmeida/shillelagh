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

import requests_cache
from shillelagh.adapters.base import Adapter
from shillelagh.exceptions import ImpossibleFilterError
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Date
from shillelagh.fields import Field
from shillelagh.fields import Float
from shillelagh.fields import Order
from shillelagh.fields import String
from shillelagh.filters import Equal
from shillelagh.filters import Filter
from shillelagh.filters import Range
from shillelagh.lib import build_sql
from shillelagh.types import RequestedOrder
from shillelagh.types import Row
from typing_extensions import TypedDict


path_regex = re.compile(r"/resource/\w{4}-\w{4}.json")


class MetadataColumn(TypedDict):
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


type_map: Dict[str, Tuple[Type[Field], List[Type[Filter]]]] = {
    "calendar_date": (Date, [Range]),
    "number": (Float, [Range]),
    "text": (String, [Equal]),
}


def get_field(col: MetadataColumn) -> Field:
    class_, filters = type_map.get(col["dataTypeName"], (String, [Equal]))
    return class_(
        filters=filters,
        order=Order.ANY,
        exact=True,
    )


class SocrataAPI(Adapter):

    safe = True

    @staticmethod
    def supports(uri: str, **kwargs: Any) -> bool:
        """https://data.cdc.gov/resource/unsk-b7fc.json"""
        parsed = urllib.parse.urlparse(uri)
        return bool(path_regex.match(parsed.path))

    @staticmethod
    def parse_uri(uri: str) -> Tuple[str, str, Optional[str]]:
        parsed = urllib.parse.urlparse(uri)
        dataset_id = Path(parsed.path).stem
        query_string = urllib.parse.parse_qs(parsed.query)
        app_token = (
            query_string["$$app_token"][0] if "$$app_token" in query_string else None
        )

        return (
            parsed.netloc,
            dataset_id,
            app_token,
        )

    def __init__(self, netloc: str, dataset_id: str, app_token: Optional[str]):
        self.netloc = netloc
        self.dataset_id = dataset_id
        self.app_token = app_token

        self._session = requests_cache.CachedSession(
            cache_name="socrata_cache",
            backend="sqlite",
            expire_after=180,
        )

        self._set_columns()

    def _set_columns(self) -> None:
        url = f"https://{self.netloc}/api/views/{self.dataset_id}"
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
        response = self._session.get(url, params={"$query": sql}, headers=headers)
        payload = response.json()

        # {'message': 'Invalid SoQL query', 'errorCode': 'query.soql.invalid', 'data': {}}
        if "errorCode" in payload:
            raise ProgrammingError(payload["message"])

        for i, row in enumerate(payload):
            row["rowid"] = i
            yield row
