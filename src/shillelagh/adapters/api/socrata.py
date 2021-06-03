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
from shillelagh.fields import DateTime
from shillelagh.fields import Field
from shillelagh.fields import Float
from shillelagh.fields import Order
from shillelagh.fields import String
from shillelagh.filters import Equal
from shillelagh.filters import Filter
from shillelagh.filters import Impossible
from shillelagh.filters import Range
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
    "calendar_date": (DateTime, [Range]),
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


def quote(value: Any) -> str:
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        escaped_value = value.replace("'", "''")
        return f"'{escaped_value}'"
    if hasattr(value, "isoformat"):
        return f"'{value.isoformat()}'"

    raise Exception(f"Can't quote value: {value}")


def build_sql(
    bounds: Dict[str, Filter],
    order: List[Tuple[str, RequestedOrder]],
) -> str:
    sql = "SELECT *"

    conditions = []
    for column_name, filter_ in bounds.items():
        if isinstance(filter_, Impossible):
            raise ImpossibleFilterError()
        if isinstance(filter_, Equal):
            conditions.append(f"{column_name} = {quote(filter_.value)}")
        elif isinstance(filter_, Range):
            if filter_.start is not None:
                op = ">=" if filter_.include_start else ">"
                conditions.append(f"{column_name} {op} {quote(filter_.start)}")
            if filter_.end is not None:
                op = "<=" if filter_.include_end else "<"
                conditions.append(f"{column_name} {op} {quote(filter_.end)}")
        else:
            raise ProgrammingError(f"Invalid filter: {filter_}")
    if conditions:
        sql = f"{sql} WHERE {' AND '.join(conditions)}"

    column_order: List[str] = []
    for column_name, requested_order in order:
        desc = " DESC" if requested_order == Order.DESCENDING else ""
        column_order.append(f"{column_name}{desc}")
    if column_order:
        sql = f"{sql} ORDER BY {', '.join(column_order)}"

    return sql


def convert_rows(columns: Dict[str, Field], rows: List[Row]) -> Iterator[Row]:
    for row in rows:
        yield {
            column_name: columns[column_name].parse(value)
            for column_name, value in row.items()
        }


class SocrataAPI(Adapter):

    safe = True

    @staticmethod
    def supports(uri: str) -> bool:
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
            sql = build_sql(bounds, order)
        except ImpossibleFilterError:
            return

        url = f"https://{self.netloc}/resource/{self.dataset_id}.json"
        headers = {"X-App-Token": self.app_token} if self.app_token else {}
        response = self._session.get(url, params={"$query": sql}, headers=headers)
        results = response.json()
        rows = convert_rows(self.columns, results)

        for i, row in enumerate(rows):
            row["rowid"] = i
            yield row
