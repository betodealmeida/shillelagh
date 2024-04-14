"""
Fake objects to simplify testing.
"""

import json
import os
import urllib.parse
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple, Type

from shillelagh.adapters.base import Adapter
from shillelagh.fields import Float, Integer, Order, String
from shillelagh.filters import Equal, Filter, Range
from shillelagh.lib import filter_data
from shillelagh.typing import RequestedOrder, Row


class FakeEntryPoint:  # pylint: disable=too-few-public-methods
    """
    A fake entry point for loading adapters.
    """

    def __init__(self, name: str, adapter: Type[Adapter]):
        self.name = name
        self.adapter = adapter

    def load(self) -> Type[Adapter]:
        """
        Load the adapter.
        """
        return self.adapter


class FakeAdapter(Adapter):
    """
    A simple adapter that keeps data in memory.
    """

    scheme = "dummy"

    safe = True

    supports_limit = True
    supports_offset = True
    supports_requested_columns = True

    age = Float(filters=[Range], order=Order.ANY, exact=True)
    name = String(filters=[Equal], order=Order.ANY, exact=True)
    pets = Integer(order=Order.ANY)

    @classmethod
    def supports(cls, uri: str, fast: bool = True, **kwargs: Any) -> Optional[bool]:
        parsed = urllib.parse.urlparse(uri)
        return parsed.scheme == cls.scheme

    @staticmethod
    def parse_uri(uri: str) -> Tuple[()]:
        return ()

    def __init__(self):
        super().__init__()

        self.data = [
            {"rowid": 0, "name": "Alice", "age": 20, "pets": 0},
            {"rowid": 1, "name": "Bob", "age": 23, "pets": 3},
        ]

    def get_data(  # pylint: disable=too-many-arguments
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        requested_columns: Optional[Set[str]] = None,
        **kwargs: Any,
    ) -> Iterator[Row]:
        yield from filter_data(
            iter(self.data),
            bounds,
            order,
            limit,
            offset,
            requested_columns,
        )

    def insert_data(self, row: Row) -> int:
        row_id: Optional[int] = row["rowid"]
        if row_id is None:
            max_rowid = max(row["rowid"] for row in self.data) if self.data else 0
            row["rowid"] = row_id = max_rowid + 1

        self.data.append(row)

        return row_id

    def delete_data(self, row_id: int) -> None:
        self.data = [row for row in self.data if row["rowid"] != row_id]


dirname, filename = os.path.split(os.path.abspath(__file__))
with open(os.path.join(dirname, "weatherapi_response.json"), encoding="utf-8") as fp:
    weatherapi_response = json.load(fp)
with open(os.path.join(dirname, "cdc_metadata_response.json"), encoding="utf-8") as fp:
    cdc_metadata_response = json.load(fp)
with open(os.path.join(dirname, "cdc_data_response.json"), encoding="utf-8") as fp:
    cdc_data_response = json.load(fp)
with open(
    os.path.join(dirname, "datasette_columns_response.json"),
    encoding="utf-8",
) as fp:
    datasette_columns_response = json.load(fp)
with open(
    os.path.join(dirname, "datasette_data_response_1.json"),
    encoding="utf-8",
) as fp:
    datasette_data_response_1 = json.load(fp)
with open(
    os.path.join(dirname, "datasette_data_response_2.json"),
    encoding="utf-8",
) as fp:
    datasette_data_response_2 = json.load(fp)
with open(
    os.path.join(dirname, "datasette_metadata_response.json"),
    encoding="utf-8",
) as fp:
    datasette_metadata_response = json.load(fp)
with open(os.path.join(dirname, "datasette_results.json"), encoding="utf-8") as fp:
    datasette_results = [tuple(row) for row in json.load(fp)]
with open(os.path.join(dirname, "incidents.json"), encoding="utf-8") as fp:
    incidents = json.load(fp)
with open(os.path.join(dirname, "github_pulls_response.json"), encoding="utf-8") as fp:
    github_pulls_response = json.load(fp)
with open(os.path.join(dirname, "github_issues_response.json"), encoding="utf-8") as fp:
    github_issues_response = json.load(fp)
with open(os.path.join(dirname, "github_single_response.json"), encoding="utf-8") as fp:
    github_single_response = json.load(fp)
