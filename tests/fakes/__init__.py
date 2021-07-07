import json
import os
import urllib.parse
from typing import Any
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple

from shillelagh.adapters.base import Adapter
from shillelagh.fields import Float
from shillelagh.fields import Integer
from shillelagh.fields import Order
from shillelagh.fields import String
from shillelagh.filters import Equal
from shillelagh.filters import Filter
from shillelagh.filters import Range
from shillelagh.lib import filter_data
from shillelagh.typing import RequestedOrder
from shillelagh.typing import Row


class FakeEntryPoint:
    def __init__(self, name: str, adapter: Adapter):
        self.name = name
        self.adapter = adapter

    def load(self) -> Adapter:
        return self.adapter


class FakeAdapter(Adapter):

    safe = False

    age = Float(filters=[Range], order=Order.ANY, exact=True)
    name = String(filters=[Equal], order=Order.ANY, exact=True)
    pets = Integer(order=Order.ANY)

    @staticmethod
    def supports(uri: str, **kwargs: Any) -> bool:
        parsed = urllib.parse.urlparse(uri)
        return parsed.scheme == "dummy"

    @staticmethod
    def parse_uri(uri: str) -> Tuple[()]:
        return ()

    def __init__(self):
        self.data = [
            {"rowid": 0, "name": "Alice", "age": 20, "pets": 0},
            {"rowid": 1, "name": "Bob", "age": 23, "pets": 3},
        ]

    def get_data(
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
    ) -> Iterator[Dict[str, Any]]:
        yield from filter_data(iter(self.data), bounds, order)

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
with open(os.path.join(dirname, "weatherapi_response.json")) as fp:
    weatherapi_response = json.load(fp)
with open(os.path.join(dirname, "cdc_metadata_response.json")) as fp:
    cdc_metadata_response = json.load(fp)
with open(os.path.join(dirname, "cdc_data_response.json")) as fp:
    cdc_data_response = json.load(fp)
