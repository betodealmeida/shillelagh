from typing import Any
from typing import Dict
from typing import Iterator
from typing import Optional

from shillelagh.adapters.base import Adapter
from shillelagh.fields import Float
from shillelagh.fields import Integer
from shillelagh.fields import Order
from shillelagh.fields import String
from shillelagh.filters import Equal
from shillelagh.filters import Filter
from shillelagh.filters import Range
from shillelagh.types import Row


class DummyAdapter(Adapter):

    age = Float(filters=[Range], order=Order.NONE, exact=True)
    name = String(filters=[Equal], order=Order.ASCENDING, exact=True)
    pets = Integer()

    def __init__(self):
        self.data = [
            {"rowid": 0, "name": "Alice", "age": 20, "pets": 0},
            {"rowid": 1, "name": "Bob", "age": 23, "pets": 3},
        ]

    def get_data(self, bounds: Dict[str, Filter]) -> Iterator[Dict[str, Any]]:
        data = self.data[:]

        for column in ["name", "age"]:
            if column in bounds:
                data = [row for row in data if bounds[column].check(row[column])]

        yield from iter(data)

    def insert_row(self, row: Row) -> int:
        row_id: Optional[int] = row["rowid"]
        if row_id is None:
            row["rowid"] = row_id = max(row["rowid"] for row in self.data) + 1

        self.data.append(row)

        return row_id

    def delete_row(self, row_id: int) -> None:
        self.data = [row for row in self.data if row["rowid"] != row_id]


def test_adapter_get_columns():
    adapter = DummyAdapter()
    assert adapter.get_columns() == {
        "age": DummyAdapter.age,
        "name": DummyAdapter.name,
        "pets": DummyAdapter.pets,
    }


def test_adapter_get_data():
    adapter = DummyAdapter()
    data = adapter.get_data({})
    assert list(data) == [
        {"rowid": 0, "name": "Alice", "age": 20, "pets": 0},
        {"rowid": 1, "name": "Bob", "age": 23, "pets": 3},
    ]

    data = adapter.get_data({"name": Equal("Alice")})
    assert list(data) == [
        {"rowid": 0, "name": "Alice", "age": 20, "pets": 0},
    ]

    data = adapter.get_data({"age": Range(20, None, False, False)})
    assert list(data) == [
        {"rowid": 1, "name": "Bob", "age": 23, "pets": 3},
    ]


def test_adapter_manipulate_rows():
    adapter = DummyAdapter()

    adapter.insert_row({"rowid": None, "name": "Charlie", "age": 6, "pets": 1})
    data = adapter.get_data({})
    assert list(data) == [
        {"rowid": 0, "name": "Alice", "age": 20, "pets": 0},
        {"rowid": 1, "name": "Bob", "age": 23, "pets": 3},
        {"rowid": 2, "name": "Charlie", "age": 6, "pets": 1},
    ]
    adapter.insert_row({"rowid": 4, "name": "Dani", "age": 40, "pets": 2})
    data = adapter.get_data({})
    assert list(data) == [
        {"rowid": 0, "name": "Alice", "age": 20, "pets": 0},
        {"rowid": 1, "name": "Bob", "age": 23, "pets": 3},
        {"rowid": 2, "name": "Charlie", "age": 6, "pets": 1},
        {"rowid": 4, "name": "Dani", "age": 40, "pets": 2},
    ]

    adapter.delete_row(0)
    data = adapter.get_data({})
    assert list(data) == [
        {"rowid": 1, "name": "Bob", "age": 23, "pets": 3},
        {"rowid": 2, "name": "Charlie", "age": 6, "pets": 1},
        {"rowid": 4, "name": "Dani", "age": 40, "pets": 2},
    ]

    adapter.update_row(1, {"rowid": 1, "name": "Bob", "age": 24, "pets": 4})
    data = adapter.get_data({})
    assert list(data) == [
        {"rowid": 2, "name": "Charlie", "age": 6, "pets": 1},
        {"rowid": 4, "name": "Dani", "age": 40, "pets": 2},
        {"rowid": 1, "name": "Bob", "age": 24, "pets": 4},
    ]
