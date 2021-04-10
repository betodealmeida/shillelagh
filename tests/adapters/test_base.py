from typing import Any
from typing import Dict
from typing import Iterator
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
from shillelagh.types import Row

from ..fakes import FakeAdapter


def test_adapter_get_columns():
    adapter = FakeAdapter()
    assert adapter.get_columns() == {
        "age": FakeAdapter.age,
        "name": FakeAdapter.name,
        "pets": FakeAdapter.pets,
    }


def test_adapter_get_data():
    adapter = FakeAdapter()

    data = adapter.get_data({}, [])
    assert list(data) == [
        {"rowid": 0, "name": "Alice", "age": 20, "pets": 0},
        {"rowid": 1, "name": "Bob", "age": 23, "pets": 3},
    ]

    data = adapter.get_data({"name": Equal("Alice")}, [])
    assert list(data) == [
        {"rowid": 0, "name": "Alice", "age": 20, "pets": 0},
    ]

    data = adapter.get_data({"age": Range(20, None, False, False)}, [])
    assert list(data) == [
        {"rowid": 1, "name": "Bob", "age": 23, "pets": 3},
    ]

    data = adapter.get_data({}, [("age", Order.DESCENDING)])
    assert list(data) == [
        {"rowid": 1, "name": "Bob", "age": 23, "pets": 3},
        {"rowid": 0, "name": "Alice", "age": 20, "pets": 0},
    ]


def test_adapter_manipulate_rows():
    adapter = FakeAdapter()

    adapter.insert_row({"rowid": None, "name": "Charlie", "age": 6, "pets": 1})
    data = adapter.get_data({}, [])
    assert list(data) == [
        {"rowid": 0, "name": "Alice", "age": 20, "pets": 0},
        {"rowid": 1, "name": "Bob", "age": 23, "pets": 3},
        {"rowid": 2, "name": "Charlie", "age": 6, "pets": 1},
    ]
    adapter.insert_row({"rowid": 4, "name": "Dani", "age": 40, "pets": 2})
    data = adapter.get_data({}, [])
    assert list(data) == [
        {"rowid": 0, "name": "Alice", "age": 20, "pets": 0},
        {"rowid": 1, "name": "Bob", "age": 23, "pets": 3},
        {"rowid": 2, "name": "Charlie", "age": 6, "pets": 1},
        {"rowid": 4, "name": "Dani", "age": 40, "pets": 2},
    ]

    adapter.delete_row(0)
    data = adapter.get_data({}, [])
    assert list(data) == [
        {"rowid": 1, "name": "Bob", "age": 23, "pets": 3},
        {"rowid": 2, "name": "Charlie", "age": 6, "pets": 1},
        {"rowid": 4, "name": "Dani", "age": 40, "pets": 2},
    ]

    adapter.update_row(1, {"rowid": 1, "name": "Bob", "age": 24, "pets": 4})
    data = adapter.get_data({}, [])
    assert list(data) == [
        {"rowid": 2, "name": "Charlie", "age": 6, "pets": 1},
        {"rowid": 4, "name": "Dani", "age": 40, "pets": 2},
        {"rowid": 1, "name": "Bob", "age": 24, "pets": 4},
    ]


def test_from_uri():
    class FakeAdapter(Adapter):
        @staticmethod
        def parse_uri(uri: str) -> Tuple[str, ...]:
            return tuple(uri.split(":", 1))

        def __init__(self, a: str, b: str):
            self.a = a
            self.b = b

    adapter = FakeAdapter.from_uri("foo:bar")
    assert adapter.a == "foo"
    assert adapter.b == "bar"
