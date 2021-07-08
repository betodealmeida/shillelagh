from datetime import datetime
from typing import List

import pytest

from ..fakes import FakeAdapter
from ..fakes import FakeEntryPoint
from shillelagh.adapters.base import Adapter
from shillelagh.backends.apsw.db import connect
from shillelagh.exceptions import NotSupportedError
from shillelagh.fields import DateTime
from shillelagh.fields import Order
from shillelagh.filters import Equal
from shillelagh.filters import Range
from shillelagh.typing import Row


class FakeAdapterWithDateTime(FakeAdapter):

    birthday = DateTime(filters=[Range], order=Order.ANY, exact=True)

    data: List[Row] = []

    def __init__(self):
        pass


class ReadOnlyAdapter(Adapter):
    """
    A read-only adapter.
    """


def test_adapter_get_columns():
    adapter = FakeAdapter()
    assert adapter.get_columns() == {
        "age": FakeAdapter.age,
        "name": FakeAdapter.name,
        "pets": FakeAdapter.pets,
    }
    adapter.close()


def test_adapter_get_metadata():
    adapter = FakeAdapter()
    assert adapter.get_metadata() == {}


def test_adapter_read_only():
    adapter = ReadOnlyAdapter()

    with pytest.raises(NotSupportedError) as excinfo:
        adapter.insert_data({"hello": "world"})
    assert str(excinfo.value) == "Adapter does not support `INSERT` statements"

    with pytest.raises(NotSupportedError) as excinfo:
        adapter.delete_data(1)
    assert str(excinfo.value) == "Adapter does not support `DELETE` statements"

    with pytest.raises(NotSupportedError) as excinfo:
        adapter.update_data(1, {"hello": "universe"})
    assert str(excinfo.value) == "Adapter does not support `DELETE` statements"


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


def test_adapter_get_rows():
    adapter = FakeAdapter()

    adapter.insert_row({"rowid": None, "name": "Charlie", "age": 6, "pets": 1})

    data = adapter.get_data({}, [])
    assert list(data) == [
        {"rowid": 0, "name": "Alice", "age": 20, "pets": 0},
        {"rowid": 1, "name": "Bob", "age": 23, "pets": 3},
        {"rowid": 2, "name": "Charlie", "age": 6.0, "pets": 1},
    ]

    data = adapter.get_rows({}, [])
    assert list(data) == [
        {"rowid": 0, "name": "Alice", "age": 20.0, "pets": 0},
        {"rowid": 1, "name": "Bob", "age": 23.0, "pets": 3},
        {"rowid": 2, "name": "Charlie", "age": 6.0, "pets": 1},
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


def test_type_conversion(mocker):

    entry_points = [FakeEntryPoint("dummy", FakeAdapterWithDateTime)]
    mocker.patch(
        "shillelagh.backends.apsw.db.iter_entry_points",
        return_value=entry_points,
    )

    connection = connect(":memory:", ["dummy"], isolation_level="IMMEDIATE")
    cursor = connection.cursor()

    cursor.execute('SELECT * FROM "dummy://"')
    assert cursor.fetchall() == []

    cursor.execute(
        'INSERT INTO "dummy://" (birthday) VALUES (?)',
        (datetime(2021, 1, 1, 0, 0),),
    )
    cursor.execute('SELECT * FROM "dummy://"')
    assert cursor.fetchall() == [
        (
            None,
            datetime(2021, 1, 1, 0, 0),
            None,
            None,
        ),
    ]

    # make sure datetime is stored as a datetime
    assert FakeAdapterWithDateTime.data == [
        {
            "age": None,
            "birthday": datetime(2021, 1, 1, 0, 0),
            "name": None,
            "pets": None,
            "rowid": 1,
        },
    ]
    assert isinstance(FakeAdapterWithDateTime.data[0]["birthday"], datetime)

    cursor.execute(
        'SELECT * FROM "dummy://" WHERE birthday > ?',
        (datetime(2020, 12, 31, 0, 0),),
    )
    assert cursor.fetchall() == [
        (None, datetime(2021, 1, 1, 0, 0), None, None),
    ]
