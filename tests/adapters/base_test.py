"""
Test for shillelagh.adapter.base.
"""

from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

import pytest

from shillelagh.adapters.base import Adapter
from shillelagh.adapters.registry import AdapterLoader
from shillelagh.backends.apsw.db import connect
from shillelagh.exceptions import NotSupportedError
from shillelagh.fields import DateTime, Order
from shillelagh.filters import Equal, Filter, Range
from shillelagh.typing import RequestedOrder, Row

from ..fakes import FakeAdapter


class FakeAdapterWithDateTime(FakeAdapter):
    """
    An adapter with a timestamp column.
    """

    birthday = DateTime(filters=[Range], order=Order.ANY, exact=True)

    data: List[Row] = []

    def __init__(self):  # pylint: disable=super-init-not-called
        pass


class ReadOnlyAdapter(Adapter):  # pylint: disable=abstract-method
    """
    A read-only adapter.
    """


def test_adapter_get_columns() -> None:
    """
    Test ``get_columns``.
    """
    adapter = FakeAdapter()
    assert adapter.get_columns() == {
        "age": FakeAdapter.age,
        "name": FakeAdapter.name,
        "pets": FakeAdapter.pets,
    }
    adapter.close()


def test_adapter_get_metadata() -> None:
    """
    Test ``get_metadata``.
    """
    adapter = FakeAdapter()
    assert adapter.get_metadata() == {}


def test_adapter_read_only() -> None:
    """
    Test a read-only adapter.
    """
    adapter = ReadOnlyAdapter()

    with pytest.raises(NotSupportedError) as excinfo:
        adapter.insert_data({"hello": "world"})
    assert str(excinfo.value) == "Adapter does not support ``INSERT`` statements"

    with pytest.raises(NotSupportedError) as excinfo:
        adapter.delete_data(1)
    assert str(excinfo.value) == "Adapter does not support ``DELETE`` statements"

    with pytest.raises(NotSupportedError) as excinfo:
        adapter.update_data(1, {"hello": "universe"})
    assert str(excinfo.value) == "Adapter does not support ``UPDATE`` statements"


def test_adapter_get_data() -> None:
    """
    Test ``get_data``.
    """
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


def test_adapter_get_rows() -> None:
    """
    Test ``get_rows``.
    """
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


def test_adapter_manipulate_rows() -> None:
    """
    Test ``DML``.
    """
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


def test_limit_offset(registry: AdapterLoader) -> None:
    """
    Test limit/offset in adapters that implement it and adapters that don't.

    Note that SQLite will always enforce the limit, even the adapter declares that it
    supports it. For offset, on the other hand, if the adapter declares support for it
    then SQLite will not apply an offset (since it couldn't know if an offset was
    applied or not).
    """

    class CustomFakeAdapter(FakeAdapter):
        """
        Custom ``FakeAdapter`` with more data.
        """

        supports_limit = False
        supports_offset = False

        def __init__(self):
            super().__init__()

            self.data = [
                {"rowid": 0, "name": "Alice", "age": 20, "pets": 0},
                {"rowid": 1, "name": "Bob", "age": 23, "pets": 3},
                {"rowid": None, "name": "Charlie", "age": 6, "pets": 1},
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
            """
            Return all data.
            """
            return iter(self.data)

    class FakeAdapterWithLimitOnly(CustomFakeAdapter):
        """
        An adapter that only supports limit (like ``s3select``)
        """

        scheme = "limit"

        supports_limit = True
        supports_offset = False

    class FakeAdapterWithLimitAndOffset(CustomFakeAdapter):
        """
        An adapter that supports both limit and offset.
        """

        scheme = "limit+offset"

        supports_limit = True
        supports_offset = True

    class FakeAdapterWithOffsetOnly(CustomFakeAdapter):
        """
        An adapter that supports only offset.
        """

        scheme = "offset"

        supports_limit = False
        supports_offset = True

    registry.add("dummy", CustomFakeAdapter)
    registry.add("limit", FakeAdapterWithLimitOnly)
    registry.add("limit+offset", FakeAdapterWithLimitAndOffset)
    registry.add("offset", FakeAdapterWithOffsetOnly)

    connection = connect(
        ":memory:",
        ["dummy", "limit", "limit+offset", "offset"],
        isolation_level="IMMEDIATE",
    )
    cursor = connection.cursor()

    # adapter returns 3 rows, SQLite applies limit/offset
    cursor.execute('SELECT * FROM "dummy://" LIMIT 1 OFFSET 1')
    assert cursor.fetchall() == [(23, "Bob", 3)]

    # adapter returns 3 rows (even though it says it supports ``LIMIT``), SQLite then
    # applies offset and enforces limit
    cursor.execute('SELECT * FROM "limit://" LIMIT 1 OFFSET 1')
    assert cursor.fetchall() == [(23, "Bob", 3)]

    # adapter returns 3 rows, SQLite enforces limit but doesn't apply offset
    cursor.execute('SELECT * FROM "limit+offset://" LIMIT 1 OFFSET 1')
    assert cursor.fetchall() == [(20, "Alice", 0)]

    # adapter returns 3 rows, SQLite enforces limit but doesn't apply offset
    # cursor.execute('SELECT * FROM "offset://" LIMIT 1 OFFSET 1')
    # assert cursor.fetchall() == [(20, "Alice", 0)]


def test_type_conversion(registry: AdapterLoader) -> None:
    """
    Test that native types are converted correctly.
    """
    registry.add("dummy", FakeAdapterWithDateTime)

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
