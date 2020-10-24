from typing import Any
from typing import Dict
from typing import Iterator

import apsw
import pytest
from shillelagh.fields import Float
from shillelagh.fields import Integer
from shillelagh.fields import Order
from shillelagh.fields import String
from shillelagh.filters import Equal
from shillelagh.filters import Filter
from shillelagh.filters import Range
from shillelagh.table import VirtualTable


class DummyTable(VirtualTable):

    age = Float(filters=[Range], order=Order.NONE, exact=True)
    name = String(filters=[Equal], order=Order.ASCENDING, exact=True)
    pets = Integer()

    def get_data(self, bounds: Dict[str, Filter]) -> Iterator[Dict[str, Any]]:
        data = [
            {"rowid": 0, "name": "Alice", "age": 20, "pets": 0},
            {"rowid": 1, "name": "Bob", "age": 23, "pets": 3},
        ]

        for column in ["name", "age"]:
            if column in bounds:
                data = [row for row in data if bounds[column].check(row[column])]

        yield from iter(data)


def test_virtual_table_get_columns():
    create_table, instance = DummyTable.create(None, "", "", "table")
    assert instance.get_columns() == {
        "age": DummyTable.age,
        "name": DummyTable.name,
        "pets": DummyTable.pets,
    }


def test_virtual_table_create_table():
    create_table, instance = DummyTable.create(None, "", "", "table")
    assert (
        create_table
        == """CREATE TABLE "table" ("age" REAL, "name" TEXT, "pets" INTEGER)"""
    )


def test_virtual_best_index():
    create_table, instance = DummyTable.create(None, "", "", "table")
    result = instance.best_index(
        [
            (1, apsw.SQLITE_INDEX_CONSTRAINT_EQ),  # name =
            (2, apsw.SQLITE_INDEX_CONSTRAINT_GT),  # pets >
            (0, apsw.SQLITE_INDEX_CONSTRAINT_LE),  # age <=
        ],
        [(1, False)],  # ORDER BY name ASC
    )
    assert result == (
        [(0, True), None, (1, True)],
        42,
        f"[[1, {apsw.SQLITE_INDEX_CONSTRAINT_EQ}], [0, {apsw.SQLITE_INDEX_CONSTRAINT_LE}]]",
        True,
        666,
    )


def test_virtual_best_index_operator_not_supported():
    create_table, instance = DummyTable.create(None, "", "", "table")
    result = instance.best_index(
        [(1, apsw.SQLITE_INDEX_CONSTRAINT_MATCH)],  # name LIKE?
        [(1, False)],  # ORDER BY name ASC
    )
    assert result == ([None], 42, "[]", True, 666)


def test_virtual_best_index_no_order_by():
    create_table, instance = DummyTable.create(None, "", "", "table")
    result = instance.best_index(
        [
            (1, apsw.SQLITE_INDEX_CONSTRAINT_EQ),  # name =
            (2, apsw.SQLITE_INDEX_CONSTRAINT_GT),  # pets >
            (0, apsw.SQLITE_INDEX_CONSTRAINT_LE),  # age <=
        ],
        [(0, True)],  # ORDER BY age DESC
    )
    assert result == (
        [(0, True), None, (1, True)],
        42,
        f"[[1, {apsw.SQLITE_INDEX_CONSTRAINT_EQ}], [0, {apsw.SQLITE_INDEX_CONSTRAINT_LE}]]",
        False,
        666,
    )


def test_virtual_disconnect():
    create_table, instance = DummyTable.create(None, "", "", "table")
    instance.disconnect()  # no-op


def test_virtual_table_get_data():
    create_table, instance = DummyTable.create(None, "", "", "table")
    data = instance.get_data({})
    assert list(data) == [
        {"rowid": 0, "name": "Alice", "age": 20, "pets": 0},
        {"rowid": 1, "name": "Bob", "age": 23, "pets": 3},
    ]

    data = instance.get_data({"name": Equal("Alice")})
    assert list(data) == [
        {"rowid": 0, "name": "Alice", "age": 20, "pets": 0},
    ]

    data = instance.get_data({"age": Range(20, None, False, False)})
    assert list(data) == [
        {"rowid": 1, "name": "Bob", "age": 23, "pets": 3},
    ]


def test_cursor():
    create_table, instance = DummyTable.create(None, "", "", "table")
    cursor = instance.open()
    cursor.filter(42, "[]", [])
    assert cursor.current_row == (0, 20, "Alice", 0)
    assert cursor.rowid() == 0
    assert cursor.column(0) == 20

    cursor.next()
    assert cursor.current_row == (1, 23, "Bob", 3)

    assert not cursor.eof()
    cursor.next()
    assert cursor.eof()
    cursor.close()


def test_cursor_with_constraints():
    create_table, instance = DummyTable.create(None, "", "", "table")
    cursor = instance.open()
    cursor.filter(42, f"[[1, {apsw.SQLITE_INDEX_CONSTRAINT_EQ}]]", ["Alice"])
    assert cursor.current_row == (0, 20, "Alice", 0)

    assert not cursor.eof()
    cursor.next()
    assert cursor.eof()


def test_cursor_with_constraints_invalid_filter():
    create_table, instance = DummyTable.create(None, "", "", "table")
    cursor = instance.open()

    with pytest.raises(Exception) as excinfo:
        cursor.filter(42, f"[[1, {apsw.SQLITE_INDEX_CONSTRAINT_MATCH}]]", ["Alice"])

    assert str(excinfo.value) == "No valid filter found"
