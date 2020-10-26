from typing import Any
from typing import Dict
from typing import Iterator

import apsw
import pytest
from shillelagh.adapters.base import Adapter
from shillelagh.backends.apsw.vt import VTModule
from shillelagh.backends.apsw.vt import VTTable
from shillelagh.fields import Float
from shillelagh.fields import Integer
from shillelagh.fields import Order
from shillelagh.fields import String
from shillelagh.filters import Equal
from shillelagh.filters import Filter
from shillelagh.filters import Range


class DummyAdapter(Adapter):

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


def test_vt_module():
    table = VTTable(DummyAdapter)
    vt_module = VTModule(DummyAdapter)
    create_table, table = vt_module.Create(None, "", "", "table")
    assert (
        create_table
        == """CREATE TABLE "table" ("age" REAL, "name" TEXT, "pets" INTEGER)"""
    )


def test_virtual_best_index():
    table = VTTable(DummyAdapter())
    result = table.BestIndex(
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
    table = VTTable(DummyAdapter())
    result = table.BestIndex(
        [(1, apsw.SQLITE_INDEX_CONSTRAINT_MATCH)],  # name LIKE?
        [(1, False)],  # ORDER BY name ASC
    )
    assert result == ([None], 42, "[]", True, 666)


def test_virtual_best_index_no_order_by():
    table = VTTable(DummyAdapter())
    result = table.BestIndex(
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
    table = VTTable(DummyAdapter())
    table.Disconnect()  # no-op


def test_cursor():
    table = VTTable(DummyAdapter())
    cursor = table.Open()
    cursor.Filter(42, "[]", [])
    assert cursor.current_row == (0, 20, "Alice", 0)
    assert cursor.Rowid() == 0
    assert cursor.Column(0) == 20

    cursor.Next()
    assert cursor.current_row == (1, 23, "Bob", 3)

    assert not cursor.Eof()
    cursor.Next()
    assert cursor.Eof()
    cursor.Close()


def test_cursor_with_constraints():
    table = VTTable(DummyAdapter())
    cursor = table.Open()
    cursor.Filter(42, f"[[1, {apsw.SQLITE_INDEX_CONSTRAINT_EQ}]]", ["Alice"])
    assert cursor.current_row == (0, 20, "Alice", 0)

    assert not cursor.Eof()
    cursor.Next()
    assert cursor.Eof()


def test_cursor_with_constraints_invalid_filter():
    table = VTTable(DummyAdapter())
    cursor = table.Open()

    with pytest.raises(Exception) as excinfo:
        cursor.Filter(42, f"[[1, {apsw.SQLITE_INDEX_CONSTRAINT_MATCH}]]", ["Alice"])

    assert str(excinfo.value) == "No valid filter found"
