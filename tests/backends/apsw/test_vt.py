import datetime
from typing import Dict

import apsw
import pytest

from ...fakes import FakeAdapter
from shillelagh.adapters.base import Adapter
from shillelagh.backends.apsw.vt import convert_rows_from_sqlite
from shillelagh.backends.apsw.vt import convert_rows_to_sqlite
from shillelagh.backends.apsw.vt import type_map
from shillelagh.backends.apsw.vt import VTModule
from shillelagh.backends.apsw.vt import VTTable
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Field
from shillelagh.fields import Float
from shillelagh.fields import Integer
from shillelagh.fields import Order
from shillelagh.fields import String
from shillelagh.filters import Equal
from shillelagh.filters import Filter
from shillelagh.filters import Range
from shillelagh.typing import Row


class FakeAdapterNoFilters(FakeAdapter):

    age = Float()
    name = String()
    pets = Integer()


class FakeAdapterOnlyEqual(FakeAdapter):

    age = Float(filters=[Equal], order=Order.NONE, exact=True)
    name = String(filters=[Equal], order=Order.ASCENDING, exact=True)
    pets = Integer()


class FakeAdapterStaticSort(FakeAdapter):

    age = Float(filters=[Equal], order=Order.NONE)
    name = String(filters=[Equal], order=Order.ASCENDING)
    pets = Integer()


class FakeAdapterNoColumns(FakeAdapter):
    def get_columns(self) -> Dict[str, Field]:
        return {}


def test_vt_module():
    table = VTTable(FakeAdapter)
    vt_module = VTModule(FakeAdapter)
    create_table, table = vt_module.Create(None, "", "", "table")
    assert (
        create_table
        == """CREATE TABLE "table" ("age" REAL, "name" TEXT, "pets" INTEGER)"""
    )


def test_virtual_best_index():
    table = VTTable(FakeAdapter())
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
        f"[[[1, {apsw.SQLITE_INDEX_CONSTRAINT_EQ}], [0, {apsw.SQLITE_INDEX_CONSTRAINT_LE}]], [[1, false]]]",
        True,
        666,
    )


def test_virtual_best_index_static_order_consumed():
    table = VTTable(FakeAdapterStaticSort())
    result = table.BestIndex(
        [
            (1, apsw.SQLITE_INDEX_CONSTRAINT_EQ),  # name =
            (2, apsw.SQLITE_INDEX_CONSTRAINT_GT),  # pets >
            (0, apsw.SQLITE_INDEX_CONSTRAINT_LE),  # age <=
        ],
        [(1, False)],  # ORDER BY name ASC
    )
    assert result == (
        [(0, False), None, None],
        42,
        f"[[[1, {apsw.SQLITE_INDEX_CONSTRAINT_EQ}]], []]",
        True,
        666,
    )


def test_virtual_best_index_static_order_not_consumed():
    table = VTTable(FakeAdapterStaticSort())
    result = table.BestIndex(
        [
            (1, apsw.SQLITE_INDEX_CONSTRAINT_EQ),  # name =
            (2, apsw.SQLITE_INDEX_CONSTRAINT_GT),  # pets >
            (0, apsw.SQLITE_INDEX_CONSTRAINT_LE),  # age <=
        ],
        [(0, True)],  # ORDER BY age DESC
    )
    assert result == (
        [(0, False), None, None],
        42,
        f"[[[1, {apsw.SQLITE_INDEX_CONSTRAINT_EQ}]], []]",
        False,
        666,
    )


def test_virtual_best_index_operator_not_supported():
    table = VTTable(FakeAdapter())
    result = table.BestIndex(
        [(1, apsw.SQLITE_INDEX_CONSTRAINT_MATCH)],  # name LIKE?
        [(1, False)],  # ORDER BY name ASC
    )
    assert result == ([None], 42, "[[], [[1, false]]]", True, 666)


def test_virtual_best_index_no_order_by():
    table = VTTable(FakeAdapter())
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
        f"[[[1, {apsw.SQLITE_INDEX_CONSTRAINT_EQ}], [0, {apsw.SQLITE_INDEX_CONSTRAINT_LE}]], [[0, true]]]",
        True,
        666,
    )


def test_virtual_disconnect():
    table = VTTable(FakeAdapter())
    table.Disconnect()  # no-op


def test_update_insert_row():
    adapter = FakeAdapter()
    table = VTTable(adapter)

    new_row_id = table.UpdateInsertRow(None, [6, "Charlie", 1])
    assert new_row_id == 2
    assert list(adapter.get_data({}, [])) == [
        {"age": 20, "name": "Alice", "pets": 0, "rowid": 0},
        {"age": 23, "name": "Bob", "pets": 3, "rowid": 1},
        {"age": 6, "name": "Charlie", "pets": 1, "rowid": 2},
    ]

    new_row_id = table.UpdateInsertRow(4, [40, "Dani", 2])
    assert new_row_id == 4
    assert list(adapter.get_data({}, [])) == [
        {"age": 20, "name": "Alice", "pets": 0, "rowid": 0},
        {"age": 23, "name": "Bob", "pets": 3, "rowid": 1},
        {"age": 6, "name": "Charlie", "pets": 1, "rowid": 2},
        {"age": 40, "name": "Dani", "pets": 2, "rowid": 4},
    ]


def test_update_delete_row():
    adapter = FakeAdapter()
    table = VTTable(adapter)

    table.UpdateDeleteRow(0)
    assert list(adapter.get_data({}, [])) == [
        {"age": 23, "name": "Bob", "pets": 3, "rowid": 1},
    ]


def test_update_change_row():
    adapter = FakeAdapter()
    table = VTTable(adapter)

    table.UpdateChangeRow(1, 1, [24, "Bob", 4])
    assert list(adapter.get_data({}, [])) == [
        {"age": 20, "name": "Alice", "pets": 0, "rowid": 0},
        {"age": 24, "name": "Bob", "pets": 4, "rowid": 1},
    ]

    table.UpdateChangeRow(1, 2, [24, "Bob", 4])
    assert list(adapter.get_data({}, [])) == [
        {"age": 20, "name": "Alice", "pets": 0, "rowid": 0},
        {"age": 24.0, "name": "Bob", "pets": 4, "rowid": 2},
    ]


def test_cursor():
    table = VTTable(FakeAdapter())
    cursor = table.Open()
    cursor.Filter(42, "[[], []]", [])
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
    table = VTTable(FakeAdapter())
    cursor = table.Open()
    cursor.Filter(42, f"[[[1, {apsw.SQLITE_INDEX_CONSTRAINT_EQ}]], []]", ["Alice"])
    assert cursor.current_row == (0, 20, "Alice", 0)

    assert not cursor.Eof()
    cursor.Next()
    assert cursor.Eof()


def test_cursor_with_constraints_invalid_filter():
    table = VTTable(FakeAdapter())
    cursor = table.Open()

    with pytest.raises(Exception) as excinfo:
        cursor.Filter(
            42,
            f"[[[1, {apsw.SQLITE_INDEX_CONSTRAINT_MATCH}]], []]",
            ["Alice"],
        )

    assert str(excinfo.value) == "Invalid constraint passed: 64"


def test_cursor_with_constraints_no_filters():
    table = VTTable(FakeAdapterNoFilters())
    cursor = table.Open()
    with pytest.raises(Exception) as excinfo:
        cursor.Filter(42, f"[[[1, {apsw.SQLITE_INDEX_CONSTRAINT_EQ}]], []]", ["Alice"])

    assert str(excinfo.value) == "No valid filter found"


def test_cursor_with_constraints_only_equal():
    table = VTTable(FakeAdapterOnlyEqual())
    cursor = table.Open()
    with pytest.raises(Exception) as excinfo:
        cursor.Filter(42, f"[[[1, {apsw.SQLITE_INDEX_CONSTRAINT_GE}]], []]", ["Alice"])

    assert str(excinfo.value) == "No valid filter found"


def test_adapter_with_no_columns():
    table = VTTable(FakeAdapter)
    vt_module = VTModule(FakeAdapterNoColumns)
    with pytest.raises(ProgrammingError) as excinfo:
        create_table, table = vt_module.Create(None, "", "", "table")

    assert str(excinfo.value) == "Virtual table table has no columns"


def test_convert_rows_to_sqlite():
    rows = [
        {
            "INTEGER": 1,
            "REAL": 1.0,
            "TEXT": "test",
            "TIMESTAMP": datetime.datetime(
                2021,
                1,
                1,
                0,
                0,
                tzinfo=datetime.timezone.utc,
            ),
            "DATE": datetime.date(2021, 1, 1),
            "TIME": datetime.time(0, 0, tzinfo=datetime.timezone.utc),
            "BOOLEAN": False,
            "BLOB": b"test",
        },
        {
            "INTEGER": None,
            "REAL": None,
            "TEXT": None,
            "TIMESTAMP": None,
            "DATE": None,
            "TIME": None,
            "BOOLEAN": None,
            "BLOB": None,
        },
    ]
    assert list(convert_rows_to_sqlite(type_map, iter(rows))) == [
        {
            "INTEGER": 1,
            "REAL": 1.0,
            "TEXT": "test",
            "TIMESTAMP": "2021-01-01T00:00:00+00:00",
            "DATE": "2021-01-01",
            "TIME": "00:00:00+00:00",
            "BOOLEAN": 0,
            "BLOB": b"test",
        },
        {
            "INTEGER": None,
            "REAL": None,
            "TEXT": None,
            "TIMESTAMP": None,
            "DATE": None,
            "TIME": None,
            "BOOLEAN": None,
            "BLOB": None,
        },
    ]


def test_convert_rows_from_sqlite():
    rows = [
        {
            "INTEGER": 1,
            "REAL": 1.0,
            "TEXT": "test",
            "TIMESTAMP": "2021-01-01T00:00:00+00:00",
            "DATE": "2021-01-01",
            "TIME": "00:00:00+00:00",
            "BOOLEAN": 0,
            "BLOB": b"test",
        },
        {
            "INTEGER": None,
            "REAL": None,
            "TEXT": None,
            "TIMESTAMP": None,
            "DATE": None,
            "TIME": None,
            "BOOLEAN": None,
            "BLOB": None,
        },
    ]
    assert list(convert_rows_from_sqlite(type_map, iter(rows))) == [
        {
            "INTEGER": 1,
            "REAL": 1.0,
            "TEXT": "test",
            "TIMESTAMP": datetime.datetime(
                2021,
                1,
                1,
                0,
                0,
                tzinfo=datetime.timezone.utc,
            ),
            "DATE": datetime.date(2021, 1, 1),
            "TIME": datetime.time(0, 0, tzinfo=datetime.timezone.utc),
            "BOOLEAN": False,
            "BLOB": b"test",
        },
        {
            "INTEGER": None,
            "REAL": None,
            "TEXT": None,
            "TIMESTAMP": None,
            "DATE": None,
            "TIME": None,
            "BOOLEAN": None,
            "BLOB": None,
        },
    ]
