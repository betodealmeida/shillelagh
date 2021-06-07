import pytest
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Float
from shillelagh.fields import Integer
from shillelagh.fields import Order
from shillelagh.fields import String
from shillelagh.filters import Equal
from shillelagh.filters import Impossible
from shillelagh.filters import Range
from shillelagh.lib import analyse
from shillelagh.lib import build_sql
from shillelagh.lib import DELETED
from shillelagh.lib import RowIDManager
from shillelagh.lib import update_order


def test_row_id_manager_empty_range():
    with pytest.raises(Exception) as excinfo:
        RowIDManager([])

    assert str(excinfo.value) == "Argument `ranges` cannot be empty"


def test_row_id_manager():
    manager = RowIDManager([range(0, 6)])
    assert list(manager) == [0, 1, 2, 3, 4, 5]

    manager.insert()
    assert list(manager) == [0, 1, 2, 3, 4, 5, 6]

    manager.insert(7)
    assert list(manager) == [0, 1, 2, 3, 4, 5, 6, 7]
    assert manager.ranges == [range(0, 8)]

    manager.insert(9)
    assert list(manager) == [0, 1, 2, 3, 4, 5, 6, 7, 9]
    assert manager.ranges == [range(0, 8), range(9, 10)]

    with pytest.raises(Exception) as excinfo:
        manager.insert(5)
    assert str(excinfo.value) == "Row ID 5 already present"

    manager.delete(9)
    assert list(manager) == [0, 1, 2, 3, 4, 5, 6, 7, -1]
    assert manager.ranges == [range(0, 8), DELETED]

    manager.delete(4)
    assert list(manager) == [0, 1, 2, 3, -1, 5, 6, 7, -1]
    assert manager.ranges == [range(0, 4), DELETED, range(5, 8), DELETED]

    with pytest.raises(Exception) as excinfo:
        manager.delete(9)
    assert str(excinfo.value) == "Row ID 9 not found"

    manager.delete(5)
    assert list(manager) == [0, 1, 2, 3, -1, -1, 6, 7, -1]
    assert manager.ranges == [
        range(0, 4),
        DELETED,
        DELETED,
        range(6, 8),
        DELETED,
    ]

    manager.delete(7)
    assert list(manager) == [0, 1, 2, 3, -1, -1, 6, -1, -1]
    assert manager.ranges == [
        range(0, 4),
        DELETED,
        DELETED,
        range(6, 7),
        DELETED,
        DELETED,
    ]


def test_analyse():
    data = [
        {"int": 1, "float": 10.0, "str": "Alice"},
        {"int": 3, "float": 9.5, "str": "Bob"},
        {"int": 2, "float": 8.0, "str": "Charlie"},
    ]
    num_rows, order, types = analyse(data)
    assert num_rows == 3
    assert order == {
        "int": Order.NONE,
        "float": Order.DESCENDING,
        "str": Order.ASCENDING,
    }
    assert types == {"int": Integer, "float": Float, "str": String}


def test_update_order():
    order = update_order(Order.NONE, previous=None, current=1, num_rows=1)
    assert order == Order.NONE

    order = update_order(order, previous=1, current=2, num_rows=2)
    assert order == Order.ASCENDING

    order = update_order(order, previous=2, current=2, num_rows=3)
    assert order == Order.ASCENDING

    order = update_order(order, previous=2, current=1, num_rows=4)
    assert order == Order.NONE


def test_build_sql():
    columns = {"a": String(), "b": Float()}

    sql = build_sql(columns, {"a": Equal("b")}, [])
    assert sql == "SELECT * WHERE a = 'b'"

    sql = build_sql(columns, {"b": Range(1, 10, False, True)}, [])
    assert sql == "SELECT * WHERE b > 1 AND b <= 10"

    sql = build_sql(columns, {"b": Range(1, None, True, False)}, [])
    assert sql == "SELECT * WHERE b >= 1"

    sql = build_sql(columns, {"b": Range(None, 10, True, False)}, [])
    assert sql == "SELECT * WHERE b < 10"

    sql = build_sql(columns, {}, [])
    assert sql == "SELECT *"

    with pytest.raises(ProgrammingError) as excinfo:
        build_sql(columns, {"a": [1, 2, 3]}, [])
    assert str(excinfo.value) == "Invalid filter: [1, 2, 3]"


def test_build_sql_with_map():
    columns = {f"col{i}_": Integer() for i in range(4)}
    bounds = {
        "col0_": Equal(1),
        "col1_": Range(start=0, end=1, include_start=True, include_end=False),
        "col2_": Range(start=None, end=1, include_start=False, include_end=True),
        "col3_": Range(start=0, end=None, include_start=False, include_end=True),
    }
    order = [("col0_", Order.ASCENDING), ("col1_", Order.DESCENDING)]
    column_map = {f"col{i}_": letter for i, letter in enumerate("ABCD")}
    sql = build_sql(columns, bounds, order, column_map)
    assert (
        sql
        == "SELECT * WHERE A = 1 AND B >= 0 AND B < 1 AND C <= 1 AND D > 0 ORDER BY A, B DESC"
    )
