import pytest

from shillelagh.exceptions import ImpossibleFilterError
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Float
from shillelagh.fields import Integer
from shillelagh.fields import Order
from shillelagh.fields import String
from shillelagh.filters import Equal
from shillelagh.filters import Impossible
from shillelagh.filters import Range
from shillelagh.lib import analyze
from shillelagh.lib import build_sql
from shillelagh.lib import combine_args_kwargs
from shillelagh.lib import DELETED
from shillelagh.lib import deserialize
from shillelagh.lib import escape
from shillelagh.lib import filter_data
from shillelagh.lib import RowIDManager
from shillelagh.lib import serialize
from shillelagh.lib import unescape
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


def test_analyze():
    data = [
        {"int": 1, "float": 10.0, "str": "Alice"},
        {"int": 3, "float": 9.5, "str": "Bob"},
        {"int": 2, "float": 8.0, "str": "Charlie"},
    ]
    num_rows, order, types = analyze(data)
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


def test_update_order_none():
    order = update_order(Order.NONE, previous=None, current=1, num_rows=1)
    order = update_order(order, previous=1, current=None, num_rows=2)
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
    sql = build_sql(columns, bounds, order, column_map, 1)
    assert (
        sql
        == "SELECT * WHERE A = 1 AND B >= 0 AND B < 1 AND C <= 1 AND D > 0 ORDER BY A, B DESC OFFSET 1"
    )


def test_build_sql_impossible():
    columns = {"a": String(), "b": Float()}

    with pytest.raises(ImpossibleFilterError):
        build_sql(columns, {"a": Impossible()}, [])


def test_escape():
    assert escape("1") == "1"
    assert escape("O'Malley's") == "O''Malley''s"


def test_unescape():
    assert unescape("1") == "1"
    assert unescape("O''Malley''s") == "O'Malley's"


def test_serialize():
    assert serialize(["O'Malley's"]) == """'["O''Malley''s"]'"""


def test_deserialize():
    assert deserialize("""'["O''Malley''s"]'""") == ["O'Malley's"]


def test_combine_args_kwargs():
    def func(a: int = 0, b: str = "test", c: float = 10.0) -> None:
        pass

    args = ()
    kwargs = {"b": "TEST"}
    assert combine_args_kwargs(func, *args, **kwargs) == (0, "TEST", 10.0)


def test_filter_data():
    data = [
        {"index": 10, "temperature": 15.2, "site": "Diamond_St"},
        {"index": 11, "temperature": 13.1, "site": "Blacktail_Loop"},
        {"index": 12, "temperature": 13.3, "site": "Platinum_St"},
        {"index": 13, "temperature": 12.1, "site": "Kodiak_Trail"},
    ]

    bounds = {"index": Equal(11)}
    assert list(filter_data(data, bounds, [])) == [
        {"index": 11, "site": "Blacktail_Loop", "temperature": 13.1},
    ]

    bounds = {"temperature": Range(13.1, None, False, False)}
    assert list(filter_data(data, bounds, [])) == [
        {"index": 10, "temperature": 15.2, "site": "Diamond_St"},
        {"index": 12, "temperature": 13.3, "site": "Platinum_St"},
    ]

    bounds = {"temperature": Range(None, 14, False, False)}
    assert list(filter_data(data, bounds, [])) == [
        {"index": 11, "temperature": 13.1, "site": "Blacktail_Loop"},
        {"index": 12, "temperature": 13.3, "site": "Platinum_St"},
        {"index": 13, "temperature": 12.1, "site": "Kodiak_Trail"},
    ]

    bounds = {"temperature": Range(13.1, 14, True, False)}
    assert list(filter_data(data, bounds, [])) == [
        {"index": 11, "temperature": 13.1, "site": "Blacktail_Loop"},
        {"index": 12, "temperature": 13.3, "site": "Platinum_St"},
    ]

    order = [("index", Order.DESCENDING)]
    assert list(filter_data(data, {}, order)) == [
        {"index": 13, "temperature": 12.1, "site": "Kodiak_Trail"},
        {"index": 12, "temperature": 13.3, "site": "Platinum_St"},
        {"index": 11, "temperature": 13.1, "site": "Blacktail_Loop"},
        {"index": 10, "temperature": 15.2, "site": "Diamond_St"},
    ]

    bounds = {"index": Impossible()}
    assert list(filter_data(data, bounds, [])) == []
