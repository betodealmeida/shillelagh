"""
Tests for shillelagh.lib.
"""

from typing import Any, Dict, Iterator, List, Tuple

import pytest
from pytest_mock import MockerFixture

from shillelagh.exceptions import ImpossibleFilterError, ProgrammingError
from shillelagh.fields import Boolean, Field, Float, Integer, Order, String
from shillelagh.filters import (
    Equal,
    Filter,
    Impossible,
    IsNotNull,
    IsNull,
    Like,
    NotEqual,
    Range,
)
from shillelagh.lib import (
    DELETED,
    RowIDManager,
    analyze,
    apply_limit_and_offset,
    build_sql,
    combine_args_kwargs,
    deserialize,
    escape_identifier,
    escape_string,
    filter_data,
    find_adapter,
    is_not_null,
    is_null,
    serialize,
    unescape_identifier,
    unescape_string,
    update_order,
)
from shillelagh.typing import RequestedOrder


def test_row_id_manager_empty_range() -> None:
    """
    Test instantiating ``RowIDManager`` with an empty range.
    """
    with pytest.raises(Exception) as excinfo:
        RowIDManager([])

    assert str(excinfo.value) == "Argument ``ranges`` cannot be empty"


def test_row_id_manager() -> None:
    """
    Test ``RowIDManager``.
    """
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


def test_analyze() -> None:
    """
    Test ``analyze``.
    """
    data: Iterator[Dict[str, Any]] = iter(
        [
            {
                "int": 1,
                "float": 10.0,
                "str": "Alice",
                "bool": False,
                "list": [1],
                "weird": set(),
            },
            {
                "int": 3,
                "float": 9.5,
                "str": "Bob",
                "bool": True,
                "list": [2],
                "weird": {1},
            },
            {
                "int": 2,
                "float": 8.0,
                "str": "Charlie",
                "bool": False,
                "list": [3],
                "weird": {1, 2},
            },
        ],
    )
    num_rows, order, types = analyze(data)
    assert num_rows == 3
    assert order == {
        "int": Order.NONE,
        "float": Order.DESCENDING,
        "str": Order.ASCENDING,
        "bool": Order.NONE,
        "list": Order.ASCENDING,
        "weird": Order.ASCENDING,
    }
    assert types == {
        "int": Integer,
        "float": Float,
        "str": String,
        "bool": Boolean,
        "list": String,
        "weird": String,
    }


def test_update_order() -> None:
    """
    Test ``update_order``.
    """
    order = update_order(Order.NONE, previous=None, current=1, num_rows=1)
    assert order == Order.NONE

    order = update_order(order, previous=1, current=2, num_rows=2)
    assert order == Order.ASCENDING

    order = update_order(order, previous=2, current=2, num_rows=3)
    assert order == Order.ASCENDING

    order = update_order(order, previous=2, current=1, num_rows=4)
    assert order == Order.NONE


def test_update_order_none() -> None:
    """
    Test ``update_order`` when original order is none.
    """
    order = update_order(Order.NONE, previous=None, current=1, num_rows=1)
    order = update_order(order, previous=1, current=None, num_rows=2)
    assert order == Order.NONE


def test_build_sql() -> None:
    """
    Test ``build_sql``.
    """
    columns: Dict[str, Field[Any, Any]] = {"a": String(), "b": Float()}

    sql = build_sql(columns, {"a": Equal("b"), "b": NotEqual(1.0)}, [])
    assert sql == "SELECT * WHERE a = 'b' AND b != 1.0"

    sql = build_sql(columns, {"a": IsNull(), "b": IsNotNull()}, [])
    assert sql == "SELECT * WHERE a IS NULL AND b IS NOT NULL"

    sql = build_sql(columns, {"b": Range(1, 10, False, True)}, [])
    assert sql == "SELECT * WHERE b > 1 AND b <= 10"

    sql = build_sql(columns, {"b": Range(1, None, True, False)}, [])
    assert sql == "SELECT * WHERE b >= 1"

    sql = build_sql(columns, {"b": Range(None, 10, True, False)}, [])
    assert sql == "SELECT * WHERE b < 10"

    sql = build_sql(columns, {"a": Like("%test%")}, [])
    assert sql == "SELECT * WHERE a LIKE '%test%'"

    sql = build_sql(columns, {}, [])
    assert sql == "SELECT *"

    sql = build_sql(columns, {}, [], "some_table")
    assert sql == "SELECT * FROM some_table"

    with pytest.raises(ProgrammingError) as excinfo:
        build_sql(columns, {"a": [1, 2, 3]}, [])  # type: ignore
    assert str(excinfo.value) == "Invalid filter: [1, 2, 3]"


def test_build_sql_with_aliases() -> None:
    """
    Test ``build_sql`` with aliases tables and columns.
    """
    columns: Dict[str, Field[Any, Any]] = {"a": String(), "b": Float()}
    order: List[Tuple[str, RequestedOrder]] = [
        ("a", Order.ASCENDING),
    ]
    sql = build_sql(
        columns,
        {"a": Equal("b"), "b": NotEqual(1.0)},
        order,
        "some_table",
        alias="t",
    )
    assert sql == (
        "SELECT * FROM some_table AS t "
        "WHERE t.a = 'b' AND t.b != 1.0 "
        "ORDER BY t.a"
    )


def test_build_sql_with_map() -> None:
    """
    Test ``build_sql`` with a column map.
    """
    columns: Dict[str, Field] = {f"col{i}_": Integer() for i in range(4)}
    bounds = {
        "col0_": Equal(1),
        "col1_": Range(start=0, end=1, include_start=True, include_end=False),
        "col2_": Range(start=None, end=1, include_start=False, include_end=True),
        "col3_": Range(start=0, end=None, include_start=False, include_end=True),
    }
    order: List[Tuple[str, RequestedOrder]] = [
        ("col0_", Order.ASCENDING),
        ("col1_", Order.DESCENDING),
    ]
    column_map = {f"col{i}_": letter for i, letter in enumerate("ABCD")}
    sql = build_sql(columns, bounds, order, None, column_map, None, 1)
    assert sql == (
        "SELECT * WHERE A = 1 AND B >= 0 AND B < 1 AND "
        "C <= 1 AND D > 0 ORDER BY A, B DESC OFFSET 1"
    )


def test_build_sql_impossible() -> None:
    """
    Test ``build_sql`` with an impossible filter.
    """
    columns: Dict[str, Field] = {"a": String(), "b": Float()}

    with pytest.raises(ImpossibleFilterError):
        build_sql(columns, {"a": Impossible()}, [])


def test_escape_string() -> None:
    """
    Test ``escape_string``.
    """
    assert escape_string("1") == "1"
    assert escape_string("O'Malley's") == "O''Malley''s"


def test_unescape_string() -> None:
    """
    Test ``unescape_string``.
    """
    assert unescape_string("1") == "1"
    assert unescape_string("O''Malley''s") == "O'Malley's"


def test_escape_identifier() -> None:
    """
    Test ``escape_identifier``.
    """
    assert escape_identifier("1") == "1"
    assert escape_identifier("O'Malley's") == "O'Malley's"
    assert (
        escape_identifier('a dove called: "Who? who? who?"')
        == 'a dove called: ""Who? who? who?""'
    )


def test_unescape_identifier() -> None:
    """
    Test ``unescape_identifier``.
    """
    assert unescape_identifier("1") == "1"
    assert unescape_identifier("O''Malley''s") == "O''Malley''s"
    assert (
        unescape_identifier('a dove called: ""Who? who? who?""')
        == 'a dove called: "Who? who? who?"'
    )


def test_serialize() -> None:
    """
    Test ``serialize``.
    """
    assert serialize(["O'Malley's"]) == "2wEAAAD6Ck8nTWFsbGV5J3M="

    def func() -> int:
        return 42

    with pytest.raises(ProgrammingError) as excinfo:
        serialize(func)
    assert str(excinfo.value) == (
        f"The argument {func} is not serializable because it has type {type(func)}. "
        "Make sure only basic types (list, dicts, strings, numbers) are passed as "
        "arguments to adapters."
    )


def test_deserialize() -> None:
    """
    Test ``deserialize``.
    """
    assert deserialize("2wEAAAD6Ck8nTWFsbGV5J3M=") == ["O'Malley's"]


def test_combine_args_kwargs() -> None:
    """
    Test ``combine_args_kwargs``.
    """

    # pylint: disable=unused-argument, invalid-name
    def func(a: int = 0, b: str = "test", c: float = 10.0) -> None:
        pass

    args = ()
    kwargs = {"b": "TEST"}
    assert combine_args_kwargs(func, *args, **kwargs) == (0, "TEST", 10.0)


def test_filter_data() -> None:
    """
    Test ``filter_data``.
    """
    data: List[Dict[str, Any]]
    bounds: Dict[str, Filter]

    data = [
        {"index": 10, "temperature": 15.2, "site": "Diamond_St"},
        {"index": 11, "temperature": 13.1, "site": "Blacktail_Loop"},
        {"index": 12, "temperature": 13.3, "site": "Platinum_St"},
        {"index": 13, "temperature": 12.1, "site": "Kodiak_Trail"},
    ]

    bounds = {"index": Equal(11)}
    assert list(filter_data(iter(data), bounds, [])) == [
        {"index": 11, "site": "Blacktail_Loop", "temperature": 13.1},
    ]

    bounds = {"temperature": Range(13.1, None, False, False)}
    assert list(filter_data(iter(data), bounds, [])) == [
        {"index": 10, "temperature": 15.2, "site": "Diamond_St"},
        {"index": 12, "temperature": 13.3, "site": "Platinum_St"},
    ]

    bounds = {"temperature": Range(None, 14, False, False)}
    assert list(filter_data(iter(data), bounds, [])) == [
        {"index": 11, "temperature": 13.1, "site": "Blacktail_Loop"},
        {"index": 12, "temperature": 13.3, "site": "Platinum_St"},
        {"index": 13, "temperature": 12.1, "site": "Kodiak_Trail"},
    ]

    bounds = {"temperature": Range(13.1, 14, True, False)}
    assert list(filter_data(iter(data), bounds, [])) == [
        {"index": 11, "temperature": 13.1, "site": "Blacktail_Loop"},
        {"index": 12, "temperature": 13.3, "site": "Platinum_St"},
    ]

    bounds = {"index": NotEqual(10)}
    assert list(filter_data(iter(data), bounds, [])) == [
        {"index": 11, "temperature": 13.1, "site": "Blacktail_Loop"},
        {"index": 12, "temperature": 13.3, "site": "Platinum_St"},
        {"index": 13, "temperature": 12.1, "site": "Kodiak_Trail"},
    ]

    order: List[Tuple[str, RequestedOrder]] = [("index", Order.DESCENDING)]
    assert list(filter_data(iter(data), {}, order)) == [
        {"index": 13, "temperature": 12.1, "site": "Kodiak_Trail"},
        {"index": 12, "temperature": 13.3, "site": "Platinum_St"},
        {"index": 11, "temperature": 13.1, "site": "Blacktail_Loop"},
        {"index": 10, "temperature": 15.2, "site": "Diamond_St"},
    ]

    bounds = {"index": Impossible()}
    assert list(filter_data(iter(data), bounds, [])) == []

    data = [{"a": None, "b": 10}, {"a": 20, "b": None}]
    bounds = {"a": IsNull()}
    assert list(filter_data(iter(data), bounds, [])) == [{"a": None, "b": 10}]
    bounds = {"a": IsNotNull()}
    assert list(filter_data(iter(data), bounds, [])) == [{"a": 20, "b": None}]

    with pytest.raises(ProgrammingError) as excinfo:
        list(filter_data(iter(data), {"a": [1, 2, 3]}, []))  # type: ignore
    assert str(excinfo.value) == "Invalid filter: [1, 2, 3]"


def test_find_adapter(mocker: MockerFixture) -> None:
    """
    Test ``find_adapter``.
    """
    adapter1 = mocker.MagicMock()
    adapter1.parse_uri.return_value = ("1",)
    adapter1.configure_mock(__name__="adapter1")
    adapter2 = mocker.MagicMock()
    adapter2.parse_uri.return_value = ("2",)
    adapter2.configure_mock(__name__="adapter2")

    uri = "https://example.com/"
    adapter_kwargs: Dict[str, Any] = {}
    adapters = [
        adapter1,
        adapter2,
    ]

    adapter1.supports.return_value = True
    adapter2.supports.side_effect = [None, False]
    assert find_adapter(uri, adapter_kwargs, adapters) == (adapter1, ("1",), {})

    adapter1.supports.return_value = False
    adapter2.supports.side_effect = [None, True]
    assert find_adapter(uri, adapter_kwargs, adapters) == (adapter2, ("2",), {})


def test_is_null() -> None:
    """
    Test ``is_null``.
    """
    assert is_null(None, 10)
    assert not is_null(20, 10)


def test_is_not_null() -> None:
    """
    Test ``is_not_null``.
    """
    assert is_not_null(20, 10)
    assert not is_not_null(None, 10)


def test_apply_limit_and_offset() -> None:
    """
    Test ``apply_limit_and_offset``.
    """
    rows = apply_limit_and_offset(iter(range(10)))
    assert list(rows) == list(range(10))

    rows = apply_limit_and_offset(iter(range(10)), limit=2)
    assert list(rows) == [0, 1]

    rows = apply_limit_and_offset(iter(range(10)), limit=2, offset=2)
    assert list(rows) == [2, 3]

    rows = apply_limit_and_offset(iter(range(10)), offset=2)
    assert list(rows) == [2, 3, 4, 5, 6, 7, 8, 9]
