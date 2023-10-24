"""
Tests for the Multicorn2 FDW.
"""

# pylint: disable=invalid-name, redefined-outer-name, no-member, redefined-builtin

from collections import defaultdict

from multicorn import Qual, SortKey
from pytest_mock import MockerFixture

from shillelagh.adapters.registry import AdapterLoader
from shillelagh.backends.multicorn.fdw import (
    MulticornForeignDataWrapper,
    get_all_bounds,
)
from shillelagh.filters import Operator

from ...fakes import FakeAdapter


def test_fdw(mocker: MockerFixture, registry: AdapterLoader) -> None:
    """
    Test the ``MulticornForeignDataWrapper`` class.
    """
    mocker.patch("shillelagh.backends.multicorn.fdw.registry", registry)

    registry.add("dummy", FakeAdapter)

    assert (
        MulticornForeignDataWrapper.import_schema("schema", {}, {}, "limit", []) == []
    )
    wrapper = MulticornForeignDataWrapper(
        {"adapter": "dummy", "args": "qQA="},
        {},
    )
    assert wrapper.rowid_column == "rowid"

    assert list(wrapper.execute([], ["rowid", "name", "age", "pets"])) == [
        {"rowid": 0, "name": "Alice", "age": 20, "pets": 0},
        {"rowid": 1, "name": "Bob", "age": 23, "pets": 3},
    ]

    assert list(
        wrapper.execute(
            [Qual("age", ">", 21)],
            ["rowid", "name", "age", "pets"],
            [],
        ),
    ) == [
        {"rowid": 1, "name": "Bob", "age": 23, "pets": 3},
    ]

    assert list(
        wrapper.execute(
            [],
            ["rowid", "name", "age", "pets"],
            [
                SortKey(
                    attname="age",
                    attnum=2,
                    is_reversed=True,
                    nulls_first=True,
                    collate=None,
                ),
            ],
        ),
    ) == [
        {"rowid": 1, "name": "Bob", "age": 23, "pets": 3},
        {"rowid": 0, "name": "Alice", "age": 20, "pets": 0},
    ]


def test_get_all_bounds() -> None:
    """
    Test ``get_all_bounds``.
    """
    quals = [
        Qual("column1", "=", 3),
        Qual("column2", "LIKE", "test%"),
        Qual("column3", ">", 10),
    ]

    assert get_all_bounds([]) == defaultdict(set)
    assert get_all_bounds([quals[0]]) == {"column1": {(Operator.EQ, 3)}}
    assert get_all_bounds(quals) == {
        "column1": {(Operator.EQ, 3)},
        "column3": {(Operator.GT, 10)},
    }
    assert get_all_bounds([Qual("column4", "unsupported_operator", 1)]) == defaultdict(
        set,
    )


def test_can_sort(mocker: MockerFixture, registry: AdapterLoader) -> None:
    """
    Test the ``can_sort`` method.
    """
    mocker.patch("shillelagh.backends.multicorn.fdw.registry", registry)

    registry.add("dummy", FakeAdapter)

    wrapper = MulticornForeignDataWrapper(
        {"adapter": "dummy", "args": "qQA="},
        {},
    )
    assert wrapper.can_sort([]) == []
    assert wrapper.can_sort(
        [
            SortKey(
                attname="age",
                attnum=2,
                is_reversed=True,
                nulls_first=True,
                collate=None,
            ),
            SortKey(
                attname="foobar",
                attnum=1,
                is_reversed=True,
                nulls_first=True,
                collate=None,
            ),
        ],
    ) == [
        SortKey(
            attname="age",
            attnum=2,
            is_reversed=True,
            nulls_first=True,
            collate=None,
        ),
    ]


def test_insert(mocker: MockerFixture, registry: AdapterLoader) -> None:
    """
    Test the ``insert`` method.
    """
    mocker.patch("shillelagh.backends.multicorn.fdw.registry", registry)

    registry.add("dummy", FakeAdapter)

    wrapper = MulticornForeignDataWrapper(
        {"adapter": "dummy", "args": "qQA="},
        {},
    )

    wrapper.insert({"rowid": 2, "name": "Charlie", "age": 6, "pets": 1})
    assert list(wrapper.execute([], ["rowid", "name", "age", "pets"])) == [
        {"rowid": 0, "name": "Alice", "age": 20, "pets": 0},
        {"rowid": 1, "name": "Bob", "age": 23, "pets": 3},
        {"rowid": 2, "name": "Charlie", "age": 6, "pets": 1},
    ]


def test_delete(mocker: MockerFixture, registry: AdapterLoader) -> None:
    """
    Test the ``delete`` method.
    """
    mocker.patch("shillelagh.backends.multicorn.fdw.registry", registry)

    registry.add("dummy", FakeAdapter)

    wrapper = MulticornForeignDataWrapper(
        {"adapter": "dummy", "args": "qQA="},
        {},
    )

    wrapper.delete({"rowid": 1, "name": "Bob", "age": 23, "pets": 3})
    assert list(wrapper.execute([], ["rowid", "name", "age", "pets"])) == [
        {"rowid": 0, "name": "Alice", "age": 20, "pets": 0},
    ]


def test_update(mocker: MockerFixture, registry: AdapterLoader) -> None:
    """
    Test the ``update`` method.
    """
    mocker.patch("shillelagh.backends.multicorn.fdw.registry", registry)

    registry.add("dummy", FakeAdapter)

    wrapper = MulticornForeignDataWrapper(
        {"adapter": "dummy", "args": "qQA="},
        {},
    )

    wrapper.update(
        {"rowid": 0, "name": "Alice", "age": 20, "pets": 0},
        {"rowid": 0, "name": "Alice", "age": 20, "pets": 1},
    )
    assert list(wrapper.execute([], ["rowid", "name", "age", "pets"])) == [
        {"rowid": 1, "name": "Bob", "age": 23, "pets": 3},
        {"rowid": 0, "name": "Alice", "age": 20, "pets": 1},
    ]


def test_get_rel_Size(mocker: MockerFixture, registry: AdapterLoader) -> None:
    """
    Test the ``get_rel_size`` method.
    """
    mocker.patch("shillelagh.backends.multicorn.fdw.registry", registry)

    registry.add("dummy", FakeAdapter)

    wrapper = MulticornForeignDataWrapper(
        {"adapter": "dummy", "args": "qQA="},
        {},
    )

    assert wrapper.get_rel_size([Qual("age", ">", 21)], ["name", "age"]) == (666, 200)
