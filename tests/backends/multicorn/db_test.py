"""
Tests for the Multicorn2 DB API 2.0 wrapper.
"""

# pylint: disable=invalid-name, redefined-outer-name, no-member, redefined-builtin

import psycopg2
import pytest
from pytest_mock import MockerFixture

from shillelagh.adapters.registry import AdapterLoader
from shillelagh.backends.multicorn.db import Cursor, CursorFactory, connect
from shillelagh.exceptions import ProgrammingError

from ...fakes import FakeAdapter


def test_connect(mocker: MockerFixture, registry: AdapterLoader) -> None:
    """
    Test the ``connect`` function.
    """
    psycopg2 = mocker.patch("shillelagh.backends.multicorn.db.psycopg2")
    CursorFactory = mocker.patch("shillelagh.backends.multicorn.db.CursorFactory")

    registry.add("dummy", FakeAdapter)

    connect(
        None,
        ["dummy"],
        user="username",
        password="password",
        host="host",
        port=1234,
        database="database",
    )
    psycopg2.connect.assert_called_with(
        None,
        cursor_factory=CursorFactory(
            {"dummy": FakeAdapter},
            {},
            "main",
        ),
        user="username",
        password="password",
        host="host",
        port=1234,
        database="database",
    )


def test_cursor_factory(mocker: MockerFixture) -> None:
    """
    Test the ``CursorFactory`` class.
    """
    Cursor = mocker.patch("shillelagh.backends.multicorn.db.Cursor")

    cursor_factory = CursorFactory(
        {"dummy": FakeAdapter},
        {},
        "main",
    )
    assert cursor_factory(
        user="username",
        password="password",
        host="host",
        port=1234,
        database="database",
    ) == Cursor(
        adapters=["dummy"],
        adapter_kwargs={},
        schema="main",
        user="username",
        password="password",
        host="host",
        port=1234,
        database="database",
    )


def test_cursor(mocker: MockerFixture) -> None:
    """
    Test the ``Cursor`` class.
    """
    mocker.patch("shillelagh.backends.multicorn.db.uuid4", return_value="uuid")
    super = mocker.patch("shillelagh.backends.multicorn.db.super", create=True)
    execute = mocker.MagicMock(name="execute")
    super.return_value.execute = execute

    cursor = Cursor(
        adapters={"dummy": FakeAdapter},
        adapter_kwargs={},
        schema="main",
    )

    cursor.execute("SELECT 1")
    execute.assert_has_calls(
        [
            mocker.call('SAVEPOINT "uuid"'),
            mocker.call("SELECT 1", None),
        ],
    )

    execute.reset_mock()
    execute.side_effect = [
        True,  # SAVEPOINT
        psycopg2.errors.UndefinedTable('relation "dummy://" does not exist'),
        True,  # ROLLBACK
        True,  # CREATE SERVER
        True,  # CREATE FOREIGN TABLE
        True,  # SAVEPOINT
        True,  # successful query
    ]

    cursor.execute('SELECT * FROM "dummy://"')
    execute.assert_has_calls(
        [
            mocker.call('SAVEPOINT "uuid"'),
            mocker.call('SELECT * FROM "dummy://"', None),
            mocker.call('ROLLBACK TO SAVEPOINT "uuid"'),
            mocker.call(
                """
CREATE SERVER shillelagh foreign data wrapper multicorn options (
    wrapper 'shillelagh.backends.multicorn.fdw.MulticornForeignDataWrapper'
);
    """,
            ),
            mocker.call(
                """
CREATE FOREIGN TABLE "dummy://" (
    "age" REAL, "name" TEXT, "pets" INTEGER
) server shillelagh options (
    adapter \'dummy\',
    args \'qQA=\'
);
        """,
            ),
            mocker.call('SAVEPOINT "uuid"'),
            mocker.call('SELECT * FROM "dummy://"', None),
        ],
    )


def test_cursor_no_table_match(mocker: MockerFixture) -> None:
    """
    Test an edge case where ``UndefinedTable`` is raised with a different message.
    """
    super = mocker.patch("shillelagh.backends.multicorn.db.super", create=True)
    execute = mocker.MagicMock(name="execute")
    super.return_value.execute = execute

    execute.side_effect = [
        True,  # SAVEPOINT
        psycopg2.errors.UndefinedTable("An unexpected error occurred"),
    ]

    cursor = Cursor(
        adapters={"dummy": FakeAdapter},
        adapter_kwargs={},
        schema="main",
    )

    with pytest.raises(ProgrammingError) as excinfo:
        cursor.execute('SELECT * FROM "dummy://"')
    assert str(excinfo.value) == "An unexpected error occurred"


def test_cursor_no_table_name(mocker: MockerFixture) -> None:
    """
    Test an edge case where we can't determine the table name from the exception.
    """
    super = mocker.patch("shillelagh.backends.multicorn.db.super", create=True)
    execute = mocker.MagicMock(name="execute")
    super.return_value.execute = execute

    execute.side_effect = [
        True,  # SAVEPOINT
        psycopg2.errors.UndefinedTable('relation "invalid://" does not exist'),
    ]

    cursor = Cursor(
        adapters={"dummy": FakeAdapter},
        adapter_kwargs={},
        schema="main",
    )

    with pytest.raises(ProgrammingError) as excinfo:
        cursor.execute('SELECT * FROM "dummy://"')
    assert str(excinfo.value) == "Could not determine table name"


def test_drop_table(mocker: MockerFixture) -> None:
    """
    Test the ``drop_table`` method.
    """
    super = mocker.patch("shillelagh.backends.multicorn.db.super", create=True)
    execute = mocker.MagicMock(name="execute")
    super.return_value.execute = execute
    adapter = mocker.MagicMock(name="adapter")
    mocker.patch(
        "shillelagh.backends.multicorn.db.find_adapter",
        return_value=(adapter, ["one"], {"two": 2}),
    )

    cursor = Cursor(
        adapters={"dummy": FakeAdapter},
        adapter_kwargs={},
        schema="main",
    )

    cursor.execute('DROP TABLE "dummy://"')
    adapter.assert_called_with("one", two=2)
    adapter().drop_table.assert_called()


def test_table_without_columns(mocker: MockerFixture) -> None:
    """
    Test an edge case where a virtual table has no columns.
    """
    super = mocker.patch("shillelagh.backends.multicorn.db.super", create=True)
    execute = mocker.MagicMock(name="execute")
    super.return_value.execute = execute
    adapter = mocker.MagicMock(name="adapter")
    adapter().get_columns.return_value = []
    mocker.patch(
        "shillelagh.backends.multicorn.db.find_adapter",
        return_value=(adapter, ["one"], {"two": 2}),
    )

    cursor = Cursor(
        adapters={"dummy": adapter},
        adapter_kwargs={},
        schema="main",
    )
    with pytest.raises(ProgrammingError) as excinfo:
        cursor._create_table("dummy://")  # pylint: disable=protected-access
    assert str(excinfo.value) == "Virtual table dummy:// has no columns"
