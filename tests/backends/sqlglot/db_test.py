"""
Tests for shillelagh.backends.sqlglot.db.
"""

# pylint: disable=protected-access, c-extension-no-member, too-few-public-methods

import datetime
from typing import Any

import pytest
from pytest_mock import MockerFixture

from shillelagh.adapters.registry import AdapterLoader, UnsafeAdaptersError
from shillelagh.backends.sqlglot.db import connect
from shillelagh.exceptions import InterfaceError, NotSupportedError, ProgrammingError
from shillelagh.fields import Boolean, DateTime, Integer, String

from ...fakes import FakeAdapter


def test_connect(registry: AdapterLoader) -> None:
    """
    Test ``connect``.
    """
    registry.add("dummy", FakeAdapter)

    connection = connect(["dummy"])
    cursor = connection.cursor()

    assert cursor.rowcount == -1

    cursor.execute('SELECT * FROM "dummy://"')
    assert cursor.fetchall() == [(20, "Alice", 0), (23, "Bob", 3)]
    assert cursor.rowcount == 2

    cursor.execute('SELECT * FROM "dummy://" WHERE age > 18 AND pets > 0')
    assert cursor.fetchall() == [(23, "Bob", 3)]
    assert cursor.rowcount == 1

    cursor.execute('SELECT * FROM "dummy://" WHERE 18 < age AND 0 < pets')
    assert cursor.fetchall() == [(23, "Bob", 3)]
    assert cursor.rowcount == 1

    cursor.execute('SELECT * FROM "dummy://"')
    assert cursor.fetchone() == (20, "Alice", 0)
    assert cursor.rowcount == 2
    assert cursor.fetchone() == (23, "Bob", 3)
    assert cursor.rowcount == 2
    assert cursor.fetchone() is None

    cursor.execute('SELECT * FROM "dummy://" WHERE age > 21')
    assert cursor.fetchone() == (23.0, "Bob", 3)
    assert cursor.rowcount == 1
    assert cursor.fetchone() is None

    cursor.execute('SELECT * FROM "dummy://"')
    assert cursor.fetchmany() == [(20.0, "Alice", 0)]
    assert cursor.fetchmany(1000) == [(23.0, "Bob", 3)]
    assert cursor.fetchall() == []
    assert cursor.rowcount == 2


def test_predicates_with_columns_to_the_right(registry: AdapterLoader) -> None:
    """
    Test that predicates with columns to the right work correctly.
    """
    registry.add("dummy", FakeAdapter)

    connection = connect(["dummy"])
    cursor = connection.cursor()

    cursor.execute('SELECT * FROM "dummy://" WHERE 18 < age AND pets > 0')
    assert cursor.fetchall() == [(23, "Bob", 3)]


def test_nested_subqueries(registry: AdapterLoader) -> None:
    """
    Test that queries with nested subqueries work.
    """
    registry.add("dummy", FakeAdapter)

    connection = connect(["dummy"])
    cursor = connection.cursor()

    cursor.execute(
        'SELECT name FROM (SELECT * FROM (SELECT * FROM "dummy://")) WHERE 18 < age AND pets > 0',
    )
    assert cursor.fetchall() == [("Bob",)]


def test_operators(registry: AdapterLoader) -> None:
    """
    Test that operators work correctly.
    """

    class FakeAdapterWithExtra(FakeAdapter):
        """
        A fake adapter with extra fields.
        """

        active = Boolean()
        last_login = DateTime()

        def __init__(self):
            super().__init__()
            self.data = [
                {
                    "rowid": 0,
                    "name": "Alice",
                    "age": 20,
                    "pets": 0,
                    "active": True,
                    "last_login": datetime.datetime(2023, 10, 1, 12, 0),
                },
                {
                    "rowid": 1,
                    "name": "Bob",
                    "age": 23,
                    "pets": 3,
                    "active": False,
                    "last_login": None,
                },
            ]

    registry.add("dummy", FakeAdapterWithExtra)

    connection = connect(["dummy"])
    cursor = connection.cursor()

    cursor.execute('SELECT * FROM "dummy://" WHERE age > 18 AND pets > 0')
    assert cursor.fetchall() == [(False, 23, None, "Bob", 3)]

    cursor.execute('SELECT name FROM "dummy://" WHERE last_login IS NULL')
    assert cursor.fetchall() == [("Bob",)]

    cursor.execute('SELECT name FROM "dummy://" WHERE last_login IS NOT NULL')
    assert cursor.fetchall() == [("Alice",)]

    cursor.execute('SELECT name FROM "dummy://" WHERE active')
    assert cursor.fetchall() == [("Alice",)]

    cursor.execute("""SELECT name FROM "dummy://" WHERE name LIKE 'A%'""")
    assert cursor.fetchall() == [("Alice",)]

    cursor.execute("""SELECT name FROM "dummy://" WHERE UPPER(name) = 'ALICE'""")
    assert cursor.fetchall() == [("Alice",)]


def test_connect_schema_prefix(registry: AdapterLoader) -> None:
    """
    Test querying a table with the schema.
    """
    registry.add("dummy", FakeAdapter)

    connection = connect(["dummy"])
    cursor = connection.cursor()

    cursor.execute('SELECT * FROM main."dummy://"')
    assert cursor.fetchmany() == [(20.0, "Alice", 0)]
    assert cursor.fetchmany(1000) == [(23.0, "Bob", 3)]
    assert cursor.fetchall() == []
    assert cursor.rowcount == 2


def test_connect_adapter_kwargs(mocker: MockerFixture, registry: AdapterLoader) -> None:
    """
    Test that ``adapter_kwargs`` are passed to the adapter.
    """
    registry.add("dummy", FakeAdapter)
    connection = mocker.patch("shillelagh.backends.sqlglot.db.SQLGlotConnection")

    connect(
        ["dummy"],
        adapter_kwargs={"dummy": {"foo": "bar"}},
    )
    connection.assert_called_with(
        [FakeAdapter],
        {"fakeadapter": {"foo": "bar"}},
        "main",
        False,
    )


def test_connect_safe(mocker: MockerFixture, registry: AdapterLoader) -> None:
    """
    Test the safe option.
    """

    class FakeAdapter1(FakeAdapter):
        """
        A safe adapter.
        """

        safe = True

    class FakeAdapter2(FakeAdapter):
        """
        An unsafe adapter.
        """

        safe = False

    class FakeAdapter3(FakeAdapter):
        """
        Another unsafe adapter.
        """

        safe = False

    registry.clear()
    registry.add("one", FakeAdapter1)
    registry.add("two", FakeAdapter2)
    registry.add("three", FakeAdapter3)
    # pylint: disable=invalid-name
    db_Connection = mocker.patch("shillelagh.backends.sqlglot.db.SQLGlotConnection")

    # if we don't specify adapters we should get all
    connect()
    db_Connection.assert_called_with(
        [FakeAdapter1, FakeAdapter2, FakeAdapter3],
        {},
        "main",
        False,
    )

    connect(["two"])
    db_Connection.assert_called_with(
        [FakeAdapter2],
        {},
        "main",
        False,
    )

    # in safe mode we need to specify adapters
    connect(safe=True)
    db_Connection.assert_called_with(
        [],
        {},
        "main",
        True,
    )

    # in safe mode only safe adapters are returned
    connect(["one", "two", "three"], safe=True)
    db_Connection.assert_called_with(
        [FakeAdapter1],
        {},
        "main",
        True,
    )

    # prevent repeated names, in case anyone registers a malicious adapter
    registry.clear()
    registry.add("one", FakeAdapter1)
    registry.add("one", FakeAdapter2)
    with pytest.raises(UnsafeAdaptersError) as excinfo:
        connect(["one"], safe=True)
    assert str(excinfo.value) == "Multiple adapters found with name one"


@pytest.mark.parametrize(
    "parameter",
    [
        datetime.datetime.now().replace(tzinfo=datetime.timezone.utc),
        datetime.date.today(),
        # remove once https://github.com/tobymao/sqlglot/pull/5409 is released
        # datetime.time(12, 0),
        True,
        False,
        None,
    ],
)
def test_execute_with_native_parameters(
    registry: AdapterLoader,
    parameter: Any,
) -> None:
    """
    Test passing native types to the cursor.
    """
    registry.add("dummy", FakeAdapter)

    connection = connect(["dummy"])
    cursor = connection.cursor()

    cursor.execute(
        'SELECT * FROM "dummy://" WHERE name = ?',
        (parameter,),
    )
    assert cursor.fetchall() == []
    assert cursor.rowcount == 0


def test_check_closed() -> None:
    """
    Test trying to use cursor/connection after closing them.
    """
    connection = connect()
    cursor = connection.cursor()

    cursor.close()
    with pytest.raises(ProgrammingError) as excinfo:
        cursor.close()
    assert str(excinfo.value) == "SQLGlotCursor already closed"

    connection.close()
    with pytest.raises(ProgrammingError) as excinfo:
        connection.close()
    assert str(excinfo.value) == "SQLGlotConnection already closed"


def test_check_result(registry: AdapterLoader) -> None:
    """
    Test exception raised when fetching results before query.
    """
    registry.add("dummy", FakeAdapter)

    connection = connect(["dummy"])
    cursor = connection.cursor()
    with pytest.raises(ProgrammingError) as excinfo:
        cursor.fetchall()

    assert str(excinfo.value) == "Called before ``execute``"


def test_check_invalid_syntax() -> None:
    """
    Test exception raised on syntax error.
    """
    connection = connect()
    with pytest.raises(ProgrammingError) as excinfo:
        connection.execute("SELLLLECT 1")
    assert str(excinfo.value) == "Invalid SQL query"


def test_unsupported_table(registry: AdapterLoader) -> None:
    """
    Test exception raised on unsupported tables.
    """
    registry.clear()
    connection = connect()
    cursor = connection.cursor()

    with pytest.raises(ProgrammingError) as excinfo:
        cursor.execute('SELECT * FROM "dummy://"')
    assert str(excinfo.value) == "Unsupported table: dummy://"


def test_description(registry: AdapterLoader) -> None:
    """
    Test cursor description.
    """
    registry.add("dummy", FakeAdapter)

    connection = connect(["dummy"])
    cursor = connection.cursor()

    assert cursor.description is None

    cursor.execute('SELECT * FROM "dummy://"')
    assert cursor.description == [
        ("age", String, None, None, None, None, True),
        ("name", String, None, None, None, None, True),
        ("pets", Integer, None, None, None, None, True),
    ]

    # Test that description is not None in the case that no rows are returned
    cursor.execute('SELECT * FROM "dummy://" WHERE age = 24')
    assert cursor.description is not None


def test_execute_many(registry: AdapterLoader) -> None:
    """
    Test ``execute_many``.
    """
    registry.add("dummy", FakeAdapter)

    connection = connect(["dummy"])
    cursor = connection.cursor()

    items: list[tuple[Any, ...]] = [(6, "Billy", 1), (7, "Timmy", 2)]
    with pytest.raises(NotSupportedError) as excinfo:
        cursor.executemany(
            """INSERT INTO "dummy://" (age, name, pets) VALUES (?, ?, ?)""",
            items,
        )
    assert (
        str(excinfo.value)
        == "``executemany`` is not supported, use ``execute`` instead"
    )


def test_setsize() -> None:
    """
    Test ``setinputsizes`` and ``setoutputsizes``.
    """
    connection = connect()
    cursor = connection.cursor()
    cursor.setinputsizes(100)
    cursor.setoutputsizes(100)


def test_close_connection(mocker: MockerFixture) -> None:
    """
    Testing closing a connection.
    """
    connection = connect()

    cursor1 = mocker.MagicMock()
    cursor1.closed = True
    cursor2 = mocker.MagicMock()
    cursor2.closed = False
    connection.cursors.extend([cursor1, cursor2])

    connection.close()

    cursor1.close.assert_not_called()
    cursor2.close.assert_called()


def test_connection_context_manager(mocker: MockerFixture) -> None:
    """
    Test that connection can be used as context manager.
    """
    execute = mocker.patch("shillelagh.backends.sqlglot.db.execute")

    with connect() as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT 2")

    execute.assert_called()


def test_connect_safe_lists_only_safe_adapters(registry: AdapterLoader) -> None:
    """
    Test the safe connection.
    """

    class UnsafeAdapter(FakeAdapter):
        """
        A safe adapter.
        """

        safe = False

    registry.clear()
    registry.add("dummy", UnsafeAdapter)

    connection = connect(["dummy"], safe=True)
    assert connection._adapters == []


def test_drop_table(mocker: MockerFixture, registry: AdapterLoader) -> None:
    """
    Test ``drop_table``.
    """
    registry.add("dummy", FakeAdapter)
    drop_table = mocker.patch.object(FakeAdapter, "drop_table")

    connection = connect(["dummy"])
    cursor = connection.cursor()

    cursor.execute('DROP TABLE "dummy://"')
    drop_table.assert_called()  # type: ignore


def test_drop_table_with_comments(
    mocker: MockerFixture,
    registry: AdapterLoader,
) -> None:
    """
    Test ``drop_table`` when the query has comments.
    """
    registry.add("dummy", FakeAdapter)
    drop_table = mocker.patch.object(FakeAdapter, "drop_table")

    connection = connect(["dummy"])
    cursor = connection.cursor()

    cursor.execute(
        """
-- hello
DROP TABLE "dummy://"
-- goodbye
    """,
    )
    drop_table.assert_called()  # type: ignore


def test_invalid_query() -> None:
    """
    Test that the cursor handles invalid queries gracefully.
    """
    connection = connect()
    cursor = connection.cursor()

    with pytest.raises(InterfaceError) as excinfo:
        cursor.execute("CREATE TABLE foo (bar INT)")
    assert str(excinfo.value) == "Only `DROP TABLE` and `SELECT` queries are supported"
