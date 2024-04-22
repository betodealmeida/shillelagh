# pylint: disable=protected-access, c-extension-no-member, too-few-public-methods
"""
Tests for shillelagh.backends.apsw.db.
"""

import datetime
from typing import Any, List, Tuple
from unittest import mock

import apsw
import pytest
from pytest_mock import MockerFixture

from shillelagh.adapters.registry import AdapterLoader, UnsafeAdaptersError
from shillelagh.backends.apsw.db import Connection, connect, convert_binding
from shillelagh.exceptions import NotSupportedError, ProgrammingError
from shillelagh.fields import Float, String, StringInteger

from ...fakes import FakeAdapter


@pytest.mark.skip("Weird apsw error")
def test_connect(registry: AdapterLoader) -> None:
    """
    Test ``connect``.
    """
    registry.add("dummy", FakeAdapter)

    connection = connect(":memory:", ["dummy"], isolation_level="IMMEDIATE")
    cursor = connection.cursor()

    assert cursor.rowcount == -1

    cursor.execute(
        """INSERT INTO "dummy://" (age, name, pets) VALUES (6, 'Billy', 1)""",
    )
    # assert cursor.rowcount == 1
    cursor.execute("""DELETE FROM "dummy://" WHERE name = 'Billy'""")
    # assert cursor.rowcount == 1

    cursor.execute('SELECT * FROM "dummy://"')
    assert cursor.fetchall() == [(20.0, "Alice", 0), (23.0, "Bob", 3)]
    assert cursor.rowcount == 2

    cursor.execute('SELECT * FROM "dummy://"')
    assert cursor.fetchone() == (20.0, "Alice", 0)
    assert cursor.rowcount == 2
    assert cursor.fetchone() == (23.0, "Bob", 3)
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


def test_connect_schema_prefix(registry: AdapterLoader) -> None:
    """
    Test querying a table with the schema.
    """
    registry.add("dummy", FakeAdapter)

    connection = connect(":memory:", ["dummy"], isolation_level="IMMEDIATE")
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
    connection = mocker.patch("shillelagh.backends.apsw.db.Connection")

    connect(
        ":memory:",
        ["dummy"],
        isolation_level="IMMEDIATE",
        adapter_kwargs={"dummy": {"foo": "bar"}},
    )
    connection.assert_called_with(
        ":memory:",
        [FakeAdapter],
        {"fakeadapter": {"foo": "bar"}},
        "IMMEDIATE",
        None,
        "main",
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
    db_Connection = mocker.patch("shillelagh.backends.apsw.db.Connection")

    # if we don't specify adapters we should get all
    connect(":memory:")
    db_Connection.assert_called_with(
        ":memory:",
        [FakeAdapter1, FakeAdapter2, FakeAdapter3],
        {},
        None,
        None,
        "main",
    )

    connect(":memory:", ["two"])
    db_Connection.assert_called_with(
        ":memory:",
        [FakeAdapter2],
        {},
        None,
        None,
        "main",
    )

    # in safe mode we need to specify adapters
    connect(":memory:", safe=True)
    db_Connection.assert_called_with(
        ":memory:",
        [],
        {},
        None,
        None,
        "main",
    )

    # in safe mode only safe adapters are returned
    connect(":memory:", ["one", "two", "three"], safe=True)
    db_Connection.assert_called_with(
        ":memory:",
        [FakeAdapter1],
        {},
        None,
        None,
        "main",
    )

    # prevent repeated names, in case anyone registers a malicious adapter
    registry.clear()
    registry.add("one", FakeAdapter1)
    registry.add("one", FakeAdapter2)
    with pytest.raises(UnsafeAdaptersError) as excinfo:
        connect(":memory:", ["one"], safe=True)
    assert str(excinfo.value) == "Multiple adapters found with name one"


def test_execute_with_native_parameters(registry: AdapterLoader) -> None:
    """
    Test passing native types to the cursor.
    """
    registry.add("dummy", FakeAdapter)

    connection = connect(":memory:", ["dummy"], isolation_level="IMMEDIATE")
    cursor = connection.cursor()

    cursor.execute(
        'SELECT * FROM "dummy://" WHERE name = ?',
        (datetime.datetime.now(),),
    )
    assert cursor.fetchall() == []
    assert cursor.rowcount == 0


def test_check_closed() -> None:
    """
    Test trying to use cursor/connection after closing them.
    """
    connection = connect(":memory:", isolation_level="IMMEDIATE")
    cursor = connection.cursor()

    cursor.close()
    with pytest.raises(ProgrammingError) as excinfo:
        cursor.close()
    assert str(excinfo.value) == "Cursor already closed"

    connection.close()
    with pytest.raises(ProgrammingError) as excinfo:
        connection.close()
    assert str(excinfo.value) == "Connection already closed"


def test_check_result(registry: AdapterLoader) -> None:
    """
    Test exception raised when fetching results before query.
    """
    registry.add("dummy", FakeAdapter)

    connection = connect(":memory:", ["dummy"], isolation_level="IMMEDIATE")
    cursor = connection.cursor()
    with pytest.raises(ProgrammingError) as excinfo:
        cursor.fetchall()

    assert str(excinfo.value) == "Called before ``execute``"


def test_check_invalid_syntax() -> None:
    """
    Test exception raised on syntax error.
    """
    connection = connect(":memory:", isolation_level="IMMEDIATE")
    with pytest.raises(ProgrammingError) as excinfo:
        connection.execute("SELLLLECT 1")
    assert str(excinfo.value) == 'SQLError: near "SELLLLECT": syntax error'


def test_unsupported_table(registry: AdapterLoader) -> None:
    """
    Test exception raised on unsupported tables.
    """
    registry.clear()
    connection = connect(":memory:", isolation_level="IMMEDIATE")
    cursor = connection.cursor()

    with pytest.raises(ProgrammingError) as excinfo:
        cursor.execute('SELECT * FROM "dummy://"')
    assert str(excinfo.value) == "Unsupported table: dummy://"


def test_description(registry: AdapterLoader) -> None:
    """
    Test cursor description.
    """
    registry.add("dummy", FakeAdapter)

    connection = connect(":memory:", ["dummy"], isolation_level="IMMEDIATE")
    cursor = connection.cursor()

    assert cursor.description is None
    cursor.execute(
        """INSERT INTO "dummy://" (age, name, pets) VALUES (6, 'Billy', 1)""",
    )
    assert cursor.description is None

    cursor.execute('SELECT * FROM "dummy://"')
    assert cursor.description == [
        ("age", Float, None, None, None, None, True),
        ("name", String, None, None, None, None, True),
        ("pets", StringInteger, None, None, None, None, True),
    ]

    # Test that description is not None in the case that no rows are returned
    cursor.execute('SELECT * FROM "dummy://" where age = -23')
    assert cursor.description is not None


def test_execute_many(registry: AdapterLoader) -> None:
    """
    Test ``execute_many``.
    """
    registry.add("dummy", FakeAdapter)

    connection = connect(":memory:", ["dummy"], isolation_level="IMMEDIATE")
    cursor = connection.cursor()

    items: List[Tuple[Any, ...]] = [(6, "Billy", 1), (7, "Timmy", 2)]
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
    connection = connect(":memory:", isolation_level="IMMEDIATE")
    cursor = connection.cursor()
    cursor.setinputsizes(100)
    cursor.setoutputsizes(100)


def test_close_connection(mocker: MockerFixture) -> None:
    """
    Testing closing a connection.
    """
    connection = connect(":memory:", isolation_level="IMMEDIATE")

    cursor1 = mocker.MagicMock()
    cursor1.closed = True
    cursor2 = mocker.MagicMock()
    cursor2.closed = False
    connection.cursors.extend([cursor1, cursor2])

    connection.close()

    cursor1.close.assert_not_called()
    cursor2.close.assert_called()


def test_transaction(registry: AdapterLoader) -> None:
    """
    Test transactions.
    """
    registry.add("dummy", FakeAdapter)

    connection = connect(":memory:", ["dummy"], isolation_level="IMMEDIATE")

    cursor = connection.cursor()
    cursor._cursor = mock.MagicMock()
    cursor._cursor.execute.side_effect = [
        "",
        apsw.SQLError("SQLError: no such table: dummy://"),
        "",
        "",
        "",
        "",
        "",
        "",
    ]

    assert not cursor.in_transaction
    connection.commit()
    cursor._cursor.execute.assert_not_called()

    cursor.execute('SELECT 1 FROM "dummy://"')
    assert cursor.in_transaction
    cursor._cursor.execute.assert_has_calls(
        [
            mock.call("BEGIN IMMEDIATE"),
            mock.call('SELECT 1 FROM "dummy://"', None),
            mock.call('CREATE VIRTUAL TABLE "dummy://" USING FakeAdapter()'),
            mock.call('SELECT 1 FROM "dummy://"', None),
        ],
    )

    connection.rollback()
    assert not cursor.in_transaction
    cursor.execute("SELECT 2")
    assert cursor.in_transaction
    cursor._cursor.execute.assert_has_calls(
        [
            mock.call("BEGIN IMMEDIATE"),
            mock.call('SELECT 1 FROM "dummy://"', None),
            mock.call('CREATE VIRTUAL TABLE "dummy://" USING FakeAdapter()'),
            mock.call('SELECT 1 FROM "dummy://"', None),
            mock.call("ROLLBACK"),
            mock.call("BEGIN IMMEDIATE"),
            mock.call("SELECT 2", None),
        ],
    )

    connection.commit()
    assert not cursor.in_transaction
    cursor._cursor.execute.assert_has_calls(
        [
            mock.call("BEGIN IMMEDIATE"),
            mock.call('SELECT 1 FROM "dummy://"', None),
            mock.call('CREATE VIRTUAL TABLE "dummy://" USING FakeAdapter()'),
            mock.call('SELECT 1 FROM "dummy://"', None),
            mock.call("ROLLBACK"),
            mock.call("BEGIN IMMEDIATE"),
            mock.call("SELECT 2", None),
            mock.call("COMMIT"),
        ],
    )

    cursor._cursor.reset_mock()
    connection.rollback()
    cursor._cursor.execute.assert_not_called()


def test_connection_context_manager() -> None:
    """
    Test that connection can be used as context manager.
    """
    with connect(":memory:", isolation_level="IMMEDIATE") as connection:
        cursor = connection.cursor()
        cursor._cursor = mock.MagicMock()
        cursor.execute("SELECT 2")

    cursor._cursor.execute.assert_has_calls(
        [
            mock.call("BEGIN IMMEDIATE"),
            mock.call("SELECT 2", None),
            mock.call("COMMIT"),
        ],
    )


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

    connection = connect(":memory:", ["dummy"], safe=True, isolation_level="IMMEDIATE")
    assert connection._adapters == []


def test_convert_binding() -> None:
    """
    Test conversion to SQLite types.
    """
    assert convert_binding(1) == 1
    assert convert_binding(1.0) == 1.0
    assert convert_binding("test") == "test"
    assert convert_binding(None) is None
    assert convert_binding(datetime.datetime(2021, 1, 1)) == "2021-01-01T00:00:00"
    assert convert_binding(datetime.date(2021, 1, 1)) == "2021-01-01"
    assert convert_binding(datetime.time(12, 0, 0)) == "12:00:00"
    assert (
        convert_binding(datetime.datetime(2021, 1, 1, tzinfo=datetime.timezone.utc))
        == "2021-01-01T00:00:00+00:00"
    )
    assert (
        convert_binding(datetime.time(12, 0, 0, tzinfo=datetime.timezone.utc))
        == "12:00:00+00:00"
    )
    assert convert_binding(True) == 1
    assert convert_binding(False) == 0
    assert convert_binding({}) == "{}"


def test_drop_table(mocker: MockerFixture, registry: AdapterLoader) -> None:
    """
    Test ``drop_table``.
    """
    registry.add("dummy", FakeAdapter)
    drop_table = mocker.patch.object(FakeAdapter, "drop_table")

    connection = connect(":memory:", ["dummy"], isolation_level="IMMEDIATE")
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

    connection = connect(":memory:", ["dummy"], isolation_level="IMMEDIATE")
    cursor = connection.cursor()

    cursor.execute(
        """
-- hello
DROP TABLE "dummy://"
-- goodbye
    """,
    )
    drop_table.assert_called()  # type: ignore


def test_best_index(mocker: MockerFixture) -> None:
    """
    Test that ``use_bestindex_object`` is only passed for apsw >= 3.41.0.0
    """
    # pylint: disable=redefined-outer-name, invalid-name
    apsw = mocker.patch("shillelagh.backends.apsw.db.apsw")
    VTModule = mocker.patch("shillelagh.backends.apsw.db.VTModule")
    adapter = mocker.MagicMock()
    adapter.__name__ = "some_adapter"
    adapter.supports_requested_columns = True

    mocker.patch(
        "shillelagh.backends.apsw.db.best_index_object_available",
        return_value=True,
    )
    Connection(":memory:", [adapter], {})
    apsw.Connection().createmodule.assert_called_with(
        "some_adapter",
        VTModule(adapter),
        use_bestindex_object=True,
    )

    mocker.patch(
        "shillelagh.backends.apsw.db.best_index_object_available",
        return_value=False,
    )
    Connection(":memory:", [adapter], {})
    apsw.Connection().createmodule.assert_called_with(
        "some_adapter",
        VTModule(adapter),
    )
