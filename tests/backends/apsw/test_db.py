import re
import urllib.parse
from typing import Any
from typing import Dict
from typing import Iterator
from typing import Optional
from typing import Tuple
from unittest import mock

import apsw
import pytest
from shillelagh.adapters.base import Adapter
from shillelagh.backends.apsw.db import connect
from shillelagh.backends.apsw.db import Connection
from shillelagh.backends.apsw.db import Cursor
from shillelagh.exceptions import NotSupportedError
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Float
from shillelagh.fields import Integer
from shillelagh.fields import Order
from shillelagh.fields import String
from shillelagh.filters import Equal
from shillelagh.filters import Filter
from shillelagh.filters import Range
from shillelagh.types import Row

from ...fakes import FakeAdapter
from ...fakes import FakeEntryPoint


def test_connect(mocker):
    entry_points = [FakeEntryPoint("dummy", FakeAdapter)]
    mocker.patch(
        "shillelagh.backends.apsw.db.iter_entry_points",
        return_value=entry_points,
    )

    connection = connect(":memory:", ["dummy"], isolation_level="IMMEDIATE")
    cursor = connection.cursor()

    assert cursor.rowcount == -1

    cursor.execute(
        """INSERT INTO "dummy://" (age, name, pets) VALUES (6, 'Billy', 1)""",
    )
    assert cursor.rowcount == -1
    cursor.execute("""DELETE FROM "dummy://" WHERE name = 'Billy'""")
    assert cursor.rowcount == -1

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


def test_check_closed(mocker):
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


def test_check_result(mocker):
    entry_points = [FakeEntryPoint("dummy", FakeAdapter)]
    mocker.patch(
        "shillelagh.backends.apsw.db.iter_entry_points",
        return_value=entry_points,
    )

    connection = connect(":memory:", ["dummy"], isolation_level="IMMEDIATE")
    cursor = connection.cursor()
    with pytest.raises(ProgrammingError) as excinfo:
        cursor.fetchall()

    assert str(excinfo.value) == "Called before `execute`"


def test_check_invalid_syntax(mocker):
    connection = connect(":memory:", isolation_level="IMMEDIATE")
    with pytest.raises(apsw.SQLError) as excinfo:
        connection.execute("SELLLLECT 1")
    assert str(excinfo.value) == 'SQLError: near "SELLLLECT": syntax error'


def test_unsupported_table(mocker):
    connection = connect(":memory:", isolation_level="IMMEDIATE")
    cursor = connection.cursor()

    with pytest.raises(ProgrammingError) as excinfo:
        cursor.execute('SELECT * FROM "dummy://"')
    assert str(excinfo.value) == "Unsupported table: dummy://"


def test_description(mocker):
    entry_points = [FakeEntryPoint("dummy", FakeAdapter)]
    mocker.patch(
        "shillelagh.backends.apsw.db.iter_entry_points",
        return_value=entry_points,
    )

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
        ("pets", Integer, None, None, None, None, True),
    ]


def test_execute_many(mocker):
    entry_points = [FakeEntryPoint("dummy", FakeAdapter)]
    mocker.patch(
        "shillelagh.backends.apsw.db.iter_entry_points",
        return_value=entry_points,
    )

    connection = connect(":memory:", ["dummy"], isolation_level="IMMEDIATE")
    cursor = connection.cursor()

    items = [(6, "Billy", 1), (7, "Timmy", 2)]
    with pytest.raises(NotSupportedError) as excinfo:
        cursor.executemany(
            """INSERT INTO "dummy://" (age, name, pets) VALUES (?, ?, ?)""",
            items,
        )
    assert str(excinfo.value) == "`executemany` is not supported, use `execute` instead"


def test_setsize():
    connection = connect(":memory:", isolation_level="IMMEDIATE")
    cursor = connection.cursor()
    cursor.setinputsizes(100)
    cursor.setoutputsizes(100)


def test_close_connection():
    connection = connect(":memory:", isolation_level="IMMEDIATE")
    cursor1 = connection.cursor()
    cursor2 = connection.cursor()

    cursor1.close()
    cursor1.close = mock.MagicMock()
    cursor2.close = mock.MagicMock()

    connection.close()

    cursor1.close.assert_not_called()
    cursor2.close.assert_called()


def test_transaction(mocker):
    entry_points = [FakeEntryPoint("dummy", FakeAdapter)]
    mocker.patch(
        "shillelagh.backends.apsw.db.iter_entry_points",
        return_value=entry_points,
    )

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


def test_connection_context_manager():
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


def test_connect_safe(mocker):
    entry_points = [FakeEntryPoint("dummy", FakeAdapter)]
    mocker.patch(
        "shillelagh.backends.apsw.db.iter_entry_points",
        return_value=entry_points,
    )

    connection = connect(":memory:", ["dummy"], safe=True, isolation_level="IMMEDIATE")
    assert connection._adapters == []
