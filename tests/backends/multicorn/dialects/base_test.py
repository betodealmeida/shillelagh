"""
Tests for the multicorn dialect.
"""

from pytest_mock import MockerFixture
from sqlalchemy.engine.url import make_url

from shillelagh.backends.multicorn import db
from shillelagh.backends.multicorn.db import Cursor
from shillelagh.backends.multicorn.dialects.base import (
    Multicorn2Dialect,
    get_adapter_for_table_name,
)
from shillelagh.exceptions import ProgrammingError

from ....fakes import FakeAdapter


def test_dbapi() -> None:
    """
    Test the ``dbapi`` and ``import_dbapi`` methods.
    """
    assert Multicorn2Dialect.dbapi() == Multicorn2Dialect.import_dbapi() == db


def test_create_connect_args() -> None:
    """
    Test ``create_connect_args``.
    """
    dialect = Multicorn2Dialect(["dummy"], {})
    assert dialect.create_connect_args(
        make_url(
            "shillelagh+multicorn2://shillelagh:shillelagh123@localhost:12345/shillelagh",
        ),
    ) == (
        [],
        {
            "adapter_kwargs": {},
            "adapters": ["dummy"],
            "user": "shillelagh",
            "password": "shillelagh123",
            "host": "localhost",
            "port": 12345,
            "dbname": "shillelagh",
        },
    )


def test_has_table(mocker: MockerFixture) -> None:
    """
    Test ``has_table``.
    """
    super = mocker.patch(  # pylint: disable=redefined-builtin
        "shillelagh.backends.multicorn.dialects.base.super",
        create=True,
    )
    has_table = mocker.MagicMock(name="has_table", return_value=False)
    super.return_value.has_table = has_table
    mocker.patch(
        "shillelagh.backends.multicorn.dialects.base.get_adapter_for_table_name",
        side_effect=[True, ProgrammingError('No adapter for table "dummy://".')],
    )
    connection = mocker.MagicMock()

    dialect = Multicorn2Dialect(["dummy"], {})
    assert dialect.has_table(connection, "dummy://") is True
    assert dialect.has_table(connection, "my_table") is False


def test_get_adapter_for_table_name(mocker: MockerFixture) -> None:
    """
    Test the ``get_adapter_for_table_name`` function.
    """
    mocker.patch("shillelagh.backends.multicorn.db.super", create=True)
    connection = mocker.MagicMock()
    connection.engine.raw_connection().cursor.return_value = Cursor(
        adapters={"dummy": FakeAdapter},
        adapter_kwargs={},
        schema="main",
    )

    assert isinstance(get_adapter_for_table_name(connection, "dummy://"), FakeAdapter)
