"""
Tests for shillelagh.backends.sqlglot.dialects.base.
"""

from unittest import mock

import pytest
from sqlalchemy import MetaData, Table, create_engine, func, inspect, select

from shillelagh.adapters.registry import AdapterLoader
from shillelagh.backends.sqlglot import db
from shillelagh.backends.sqlglot.dialects.base import SQLGlotDialect
from shillelagh.exceptions import ProgrammingError

from ....fakes import FakeAdapter


def test_dbapi() -> None:
    """
    Test the ``dbapi`` and ``import_dbapi`` methods.
    """
    assert SQLGlotDialect.dbapi() == SQLGlotDialect.import_dbapi() == db


def test_create_engine(registry: AdapterLoader) -> None:
    """
    Test ``create_engine``.
    """
    registry.add("dummy", FakeAdapter)

    engine = create_engine("shillelagh+sqlglot://")
    metadata = MetaData()
    metadata.reflect(engine)

    table = Table("dummy://", metadata, autoload_with=engine)
    query = select(
        func.sum(table.columns.pets),  # pylint: disable=no-member, not-callable
    )
    connection = engine.connect()
    assert connection.execute(query).scalar() == 3


def test_create_engine_no_adapters(registry: AdapterLoader) -> None:
    """
    Test ``create_engine`` with invalid adapter.
    """
    registry.clear()
    engine = create_engine("shillelagh+sqlglot://")
    metadata = MetaData()
    metadata.reflect(engine)

    with pytest.raises(ProgrammingError) as excinfo:
        Table("dummy://", metadata, autoload_with=engine)
    assert str(excinfo.value) == "Unsupported table: dummy://"


def test_dialect_ping() -> None:
    """
    Test ``do_ping``.
    """
    mock_dbapi_connection = mock.MagicMock()
    dialect = SQLGlotDialect()
    assert dialect.do_ping(mock_dbapi_connection) is True


def test_has_table(registry: AdapterLoader) -> None:
    """
    Test ``has_table``.
    """
    registry.add("dummy", FakeAdapter)

    engine = create_engine("shillelagh+sqlglot://")
    inspector = inspect(engine)
    assert inspector.has_table("dummy://a")
    assert inspector.has_table("dummy://b")
    assert not inspector.has_table("funny://b")


def test_get_schema_names(registry: AdapterLoader) -> None:
    """
    Test ``get_schema_names``.
    """
    registry.add("dummy", FakeAdapter)

    engine = create_engine("shillelagh+sqlglot://")
    inspector = inspect(engine)
    assert inspector.get_schema_names() == ["main"]


def test_import_dbapi() -> None:
    """
    Test ``import_dbapi``.
    """
    assert SQLGlotDialect.import_dbapi() == SQLGlotDialect.dbapi()
