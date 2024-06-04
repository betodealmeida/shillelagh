"""
Tests for shillelagh.backends.apsw.dialects.base.
"""

from unittest import mock

import pytest
from sqlalchemy import MetaData, Table, create_engine, func, inspect, select

from shillelagh.adapters.registry import AdapterLoader
from shillelagh.backends.apsw.dialects.base import APSWDialect
from shillelagh.exceptions import ProgrammingError

from ....fakes import FakeAdapter


def test_create_engine(registry: AdapterLoader) -> None:
    """
    Test ``create_engine``.
    """
    registry.add("dummy", FakeAdapter)

    engine = create_engine("shillelagh://")
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
    engine = create_engine("shillelagh://")
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
    dialect = APSWDialect()
    assert dialect.do_ping(mock_dbapi_connection) is True


def test_has_table(registry: AdapterLoader) -> None:
    """
    Test ``has_table``.
    """
    registry.add("dummy", FakeAdapter)

    engine = create_engine("shillelagh://")
    inspector = inspect(engine)
    assert inspector.has_table("dummy://a")
    assert inspector.has_table("dummy://b")
    assert not inspector.has_table("funny://b")


def test_import_dbapi() -> None:
    """
    Test ``import_dbapi``.
    """
    assert APSWDialect.import_dbapi() == APSWDialect.dbapi()
