"""
Tests for shillelagh.backends.apsw.dialects.base.
"""
from unittest import mock

import pytest
from sqlalchemy import MetaData, Table, create_engine, func, select

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

    table = Table("dummy://", MetaData(bind=engine), autoload=True)
    query = select(
        [func.sum(table.columns.pets)],  # pylint: disable=no-member
        from_obj=table,
    )
    assert query.scalar() == 3


def test_create_engine_no_adapters(registry: AdapterLoader) -> None:
    """
    Test ``create_engine`` with invalid adapter.
    """
    registry.clear()
    engine = create_engine("shillelagh://")

    with pytest.raises(ProgrammingError) as excinfo:
        Table("dummy://", MetaData(bind=engine), autoload=True)
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
    assert engine.has_table("dummy://a")
    assert engine.has_table("dummy://b")
    assert not engine.has_table("funny://b")
