"""
Tests for shillelagh.backends.apsw.dialects.base.
"""
from unittest import mock

import pytest
from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import Table

from ....fakes import FakeAdapter
from ....fakes import FakeEntryPoint
from shillelagh.backends.apsw.dialects.base import APSWDialect
from shillelagh.exceptions import ProgrammingError


def test_create_engine(mocker):
    """
    Test ``create_engine``.
    """
    entry_points = [FakeEntryPoint("dummy", FakeAdapter)]
    mocker.patch(
        "shillelagh.backends.apsw.db.iter_entry_points",
        return_value=entry_points,
    )

    engine = create_engine("shillelagh://")

    table = Table("dummy://", MetaData(bind=engine), autoload=True)
    query = select(
        [func.sum(table.columns.pets)],  # pylint: disable=no-member
        from_obj=table,
    )
    assert query.scalar() == 3


def test_create_engine_no_adapters():
    """
    Test ``create_engine`` with invalid adapter.
    """
    engine = create_engine("shillelagh://")

    with pytest.raises(ProgrammingError) as excinfo:
        Table("dummy://", MetaData(bind=engine), autoload=True)
    assert str(excinfo.value) == "Unsupported table: dummy://"


def test_dialect_ping():
    """
    Test ``do_ping``.
    """
    mock_dbapi_connection = mock.MagicMock()
    dialect = APSWDialect()
    assert dialect.do_ping(mock_dbapi_connection) is True


def test_has_table(mocker):
    """
    Test ``has_table``.
    """
    entry_points = [FakeEntryPoint("dummy", FakeAdapter)]
    mocker.patch(
        "shillelagh.backends.apsw.db.iter_entry_points",
        return_value=entry_points,
    )

    engine = create_engine("shillelagh://")
    assert engine.has_table("dummy://a")
    assert engine.has_table("dummy://b")
    assert not engine.has_table("funny://b")
