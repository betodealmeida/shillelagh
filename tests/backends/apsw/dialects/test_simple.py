"""
Tests for shillelagh.backends.apsw.dialects.simple.
"""

import os
from pathlib import Path

import apsw
import pytest
from pytest_mock import MockerFixture
from sqlalchemy.engine import create_engine
from sqlalchemy.engine.url import make_url

from shillelagh.backends.apsw.dialects.simple import APSWSimpleDialect


def test_simple_dialect(mocker: MockerFixture) -> None:
    """
    Test the ``simple://`` parameters.
    """
    tempfile = mocker.patch("shillelagh.backends.apsw.dialects.simple.tempfile")
    tempfile.mkdtemp.return_value = "/path/to/file"

    dialect = APSWSimpleDialect()
    assert dialect.create_connect_args(make_url("simple://")) == (
        (),
        {
            "path": "/path/to/file/simple.db",
            "adapters": None,
            "adapter_kwargs": {},
            "safe": True,
            "isolation_level": None,
            "apsw_connection_kwargs": {"vfs": "simple"},
        },
    )
    assert dialect.vfs.quota == 1000


def test_simple_dialect_environ(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Test the ``simple://`` parameters reading from the environment.
    """
    monkeypatch.setenv("SIMPLE_DB_PATH", "/path/to/file/simple.db")
    monkeypatch.setenv("SIMPLE_DB_QUOTA_MB", "1000000")

    dialect = APSWSimpleDialect()
    assert dialect.create_connect_args(make_url("simple://")) == (
        (),
        {
            "path": "/path/to/file/simple.db",
            "adapters": None,
            "adapter_kwargs": {},
            "safe": True,
            "isolation_level": None,
            "apsw_connection_kwargs": {"vfs": "simple"},
        },
    )
    assert dialect.vfs.quota == 1000000


def test_simple_create_engine(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Test the ``simple://`` engine.
    """
    path = Path("simple.db")
    if path.exists():
        os.unlink(path)

    monkeypatch.setenv("SIMPLE_DB_PATH", path)
    monkeypatch.setenv("SIMPLE_DB_QUOTA_MB", "1")

    engine = create_engine("simple://")
    connect = engine.connect()
    connect.execute("CREATE TABLE t (A TEXT)")
    connect.execute("INSERT INTO t (A) VALUES (?)", "test")
    with pytest.raises(
        apsw.FullError,  # pylint: disable=c-extension-no-member
    ) as excinfo:
        connect.execute("INSERT INTO t (A) VALUES (?)", "a" * 1000000)
    assert str(excinfo.value) == "FullError: database or disk is full"

    os.unlink(path)
