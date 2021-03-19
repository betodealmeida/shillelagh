import json
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
from shillelagh.backends.apsw.dialect import APSWDialect
from shillelagh.backends.apsw.dialect import APSWGSheetsDialect
from shillelagh.backends.apsw.dialect import APSWSafeDialect
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
from shillelagh.types import STRING
from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import Table
from sqlalchemy.engine.url import make_url

from ...fakes import FakeAdapter
from ...fakes import FakeEntryPoint


def test_create_engine(mocker):
    entry_points = [FakeEntryPoint("dummy", FakeAdapter)]
    mocker.patch(
        "shillelagh.backends.apsw.db.iter_entry_points",
        return_value=entry_points,
    )

    engine = create_engine("shillelagh://")

    table = Table("dummy://", MetaData(bind=engine), autoload=True)
    query = select([func.sum(table.columns.pets)], from_obj=table)
    assert query.scalar() == 3


def test_create_engine_no_adapters(mocker):
    engine = create_engine("shillelagh://")

    with pytest.raises(ProgrammingError) as excinfo:
        Table("dummy://", MetaData(bind=engine), autoload=True)
    assert str(excinfo.value) == "Unsupported table: dummy://"


def test_dialect_ping():
    mock_dbapi_connection = mock.MagicMock()
    dialect = APSWDialect()
    assert dialect.do_ping(mock_dbapi_connection) is True


def test_gsheets_dialect(fs):
    dialect = APSWGSheetsDialect()
    assert dialect.create_connect_args(make_url("gsheets://")) == (
        (
            ":memory:",
            ["gsheetsapi"],
            {},
            True,
            None,
        ),
        {},
    )

    dialect = APSWGSheetsDialect(
        service_account_info={"secret": "XXX"},
        subject="user@example.com",
    )
    assert dialect.create_connect_args(make_url("gsheets://")) == (
        (
            ":memory:",
            ["gsheetsapi"],
            {"gsheetsapi": ({"secret": "XXX"}, "user@example.com")},
            True,
            None,
        ),
        {},
    )

    with open("credentials.json", "w") as fp:
        json.dump({"secret": "YYY"}, fp)

    dialect = APSWGSheetsDialect(
        service_account_file="credentials.json",
        subject="user@example.com",
    )
    assert dialect.create_connect_args(make_url("gsheets://")) == (
        (
            ":memory:",
            ["gsheetsapi"],
            {"gsheetsapi": ({"secret": "YYY"}, "user@example.com")},
            True,
            None,
        ),
        {},
    )

    mock_dbapi_connection = mock.MagicMock()
    assert dialect.get_schema_names(mock_dbapi_connection) == []


def test_safe_dialect(fs):
    dialect = APSWSafeDialect()
    assert dialect.create_connect_args(make_url("shillelagh+safe://")) == (
        (
            ":memory:",
            None,
            None,
            True,
            None,
        ),
        {},
    )
