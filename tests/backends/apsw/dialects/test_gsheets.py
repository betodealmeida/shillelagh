import json
from unittest import mock

from shillelagh.backends.apsw.dialects.gsheets import APSWGSheetsDialect
from sqlalchemy.engine.url import make_url


def test_gsheets_dialect(fs):
    dialect = APSWGSheetsDialect()
    assert dialect.create_connect_args(make_url("gsheets://")) == (
        (
            ":memory:",
            ["gsheetsapi"],
            {"gsheetsapi": (None, None, None, None)},
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
            {"gsheetsapi": (None, None, {"secret": "XXX"}, "user@example.com")},
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
            {"gsheetsapi": (None, "credentials.json", None, "user@example.com")},
            True,
            None,
        ),
        {},
    )

    mock_dbapi_connection = mock.MagicMock()
    assert dialect.get_schema_names(mock_dbapi_connection) == []
