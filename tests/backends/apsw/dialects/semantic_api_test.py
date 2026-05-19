"""
Tests for shillelagh.backends.apsw.dialects.semantic_api.
"""

# pylint: disable=protected-access

from unittest import mock
from unittest.mock import MagicMock

import pytest
import sqlalchemy
from sqlalchemy.engine.url import make_url

from shillelagh.backends.apsw.dialects.semantic_api import (
    SemanticAPIDialect,
    TableSemanticAPI,
    get_sqla_type,
)
from shillelagh.fields import (
    Boolean,
    Date,
    DateTime,
    Decimal,
    Float,
    Integer,
    String,
    Time,
)


def test_get_sqla_type() -> None:
    """
    Map every field type to its SQLAlchemy counterpart.
    """
    assert get_sqla_type(Boolean()) is sqlalchemy.types.BOOLEAN
    assert get_sqla_type(Integer()) is sqlalchemy.types.INT
    assert get_sqla_type(Float()) is sqlalchemy.types.FLOAT
    assert get_sqla_type(Decimal()) is sqlalchemy.types.DECIMAL
    assert get_sqla_type(DateTime()) is sqlalchemy.types.TIMESTAMP
    assert get_sqla_type(Date()) is sqlalchemy.types.DATE
    assert get_sqla_type(Time()) is sqlalchemy.types.TIME
    assert get_sqla_type(String()) is sqlalchemy.types.TEXT

    unknown = Integer()
    unknown.type = "UNKNOWN"
    assert get_sqla_type(unknown) is sqlalchemy.types.TEXT


def test_table_semantic_api_supports() -> None:
    """
    Recognise only the configured table name.
    """
    TableSemanticAPI.table_name = "sales"
    assert TableSemanticAPI.supports("sales") is True
    assert TableSemanticAPI.supports("orders") is False


def test_table_semantic_api_init() -> None:
    """
    Forward construction arguments to the base ``SemanticAPI``.
    """
    with mock.patch(
        "shillelagh.backends.apsw.dialects.semantic_api.SemanticAPI.__init__",
    ) as base_init:
        base_init.return_value = None
        TableSemanticAPI(
            table="sales",
            view_url="http://h/views/sales",
            additional_configuration={"workspace": "acme"},
            request_timeout=5.0,
        )
        base_init.assert_called_once_with(
            "http://h/views/sales",
            "sales",
            {"workspace": "acme"},
            5.0,
        )


def test_dialect_name_and_cache() -> None:
    """
    Expose the right dialect name and enable statement caching.
    """
    dialect = SemanticAPIDialect()
    assert dialect.name == "semanticapi"
    assert dialect.supports_statement_cache is True


def test_create_connect_args_http() -> None:
    """
    A bare URL produces an ``http`` view URL.
    """
    dialect = SemanticAPIDialect()
    url = make_url("semanticapi://localhost:8000/sales")

    with mock.patch(
        "shillelagh.backends.apsw.dialects.semantic_api.registry",
    ) as mock_registry:
        mock_registry.loaders = {}

        args, kwargs = dialect.create_connect_args(url)

        assert args == ()
        assert kwargs == {
            "path": ":memory:",
            "adapters": ["tablesemanticapi"],
            "adapter_kwargs": {
                "tablesemanticapi": {
                    "view_url": "http://localhost:8000/views/sales",
                },
            },
            "safe": True,
            "isolation_level": None,
        }
        mock_registry.add.assert_called_once_with(
            "tablesemanticapi",
            TableSemanticAPI,
        )
        assert TableSemanticAPI.table_name == "sales"


def test_create_connect_args_https_and_config() -> None:
    """
    ``secure=true`` switches the scheme; ``additional_configuration`` is parsed.
    """
    dialect = SemanticAPIDialect()
    url = make_url(
        "semanticapi://prod.example.com/orders"
        "?secure=true&additional_configuration=%7B%22workspace%22%3A%22acme%22%7D",
    )

    with mock.patch(
        "shillelagh.backends.apsw.dialects.semantic_api.registry",
    ) as mock_registry:
        mock_registry.loaders = {}

        _, kwargs = dialect.create_connect_args(url)

        assert kwargs["adapter_kwargs"]["tablesemanticapi"] == {
            "view_url": "https://prod.example.com/views/orders",
            "additional_configuration": {"workspace": "acme"},
        }


def test_create_connect_args_no_port() -> None:
    """
    Hosts without an explicit port still produce a valid view URL.
    """
    dialect = SemanticAPIDialect()
    url = make_url("semanticapi://host/sales")

    with mock.patch(
        "shillelagh.backends.apsw.dialects.semantic_api.registry",
    ) as mock_registry:
        mock_registry.loaders = {}

        _, kwargs = dialect.create_connect_args(url)

        assert (
            kwargs["adapter_kwargs"]["tablesemanticapi"]["view_url"]
            == "http://host/views/sales"
        )


def test_create_connect_args_missing_view_name() -> None:
    """
    A URL without a view name raises ``ValueError``.
    """
    dialect = SemanticAPIDialect()
    url = make_url("semanticapi://host/")

    with mock.patch(
        "shillelagh.backends.apsw.dialects.semantic_api.registry",
    ) as mock_registry:
        mock_registry.loaders = {}

        with pytest.raises(ValueError, match="view name"):
            dialect.create_connect_args(url)


def test_create_connect_args_adapter_already_registered() -> None:
    """
    Re-using an already-registered adapter does not double-register.
    """
    dialect = SemanticAPIDialect()
    url = make_url("semanticapi://host/sales")

    with mock.patch(
        "shillelagh.backends.apsw.dialects.semantic_api.registry",
    ) as mock_registry:
        mock_registry.loaders = {"tablesemanticapi": "already_registered"}

        dialect.create_connect_args(url)
        mock_registry.add.assert_not_called()


def test_get_table_names() -> None:
    """
    Report the configured view name as the only table.
    """
    TableSemanticAPI.table_name = "sales"
    dialect = SemanticAPIDialect()
    assert dialect.get_table_names(MagicMock()) == ["sales"]


def test_has_table() -> None:
    """
    Recognise only the configured view name.
    """
    TableSemanticAPI.table_name = "sales"
    dialect = SemanticAPIDialect()
    assert dialect.has_table(MagicMock(), "sales") is True
    assert dialect.has_table(MagicMock(), "orders") is False


def test_get_columns() -> None:
    """
    Build a SQLAlchemy column descriptor for every dimension and metric.
    """
    TableSemanticAPI.table_name = "sales"
    dialect = SemanticAPIDialect()

    adapter = MagicMock()
    adapter.metric_ids = {"total_revenue": "sales.total_revenue"}
    adapter.dimension_ids = {"region": "sales.region"}
    adapter.get_columns.return_value = {
        "region": String(),
        "total_revenue": Float(),
    }

    with mock.patch(
        "shillelagh.backends.apsw.dialects.semantic_api.get_adapter_for_table_name",
        return_value=adapter,
    ):
        result = dialect.get_columns(MagicMock(), "sales")

    assert result == [
        {
            "name": "region",
            "type": sqlalchemy.types.TEXT,
            "nullable": True,
            "default": None,
            "comment": "dimension",
        },
        {
            "name": "total_revenue",
            "type": sqlalchemy.types.FLOAT,
            "nullable": True,
            "default": None,
            "comment": "metric",
            "computed": {"sqltext": "total_revenue", "persisted": True},
        },
    ]


def test_get_schema_names() -> None:
    """
    Report a single ``main`` schema.
    """
    assert SemanticAPIDialect().get_schema_names(MagicMock()) == ["main"]


def test_get_pk_constraint_and_foreign_keys() -> None:
    """
    Report no constraints, foreign keys, or aliased helpers.
    """
    dialect = SemanticAPIDialect()
    assert dialect.get_pk_constraint(MagicMock(), "sales") == {
        "constrained_columns": [],
        "name": None,
    }
    assert dialect.get_foreign_keys(MagicMock(), "sales") == []
    assert dialect.get_check_constraints == dialect.get_foreign_keys
    assert dialect.get_indexes == dialect.get_foreign_keys
    assert dialect.get_unique_constraints == dialect.get_foreign_keys


def test_get_table_comment() -> None:
    """
    Provide a static description of the virtual table.
    """
    dialect = SemanticAPIDialect()
    assert dialect.get_table_comment(MagicMock(), "sales") == {
        "text": "A semantic view exposed as a virtual table.",
    }
