"""
Tests for shillelagh.backends.apsw.dialects.semantic_api.
"""

# pylint: disable=protected-access

from typing import Any
from unittest import mock
from unittest.mock import MagicMock

import pytest
import sqlalchemy
from requests_mock.mocker import Mocker
from sqlalchemy.engine.url import make_url

from shillelagh.backends.apsw.dialects.semantic_api import (
    SemanticAPIDialect,
    TableSemanticAPI,
    adapter_class,
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

VIEWS_LIST_PAYLOAD: list[dict[str, Any]] = [
    {"name": "sales", "uid": "pandas.sales", "features": []},
    {"name": "marketing", "uid": "pandas.marketing", "features": []},
]


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


def test_adapter_class_isolates_per_server() -> None:
    """
    Different ``(base_url, configuration)`` pairs produce distinct classes,
    each with its own ``table_names`` set.
    """
    name_a, cls_a = adapter_class("http://a", {})
    name_b, cls_b = adapter_class("http://b", {})
    assert name_a != name_b
    assert cls_a is not cls_b

    cls_a.table_names = {"sales"}
    cls_b.table_names = {"orders"}
    assert cls_a.table_names == {"sales"}
    assert cls_b.table_names == {"orders"}


def test_adapter_class_memoised() -> None:
    """
    Calling ``adapter_class`` twice with the same input reuses the class.
    """
    name_1, cls_1 = adapter_class("http://h", {"workspace": "acme"})
    name_2, cls_2 = adapter_class("http://h", {"workspace": "acme"})
    assert name_1 == name_2
    assert cls_1 is cls_2


def test_adapter_class_configuration_segments() -> None:
    """
    Same server but different config still produces distinct classes.
    """
    _, cls_a = adapter_class("http://h", {"workspace": "acme"})
    _, cls_b = adapter_class("http://h", {"workspace": "beta"})
    assert cls_a is not cls_b


def test_table_semantic_api_supports() -> None:
    """
    Recognise only the names tracked on the subclass.
    """
    _, cls = adapter_class("http://supports", {})
    cls.table_names = {"sales", "marketing"}
    assert cls.supports("sales") is True
    assert cls.supports("orders") is False


def test_table_semantic_api_init() -> None:
    """
    Build ``view_url`` from the class-level ``server_url`` + table name.
    """
    _, cls = adapter_class("http://h:8000/", {"workspace": "acme"})

    with mock.patch(
        "shillelagh.backends.apsw.dialects.semantic_api.SemanticAPI.__init__",
    ) as base_init:
        base_init.return_value = None
        cls(table="sales", request_timeout=5.0)
        base_init.assert_called_once_with(
            "http://h:8000/views/sales",
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


def test_create_connect_args_http(requests_mock: Mocker) -> None:
    """
    A bare URL produces an ``http`` base URL and discovers views eagerly.
    """
    requests_mock.post(
        "http://localhost:8000/views/list",
        json=VIEWS_LIST_PAYLOAD,
    )
    dialect = SemanticAPIDialect()
    url = make_url("semanticapi://localhost:8000/")

    with mock.patch(
        "shillelagh.backends.apsw.dialects.semantic_api.registry",
    ) as mock_registry:
        mock_registry.loaders = {}

        args, kwargs = dialect.create_connect_args(url)

        assert args == ()
        assert kwargs["path"] == ":memory:"
        assert kwargs["safe"] is True
        assert kwargs["isolation_level"] is None
        assert kwargs["adapters"] == [dialect._adapter_name]
        assert kwargs["adapter_kwargs"] == {dialect._adapter_name: {}}
        mock_registry.add.assert_called_once_with(
            dialect._adapter_name,
            dialect._adapter_cls,
        )
    assert dialect._adapter_cls is not None
    assert dialect._adapter_cls.table_names == {"sales", "marketing"}
    assert dialect._adapter_cls.server_url == "http://localhost:8000"


def test_create_connect_args_https_and_config(requests_mock: Mocker) -> None:
    """
    ``secure=true`` switches the scheme; ``additional_configuration`` is parsed.
    """
    matcher = requests_mock.post(
        "https://prod.example.com/views/list",
        json=VIEWS_LIST_PAYLOAD,
    )
    dialect = SemanticAPIDialect()
    url = make_url(
        "semanticapi://prod.example.com/"
        "?secure=true&additional_configuration=%7B%22workspace%22%3A%22acme%22%7D",
    )

    with mock.patch(
        "shillelagh.backends.apsw.dialects.semantic_api.registry",
    ) as mock_registry:
        mock_registry.loaders = {}
        dialect.create_connect_args(url)

    assert dialect._adapter_cls is not None
    assert dialect._adapter_cls.server_url == "https://prod.example.com"
    assert dialect._adapter_cls.server_configuration == {"workspace": "acme"}
    assert matcher.last_request.json() == {
        "runtime_configuration": {"workspace": "acme"},
    }


def test_create_connect_args_no_port(requests_mock: Mocker) -> None:
    """
    Hosts without an explicit port still produce a valid base URL.
    """
    requests_mock.post("http://hostonly/views/list", json=VIEWS_LIST_PAYLOAD)
    dialect = SemanticAPIDialect()
    url = make_url("semanticapi://hostonly/")

    with mock.patch(
        "shillelagh.backends.apsw.dialects.semantic_api.registry",
    ) as mock_registry:
        mock_registry.loaders = {}
        dialect.create_connect_args(url)

    assert dialect._adapter_cls is not None
    assert dialect._adapter_cls.server_url == "http://hostonly"


def test_create_connect_args_adapter_already_registered(
    requests_mock: Mocker,
) -> None:
    """
    Re-using an already-registered adapter does not double-register.
    """
    requests_mock.post("http://reused/views/list", json=VIEWS_LIST_PAYLOAD)
    dialect = SemanticAPIDialect()
    url = make_url("semanticapi://reused/")

    with mock.patch(
        "shillelagh.backends.apsw.dialects.semantic_api.registry",
    ) as mock_registry:
        mock_registry.loaders = {dialect._adapter_name: "anything"}

        with mock.patch.object(
            SemanticAPIDialect,
            "_list_views",
            return_value=["sales"],
        ):
            # _adapter_name is "" until create_connect_args runs, so prime it.
            mock_registry.loaders = set()  # type: ignore[assignment]
            dialect.create_connect_args(url)

        # the adapter name is known after create_connect_args, register it...
        mock_registry.loaders = {dialect._adapter_name: "anything"}
        dialect.create_connect_args(url)
        # ...and assert the second call did NOT re-add.
        assert mock_registry.add.call_count == 1


def test_get_table_names_calls_server(requests_mock: Mocker) -> None:
    """
    ``get_table_names`` re-queries the server and updates the cached set.
    """
    requests_mock.post("http://host/views/list", json=VIEWS_LIST_PAYLOAD)
    dialect = SemanticAPIDialect()
    dialect._base_url = "http://host"
    dialect._configuration = {}
    _, dialect._adapter_cls = adapter_class("http://host", {})
    dialect._adapter_cls.table_names = set()

    assert dialect.get_table_names(MagicMock()) == ["marketing", "sales"]
    assert dialect._adapter_cls.table_names == {"sales", "marketing"}


def test_get_table_names_without_adapter() -> None:
    """
    Before ``create_connect_args`` the dialect has no adapter class, but the
    raw view list is still returned.
    """
    dialect = SemanticAPIDialect()
    dialect._base_url = "http://host"
    dialect._configuration = {}

    with mock.patch.object(
        SemanticAPIDialect,
        "_list_views",
        return_value=["a"],
    ):
        assert dialect.get_table_names(MagicMock()) == ["a"]


def test_get_table_names_http_error(requests_mock: Mocker) -> None:
    """
    HTTP errors surface from ``_list_views``.
    """
    requests_mock.post("http://host/views/list", status_code=500)
    dialect = SemanticAPIDialect()
    dialect._base_url = "http://host"
    dialect._configuration = {}

    with pytest.raises(Exception, match="500"):
        dialect.get_table_names(MagicMock())


def test_has_table() -> None:
    """
    ``has_table`` checks the per-dialect adapter class.
    """
    dialect = SemanticAPIDialect()
    _, dialect._adapter_cls = adapter_class("http://has", {})
    dialect._adapter_cls.table_names = {"sales"}

    assert dialect.has_table(MagicMock(), "sales") is True
    assert dialect.has_table(MagicMock(), "orders") is False


def test_has_table_without_adapter() -> None:
    """
    Without a configured adapter class, ``has_table`` is always False.
    """
    dialect = SemanticAPIDialect()
    assert dialect.has_table(MagicMock(), "sales") is False


def test_get_columns() -> None:
    """
    Build a SQLAlchemy column descriptor for every dimension and metric.
    """
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


def test_table_semantic_api_base_class_supports() -> None:
    """
    The base class itself has an empty ``table_names``.
    """
    assert TableSemanticAPI.supports("anything") is False
