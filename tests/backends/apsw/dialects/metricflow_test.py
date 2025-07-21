"""
Tests for shillelagh.backends.apsw.dialects.metricflow.
"""

from unittest import mock
from unittest.mock import MagicMock

import sqlalchemy
from sqlalchemy.engine.url import make_url

from shillelagh.backends.apsw.dialects.metricflow import (
    MetricFlowDialect,
    TableMetricFlowAPI,
    get_sqla_type,
)
from shillelagh.fields import Boolean, DateTime, Decimal, Integer, String


def test_get_sqla_type() -> None:
    """
    Test ``get_sqla_type`` function.
    """
    # Test all supported field types
    field_boolean = Boolean()
    assert get_sqla_type(field_boolean) == sqlalchemy.types.BOOLEAN

    field_integer = Integer()
    assert get_sqla_type(field_integer) == sqlalchemy.types.INT

    field_decimal = Decimal()
    assert get_sqla_type(field_decimal) == sqlalchemy.types.DECIMAL

    field_timestamp = DateTime()
    assert get_sqla_type(field_timestamp) == sqlalchemy.types.TIMESTAMP

    # Create a field with DATE type by mocking
    field_date = Integer()  # Use as base
    field_date.type = "DATE"
    assert get_sqla_type(field_date) == sqlalchemy.types.DATE

    # Create a field with TIME type by mocking
    field_time = Integer()  # Use as base
    field_time.type = "TIME"
    assert get_sqla_type(field_time) == sqlalchemy.types.TIME

    field_text = String()
    assert get_sqla_type(field_text) == sqlalchemy.types.TEXT

    # Test unknown type defaults to TEXT
    field_unknown = Integer()  # Use as base
    field_unknown.type = "UNKNOWN"
    assert get_sqla_type(field_unknown) == sqlalchemy.types.TEXT


def test_table_metricflow_api_supports() -> None:
    """
    Test ``TableMetricFlowAPI.supports`` method.
    """
    # Should support only "metrics" table
    assert TableMetricFlowAPI.supports("metrics") is True
    assert TableMetricFlowAPI.supports("other_table") is False
    assert TableMetricFlowAPI.supports("https://example.com") is False


def test_table_metricflow_api_init() -> None:
    """
    Test ``TableMetricFlowAPI`` initialization.
    """
    with mock.patch(
        "shillelagh.backends.apsw.dialects.metricflow.DbtMetricFlowAPI.__init__",
    ) as mock_super_init:
        mock_super_init.return_value = None

        api = TableMetricFlowAPI(
            table="metrics",
            service_token="test_token",
            environment_id=123,
            url="https://example.com/",
        )

        # Should call super().__init__ with URL as the table name
        mock_super_init.assert_called_once_with(
            "https://example.com/",
            "test_token",
            123,
        )

        # Should set table attribute correctly
        assert api.table == "metrics"


def test_metricflow_dialect_name() -> None:
    """
    Test ``MetricFlowDialect`` name attribute.
    """
    dialect = MetricFlowDialect()
    assert dialect.name == "metricflow"


def test_metricflow_dialect_supports_statement_cache() -> None:
    """
    Test ``MetricFlowDialect`` supports_statement_cache attribute.
    """
    dialect = MetricFlowDialect()
    assert dialect.supports_statement_cache is True


def test_metricflow_dialect_create_connect_args_default_host() -> None:
    """
    Test ``MetricFlowDialect.create_connect_args`` with default host.
    """
    dialect = MetricFlowDialect()
    url = make_url("metricflow:///123?service_token=test_token")

    with mock.patch(
        "shillelagh.backends.apsw.dialects.metricflow.registry",
    ) as mock_registry:
        mock_registry.loaders = {}

        args, kwargs = dialect.create_connect_args(url)

        assert args == ()
        assert kwargs == {
            "path": ":memory:",
            "adapters": ["tablemetricflowapi"],
            "adapter_kwargs": {
                "tablemetricflowapi": {
                    "service_token": "test_token",
                    "environment_id": 123,
                    "url": "https://semantic-layer.cloud.getdbt.com/",
                },
            },
            "safe": True,
            "isolation_level": None,
        }

        # Should register the adapter
        mock_registry.add.assert_called_once_with(
            "tablemetricflowapi",
            TableMetricFlowAPI,
        )


def test_metricflow_dialect_create_connect_args_custom_host() -> None:
    """
    Test ``MetricFlowDialect.create_connect_args`` with custom host.
    """
    dialect = MetricFlowDialect()
    url = make_url("metricflow://custom.example.com/123?service_token=test_token")

    with mock.patch(
        "shillelagh.backends.apsw.dialects.metricflow.registry",
    ) as mock_registry:
        mock_registry.loaders = {}

        args, kwargs = dialect.create_connect_args(url)

        assert args == ()
        assert kwargs == {
            "path": ":memory:",
            "adapters": ["tablemetricflowapi"],
            "adapter_kwargs": {
                "tablemetricflowapi": {
                    "service_token": "test_token",
                    "environment_id": 123,
                    "url": "https://custom.example.com/",
                },
            },
            "safe": True,
            "isolation_level": None,
        }


def test_metricflow_dialect_create_connect_args_adapter_already_registered() -> None:
    """
    Test ``MetricFlowDialect.create_connect_args`` when adapter is already registered.
    """
    dialect = MetricFlowDialect()
    url = make_url("metricflow:///123?service_token=test_token")

    with mock.patch(
        "shillelagh.backends.apsw.dialects.metricflow.registry",
    ) as mock_registry:
        mock_registry.loaders = {"tablemetricflowapi": "already_registered"}

        dialect.create_connect_args(url)

        # Should not call add when already registered
        mock_registry.add.assert_not_called()


def test_metricflow_dialect_get_table_names() -> None:
    """
    Test ``MetricFlowDialect.get_table_names`` method.
    """
    dialect = MetricFlowDialect()
    mock_connection = MagicMock()

    result = dialect.get_table_names(mock_connection)
    assert result == ["metrics"]

    # Test with different parameters
    result = dialect.get_table_names(mock_connection, schema="test_schema")
    assert result == ["metrics"]

    result = dialect.get_table_names(mock_connection, sqlite_include_internal=True)
    assert result == ["metrics"]


def test_metricflow_dialect_has_table() -> None:
    """
    Test ``MetricFlowDialect.has_table`` method.
    """
    dialect = MetricFlowDialect()
    mock_connection = MagicMock()

    assert dialect.has_table(mock_connection, "metrics") is True
    assert dialect.has_table(mock_connection, "other_table") is False

    # Test with schema parameter
    assert dialect.has_table(mock_connection, "metrics", schema="test_schema") is True
    assert (
        dialect.has_table(mock_connection, "other_table", schema="test_schema") is False
    )


def test_metricflow_dialect_get_columns() -> None:
    """
    Test ``MetricFlowDialect.get_columns`` method.
    """
    dialect = MetricFlowDialect()
    mock_connection = MagicMock()

    # Create mock adapter
    mock_adapter = MagicMock()
    mock_adapter.dimensions = {"dim1": "Dimension 1", "dim2": "Dimension 2"}
    mock_adapter.grains = {"dim1": ["grain1"]}
    mock_adapter.metrics = {"metric1": "Metric 1", "metric2": "Metric 2"}
    mock_adapter.columns = {
        "dim1": String(),
        "dim2": Integer(),
        "metric1": Decimal(),
        "metric2": DateTime(),
    }

    with mock.patch(
        "shillelagh.backends.apsw.dialects.metricflow.get_adapter_for_table_name",
    ) as mock_get_adapter:
        mock_get_adapter.return_value = mock_adapter

        result = dialect.get_columns(mock_connection, "metrics")

        expected = [
            {
                "name": "grain1",
                "type": sqlalchemy.types.TEXT,
                "nullable": True,
                "default": None,
                "comment": "",
            },
            {
                "name": "dim2",
                "type": sqlalchemy.types.INT,
                "nullable": True,
                "default": None,
                "comment": "Dimension 2",
            },
            {
                "name": "metric1",
                "type": sqlalchemy.types.DECIMAL,
                "nullable": True,
                "default": None,
                "comment": "Metric 1",
                "computed": {"sqltext": "metric1", "persisted": True},
            },
            {
                "name": "metric2",
                "type": sqlalchemy.types.TIMESTAMP,
                "nullable": True,
                "default": None,
                "comment": "Metric 2",
                "computed": {"sqltext": "metric2", "persisted": True},
            },
        ]

        assert result == expected


def test_metricflow_dialect_get_schema_names() -> None:
    """
    Test ``MetricFlowDialect.get_schema_names`` method.
    """
    dialect = MetricFlowDialect()
    mock_connection = MagicMock()

    result = dialect.get_schema_names(mock_connection)
    assert result == ["main"]


def test_metricflow_dialect_get_pk_constraint() -> None:
    """
    Test ``MetricFlowDialect.get_pk_constraint`` method.
    """
    dialect = MetricFlowDialect()
    mock_connection = MagicMock()

    result = dialect.get_pk_constraint(mock_connection, "metrics")
    assert result == {"constrained_columns": [], "name": None}

    # Test with schema parameter
    result = dialect.get_pk_constraint(mock_connection, "metrics", schema="test_schema")
    assert result == {"constrained_columns": [], "name": None}


def test_metricflow_dialect_get_foreign_keys() -> None:
    """
    Test ``MetricFlowDialect.get_foreign_keys`` method.
    """
    dialect = MetricFlowDialect()
    mock_connection = MagicMock()

    result = dialect.get_foreign_keys(mock_connection, "metrics")
    assert result == []

    # Test with schema parameter
    result = dialect.get_foreign_keys(mock_connection, "metrics", schema="test_schema")
    assert result == []


def test_metricflow_dialect_constraint_methods() -> None:
    """
    Test that constraint methods are properly aliased.
    """
    dialect = MetricFlowDialect()

    # These methods should be aliases to get_foreign_keys
    assert dialect.get_check_constraints == dialect.get_foreign_keys
    assert dialect.get_indexes == dialect.get_foreign_keys
    assert dialect.get_unique_constraints == dialect.get_foreign_keys


def test_metricflow_dialect_get_table_comment() -> None:
    """
    Test ``MetricFlowDialect.get_table_comment`` method.
    """
    dialect = MetricFlowDialect()
    mock_connection = MagicMock()

    result = dialect.get_table_comment(mock_connection, "metrics")
    expected = {
        "text": "A virtual table that gives access to all dbt metrics & dimensions.",
    }
    assert result == expected

    # Test with schema parameter
    result = dialect.get_table_comment(mock_connection, "metrics", schema="test_schema")
    assert result == expected
