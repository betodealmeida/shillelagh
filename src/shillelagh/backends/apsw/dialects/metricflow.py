# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# pylint: disable=unused-argument, abstract-method

"""
A SQLAlchemy dialect for dbt Metric Flow.
"""

from __future__ import annotations

from typing import Any, cast

import sqlalchemy.types
from sqlalchemy.engine.base import Connection
from sqlalchemy.engine.url import URL
from sqlalchemy.sql.type_api import TypeEngine

from shillelagh.adapters.api.dbt_metricflow import DbtMetricFlowAPI
from shillelagh.adapters.registry import registry
from shillelagh.backends.apsw.dialects.base import (
    APSWDialect,
    SQLAlchemyColumn,
    get_adapter_for_table_name,
)
from shillelagh.fields import Field

TABLE_NAME = "metrics"


def get_sqla_type(field: Field) -> type[TypeEngine]:
    """
    Convert from Shillelagh to SQLAlchemy types.
    """
    type_map: dict[str, type[TypeEngine]] = {
        "BOOLEAN": sqlalchemy.types.BOOLEAN,
        "INTEGER": sqlalchemy.types.INT,
        "DECIMAL": sqlalchemy.types.DECIMAL,
        "TIMESTAMP": sqlalchemy.types.TIMESTAMP,
        "DATE": sqlalchemy.types.DATE,
        "TIME": sqlalchemy.types.TIME,
        "TEXT": sqlalchemy.types.TEXT,
    }

    return type_map.get(field.type, sqlalchemy.types.TEXT)


class TableMetricFlowAPI(DbtMetricFlowAPI):
    """
    Custom API adapter for dbt Metric Flow API.

    In the original adapter, the SQL queries a base dbt API URL, eg:

        SELECT * FROM "https://semantic-layer.cloud.getdbt.com/";
        SELECT * FROM "https://ab123.us1.dbt.com/";  -- custom user URL

    For this adapter, we want a leaner URI, mimicking a table:

        SELECT * FROM metrics;

    In order to do this, we override the ``supports`` method to only accept
    ``$TABLE_NAME`` instead of the URL, which is then passed to the adapter when it is
    instantiated.
    """

    @staticmethod
    def supports(uri: str, fast: bool = True, **kwargs: Any) -> bool:
        return uri == TABLE_NAME

    def __init__(
        self,
        table: str,
        service_token: str,
        environment_id: int,
        url: str,
    ) -> None:
        # pass URL as the table name to the base class, since that's the actual URL
        super().__init__(url, service_token, environment_id)

        # but make sure we set the table name to the expected one, since `get_data`
        # needs it
        self.table = table


class MetricFlowDialect(APSWDialect):
    """
    A dbt Metric Flow dialect.

    URL should look like:

        metricflow:///<environment_id>?service_token=<service_token>

    Or when using a custom URL:

        metricflow://ab123.us1.dbt.com/<environment_id>?service_token=<service_token>

    """

    name = "metricflow"

    supports_statement_cache = True

    def __init__(
        self,
        service_token: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        self.service_token = service_token

    def create_connect_args(self, url: URL) -> tuple[tuple[()], dict[str, Any]]:
        baseurl = (
            f"https://{url.host}/"
            if url.host
            else "https://semantic-layer.cloud.getdbt.com/"
        )

        if "tablemetricflowapi" not in registry.loaders:
            registry.add("tablemetricflowapi", TableMetricFlowAPI)

        return (
            (),
            {
                "path": ":memory:",
                "adapters": ["tablemetricflowapi"],
                "adapter_kwargs": {
                    "tablemetricflowapi": {
                        "service_token": url.query.get(
                            "service_token",
                            self.service_token,
                        ),
                        "environment_id": int(url.database),
                        "url": baseurl,
                    },
                },
                "safe": True,
                "isolation_level": self.isolation_level,
            },
        )

    def get_table_names(
        self,
        connection: Connection,
        schema: str | None = None,
        sqlite_include_internal: bool = False,
        **kwargs: Any,
    ) -> list[str]:
        return [TABLE_NAME]

    def has_table(
        self,
        connection: Connection,
        table_name: str,
        schema: str | None = None,
        **kwargs: Any,
    ) -> bool:
        return table_name == TABLE_NAME

    def get_columns(
        self,
        connection: Connection,
        table_name: str,
        schema: str | None = None,
        **kwargs: Any,
    ) -> list[SQLAlchemyColumn]:
        columns: list[SQLAlchemyColumn] = []
        adapter = cast(
            DbtMetricFlowAPI,
            get_adapter_for_table_name(connection, table_name),
        )

        dimensions = {
            (
                adapter.grains[dimension][0]
                if dimension in adapter.grains
                else dimension
            ): adapter.columns[dimension]
            for dimension in adapter.dimensions
        }
        for name, field in dimensions.items():
            columns.append(
                {
                    "name": name,
                    "type": get_sqla_type(field),
                    "nullable": True,
                    "default": None,
                    "comment": adapter.dimensions.get(name, ""),
                },
            )

        metrics = {metric: adapter.columns[metric] for metric in adapter.metrics}
        for name, field in metrics.items():
            columns.append(
                {
                    "name": name,
                    "type": get_sqla_type(field),
                    "nullable": True,
                    "default": None,
                    "comment": adapter.metrics.get(name, ""),
                    "computed": {"sqltext": name, "persisted": True},
                },
            )

        return columns

    def get_schema_names(
        self,
        connection: Connection,
        **kwargs: Any,
    ) -> list[str]:
        return ["main"]

    def get_pk_constraint(
        self,
        connection: Connection,
        table_name: str,
        schema: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        return {"constrained_columns": [], "name": None}

    def get_foreign_keys(
        self,
        connection: Connection,
        table_name: str,
        schema: str | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        return []

    get_check_constraints = get_foreign_keys
    get_indexes = get_foreign_keys
    get_unique_constraints = get_foreign_keys

    def get_table_comment(self, connection, table_name, schema=None, **kwargs):
        return {
            "text": "A virtual table that gives access to all dbt metrics & dimensions.",
        }
