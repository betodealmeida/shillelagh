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
A SQLAlchemy dialect for the Semantic Layer REST API.
"""

from __future__ import annotations

import json
from typing import Any, ClassVar, cast

import sqlalchemy.types
from sqlalchemy.engine.base import Connection
from sqlalchemy.engine.url import URL
from sqlalchemy.sql.type_api import TypeEngine

from shillelagh.adapters.api.semantic_api import SemanticAPI
from shillelagh.adapters.registry import registry
from shillelagh.backends.apsw.dialects.base import (
    APSWDialect,
    SQLAlchemyColumn,
    get_adapter_for_table_name,
)
from shillelagh.fields import Field

_TRUTHY = {"1", "true", "yes", "on"}


def get_sqla_type(field: Field) -> type[TypeEngine]:
    """
    Convert from Shillelagh to SQLAlchemy types.
    """
    type_map: dict[str, type[TypeEngine]] = {
        "BOOLEAN": sqlalchemy.types.BOOLEAN,
        "INTEGER": sqlalchemy.types.INT,
        "REAL": sqlalchemy.types.FLOAT,
        "DECIMAL": sqlalchemy.types.DECIMAL,
        "TIMESTAMP": sqlalchemy.types.TIMESTAMP,
        "DATE": sqlalchemy.types.DATE,
        "TIME": sqlalchemy.types.TIME,
        "TEXT": sqlalchemy.types.TEXT,
    }

    return type_map.get(field.type, sqlalchemy.types.TEXT)


class TableSemanticAPI(SemanticAPI):
    """
    Custom API adapter that exposes a semantic view under a SQL table name.

    The base adapter requires the full ``semantic-api+http://…/views/<view>``
    URI in the SQL statement. The dialect surfaces a friendlier form:

        SELECT * FROM sales;

    The table name is taken from the SQLAlchemy URL at connection time and
    stored on the class.
    """

    table_name: ClassVar[str] = ""

    @classmethod
    def supports(cls, uri: str, fast: bool = True, **kwargs: Any) -> bool:
        return uri == cls.table_name

    def __init__(
        self,
        table: str,
        view_url: str,
        additional_configuration: dict[str, Any] | None = None,
        request_timeout: float = 60.0,
    ) -> None:
        super().__init__(view_url, table, additional_configuration, request_timeout)


class SemanticAPIDialect(APSWDialect):
    """
    A Semantic Layer REST API dialect.

    URL should look like::

        semanticapi://host[:port]/<view_name>

    Query parameters:
        ``secure``                    set to ``true`` to use HTTPS
        ``additional_configuration``  JSON object forwarded to the view
    """

    name = "semanticapi"

    supports_statement_cache = True

    def create_connect_args(self, url: URL) -> tuple[tuple[()], dict[str, Any]]:
        view_name = (url.database or "").strip("/")
        if not view_name:
            raise ValueError(
                f"URL {url!r} must include a view name as the database segment.",
            )

        secure_flag = str(url.query.get("secure", "")).lower() in _TRUTHY
        scheme = "https" if secure_flag else "http"
        netloc = f"{url.host}:{url.port}" if url.port else url.host
        view_url = f"{scheme}://{netloc}/views/{view_name}"

        adapter_kwargs: dict[str, Any] = {"view_url": view_url}
        if config := url.query.get("additional_configuration"):
            adapter_kwargs["additional_configuration"] = json.loads(config)

        TableSemanticAPI.table_name = view_name
        if "tablesemanticapi" not in registry.loaders:
            registry.add("tablesemanticapi", TableSemanticAPI)

        return (
            (),
            {
                "path": ":memory:",
                "adapters": ["tablesemanticapi"],
                "adapter_kwargs": {"tablesemanticapi": adapter_kwargs},
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
        return [TableSemanticAPI.table_name]

    def has_table(
        self,
        connection: Connection,
        table_name: str,
        schema: str | None = None,
        **kwargs: Any,
    ) -> bool:
        return table_name == TableSemanticAPI.table_name

    def get_columns(
        self,
        connection: Connection,
        table_name: str,
        schema: str | None = None,
        **kwargs: Any,
    ) -> list[SQLAlchemyColumn]:
        adapter = cast(
            SemanticAPI,
            get_adapter_for_table_name(connection, table_name),
        )
        metric_names = set(adapter.metric_ids)

        columns: list[SQLAlchemyColumn] = []
        for name, field in adapter.get_columns().items():
            column: SQLAlchemyColumn = {
                "name": name,
                "type": get_sqla_type(field),
                "nullable": True,
                "default": None,
                "comment": "metric" if name in metric_names else "dimension",
            }
            if name in metric_names:
                column["computed"] = {"sqltext": name, "persisted": True}
            columns.append(column)
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
        return {"text": "A semantic view exposed as a virtual table."}
