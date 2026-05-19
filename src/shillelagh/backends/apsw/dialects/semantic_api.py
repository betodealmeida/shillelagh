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

import hashlib
import json
from typing import Any, ClassVar, cast

import requests
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
_DEFAULT_TIMEOUT = 60.0


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
    Base class for per-server SemanticAPI adapter subclasses.

    Each ``(server URL, additional_configuration)`` pair gets its own
    dynamically generated subclass — see :func:`adapter_class` — so the set
    of discovered table names is isolated per tenant. The base class is never
    registered directly.
    """

    table_names: ClassVar[set[str]] = set()
    server_url: ClassVar[str] = ""
    server_configuration: ClassVar[dict[str, Any]] = {}

    @classmethod
    def supports(cls, uri: str, fast: bool = True, **kwargs: Any) -> bool:
        return uri in cls.table_names

    def __init__(self, table: str, request_timeout: float = _DEFAULT_TIMEOUT) -> None:
        view_url = f"{type(self).server_url.rstrip('/')}/views/{table}"
        super().__init__(
            view_url,
            table,
            type(self).server_configuration,
            request_timeout,
        )


_ADAPTER_CACHE: dict[str, type[TableSemanticAPI]] = {}


def adapter_class(
    base_url: str,
    configuration: dict[str, Any],
) -> tuple[str, type[TableSemanticAPI]]:
    """
    Return ``(registry name, adapter class)`` for one ``(server, config)``.

    Subclasses are cached so repeat connections from the same tenant reuse a
    single class — different tenants get their own.
    """
    fingerprint = json.dumps([base_url, configuration], sort_keys=True).encode()
    digest = hashlib.blake2b(fingerprint, digest_size=8).hexdigest()
    name = f"semanticapi_{digest}"
    if name in _ADAPTER_CACHE:
        return name, _ADAPTER_CACHE[name]

    cls = cast(
        type[TableSemanticAPI],
        type(
            f"TableSemanticAPI_{digest}",
            (TableSemanticAPI,),
            {
                "table_names": set(),
                "server_url": base_url,
                "server_configuration": dict(configuration),
            },
        ),
    )
    _ADAPTER_CACHE[name] = cls
    return name, cls


class SemanticAPIDialect(APSWDialect):
    """
    A Semantic Layer REST API dialect.

    URL should look like::

        semanticapi://host[:port]/

    Query parameters:
        ``secure``                    set to ``true`` to use HTTPS
        ``additional_configuration``  JSON object forwarded to the view
                                      (used as ``runtime_configuration``
                                      when listing views)

    Each semantic view on the server is exposed as a SQL table named after
    the view itself.
    """

    name = "semanticapi"

    supports_statement_cache = True

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._base_url: str = ""
        self._configuration: dict[str, Any] = {}
        self._adapter_name: str = ""
        self._adapter_cls: type[TableSemanticAPI] | None = None

    def create_connect_args(self, url: URL) -> tuple[tuple[()], dict[str, Any]]:
        secure = str(url.query.get("secure", "")).lower() in _TRUTHY
        scheme = "https" if secure else "http"
        netloc = f"{url.host}:{url.port}" if url.port else url.host
        self._base_url = f"{scheme}://{netloc}"

        self._configuration = {}
        if raw := url.query.get("additional_configuration"):
            self._configuration = json.loads(raw)

        self._adapter_name, self._adapter_cls = adapter_class(
            self._base_url,
            self._configuration,
        )
        # populate the known-table set so ``supports`` works on first SQL call
        self._adapter_cls.table_names = set(self._list_views())
        if self._adapter_name not in registry.loaders:
            registry.add(self._adapter_name, self._adapter_cls)

        return (
            (),
            {
                "path": ":memory:",
                "adapters": [self._adapter_name],
                "adapter_kwargs": {self._adapter_name: {}},
                "safe": True,
                "isolation_level": self.isolation_level,
            },
        )

    def _list_views(self) -> list[str]:
        response = requests.post(
            f"{self._base_url}/views/list",
            json={"runtime_configuration": self._configuration},
            timeout=_DEFAULT_TIMEOUT,
        )
        response.raise_for_status()
        return sorted(view["name"] for view in response.json())

    def get_table_names(
        self,
        connection: Connection,
        schema: str | None = None,
        sqlite_include_internal: bool = False,
        **kwargs: Any,
    ) -> list[str]:
        views = self._list_views()
        if self._adapter_cls is not None:
            self._adapter_cls.table_names = set(views)
        return views

    def has_table(
        self,
        connection: Connection,
        table_name: str,
        schema: str | None = None,
        **kwargs: Any,
    ) -> bool:
        return (
            self._adapter_cls is not None
            and table_name in self._adapter_cls.table_names
        )

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
