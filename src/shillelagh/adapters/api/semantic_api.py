"""An adapter for the Semantic Layer REST API.

Each adapter instance represents a single semantic view exposed by a server
that speaks the protocol documented in
``pandas-semantic-layer/server/SPEC.md``. The view's union of dimensions and
metrics is presented as a single virtual table; ``SELECT``s are translated
into ``POST /views/{name}/query`` requests, and SQLite's ``GROUP BY`` semantics
take care of the rest.

URIs are of the form ``semantic-api+http://host[:port]/views/<view_name>``
(or ``+https`` for TLS). The view portion of the path is what the server
recognises as the view's name; everything before ``/views/`` is treated as
the server's base URL, so a server mounted at ``/api/v1`` is addressable as
``semantic-api+http://host/api/v1/views/sales``.

Example::

    SELECT product_category, total_revenue
    FROM "semantic-api+http://localhost:8000/views/sales"
    WHERE region IN ('North', 'East')
    GROUP BY product_category
    ORDER BY total_revenue DESC
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterator
from typing import Any
from urllib.parse import urlparse, urlunparse

import requests

from shillelagh.adapters.base import Adapter
from shillelagh.exceptions import ImpossibleFilterError, InternalError, ProgrammingError
from shillelagh.fields import (
    Boolean,
    Field,
    Float,
    Integer,
    ISODate,
    ISODateTime,
    Order,
    String,
    Unknown,
)
from shillelagh.filters import (
    Equal,
    Filter,
    Impossible,
    IsNotNull,
    IsNull,
    NotEqual,
    Range,
)
from shillelagh.typing import RequestedOrder, Row

_logger = logging.getLogger(__name__)

_SCHEME = re.compile(r"^semantic-api\+(?P<inner>https?)$")
_VIEWS_SEGMENT = "/views/"

_SCALAR_FILTERS = [Equal, NotEqual, Range, IsNull, IsNotNull]
_NON_SCALAR_FILTERS = [Equal, NotEqual, IsNull, IsNotNull]

_INTEGER_TYPES = {
    "int8",
    "int16",
    "int32",
    "int64",
    "uint8",
    "uint16",
    "uint32",
    "uint64",
}
_FLOAT_TYPES = {"halffloat", "float", "double", "float16", "float32", "float64"}
_STRING_TYPES = {"string", "utf8", "large_string", "large_utf8"}


def _field_for(arrow_type: str, exact: bool = True) -> Field:
    """Map a PyArrow type string (eg ``date32[day]``) to a Shillelagh field.

    Metric columns pass ``exact=False`` so SQLite re-applies the filter after
    the API call — the underlying layer may not implement ``HAVING`` precisely.
    """
    base = arrow_type.split("[", 1)[0]
    if base in _STRING_TYPES:
        return String(filters=_SCALAR_FILTERS, order=Order.ANY, exact=exact)
    if base == "bool":
        return Boolean(filters=_NON_SCALAR_FILTERS, order=Order.ANY, exact=exact)
    if base in _INTEGER_TYPES:
        return Integer(filters=_SCALAR_FILTERS, order=Order.ANY, exact=exact)
    if base in _FLOAT_TYPES:
        return Float(filters=_SCALAR_FILTERS, order=Order.ANY, exact=exact)
    if base in {"date32", "date64"}:
        return ISODate(filters=_SCALAR_FILTERS, order=Order.ANY, exact=exact)
    if base == "timestamp":
        return ISODateTime(filters=_SCALAR_FILTERS, order=Order.ANY, exact=exact)
    return Unknown(filters=_NON_SCALAR_FILTERS, order=Order.ANY, exact=exact)


class SemanticAPI(Adapter):
    """A Shillelagh adapter exposing one semantic view as a virtual table."""

    safe = True
    supports_limit = True
    supports_offset = True
    supports_requested_columns = True

    @staticmethod
    def supports(uri: str, fast: bool = True, **kwargs: Any) -> bool | None:
        parsed = urlparse(uri)
        return bool(
            _SCHEME.match(parsed.scheme)
            and _VIEWS_SEGMENT in parsed.path
            and parsed.path.rsplit(_VIEWS_SEGMENT, 1)[-1],
        )

    @staticmethod
    def parse_uri(uri: str) -> tuple[str, str]:
        parsed = urlparse(uri)
        match = _SCHEME.match(parsed.scheme)
        if match is None or _VIEWS_SEGMENT not in parsed.path:
            raise ProgrammingError(f"Invalid Semantic API URI: {uri!r}")

        prefix, _, view_name = parsed.path.partition(_VIEWS_SEGMENT)
        view_name = view_name.strip("/")
        if not view_name:
            raise ProgrammingError(f"Missing view name in URI: {uri!r}")

        base_url = urlunparse(
            (
                match["inner"],
                parsed.netloc,
                f"{prefix}{_VIEWS_SEGMENT}{view_name}",
                "",
                "",
                "",
            ),
        )
        return base_url, view_name

    def __init__(
        self,
        view_url: str,
        view_name: str,
        additional_configuration: dict[str, Any] | None = None,
        request_timeout: float = 60.0,
    ):
        super().__init__()
        self.view_url = view_url.rstrip("/")
        self.view_name = view_name
        self.additional_configuration = additional_configuration or {}
        self.request_timeout = request_timeout

        self._load_metadata()

    def _post(self, suffix: str, body: dict[str, Any]) -> dict[str, Any]:
        response = requests.post(
            f"{self.view_url}{suffix}",
            json={"additional_configuration": self.additional_configuration, **body},
            timeout=self.request_timeout,
        )
        if response.status_code == 404:
            raise ProgrammingError(response.json().get("detail", response.text))
        response.raise_for_status()
        return response.json()

    def _load_metadata(self) -> None:
        view = self._post("", {})
        self.dimension_ids: dict[str, str] = {
            d["name"]: d["id"] for d in view["dimensions"]
        }
        self.metric_ids: dict[str, str] = {m["name"]: m["id"] for m in view["metrics"]}

        columns: dict[str, Field] = {}
        for dimension in view["dimensions"]:
            columns[dimension["name"]] = _field_for(dimension["type"], exact=True)
        for metric in view["metrics"]:
            columns[metric["name"]] = _field_for(metric["type"], exact=False)
        self.columns = dict(sorted(columns.items()))

    def get_columns(self) -> dict[str, Field]:
        return self.columns

    def get_data(  # noqa: PLR0913 - mirrors the base signature
        self,
        bounds: dict[str, Filter],
        order: list[tuple[str, RequestedOrder]],
        limit: int | None = None,
        offset: int | None = None,
        requested_columns: set[str] | None = None,
        **kwargs: Any,
    ) -> Iterator[Row]:
        wanted = set(requested_columns) if requested_columns else set(self.columns)
        metrics = [self.metric_ids[name] for name in wanted if name in self.metric_ids]
        dimensions = [
            self.dimension_ids[name] for name in wanted if name in self.dimension_ids
        ]
        if not metrics and not dimensions:
            return

        payload = self._post(
            "/query",
            {
                "query": {
                    "metrics": metrics,
                    "dimensions": dimensions,
                    "filters": self._build_filters(bounds),
                    "order": self._build_order(order),
                    "limit": limit,
                    "offset": offset,
                },
            },
        )
        yield from payload["results"]["rows"]

    def _build_filters(self, bounds: dict[str, Filter]) -> list[dict[str, Any]]:
        filters: list[dict[str, Any]] = []
        for name, filter_ in bounds.items():
            if isinstance(filter_, Impossible):
                raise ImpossibleFilterError()

            column_id = self.metric_ids.get(name) or self.dimension_ids.get(name)
            predicate = "HAVING" if name in self.metric_ids else "WHERE"

            if isinstance(filter_, Equal):
                filters.append(self._filter(predicate, column_id, "=", filter_.value))
            elif isinstance(filter_, NotEqual):
                filters.append(self._filter(predicate, column_id, "!=", filter_.value))
            elif isinstance(filter_, IsNull):
                filters.append(self._filter(predicate, column_id, "IS NULL", None))
            elif isinstance(filter_, IsNotNull):
                filters.append(self._filter(predicate, column_id, "IS NOT NULL", None))
            elif isinstance(filter_, Range):
                if filter_.start is not None:
                    op = ">=" if filter_.include_start else ">"
                    filters.append(
                        self._filter(predicate, column_id, op, filter_.start)
                    )
                if filter_.end is not None:
                    op = "<=" if filter_.include_end else "<"
                    filters.append(self._filter(predicate, column_id, op, filter_.end))
            else:
                raise InternalError(f"Unsupported filter: {filter_!r}")

        return filters

    @staticmethod
    def _filter(
        type_: str,
        column: str | None,
        operator: str,
        value: Any,
    ) -> dict[str, Any]:
        return {"type": type_, "column": column, "operator": operator, "value": value}

    def _build_order(
        self,
        order: list[tuple[str, RequestedOrder]],
    ) -> list[dict[str, str]]:
        result: list[dict[str, str]] = []
        for name, requested in order:
            target = self.metric_ids.get(name) or self.dimension_ids.get(name)
            if target is None:
                raise ProgrammingError(f"Cannot order by unknown column {name!r}.")
            direction = "DESC" if requested == Order.DESCENDING else "ASC"
            result.append({"by": target, "direction": direction})
        return result
