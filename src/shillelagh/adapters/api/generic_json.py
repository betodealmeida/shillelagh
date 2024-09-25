"""
An adapter for fetching JSON data.
"""

# pylint: disable=invalid-name

import logging
from collections.abc import Iterator
from copy import deepcopy
from datetime import timedelta
from typing import Any, Optional, TypedDict

import jsonpath
import prison
from requests_cache import CachedSession
from yarl import URL

from shillelagh.adapters.base import Adapter
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Field, Order
from shillelagh.filters import Filter
from shillelagh.lib import (
    SimpleCostModel,
    analyze,
    flatten,
    get_session,
    seq_startswith,
)
from shillelagh.typing import Maybe, RequestedOrder, Row

_logger = logging.getLogger(__name__)

SUPPORTED_PROTOCOLS = {"http", "https"}
AVERAGE_NUMBER_OF_ROWS = 100
REQUEST_HEADERS_KEY = "_s_headers"
CACHE_EXPIRATION = timedelta(minutes=3)


class URLConfig(TypedDict, total=False):
    request_headers: dict[str, str]
    cache_expiration: float


class Config(URLConfig, total=False):
    url_configs: dict[str, URLConfig]


class GenericJSONAPI(Adapter):
    """
    An adapter for fetching JSON data.
    """

    safe = True

    supports_limit = False
    supports_offset = False
    supports_requested_columns = True

    content_type = "application/json"
    default_path = "$[*]"
    cache_name = "generic_json_cache"

    @classmethod
    def supports(cls, uri: str, fast: bool = True, **kwargs: Any) -> Optional[bool]:
        parsed_uri = URL(uri)
        if parsed_uri.scheme not in SUPPORTED_PROTOCOLS:
            return False
        if fast:
            return Maybe

        parsed_uri, session = cls._get_session(parsed_uri, **kwargs)
        response = session.head(str(parsed_uri))
        return cls.content_type in response.headers.get("content-type", "")

    @classmethod
    def parse_uri(cls, uri: str) -> tuple[str, str]:
        parsed = URL(uri)
        return (str(parsed.with_fragment("")), parsed.fragment or cls.default_path)

    def __init__(self, uri: str, path: str, **kwargs):
        super().__init__()

        # may be decorated with json path (as fragment) or headers (as query param)
        self.path = path
        self.uri, self._session = self._get_session(URL(uri), **kwargs)

        self._set_columns()

    @classmethod
    def _get_session(cls, url: URL, **kwargs) -> tuple[URL, CachedSession]:
        config: Config = deepcopy(kwargs)  # type: ignore (need PEP 692 in Python 3.12+)

        url_config: URLConfig = {
            "request_headers": config.pop("request_headers", {}),
            "cache_expiration": config.pop(
                "cache_expiration", CACHE_EXPIRATION.total_seconds()
            ),
        }

        mutable_query = url.query.copy()
        query_request_header_dicts = [
            prison.loads(q) for q in mutable_query.popall(REQUEST_HEADERS_KEY, [])
        ]
        url = url.with_query(mutable_query)

        if url_configs := config.pop("url_configs", None):
            for url_pat_str, url_pat_config in url_configs.items():
                url_pat = URL(url_pat_str)

                if (
                    url.origin() == url_pat.origin()
                    and seq_startswith(url.parts, url_pat.parts)
                    and set(url.query.values()) >= set(url_pat.query.values())
                ):
                    url_config["request_headers"].update(
                        url_pat_config.pop("request_headers", {})
                    )
                    url_config.update(url_pat_config)

        # apply query headers last
        for query_request_header_dict in query_request_header_dicts:
            url_config["request_headers"].update(query_request_header_dict)

        return url, get_session(
            url_config["request_headers"],
            cls.cache_name,
            timedelta(seconds=url_config["cache_expiration"]),
        )

    def _set_columns(self) -> None:
        rows = list(self.get_data({}, []))
        column_names = list(rows[0].keys()) if rows else []

        _, order, types = analyze(iter(rows))

        self.columns = {
            column_name: types[column_name](
                filters=[],
                order=order.get(column_name, Order.NONE),
                exact=False,
            )
            for column_name in column_names
            if column_name != "rowid"
        }

    def get_columns(self) -> dict[str, Field]:
        return self.columns

    get_cost = SimpleCostModel(AVERAGE_NUMBER_OF_ROWS)

    def get_data(  # pylint: disable=unused-argument, too-many-arguments
        self,
        bounds: dict[str, Filter],
        order: list[tuple[str, RequestedOrder]],
        requested_columns: Optional[set[str]] = None,
        **kwargs: Any,
    ) -> Iterator[Row]:
        response = self._session.get(self.uri)
        if not response.ok:
            raise ProgrammingError(f"Error: {response.text}")

        payload = response.json()

        for i, row in enumerate(jsonpath.findall(self.path, payload)):
            if isinstance(row, list):
                row = {f"col_{i}": value for i, value in enumerate(row)}
            elif isinstance(row, str):
                row = {"col_0": row}
            elif row is None:
                row = {}

            row = {
                k: v
                for k, v in row.items()
                if requested_columns is None or k in requested_columns
            }
            row["rowid"] = i
            _logger.debug(row)
            yield flatten(row)
