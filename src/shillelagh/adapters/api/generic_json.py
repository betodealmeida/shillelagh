"""
An adapter for fetching JSON data.
"""

# pylint: disable=invalid-name

import logging
from collections.abc import Iterator
from datetime import timedelta
from typing import Any, Optional, Union

import jsonpath
import prison
from yarl import URL

from shillelagh.adapters.base import Adapter, current_network_resource
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Field, Order
from shillelagh.filters import Filter
from shillelagh.lib import SimpleCostModel, analyze, flatten
from shillelagh.resources.resource import NetworkResource
from shillelagh.typing import Maybe, RequestedOrder, Row

_logger = logging.getLogger(__name__)

AVERAGE_NUMBER_OF_ROWS = 100
REQUEST_HEADERS_KEY = "_s_headers"
CACHE_EXPIRATION = timedelta(minutes=3)


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
        parsed = URL(uri)
        if not NetworkResource.is_protocol_supported(parsed.scheme):
            return False
        if fast:
            return Maybe

        if REQUEST_HEADERS_KEY in parsed.query:
            request_headers = prison.loads(parsed.query[REQUEST_HEADERS_KEY])
            parsed = parsed.with_query(
                {k: v for k, v in parsed.query.items() if k != REQUEST_HEADERS_KEY},
            )
        else:
            request_headers = kwargs.get("request_headers", {})

        cache_expiration = kwargs.get(
            "cache_expiration",
            CACHE_EXPIRATION.total_seconds(),
        )

        # create network resource and get content type
        resource = NetworkResource(
            parsed,
            request_headers=request_headers,
            cache_name=cls.cache_name,
            cache_expiration=cache_expiration,
        )
        if resource.assert_content_type(cls.content_type):
            current_network_resource.set(resource)
            return True

        # content type mismatch
        return False

    @classmethod
    def parse_uri(
        cls,
        uri: str,
    ) -> Union[tuple[str, str], tuple[str, str, dict[str, str]]]:
        parsed = URL(uri)

        path = parsed.fragment or cls.default_path
        parsed = parsed.with_fragment("")

        if REQUEST_HEADERS_KEY in parsed.query:
            request_headers = prison.loads(parsed.query[REQUEST_HEADERS_KEY])
            parsed = parsed.with_query(
                {k: v for k, v in parsed.query.items() if k != REQUEST_HEADERS_KEY},
            )
            return str(parsed), path, request_headers

        return str(parsed), path

    def __init__(
        self,
        uri: str,
        path: Optional[str] = None,
        request_headers: Optional[dict[str, str]] = None,
        cache_expiration: float = CACHE_EXPIRATION.total_seconds(),
    ):
        super().__init__()

        self.uri = uri
        self.path = path or self.default_path
        self.request_headers = request_headers
        self.cache_expiration = cache_expiration

        network_resource = current_network_resource.get()
        if not network_resource:
            self.network_resource = NetworkResource(
                self.uri,
                request_headers=self.request_headers,
                cache_name=self.cache_name,
                cache_expiration=self.cache_expiration,
            )
        else:
            self.network_resource = network_resource

        self._set_columns()

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
        response = self.network_resource.get_data()
        payload = response.json()
        if not response.ok:
            raise ProgrammingError(f'Error: {payload["error"]["message"]}')

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

    def close(self) -> None:
        """
        Unset network resource
        """
        current_network_resource.set(None)
