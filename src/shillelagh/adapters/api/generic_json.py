"""
An adapter for fetching JSON data.
"""

# pylint: disable=invalid-name

import logging
from datetime import timedelta
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple, Union

import jsonpath
import prison
from yarl import URL

from shillelagh.adapters.base import Adapter
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Field, Order
from shillelagh.filters import Filter
from shillelagh.lib import SimpleCostModel, analyze, flatten, get_session
from shillelagh.typing import Maybe, RequestedOrder, Row

_logger = logging.getLogger(__name__)

SUPPORTED_PROTOCOLS = {"http", "https"}
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
        if parsed.scheme not in SUPPORTED_PROTOCOLS:
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
        session = get_session(
            request_headers,
            cls.cache_name,
            timedelta(seconds=cache_expiration),
        )
        response = session.head(str(parsed))
        return cls.content_type in response.headers.get("content-type", "")

    @classmethod
    def parse_uri(
        cls,
        uri: str,
    ) -> Union[Tuple[str, str], Tuple[str, str, Dict[str, str]]]:
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
        request_headers: Optional[Dict[str, str]] = None,
        cache_expiration: float = CACHE_EXPIRATION.total_seconds(),
    ):
        super().__init__()

        self.uri = uri
        self.path = path or self.default_path

        self._session = get_session(
            request_headers or {},
            self.cache_name,
            timedelta(seconds=cache_expiration),
        )

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

    def get_columns(self) -> Dict[str, Field]:
        return self.columns

    get_cost = SimpleCostModel(AVERAGE_NUMBER_OF_ROWS)

    def get_data(  # pylint: disable=unused-argument, too-many-arguments
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        requested_columns: Optional[Set[str]] = None,
        **kwargs: Any,
    ) -> Iterator[Row]:
        response = self._session.get(self.uri)
        payload = response.json()
        if not response.ok:
            raise ProgrammingError(f'Error: {payload["message"]}')

        for i, row in enumerate(jsonpath.findall(self.path, payload)):
            row = {
                k: v
                for k, v in (row or {}).items()
                if requested_columns is None or k in requested_columns
            }
            row["rowid"] = i
            _logger.debug(row)
            yield flatten(row)
