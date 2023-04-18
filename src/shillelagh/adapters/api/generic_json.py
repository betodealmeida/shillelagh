"""
An adapter for fetching JSON data.
"""

# pylint: disable=invalid-name

import logging
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple, Union

import prison
import requests_cache
from jsonpath import JSONPath
from yarl import URL

from shillelagh.adapters.base import Adapter
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Field
from shillelagh.filters import Filter
from shillelagh.lib import SimpleCostModel, analyze, flatten
from shillelagh.typing import Maybe, RequestedOrder, Row

_logger = logging.getLogger(__name__)

SUPPORTED_PROTOCOLS = {"http", "https"}
AVERAGE_NUMBER_OF_ROWS = 100
CACHE_EXPIRATION = 180
REQUEST_HEADERS_KEY = "_s_headers"


def get_session(request_headers: Dict[str, str]) -> requests_cache.CachedSession:
    """
    Return a cached session.
    """
    session = requests_cache.CachedSession(
        cache_name="generic_json_cache",
        backend="sqlite",
        expire_after=CACHE_EXPIRATION,
    )
    session.headers.update(request_headers)

    return session


class GenericJSONAPI(Adapter):

    """
    An adapter for fetching JSON data.
    """

    safe = True

    supports_limit = False
    supports_offset = False
    supports_requested_columns = True

    @staticmethod
    def supports(uri: str, fast: bool = True, **kwargs: Any) -> Optional[bool]:
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

        session = get_session(request_headers)
        response = session.head(str(parsed))
        return "application/json" in response.headers.get("content-type", "")

    @staticmethod
    def parse_uri(uri: str) -> Union[Tuple[str, str], Tuple[str, str, Dict[str, str]]]:
        parsed = URL(uri)

        path = parsed.fragment or "$[*]"
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
        path: str = "$[*]",
        request_headers: Optional[Dict[str, str]] = None,
    ):
        super().__init__()

        self.uri = uri
        self.path = path

        self._session = get_session(request_headers or {})

        self._set_columns()

    def _set_columns(self) -> None:
        rows = list(self.get_data({}, []))
        column_names = list(rows[0].keys()) if rows else []

        _, order, types = analyze(iter(rows))

        self.columns = {
            column_name: types[column_name](
                filters=[],
                order=order[column_name],
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

        parser = JSONPath(self.path)
        for i, row in enumerate(parser.parse(payload)):
            row = {
                k: v
                for k, v in row.items()
                if requested_columns is None or k in requested_columns
            }
            row["rowid"] = i
            _logger.debug(row)
            yield flatten(row)
