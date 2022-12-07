"""
An adapter for fetching JSON data.
"""

# pylint: disable=invalid-name

import logging
import urllib.parse
from typing import Any, Dict, Iterator, List, Optional, Tuple

import requests_cache
from jsonpath import JSONPath

from shillelagh.adapters.base import Adapter
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Field
from shillelagh.filters import Filter
from shillelagh.lib import SimpleCostModel, analyze, flatten
from shillelagh.typing import Maybe, RequestedOrder, Row

_logger = logging.getLogger(__name__)

SUPPORTED_PROTOCOLS = {"http", "https"}
AVERAGE_NUMBER_OF_ROWS = 100


def get_session() -> requests_cache.CachedSession:
    """
    Return a cached session.
    """
    return requests_cache.CachedSession(
        cache_name="generic_json_cache",
        backend="sqlite",
        expire_after=180,
    )


class GenericJSONAPI(Adapter):

    """
    An adapter for fetching JSON data.
    """

    safe = True

    supports_limit = False
    supports_offset = False

    @staticmethod
    def supports(uri: str, fast: bool = True, **kwargs: Any) -> Optional[bool]:
        parsed = urllib.parse.urlparse(uri)
        if parsed.scheme not in SUPPORTED_PROTOCOLS:
            return False
        if fast:
            return Maybe

        session = get_session()
        response = session.head(uri)
        return "application/json" in response.headers.get("content-type", "")

    @staticmethod
    def parse_uri(uri: str) -> Tuple[str, str]:
        parsed = urllib.parse.urlparse(uri)

        path = urllib.parse.unquote(parsed.fragment) or "$[*]"
        uri = urllib.parse.urlunparse(parsed._replace(fragment=""))

        return uri, path

    def __init__(self, uri: str, path: str = "$[*]"):
        super().__init__()

        self.uri = uri
        self.path = path

        self._session = get_session()

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

    def get_data(  # pylint: disable=unused-argument
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        **kwargs: Any,
    ) -> Iterator[Row]:
        response = self._session.get(self.uri)
        payload = response.json()
        if not response.ok:
            raise ProgrammingError(f'Error: {payload["message"]}')

        parser = JSONPath(self.path)
        for i, row in enumerate(parser.parse(payload)):
            row["rowid"] = i
            _logger.debug(row)
            yield flatten(row)
