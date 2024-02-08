"""
An adapter to CWEs.
"""
import logging
import urllib.parse
from typing import Any, Dict, Iterator, List, Optional, Tuple

import requests_cache

from shillelagh.adapters.base import Adapter
from shillelagh.fields import String
from shillelagh.filters import Filter
from shillelagh.typing import RequestedOrder, Row

_logger = logging.getLogger(__name__)

INITIAL_COST = 0
FETCHING_COST = 1000


class CwesAPI(Adapter):

    """
    An adapter for CWEs JSON (https://github.com/OWASP/cwe-sdk-javascript).

    The adapter expects an URL like::

        cwes://
    """

    safe = True

    supports_limit = False
    supports_offset = False

    cwe_id = String()
    description = String()

    @staticmethod
    def supports(uri: str, fast: bool = True, **kwargs: Any) -> Optional[bool]:
        """cwes://"""
        parsed = urllib.parse.urlparse(uri)
        return parsed.scheme == "cwes"

    @staticmethod
    def parse_uri(uri: str) -> Tuple[None, ...]:
        return tuple()

    def __init__(self):
        super().__init__()
        self._url = (
            "https://raw.githubusercontent.com/OWASP/"
            "cwe-sdk-javascript/master/raw/cwe-dictionary.json"
        )

        # use a cache, since the adapter does a lot of similar API requests,
        # and the data should rarely (never?) change
        self._session = requests_cache.CachedSession(
            cache_name="cwes_cache",
            backend="sqlite",
            expire_after=1800,
        )

    def get_data(  # pylint: disable=too-many-locals
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
        **kwargs: Any,
    ) -> Iterator[Row]:
        response = self._session.get(self._url)
        if not response.ok:
            return
        payload = response.json()
        for cwe_id, item in payload.items():
            row = {
                "cwe_id": f"CWE-{cwe_id}",
                "description": item["Description"],
            }
            yield row
