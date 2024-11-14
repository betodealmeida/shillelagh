"""
Class with implementation network resource for HTTP and HTTPS protocols
"""

from datetime import timedelta
from typing import Optional

from requests import Session
from requests_cache import Response
from yarl import URL

from shillelagh.lib import get_session
from shillelagh.resources.base import NetworkResourceImplementation

CACHE_EXPIRATION = timedelta(minutes=3)


class HTTPNetworkResourceImplementation(NetworkResourceImplementation):
    """
    Implement logic for HTTP and HTTPS protocols.
    """

    def __init__(self, url: URL, cache_name: Optional[str] = None, **kwargs) -> None:
        super().__init__(url, **kwargs)

        __cache_expiration = kwargs.get(
            "cache_expiration",
            CACHE_EXPIRATION.total_seconds(),
        )

        if cache_name:
            self._session = get_session(
                kwargs.get("request_headers", {}),
                cache_name,
                timedelta(seconds=__cache_expiration),
            )
        else:
            self._session = Session()

    def get_content_type(self) -> str:
        response = self._session.head(str(self._url))
        # For mypy passing (it will be str any way)
        content_type = str(response.headers.get("content-type", ""))
        return content_type

    def get_data(self) -> Response:
        return self._session.get(str(self._url))
