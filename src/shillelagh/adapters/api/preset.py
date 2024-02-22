"""
Simple adapter for the Preset API (https://preset.io/).

This is a derivation of the generic JSON adapter that handles Preset auth.
"""

import logging
import re
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple, cast

import jsonpath
import prison
import requests
from yarl import URL

from shillelagh.adapters.api.generic_json import CACHE_EXPIRATION, GenericJSONAPI
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Order
from shillelagh.filters import Filter
from shillelagh.lib import analyze, flatten
from shillelagh.typing import RequestedOrder, Row

_logger = logging.getLogger(__name__)

MAX_PAGE_SIZE = 100


def get_jwt_token(uri: str, access_token: str, access_secret: str) -> str:
    """
    Get JWT token from access token and access secret.
    """
    parsed = URL(uri)
    environment = parsed.host.split(".")[-3]
    api_uri = f"https://api.{environment}.preset.io/v1/auth/"

    response = requests.post(
        api_uri,
        json={"name": access_token, "secret": access_secret},
        headers={"Content-Type": "application/json"},
        timeout=60,
    )
    response.raise_for_status()
    payload = response.json()
    return cast(str, payload["payload"]["access_token"])


class PresetAPI(GenericJSONAPI):
    """
    Custom JSON adapter that handles Preset auth.
    """

    default_path = "$.payload[*]"
    cache_name = "preset_cache"

    @classmethod
    def supports(cls, uri: str, fast: bool = True, **kwargs: Any) -> Optional[bool]:
        parsed = URL(uri)
        return (
            parsed.scheme in ("http", "https")
            and re.match(r"api\.app(-\w+)?\.preset\.io", parsed.host) is not None
        )

    def __init__(  # pylint: disable=too-many-arguments
        self,
        uri: str,
        path: Optional[str] = None,
        access_token: Optional[str] = None,
        access_secret: Optional[str] = None,
        cache_expiration: float = CACHE_EXPIRATION.total_seconds(),
    ):
        if access_token is None or access_secret is None:
            raise ValueError("access_token and access_secret must be provided")

        jwt_token = get_jwt_token(uri, access_token, access_secret)
        request_headers = {"Authorization": f"Bearer {jwt_token}"}
        super().__init__(
            uri,
            path=path,
            request_headers=request_headers,
            cache_expiration=cache_expiration,
        )


def get_urls(
    resource_url: str,
    offset: Optional[int] = None,
    limit: Optional[int] = None,
    page_size: int = MAX_PAGE_SIZE,
) -> Iterator[Tuple[str, slice]]:
    """
    Get all paginated URLs to download data together with a limit/offset slice.
    """
    start = offset or 0
    stop = start + limit if limit is not None else None

    baseurl = URL(resource_url)
    query = baseurl.query.get("q", "()")
    try:
        params = prison.loads(query)
    except Exception:  # pylint: disable=broad-except
        yield str(baseurl), slice(start, stop)
        return

    # assume the user knows better and keep the URL unmodified
    if "page" in params or "page_size" in params:
        yield str(baseurl), slice(start, stop)
        return

    page = start // page_size
    start = start % page_size
    remaining = limit if limit is not None else float("inf")
    while True:
        params["page"] = page
        params["page_size"] = min(start + remaining, page_size)
        yield str(baseurl.with_query({"q": prison.dumps(params)})), slice(start, None)

        remaining -= page_size - start
        if remaining <= 0:
            break

        page += 1
        start = 0


class PresetWorkspaceAPI(PresetAPI):
    """
    Adapter for Preset workspaces.
    """

    supports_limit = True
    supports_offset = True

    default_path = "$.result[*]"
    cache_name = "preset_cache"

    @classmethod
    def supports(cls, uri: str, fast: bool = True, **kwargs: Any) -> Optional[bool]:
        parsed = URL(uri)
        return (
            parsed.scheme in ("http", "https")
            and parsed.host != "api.app.preset.io"
            and parsed.host.endswith(".preset.io")
        )

    def _set_columns(self) -> None:
        # request only a single page of results to infer schema
        rows = list(self.get_data({}, [], limit=MAX_PAGE_SIZE))
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

    def get_data(  # pylint: disable=unused-argument, too-many-arguments, too-many-locals
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        requested_columns: Optional[Set[str]] = None,
        **kwargs: Any,
    ) -> Iterator[Row]:
        for url, slice_ in get_urls(self.uri, offset, limit, MAX_PAGE_SIZE):
            response = self._session.get(str(url))
            payload = response.json()
            if not response.ok:
                messages = "\n".join(
                    error.get("message", str(error))
                    for error in payload.get("errors", [])
                )
                raise ProgrammingError(f"Error: {messages}")

            rows = jsonpath.findall(self.path, payload)[slice_]
            if not rows:
                break

            for i, row in enumerate(rows):
                row = {
                    k: v
                    for k, v in (row or {}).items()
                    if requested_columns is None or k in requested_columns
                }
                row["rowid"] = i
                _logger.debug(row)
                yield flatten(row)
