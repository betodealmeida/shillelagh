"""
An adapter for GitHub.
"""
import logging
import urllib.parse
from typing import Any
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple

import requests_cache
from jsonpath import JSONPath

from shillelagh.adapters.base import Adapter
from shillelagh.fields import Boolean
from shillelagh.fields import Field
from shillelagh.fields import Integer
from shillelagh.fields import ISODateTime
from shillelagh.fields import String
from shillelagh.filters import Equal
from shillelagh.filters import Filter
from shillelagh.typing import RequestedOrder
from shillelagh.typing import Row

_logger = logging.getLogger(__name__)

PAGE_SIZE = 100

# a mapping from the column name (eg, ``userid``) to the path in the JSON
# response (``{"user": {"id": 42}}`` => ``user.id``) together with the field
TABLES: Dict[str, Dict[str, Dict[str, Tuple[str, Field]]]] = {
    "repos": {
        "pulls": {
            "url": ("html_url", String()),
            "id": ("id", Integer()),
            "number": ("number", Integer(filters=[Equal])),
            "state": ("state", String(filters=[Equal])),
            "title": ("title", String()),
            "userid": ("user.id", Integer()),
            "username": ("user.login", Integer()),
            "draft": ("draft", Boolean()),
            "head": ("head.ref", String(filters=[Equal])),  # head.label?
            "created_at": ("created_at", ISODateTime()),
            "updated_at": ("updated_at", ISODateTime()),
            "closed_at": ("closed_at", ISODateTime()),
            "merged_at": ("merged_at", ISODateTime()),
        },
    },
}


class GitHubAPI(Adapter):

    """
    An adapter for GitHub.
    """

    safe = True

    @staticmethod
    def supports(uri: str, fast: bool = True, **kwargs: Any) -> Optional[bool]:
        parsed = urllib.parse.urlparse(uri)

        if parsed.path.count("/") != 4:
            return False

        # pylint: disable=unused-variable
        _, base, owner, repo, resource = parsed.path.rsplit("/", 4)
        return (
            parsed.netloc == "api.github.com"
            and base in TABLES
            and resource in TABLES[base]
        )

    @staticmethod
    def parse_uri(uri: str) -> Tuple[str, str, str, str]:
        parsed = urllib.parse.urlparse(uri)
        _, base, owner, repo, resource = parsed.path.rsplit("/", 4)
        return (
            base,
            owner,
            repo,
            resource,
        )

    def __init__(self, base: str, owner: str, repo: str, resource: str):
        super().__init__()

        self.base = base
        self.owner = owner
        self.repo = repo
        self.resource = resource

        # use a cache for the API requests
        self._session = requests_cache.CachedSession(
            cache_name="github_cache",
            backend="sqlite",
            expire_after=180,
        )

    def get_columns(self) -> Dict[str, Field]:
        return {
            name: field for name, (_, field) in TABLES[self.base][self.resource].items()
        }

    def get_data(
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
    ) -> Iterator[Row]:
        if "number" in bounds:
            number = bounds.pop("number").value  # type: ignore
            return self._get_single_resource(number)
        return self._get_multiple_resources(bounds)

    def _get_single_resource(self, number: int) -> Iterator[Row]:
        """
        Return a specific resource.
        """
        column_map = {
            key: value for key, (value, _) in TABLES[self.base][self.resource].items()
        }

        headers = {"Accept": "application/vnd.github.v3+json"}
        url = (
            f"https://api.github.com/{self.base}/{self.owner}/"
            f"{self.repo}/{self.resource}/{number}"
        )

        _logger.info("GET %s", url)
        response = self._session.get(url, headers=headers)
        payload = response.json()

        row = {
            column_name: JSONPath(path).parse(payload)[0]
            for column_name, path in column_map.items()
        }
        row["rowid"] = 0
        _logger.debug(row)
        yield row

    def _get_multiple_resources(self, bounds: Dict[str, Filter]) -> Iterator[Row]:
        column_map = {
            key: value for key, (value, _) in TABLES[self.base][self.resource].items()
        }

        headers = {"Accept": "application/vnd.github.v3+json"}
        url = f"https://api.github.com/{self.base}/{self.owner}/{self.repo}/{self.resource}"

        # map filters in ``bounds`` to query params or path
        params = {name: filter_.value for name, filter_ in bounds.items()}  # type: ignore
        params["per_page"] = PAGE_SIZE

        page = 1
        while True:
            _logger.info("GET %s (page %d)", url, page)
            params["page"] = page
            response = self._session.get(url, headers=headers, params=params)
            payload = response.json()

            if not payload:
                break

            for i, resource in enumerate(payload):
                row = {
                    column_name: JSONPath(path).parse(resource)[0]
                    for column_name, path in column_map.items()
                }
                row["rowid"] = i + (page - 1) * PAGE_SIZE
                _logger.debug(row)
                yield row

            page += 1
