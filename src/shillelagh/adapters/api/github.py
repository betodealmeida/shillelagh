"""
An adapter for GitHub.
"""
import logging
import urllib.parse
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Tuple

import requests_cache
from jsonpath import JSONPath

from shillelagh.adapters.base import Adapter
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Boolean, Field, Integer, ISODateTime, String
from shillelagh.filters import Equal, Filter
from shillelagh.typing import RequestedOrder, Row

_logger = logging.getLogger(__name__)

PAGE_SIZE = 100


@dataclass
class Column:
    """
    A class to track columns in GitHub resources.
    """

    # The name of the column in the virtual table, eg, ``userid``.
    name: str

    # The JSON path to the value in the GitHub response. Eg, user ID is
    # returned as ``{"user": {"id": 42}}``, so the JSON path would be
    # ``user.id``.
    json_path: str

    # The Shillelagh field to be used for the column.
    field: Field

    # A default value for when the column is not specified. Eg, for ``pulls``
    # the API defaults to show only PRs with an open state, so we need to
    # default the column to ``all`` to fetch all PRs when state is not
    # specified in the query.
    default: Optional[Filter] = None


# a mapping from the column name (eg, ``userid``) to the path in the JSON
# response (``{"user": {"id": 42}}`` => ``user.id``) together with the field
TABLES: Dict[str, Dict[str, List[Column]]] = {
    "repos": {
        "pulls": [
            Column("url", "html_url", String()),
            Column("id", "id", Integer()),
            Column("number", "number", Integer(filters=[Equal])),
            Column("state", "state", String(filters=[Equal]), Equal("all")),
            Column("title", "title", String()),
            Column("userid", "user.id", Integer()),
            Column("username", "user.login", Integer()),
            Column("draft", "draft", Boolean()),
            Column("head", "head.ref", String(filters=[Equal])),  # head.label?
            Column("created_at", "created_at", ISODateTime()),
            Column("updated_at", "updated_at", ISODateTime()),
            Column("closed_at", "closed_at", ISODateTime()),
            Column("merged_at", "merged_at", ISODateTime()),
        ],
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

    def __init__(  # pylint: disable=too-many-arguments
        self,
        base: str,
        owner: str,
        repo: str,
        resource: str,
        access_token: Optional[str] = None,
    ):
        super().__init__()

        self.base = base
        self.owner = owner
        self.repo = repo
        self.resource = resource
        self.access_token = access_token

        # use a cache for the API requests
        self._session = requests_cache.CachedSession(
            cache_name="github_cache",
            backend="sqlite",
            expire_after=180,
        )

    def get_columns(self) -> Dict[str, Field]:
        return {
            column.name: column.field for column in TABLES[self.base][self.resource]
        }

    def get_data(
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
    ) -> Iterator[Row]:
        # apply default values
        for column in TABLES[self.base][self.resource]:
            if column.default is not None and column.name not in bounds:
                bounds[column.name] = column.default

        if "number" in bounds:
            number = bounds.pop("number").value  # type: ignore
            return self._get_single_resource(number)

        return self._get_multiple_resources(bounds)

    def _get_single_resource(self, number: int) -> Iterator[Row]:
        """
        Return a specific resource.
        """
        headers = {"Accept": "application/vnd.github.v3+json"}
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"

        url = (
            f"https://api.github.com/{self.base}/{self.owner}/"
            f"{self.repo}/{self.resource}/{number}"
        )

        _logger.info("GET %s", url)
        response = self._session.get(url, headers=headers)
        payload = response.json()

        row = {
            column.name: JSONPath(column.json_path).parse(payload)[0]
            for column in TABLES[self.base][self.resource]
        }
        row["rowid"] = 0
        _logger.debug(row)
        yield row

    def _get_multiple_resources(self, bounds: Dict[str, Filter]) -> Iterator[Row]:
        """
        Return multiple resources.
        """
        headers = {"Accept": "application/vnd.github.v3+json"}
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"

        url = f"https://api.github.com/{self.base}/{self.owner}/{self.repo}/{self.resource}"

        # map filters in ``bounds`` to query params
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

            if not response.ok:
                raise ProgrammingError(payload["message"])

            for i, resource in enumerate(payload):
                row = {
                    column.name: JSONPath(column.json_path).parse(resource)[0]
                    for column in TABLES[self.base][self.resource]
                }
                row["rowid"] = i + (page - 1) * PAGE_SIZE
                _logger.debug(row)
                yield row

            page += 1
