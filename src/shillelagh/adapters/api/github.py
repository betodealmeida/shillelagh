"""
An adapter for GitHub.
"""

import json
import logging
import urllib.parse
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional, TypedDict

import jsonpath

from shillelagh.adapters.base import Adapter
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Boolean, DateTime, Field, Integer, String, StringDateTime
from shillelagh.filters import Equal, Filter
from shillelagh.lib import get_session
from shillelagh.typing import RequestedOrder, Row

_logger = logging.getLogger(__name__)

PAGE_SIZE = 100


class JSONString(Field[Any, str]):
    """
    A field to handle JSON values.
    """

    type = "TEXT"
    db_api_type = "STRING"

    def parse(self, value: Any) -> Optional[str]:
        return value if value is None else json.dumps(value)


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
    # the API defaults to showing only PRs with an open state, so we need to
    # default the column to ``all`` to fetch all PRs when state is not
    # specified in the query.
    default: Optional[Filter] = None


def participation_processor(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Process participation data.

    https://docs.github.com/en/rest/metrics/statistics?apiVersion=2022-11-28#get-the-weekly-commit-count
    """
    today_utc_midnight = datetime.now(timezone.utc).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    start = today_utc_midnight - timedelta(weeks=len(payload["all"]))

    return [
        {
            "start_at": start + timedelta(weeks=i),
            "end_at": start + timedelta(weeks=i + 1),
            "all": all,
            "owner": owner,
        }
        for i, (all, owner) in enumerate(zip(payload["all"], payload["owner"]))
    ]


class EndPointDefinition(TypedDict):
    """
    A definition for an endpoint.

    This is used to define the columns and the path to the values in the JSON response.
    It can also specify if the endpoint is paginated (most are) and a processor to
    transform the payload.
    """

    columns: list[Column]
    paginated: bool
    processor: Optional[Callable[[dict[str, Any]], list[dict[str, Any]]]]


# a mapping from the column name (eg, ``userid``) to the path in the JSON
# response (``{"user": {"id": 42}}`` => ``user.id``) together with the field
TABLES: dict[str, dict[str, EndPointDefinition]] = {
    "repos": {
        "pulls": {
            "columns": [
                Column("url", "html_url", String()),
                Column("id", "id", Integer()),
                Column("number", "number", Integer(filters=[Equal])),
                Column("state", "state", String(filters=[Equal]), Equal("all")),
                Column("title", "title", String()),
                Column("userid", "user.id", Integer()),
                Column("username", "user.login", String()),
                Column("draft", "draft", Boolean()),
                Column("head", "head.ref", String(filters=[Equal])),  # head.label?
                Column("created_at", "created_at", StringDateTime()),
                Column("updated_at", "updated_at", StringDateTime()),
                Column("closed_at", "closed_at", StringDateTime()),
                Column("merged_at", "merged_at", StringDateTime()),
            ],
            "paginated": True,
            "processor": None,
        },
        "issues": {
            "columns": [
                Column("url", "html_url", String()),
                Column("id", "id", Integer()),
                Column("number", "number", Integer(filters=[Equal])),
                Column("state", "state", String(filters=[Equal]), Equal("all")),
                Column("title", "title", String()),
                Column("userid", "user.id", Integer()),
                Column("username", "user.login", String()),
                Column("draft", "draft", Boolean()),
                Column("locked", "locked", Boolean()),
                Column("comments", "comments", Integer()),
                Column("created_at", "created_at", StringDateTime()),
                Column("updated_at", "updated_at", StringDateTime()),
                Column("closed_at", "closed_at", StringDateTime()),
                Column("body", "body", String()),
                Column("author_association", "author_association", String()),
                Column("labels", "labels[*].name", JSONString()),
                Column("assignees", "assignees[*].login", JSONString()),
                Column("reactions", "reactions", JSONString()),
            ],
            "paginated": True,
            "processor": None,
        },
        "stats/punch_card": {
            "columns": [
                Column("dow", "$.[0]", Integer()),
                Column("hour", "$.[1]", Integer()),
                Column("commits", "$.[2]", Integer()),
            ],
            "paginated": True,
            "processor": None,
        },
        "stats/participation": {
            "columns": [
                Column("start_at", "$.start_at", DateTime()),
                Column("end_at", "$.end_at", DateTime()),
                Column("all", "$.all", Integer()),
                Column("owner", "$.owner", Integer()),
            ],
            "paginated": False,
            "processor": participation_processor,
        },
    },
}


class GitHubAPI(Adapter):
    """
    An adapter for GitHub.
    """

    safe = True

    supports_limit = False
    supports_offset = False

    @staticmethod
    def supports(uri: str, fast: bool = True, **kwargs: Any) -> Optional[bool]:
        parsed = urllib.parse.urlparse(uri)

        if parsed.path.count("/") < 4:
            return False

        # pylint: disable=unused-variable
        _, base, owner, repo, resource = parsed.path.split("/", 4)
        return (
            parsed.netloc == "api.github.com"
            and base in TABLES
            and resource in TABLES[base]
        )

    @staticmethod
    def parse_uri(uri: str) -> tuple[str, str, str, str]:
        parsed = urllib.parse.urlparse(uri)
        _, base, owner, repo, resource = parsed.path.split("/", 4)
        return (
            base,
            owner,
            repo,
            resource,
        )

    def __init__(  # pylint: disable=too-many-arguments, too-many-positional-arguments
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

        self._session = get_session(
            request_headers={},
            cache_name="github_cache",
            expire_after=timedelta(minutes=3),
        )

    def get_columns(self) -> dict[str, Field]:
        return {
            column.name: column.field
            for column in TABLES[self.base][self.resource]["columns"]
        }

    def get_data(
        self,
        bounds: dict[str, Filter],
        order: list[tuple[str, RequestedOrder]],
        **kwargs: Any,
    ) -> Iterator[Row]:
        # apply default values
        for column in TABLES[self.base][self.resource]["columns"]:
            if column.default is not None and column.name not in bounds:
                bounds[column.name] = column.default

        if "number" in bounds:
            number = bounds.pop("number").value  # type: ignore
            return self._get_single_resource(number)

        return self._get_multiple_resources(bounds)

    def _get_single_resource(
        self,
        number: int,
    ) -> Iterator[Row]:
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
            column.name: get_value(column, payload)
            for column in TABLES[self.base][self.resource]["columns"]
        }
        row["rowid"] = 0
        _logger.debug(row)
        yield row

    def _get_multiple_resources(
        self,
        bounds: dict[str, Filter],
    ) -> Iterator[Row]:
        """
        Return multiple resources.
        """
        headers = {"Accept": "application/vnd.github.v3+json"}
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"

        url = f"https://api.github.com/{self.base}/{self.owner}/{self.repo}/{self.resource}"
        config = TABLES[self.base][self.resource]

        # map filters in ``bounds`` to query params
        params = {name: filter_.value for name, filter_ in bounds.items()}  # type: ignore

        page = 1
        rowid = 0
        while True:
            if config["paginated"]:
                _logger.info("GET %s (page %d)", url, page)
                params.update(
                    {
                        "per_page": PAGE_SIZE,
                        "page": page,
                    },
                )

            response = self._session.get(url, headers=headers, params=params)

            payload = response.json()
            if not payload:
                break

            if not response.ok:
                raise ProgrammingError(payload["message"])

            if processor := config["processor"]:
                payload = processor(payload)

            for resource in payload:
                row = {
                    column.name: get_value(column, resource)
                    for column in config["columns"]
                }
                row["rowid"] = rowid
                _logger.debug(row)
                yield row
                rowid += 1

            if not config["paginated"]:
                break

            page += 1


def get_value(column: Column, resource: dict[str, Any]) -> Any:
    """
    Extract the value of a column from a resource.
    """
    values = jsonpath.findall(column.json_path, resource)

    if isinstance(column.field, JSONString):
        return values

    try:
        return values[0]
    except IndexError:
        return None
