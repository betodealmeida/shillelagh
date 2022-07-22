# pylint: disable=too-many-lines
"""
Tests for the Datasette adapter.
"""
import datetime

import pytest
from pytest_mock import MockerFixture
from requests import Session
from requests_mock.mocker import Mocker

from shillelagh.backends.apsw.db import connect
from shillelagh.exceptions import ProgrammingError

from ...fakes import github_response, github_single_response


def test_github(mocker: MockerFixture, requests_mock: Mocker) -> None:
    """
    Test a simple request.
    """
    mocker.patch(
        "shillelagh.adapters.api.github.requests_cache.CachedSession",
        return_value=Session(),
    )

    page1_url = "https://api.github.com/repos/apache/superset/pulls?state=all&per_page=100&page=1"
    requests_mock.get(page1_url, json=github_response)
    page2_url = "https://api.github.com/repos/apache/superset/pulls?state=all&per_page=100&page=2"
    requests_mock.get(page2_url, json=[])

    connection = connect(":memory:")
    cursor = connection.cursor()

    sql = """
        SELECT * FROM
        "https://api.github.com/repos/apache/superset/pulls"
    """
    data = list(cursor.execute(sql))
    assert data == [
        (
            "https://github.com/apache/superset/pull/16581",
            726927278,
            16581,
            "open",
            "feat: Arash/datasets and reports",
            48933336,
            "AAfghahi",
            False,
            "arash/datasetsAndReports",
            datetime.datetime(2021, 9, 3, 15, 57, 37, tzinfo=datetime.timezone.utc),
            datetime.datetime(2021, 9, 3, 15, 57, 39, tzinfo=datetime.timezone.utc),
            None,
            None,
        ),
        (
            "https://github.com/apache/superset/pull/16576",
            726586317,
            16576,
            "open",
            "chore(viz): remove legacy table viz code",
            33317356,
            "villebro",
            True,
            "villebro/remove-table-viz",
            datetime.datetime(2021, 9, 3, 7, 52, 18, tzinfo=datetime.timezone.utc),
            datetime.datetime(2021, 9, 3, 8, 48, 45, tzinfo=datetime.timezone.utc),
            None,
            None,
        ),
        (
            "https://github.com/apache/superset/pull/16571",
            726278427,
            16571,
            "open",
            "chore(deps-dev): bump storybook-addon-jsx from 7.3.3 to 7.3.13 in /superset-frontend",
            49699333,
            "dependabot[bot]",
            False,
            "dependabot/npm_and_yarn/superset-frontend/storybook-addon-jsx-7.3.13",
            datetime.datetime(2021, 9, 2, 20, 51, 50, tzinfo=datetime.timezone.utc),
            datetime.datetime(2021, 9, 2, 21, 22, 54, tzinfo=datetime.timezone.utc),
            None,
            None,
        ),
        (
            "https://github.com/apache/superset/pull/16569",
            726107410,
            16569,
            "open",
            "docs: versioned _export Stable",
            47772523,
            "amitmiran137",
            False,
            "version_export_ff_on",
            datetime.datetime(2021, 9, 2, 16, 52, 34, tzinfo=datetime.timezone.utc),
            datetime.datetime(2021, 9, 2, 18, 6, 27, tzinfo=datetime.timezone.utc),
            None,
            None,
        ),
        (
            "https://github.com/apache/superset/pull/16566",
            725808286,
            16566,
            "open",
            "fix(docker): add ecpg to docker image",
            33317356,
            "villebro",
            False,
            "villebro/libecpg",
            datetime.datetime(2021, 9, 2, 12, 1, 2, tzinfo=datetime.timezone.utc),
            datetime.datetime(2021, 9, 2, 12, 6, 50, tzinfo=datetime.timezone.utc),
            None,
            None,
        ),
        (
            "https://github.com/apache/superset/pull/16564",
            725669631,
            16564,
            "open",
            "refactor: orderby control refactoring",
            2016594,
            "zhaoyongjie",
            True,
            "refactor_orderby",
            datetime.datetime(2021, 9, 2, 9, 45, 40, tzinfo=datetime.timezone.utc),
            datetime.datetime(2021, 9, 3, 10, 31, 4, tzinfo=datetime.timezone.utc),
            None,
            None,
        ),
        (
            "https://github.com/apache/superset/pull/16554",
            724863880,
            16554,
            "open",
            "refactor: Update async query init to support runtime feature flags",
            296227,
            "robdiciuccio",
            False,
            "rd/async-query-init-refactor",
            datetime.datetime(2021, 9, 1, 19, 51, 51, tzinfo=datetime.timezone.utc),
            datetime.datetime(2021, 9, 1, 22, 29, 46, tzinfo=datetime.timezone.utc),
            None,
            None,
        ),
        (
            "https://github.com/apache/superset/pull/16549",
            724669525,
            16549,
            "open",
            "feat(dashboard): Native filters - add type to native filter configuration",
            12539911,
            "m-ajay",
            False,
            "feat/migration-add-type-to-native-filter",
            datetime.datetime(2021, 9, 1, 16, 35, 50, tzinfo=datetime.timezone.utc),
            datetime.datetime(2021, 9, 3, 17, 33, 42, tzinfo=datetime.timezone.utc),
            None,
            None,
        ),
        (
            "https://github.com/apache/superset/pull/16548",
            724668038,
            16548,
            "open",
            "refactor: sql_json view endpoint: encapsulate ctas parameters",
            35701650,
            "ofekisr",
            False,
            "refactor/sql_json_view4",
            datetime.datetime(2021, 9, 1, 16, 33, 45, tzinfo=datetime.timezone.utc),
            datetime.datetime(2021, 9, 1, 17, 6, 32, tzinfo=datetime.timezone.utc),
            None,
            None,
        ),
        (
            "https://github.com/apache/superset/pull/16545",
            724509555,
            16545,
            "open",
            "perf(dashboard): decrease number of rerenders of FiltersBadge",
            15073128,
            "kgabryje",
            False,
            "perf/dashboard-rerenders-4",
            datetime.datetime(2021, 9, 1, 13, 41, 12, tzinfo=datetime.timezone.utc),
            datetime.datetime(2021, 9, 2, 15, 39, 15, tzinfo=datetime.timezone.utc),
            None,
            None,
        ),
    ]


def test_github_single_resource(mocker: MockerFixture, requests_mock: Mocker) -> None:
    """
    Test a request to a single resource.
    """
    mocker.patch(
        "shillelagh.adapters.api.github.requests_cache.CachedSession",
        return_value=Session(),
    )

    url = "https://api.github.com/repos/apache/superset/pulls/16581"
    requests_mock.get(url, json=github_single_response)

    connection = connect(":memory:")
    cursor = connection.cursor()

    sql = """
        SELECT * FROM
        "https://api.github.com/repos/apache/superset/pulls"
        WHERE number = 16581
    """
    data = list(cursor.execute(sql))
    assert data == [
        (
            "https://github.com/apache/superset/pull/16581",
            726927278,
            16581,
            "open",
            "feat: Arash/datasets and reports",
            48933336,
            "AAfghahi",
            False,
            "arash/datasetsAndReports",
            datetime.datetime(2021, 9, 3, 15, 57, 37, tzinfo=datetime.timezone.utc),
            datetime.datetime(2021, 9, 3, 15, 57, 39, tzinfo=datetime.timezone.utc),
            None,
            None,
        ),
    ]


def test_github_rate_limit(mocker: MockerFixture, requests_mock: Mocker) -> None:
    """
    Test that the adapter was rate limited by the API.
    """
    mocker.patch(
        "shillelagh.adapters.api.github.requests_cache.CachedSession",
        return_value=Session(),
    )

    url = "https://api.github.com/repos/apache/superset/pulls?state=all&per_page=100&page=1"
    requests_mock.get(
        url,
        status_code=403,
        json={
            "message": (
                "API rate limit exceeded for 66.220.13.38. (But here's the good "
                "news: Authenticated requests get a higher rate limit. Check out "
                "the documentation for more details.)"
            ),
            "documentation_url": (
                "https://docs.github.com/rest/overview/"
                "resources-in-the-rest-api#rate-limiting"
            ),
        },
    )

    connection = connect(":memory:")
    cursor = connection.cursor()

    sql = """
        SELECT * FROM
        "https://api.github.com/repos/apache/superset/pulls"
    """
    with pytest.raises(ProgrammingError) as excinfo:
        list(cursor.execute(sql))
    assert str(excinfo.value) == (
        "API rate limit exceeded for 66.220.13.38. (But here's the good "
        "news: Authenticated requests get a higher rate limit. Check out "
        "the documentation for more details.)"
    )


def test_github_auth_token(mocker: MockerFixture, requests_mock: Mocker) -> None:
    """
    Test a simple request.
    """
    mocker.patch(
        "shillelagh.adapters.api.github.requests_cache.CachedSession",
        return_value=Session(),
    )

    single_resource_url = "https://api.github.com/repos/apache/superset/pulls/16581"
    single_resource_mock = requests_mock.get(
        single_resource_url,
        json=github_single_response,
    )
    multiple_resources_url = (
        "https://api.github.com/repos/apache/superset/pulls?"
        "state=all&per_page=100&page=1"
    )
    multiple_resources_mock = requests_mock.get(multiple_resources_url, json=[])

    connection = connect(
        ":memory:",
        adapter_kwargs={"githubapi": {"access_token": "XXX"}},
    )
    cursor = connection.cursor()

    sql = """
        SELECT * FROM
        "https://api.github.com/repos/apache/superset/pulls"
        WHERE number = 16581
    """
    list(cursor.execute(sql))
    assert single_resource_mock.last_request.headers["Authorization"] == "Bearer XXX"

    sql = """
        SELECT * FROM
        "https://api.github.com/repos/apache/superset/pulls"
    """
    list(cursor.execute(sql))
    assert multiple_resources_mock.last_request.headers["Authorization"] == "Bearer XXX"
