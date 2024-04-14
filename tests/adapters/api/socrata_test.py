"""
Tests for the Socrata adapter.
"""

from datetime import date

import pytest
from pytest_mock import MockerFixture
from requests import Session
from requests_mock.mocker import Mocker

from shillelagh.adapters.api.socrata import Number, SocrataAPI
from shillelagh.backends.apsw.db import connect
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Order
from shillelagh.filters import Impossible, Operator

from ...fakes import cdc_data_response, cdc_metadata_response


def test_socrata(mocker: MockerFixture, requests_mock: Mocker) -> None:
    """
    Test a simple query.
    """
    mocker.patch(
        "shillelagh.adapters.api.socrata.requests_cache.CachedSession",
        return_value=Session(),
    )

    metadata_url = "https://data.cdc.gov/api/views/unsk-b7fc"
    requests_mock.get(metadata_url, json=cdc_metadata_response)

    data_url = (
        "https://data.cdc.gov/resource/unsk-b7fc.json?"
        "%24query=SELECT+%2A+WHERE+location+%3D+%27US%27+ORDER+BY+date+DESC+LIMIT+7"
    )
    requests_mock.get(data_url, json=cdc_data_response)

    connection = connect(":memory:")
    cursor = connection.cursor()
    sql = """
        SELECT date, administered_dose1_recip_4
        FROM "https://data.cdc.gov/resource/unsk-b7fc.json"
        WHERE location = 'US'
        ORDER BY date DESC
        LIMIT 7
    """
    data = list(cursor.execute(sql))
    assert data == [
        (date(2021, 6, 3), 63.0),
        (date(2021, 6, 2), 62.9),
        (date(2021, 6, 1), 62.8),
        (date(2021, 5, 31), 62.7),
        (date(2021, 5, 30), 62.6),
        (date(2021, 5, 29), 62.4),
        (date(2021, 5, 28), 62.2),
    ]


def test_socrata_app_token_url(mocker: MockerFixture, requests_mock: Mocker) -> None:
    """
    Test app token being passed via the URL.
    """
    mocker.patch(
        "shillelagh.adapters.api.socrata.requests_cache.CachedSession",
        return_value=Session(),
    )

    metadata_url = "https://data.cdc.gov/api/views/unsk-b7fc"
    requests_mock.get(metadata_url, json=cdc_metadata_response)

    data_url = (
        "https://data.cdc.gov/resource/unsk-b7fc.json?"
        "%24query=SELECT+%2A+WHERE+location+%3D+%27OK%27+ORDER+BY+date+DESC+LIMIT+7"
    )
    data = requests_mock.get(data_url, json=cdc_data_response)

    connection = connect(":memory:")
    cursor = connection.cursor()
    sql = """
        SELECT date, administered_dose1_recip_4
        FROM "https://data.cdc.gov/resource/unsk-b7fc.json?$$app_token=XXX"
        WHERE location = 'OK'
        ORDER BY date DESC
        LIMIT 7
    """
    cursor.execute(sql)
    assert data.last_request.headers == {"X-App-Token": "XXX"}


def test_socrata_app_token_connection(
    mocker: MockerFixture,
    requests_mock: Mocker,
) -> None:
    """
    Test app token being passed via the connection instead of the URL.
    """
    mocker.patch(
        "shillelagh.adapters.api.socrata.requests_cache.CachedSession",
        return_value=Session(),
    )

    metadata_url = "https://data.cdc.gov/api/views/unsk-b7fc"
    requests_mock.get(metadata_url, json=cdc_metadata_response)

    data_url = (
        "https://data.cdc.gov/resource/unsk-b7fc.json?"
        "%24query=SELECT+%2A+WHERE+location+%3D+%27NY%27+ORDER+BY+date+DESC+LIMIT+7"
    )
    data = requests_mock.get(data_url, json=cdc_data_response)
    connection = connect(
        ":memory:",
        adapter_kwargs={"socrataapi": {"app_token": "YYY"}},
    )
    cursor = connection.cursor()
    sql = """
        SELECT date, administered_dose1_recip_4
        FROM "https://data.cdc.gov/resource/unsk-b7fc.json"
        WHERE location = 'NY'
        ORDER BY date DESC
        LIMIT 7
    """
    cursor.execute(sql)
    assert data.last_request.headers == {"X-App-Token": "YYY"}


def test_socrata_no_data(mocker: MockerFixture, requests_mock: Mocker) -> None:
    """
    Test that some queries return no data.
    """
    mocker.patch(
        "shillelagh.adapters.api.socrata.requests_cache.CachedSession",
        return_value=Session(),
    )

    metadata_url = "https://data.cdc.gov/api/views/unsk-b7fc"
    requests_mock.get(metadata_url, json=cdc_metadata_response)

    data_url = (
        "https://data.cdc.gov/resource/unsk-b7fc.json?"
        "%24query=SELECT+%2A+WHERE+location+%3D+%27BR%27+ORDER+BY+date+DESC+LIMIT+7"
    )
    requests_mock.get(data_url, json=[])

    connection = connect(":memory:")
    cursor = connection.cursor()
    sql = """
        SELECT date, administered_dose1_recip_4
        FROM "https://data.cdc.gov/resource/unsk-b7fc.json"
        WHERE location = 'BR'
        ORDER BY date DESC
        LIMIT 7
    """
    data = list(cursor.execute(sql))
    assert data == []


def test_socrata_impossible(mocker: MockerFixture, requests_mock: Mocker) -> None:
    """
    Test that impossible queries return no data.
    """
    mocker.patch(
        "shillelagh.adapters.api.socrata.requests_cache.CachedSession",
        return_value=Session(),
    )

    metadata_url = "https://data.cdc.gov/api/views/unsk-b7fc"
    requests_mock.get(metadata_url, json=cdc_metadata_response)

    connection = connect(":memory:")
    cursor = connection.cursor()
    sql = """
        SELECT date, administered_dose1_recip_4
        FROM "https://data.cdc.gov/resource/unsk-b7fc.json"
        WHERE location = 'US' AND location = 'AZ'
        ORDER BY date DESC
        LIMIT 7
    """
    data = list(cursor.execute(sql))
    assert data == []

    # test for apsw 3.36, since it doesn't call ``get_data`` when the
    # query is impossible to resolve
    adapter = SocrataAPI("data.cdc.gov", "unsk-b7fc")
    assert list(adapter.get_data({"location": Impossible()}, [])) == []


def test_socrata_invalid_query(mocker: MockerFixture, requests_mock: Mocker) -> None:
    """
    Test that invalid queries are handled correctly.
    """
    mocker.patch(
        "shillelagh.adapters.api.socrata.requests_cache.CachedSession",
        return_value=Session(),
    )

    metadata_url = "https://data.cdc.gov/api/views/unsk-b7fc"
    requests_mock.get(metadata_url, json=cdc_metadata_response)

    data_url = (
        "https://data.cdc.gov/resource/unsk-b7fc.json?"
        "%24query=SELECT+%2A+WHERE+location+%3D+%27CA%27+ORDER+BY+date+DESC+LIMIT+7"
    )
    requests_mock.get(
        data_url,
        json={
            "message": "Invalid SoQL query",
            "errorCode": "query.soql.invalid",
            "data": {},
        },
    )

    connection = connect(":memory:")
    cursor = connection.cursor()
    sql = """
        SELECT date, administered_dose1_recip_4
        FROM "https://data.cdc.gov/resource/unsk-b7fc.json"
        WHERE location = 'CA'
        ORDER BY date DESC
        LIMIT 7
    """
    with pytest.raises(ProgrammingError) as excinfo:
        cursor.execute(sql)
    assert str(excinfo.value) == "Invalid SoQL query"


def test_number() -> None:
    """
    Test that numbers are converted correctly.
    """
    assert Number().parse("1.0") == 1.0
    assert Number().parse(None) is None
    assert Number().format(1.0) == "1.0"
    assert Number().format(None) is None
    assert Number().quote("1.0") == "1.0"
    assert Number().quote(None) == "NULL"


@pytest.mark.integration_test
def test_integration(adapter_kwargs) -> None:
    """
    Test fetching data from the CDC.
    """
    connection = connect(":memory:", adapter_kwargs=adapter_kwargs)
    cursor = connection.cursor()

    sql = """
        SELECT administered
        FROM "https://data.cdc.gov/resource/unsk-b7fc.json"
        WHERE location = ? AND date = ?
    """
    cursor.execute(sql, ("US", date(2023, 3, 1)))
    assert cursor.fetchall() == [(672076105.0,)]


def test_get_cost(mocker: MockerFixture) -> None:
    """
    Test ``get_cost``.
    """
    mocker.patch(
        "shillelagh.adapters.api.socrata.requests_cache.CachedSession",
    )

    adapter = SocrataAPI("netloc", "dataset", "XXX")

    assert adapter.get_cost([], []) == 0
    assert adapter.get_cost([("one", Operator.EQ)], []) == 1000
    assert adapter.get_cost([("one", Operator.EQ), ("two", Operator.GT)], []) == 2000
    assert (
        adapter.get_cost(
            [("one", Operator.EQ), ("two", Operator.GT)],
            [("one", Order.ASCENDING)],
        )
        == 11965
    )
    assert (
        adapter.get_cost(
            [("one", Operator.EQ), ("two", Operator.GT)],
            [("one", Order.ASCENDING), ("two", Order.DESCENDING)],
        )
        == 21931
    )
