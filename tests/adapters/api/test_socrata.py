from datetime import date

import pytest
from shillelagh.backends.apsw.db import connect
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import DateTime
from shillelagh.fields import String
from shillelagh.filters import Range

from ...fakes import cdc_data_response
from ...fakes import cdc_metadata_response


def test_socrata(requests_mock):
    metadata_url = "https://data.cdc.gov/api/views/unsk-b7fc"
    requests_mock.get(metadata_url, json=cdc_metadata_response)

    data_url = (
        "https://data.cdc.gov/resource/unsk-b7fc.json?"
        "%24query=SELECT+%2A+WHERE+location+%3D+%27US%27+ORDER+BY+date+DESC"
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


def test_socrata_no_data(requests_mock):
    metadata_url = "https://data.cdc.gov/api/views/unsk-b7fc"
    requests_mock.get(metadata_url, json=cdc_metadata_response)

    data_url = (
        "https://data.cdc.gov/resource/unsk-b7fc.json?"
        "%24query=SELECT+%2A+WHERE+location+%3D+%27BR%27+ORDER+BY+date+DESC"
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


def test_socrata_impossible(requests_mock):
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


def test_socrata_invalid_query(requests_mock):
    metadata_url = "https://data.cdc.gov/api/views/unsk-b7fc"
    requests_mock.get(metadata_url, json=cdc_metadata_response)

    data_url = (
        "https://data.cdc.gov/resource/unsk-b7fc.json?"
        "%24query=SELECT+%2A+WHERE+location+%3D+%27CA%27+ORDER+BY+date+DESC"
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
