from datetime import datetime
from datetime import timezone

import pytest
from shillelagh.adapters.api.socrata import build_sql
from shillelagh.adapters.api.socrata import convert_rows
from shillelagh.adapters.api.socrata import quote
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
        (datetime(2021, 6, 3, 0, 0, tzinfo=timezone.utc), 63.0),
        (datetime(2021, 6, 2, 0, 0, tzinfo=timezone.utc), 62.9),
        (datetime(2021, 6, 1, 0, 0, tzinfo=timezone.utc), 62.8),
        (datetime(2021, 5, 31, 0, 0, tzinfo=timezone.utc), 62.7),
        (datetime(2021, 5, 30, 0, 0, tzinfo=timezone.utc), 62.6),
        (datetime(2021, 5, 29, 0, 0, tzinfo=timezone.utc), 62.4),
        (datetime(2021, 5, 28, 0, 0, tzinfo=timezone.utc), 62.2),
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


def test_build_sql():
    sql = build_sql({"a": Range(1, 10, False, True)}, [])
    assert sql == "SELECT * WHERE a > 1 AND a <= 10"

    sql = build_sql({"a": Range(1, None, True, False)}, [])
    assert sql == "SELECT * WHERE a >= 1"

    sql = build_sql({"a": Range(None, 10, True, False)}, [])
    assert sql == "SELECT * WHERE a < 10"

    sql = build_sql({}, [])
    assert sql == "SELECT *"

    with pytest.raises(ProgrammingError) as excinfo:
        build_sql({"a": [1, 2, 3]}, [])
    assert str(excinfo.value) == "Invalid filter: [1, 2, 3]"


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


def test_quote():
    assert quote(1) == "1"
    assert quote(1.0) == "1.0"
    assert quote("one") == "'one'"
    assert (
        quote(datetime(2021, 6, 3, 7, 0, tzinfo=timezone.utc))
        == "'2021-06-03T07:00:00+00:00'"
    )

    with pytest.raises(Exception) as excinfo:
        quote([1])
    assert str(excinfo.value) == "Can't quote value: [1]"


def test_convert_rows():
    columns = {"a": String(), "b": DateTime()}
    rows = [
        {"a": "one", "b": "2021-06-03T07:00:00+00:00"},
    ]
    converted = convert_rows(columns, rows)
    assert list(converted) == [
        {"a": "one", "b": datetime(2021, 6, 3, 7, 0, tzinfo=timezone.utc)},
    ]
