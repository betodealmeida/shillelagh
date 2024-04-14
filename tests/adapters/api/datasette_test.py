# pylint: disable=too-many-lines
"""
Tests for the Datasette adapter.
"""

from datetime import timedelta

import pytest
from pytest_mock import MockerFixture
from requests_mock.mocker import Mocker

from shillelagh.adapters.api.datasette import (
    DatasetteAPI,
    get_field,
    is_datasette,
    is_known_domain,
)
from shillelagh.backends.apsw.db import connect
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Float, Integer, ISODate, ISODateTime, String

from ...fakes import (
    datasette_columns_response,
    datasette_data_response_1,
    datasette_data_response_2,
    datasette_metadata_response,
    datasette_results,
)

DO_NOT_CACHE = timedelta(seconds=-1)


def test_datasette(mocker: MockerFixture, requests_mock: Mocker) -> None:
    """
    Test a simple query.
    """
    mocker.patch("shillelagh.adapters.api.datasette.CACHE_EXPIRATION", DO_NOT_CACHE)

    columns_url = (
        "https://global-power-plants.datasettes.com/global-power-plants.json?"
        "sql=SELECT+*+FROM+%22global-power-plants%22+LIMIT+0"
    )
    requests_mock.get(columns_url, json=datasette_columns_response)
    metadata_url = (
        "https://global-power-plants.datasettes.com/global-power-plants.json?sql=SELECT+"
        "MAX%28%22country%22%29%2C+"
        "MAX%28%22country_long%22%29%2C+"
        "MAX%28%22name%22%29%2C+"
        "MAX%28%22gppd_idnr%22%29%2C+"
        "MAX%28%22capacity_mw%22%29%2C+"
        "MAX%28%22latitude%22%29%2C+"
        "MAX%28%22longitude%22%29%2C+"
        "MAX%28%22primary_fuel%22%29%2C+"
        "MAX%28%22other_fuel1%22%29%2C+"
        "MAX%28%22other_fuel2%22%29%2C+"
        "MAX%28%22other_fuel3%22%29%2C+"
        "MAX%28%22commissioning_year%22%29%2C+"
        "MAX%28%22owner%22%29%2C+"
        "MAX%28%22source%22%29%2C+"
        "MAX%28%22url%22%29%2C+"
        "MAX%28%22geolocation_source%22%29%2C+"
        "MAX%28%22wepp_id%22%29%2C+"
        "MAX%28%22year_of_capacity_data%22%29%2C+"
        "MAX%28%22generation_gwh_2013%22%29%2C+"
        "MAX%28%22generation_gwh_2014%22%29%2C+"
        "MAX%28%22generation_gwh_2015%22%29%2C+"
        "MAX%28%22generation_gwh_2016%22%29%2C+"
        "MAX%28%22generation_gwh_2017%22%29%2C+"
        "MAX%28%22generation_data_source%22%29%2C+"
        "MAX%28%22estimated_generation_gwh%22%29+"
        "FROM+%22global-power-plants%22+LIMIT+1"
    )
    requests_mock.get(metadata_url, json=datasette_metadata_response)
    data_url_1 = (
        "https://global-power-plants.datasettes.com/global-power-plants.json?"
        "sql=select+*+FROM+%22global-power-plants%22+WHERE+country+%3D+%27CAN%27+"
        "LIMIT+1001+OFFSET+0"
    )
    requests_mock.get(data_url_1, json=datasette_data_response_1)
    data_url_2 = (
        "https://global-power-plants.datasettes.com/global-power-plants.json?"
        "sql=select+*+FROM+%22global-power-plants%22+WHERE+country+%3D+%27CAN%27+"
        "LIMIT+1001+OFFSET+1000"
    )
    requests_mock.get(data_url_2, json=datasette_data_response_2)

    connection = connect(":memory:")
    cursor = connection.cursor()
    sql = """
        SELECT * FROM
        "https://global-power-plants.datasettes.com/global-power-plants/global-power-plants"
        WHERE country='CAN'
    """
    data = list(cursor.execute(sql))
    assert data == datasette_results


def test_datasette_limit_offset(mocker: MockerFixture, requests_mock: Mocker) -> None:
    """
    Test a simple query with limit/offset.
    """
    mocker.patch("shillelagh.adapters.api.datasette.CACHE_EXPIRATION", DO_NOT_CACHE)

    columns_url = (
        "https://global-power-plants.datasettes.com/global-power-plants.json?"
        "sql=SELECT+*+FROM+%22global-power-plants%22+LIMIT+0"
    )
    requests_mock.get(columns_url, json=datasette_columns_response)
    metadata_url = (
        "https://global-power-plants.datasettes.com/global-power-plants.json?sql=SELECT+"
        "MAX%28%22country%22%29%2C+"
        "MAX%28%22country_long%22%29%2C+"
        "MAX%28%22name%22%29%2C+"
        "MAX%28%22gppd_idnr%22%29%2C+"
        "MAX%28%22capacity_mw%22%29%2C+"
        "MAX%28%22latitude%22%29%2C+"
        "MAX%28%22longitude%22%29%2C+"
        "MAX%28%22primary_fuel%22%29%2C+"
        "MAX%28%22other_fuel1%22%29%2C+"
        "MAX%28%22other_fuel2%22%29%2C+"
        "MAX%28%22other_fuel3%22%29%2C+"
        "MAX%28%22commissioning_year%22%29%2C+"
        "MAX%28%22owner%22%29%2C+"
        "MAX%28%22source%22%29%2C+"
        "MAX%28%22url%22%29%2C+"
        "MAX%28%22geolocation_source%22%29%2C+"
        "MAX%28%22wepp_id%22%29%2C+"
        "MAX%28%22year_of_capacity_data%22%29%2C+"
        "MAX%28%22generation_gwh_2013%22%29%2C+"
        "MAX%28%22generation_gwh_2014%22%29%2C+"
        "MAX%28%22generation_gwh_2015%22%29%2C+"
        "MAX%28%22generation_gwh_2016%22%29%2C+"
        "MAX%28%22generation_gwh_2017%22%29%2C+"
        "MAX%28%22generation_data_source%22%29%2C+"
        "MAX%28%22estimated_generation_gwh%22%29+"
        "FROM+%22global-power-plants%22+LIMIT+1"
    )
    requests_mock.get(metadata_url, json=datasette_metadata_response)
    data_url_1 = (
        "https://global-power-plants.datasettes.com/global-power-plants.json?"
        "sql=select+*+FROM+%22global-power-plants%22+WHERE+country+%3D+%27CAN%27+"
        "LIMIT+10+OFFSET+1500"
    )
    requests_mock.get(data_url_1, json=datasette_data_response_2)
    data_url_2 = (
        "https://global-power-plants.datasettes.com/global-power-plants.json?"
        "sql=select+*+FROM+%22global-power-plants%22+WHERE+country+%3D+%27CAN%27+"
        "LIMIT+1001+OFFSET+0"
    )
    requests_mock.get(data_url_2, json=datasette_data_response_1)
    data_url_3 = (
        "https://global-power-plants.datasettes.com/global-power-plants.json?"
        "sql=select+*+FROM+%22global-power-plants%22+WHERE+country+%3D+%27CAN%27+"
        "LIMIT+500+OFFSET+1000"
    )
    requests_mock.get(data_url_3, json=datasette_data_response_2)

    connection = connect(":memory:")
    cursor = connection.cursor()

    sql = """
        SELECT * FROM
        "https://global-power-plants.datasettes.com/global-power-plants/global-power-plants"
        WHERE country='CAN'
        LIMIT 10 OFFSET 1500
    """
    data = list(cursor.execute(sql))
    assert data == datasette_results[1000:1010]

    sql = """
        SELECT * FROM
        "https://global-power-plants.datasettes.com/global-power-plants/global-power-plants"
        WHERE country='CAN'
        LIMIT 1500
    """
    data = list(cursor.execute(sql))
    assert data == datasette_results


def test_datasette_no_data(mocker: MockerFixture) -> None:
    """
    Test result with no rows.
    """
    get_session = mocker.patch("shillelagh.adapters.api.datasette.get_session")
    get_session().get().json.return_value = {
        "columns": [],
        "rows": [],
    }

    with pytest.raises(ProgrammingError) as excinfo:
        DatasetteAPI("https://example.com", "database", "table")
    assert str(excinfo.value) == 'Table "table" has no data'


def test_get_metadata(requests_mock: Mocker) -> None:
    """
    Test ``get_metadata``.
    """
    requests_mock.get(
        "https://example.com/database.json?sql=SELECT+%2A+FROM+%22table%22+LIMIT+0",
        json={"columns": ["a", "b"]},
    )
    requests_mock.get(
        (
            "https://example.com/database.json?"
            "sql=SELECT+MAX%28%22a%22%29%2C+MAX%28%22b%22%29+FROM+%22table%22+LIMIT+1"
        ),
        json={"rows": [[1, 2], [3, 4]]},
    )
    requests_mock.get(
        "https://example.com/-/metadata.json",
        json={
            "databases": {"database": {"tables": {"table": {"foo": "bar"}}}},
        },
    )

    adapter = DatasetteAPI("https://example.com", "database", "table")
    assert adapter.get_metadata() == {"foo": "bar"}


def test_get_field() -> None:
    """
    Test ``get_field``.
    """
    assert isinstance(get_field(1), Integer)
    assert isinstance(get_field(1.1), Float)
    assert isinstance(get_field("test"), String)
    assert isinstance(get_field("2021-01-01"), ISODate)
    assert isinstance(get_field("2021-01-01 00:00:00"), ISODateTime)
    assert isinstance(get_field(None), String)


def test_is_known_domain() -> None:
    """
    Test ``is_known_domain``.
    """
    assert is_known_domain("latest.datasette.io")
    assert is_known_domain("san-francisco.datasettes.com")
    assert not is_known_domain("example.com")


def test_is_datasette(mocker: MockerFixture, requests_mock: Mocker) -> None:
    """
    Test ``is_datasette``.
    """
    mocker.patch("shillelagh.adapters.api.datasette.CACHE_EXPIRATION", DO_NOT_CACHE)

    assert not is_datasette("https://example.com/")

    requests_mock.get(
        "https://example.com/-/versions.json",
        json={
            "python": {
                "version": "3.8.11",
                "full": "3.8.11 (default, Aug 17 2021, 15:56:41) \n[GCC 10.2.1 20210110]",
            },
            "datasette": {
                "version": "0.59a1",
                "note": "7e15422aacfa9e9735cb9f9beaa32250edbf4905",
            },
            "asgi": "3.0",
            "uvicorn": "0.15.0",
            "sqlite": {
                "version": "3.35.4",
                "fts_versions": ["FTS5", "FTS4", "FTS3"],
                "extensions": {"json1": None},
                "compile_options": [
                    "ALLOW_COVERING_INDEX_SCAN",
                    "COMPILER=gcc-4.8.2 20140120 (Red Hat 4.8.2-15)",
                    "ENABLE_FTS3",
                    "ENABLE_FTS3_PARENTHESIS",
                    "ENABLE_FTS4",
                    "ENABLE_FTS5",
                    "ENABLE_JSON1",
                    "ENABLE_LOAD_EXTENSION",
                    "ENABLE_RTREE",
                    "ENABLE_STAT4",
                    "ENABLE_UPDATE_DELETE_LIMIT",
                    "MAX_MMAP_SIZE=1099511627776",
                    "MAX_VARIABLE_NUMBER=250000",
                    "SOUNDEX",
                    "TEMP_STORE=3",
                    "THREADSAFE=1",
                    "USE_URI",
                ],
            },
            "pysqlite3": "0.4.6",
        },
    )
    assert is_datasette("https://example.com/database/table")

    requests_mock.get(
        "https://example.com/-/versions.json",
        text="Invalid page",
    )
    assert not is_datasette("https://example.com/database/table")


def test_datasette_error(mocker: MockerFixture, requests_mock: Mocker) -> None:
    """
    Test error handling.
    """
    mocker.patch("shillelagh.adapters.api.datasette.CACHE_EXPIRATION", DO_NOT_CACHE)

    columns_url = (
        "https://global-power-plants.datasettes.com/global-power-plants.json?"
        "sql=SELECT+*+FROM+%22global-power-plants%22+LIMIT+0"
    )
    requests_mock.get(columns_url, json=datasette_columns_response)
    metadata_url = (
        "https://global-power-plants.datasettes.com/global-power-plants.json?sql=SELECT+"
        "MAX%28%22country%22%29%2C+"
        "MAX%28%22country_long%22%29%2C+"
        "MAX%28%22name%22%29%2C+"
        "MAX%28%22gppd_idnr%22%29%2C+"
        "MAX%28%22capacity_mw%22%29%2C+"
        "MAX%28%22latitude%22%29%2C+"
        "MAX%28%22longitude%22%29%2C+"
        "MAX%28%22primary_fuel%22%29%2C+"
        "MAX%28%22other_fuel1%22%29%2C+"
        "MAX%28%22other_fuel2%22%29%2C+"
        "MAX%28%22other_fuel3%22%29%2C+"
        "MAX%28%22commissioning_year%22%29%2C+"
        "MAX%28%22owner%22%29%2C+"
        "MAX%28%22source%22%29%2C+"
        "MAX%28%22url%22%29%2C+"
        "MAX%28%22geolocation_source%22%29%2C+"
        "MAX%28%22wepp_id%22%29%2C+"
        "MAX%28%22year_of_capacity_data%22%29%2C+"
        "MAX%28%22generation_gwh_2013%22%29%2C+"
        "MAX%28%22generation_gwh_2014%22%29%2C+"
        "MAX%28%22generation_gwh_2015%22%29%2C+"
        "MAX%28%22generation_gwh_2016%22%29%2C+"
        "MAX%28%22generation_gwh_2017%22%29%2C+"
        "MAX%28%22generation_data_source%22%29%2C+"
        "MAX%28%22estimated_generation_gwh%22%29+"
        "FROM+%22global-power-plants%22+LIMIT+1"
    )
    requests_mock.get(metadata_url, json=datasette_metadata_response)
    data_url_1 = (
        "https://global-power-plants.datasettes.com/global-power-plants.json?"
        "sql=select+*+FROM+%22global-power-plants%22+WHERE+country+%3D+%27CAN%27+"
        "LIMIT+1001+OFFSET+0"
    )
    requests_mock.get(
        data_url_1,
        json={
            "ok": False,
            "error": "no such table: invalid",
            "status": 400,
            "title": "Invalid SQL",
        },
    )

    connection = connect(":memory:")
    cursor = connection.cursor()
    sql = """
        SELECT * FROM
        "https://global-power-plants.datasettes.com/global-power-plants/global-power-plants"
        WHERE country='CAN'
    """
    with pytest.raises(ProgrammingError) as excinfo:
        list(cursor.execute(sql))
    assert str(excinfo.value) == "Error (Invalid SQL): no such table: invalid"


@pytest.mark.integration_test
def test_integration(adapter_kwargs) -> None:
    """
    Test fetching data from the demo Datasette.
    """
    connection = connect(":memory:", adapter_kwargs=adapter_kwargs)
    cursor = connection.cursor()

    sql = """
        SELECT *
        FROM "https://global-power-plants.datasettes.com/global-power-plants/global-power-plants"
        WHERE wepp_id = ? AND year_of_capacity_data = ?
    """
    cursor.execute(sql, ("67644", 2019))
    assert cursor.fetchall() == [
        (
            "USA",
            "United States of America",
            "145 Talmadge Solar",
            "USA0057458",
            3.8,
            40.5358,
            -74.3913,
            "Solar",
            None,
            None,
            None,
            2011.0,
            "Avidan Energy Solutions",
            "U.S. Energy Information Administration",
            "http://www.eia.gov/electricity/data/browser/",
            "U.S. Energy Information Administration",
            "67644",
            2019,
            5.0360000000000005,
            4.524,
            4.8020000000000005,
            5.051,
            4.819,
            4.626,
            5.01,
            "U.S. Energy Information Administration",
            5.84,
            5.82,
            6.06,
            5.41,
            5.17,
            "SOLAR-V1",
            "SOLAR-V1",
            "SOLAR-V1",
            "SOLAR-V1",
            "SOLAR-V1",
        ),
    ]
