"""
Test the HTML table scraper.
"""

import pandas as pd
import pytest
from pytest_mock import MockerFixture

from shillelagh.backends.apsw.db import connect
from shillelagh.exceptions import ProgrammingError


def test_html_table(mocker: MockerFixture) -> None:
    """
    Test basic operations with a dataframe.
    """
    df = pd.DataFrame(  # noqa: F841  pylint: disable=unused-variable, invalid-name
        [
            {"index": 10, "temperature": 15.2, "site": "Diamond_St"},
            {"index": 11, "temperature": 13.1, "site": "Blacktail_Loop"},
            {"index": 12, "temperature": 13.3, "site": "Platinum_St"},
            {"index": 13, "temperature": 12.1, "site": "Kodiak_Trail"},
        ],
    )
    mock_pd = mocker.patch("shillelagh.adapters.api.html_table.pd")
    mock_pd.read_html.return_value = [df]

    connection = connect(":memory:", adapters=["htmltableapi"])
    cursor = connection.cursor()
    sql = 'SELECT * FROM "https://example.org/"'
    cursor.execute(sql)
    assert cursor.fetchall() == [
        (10, 15.2, "Diamond_St"),
        (11, 13.1, "Blacktail_Loop"),
        (12, 13.3, "Platinum_St"),
        (13, 12.1, "Kodiak_Trail"),
    ]

    mock_pd.read_html.side_effect = Exception("Invalid URL")

    connection = connect(":memory:", adapters=["htmltableapi"])
    cursor = connection.cursor()
    sql = 'SELECT * FROM "https://example.org/"'
    with pytest.raises(ProgrammingError) as excinfo:
        cursor.execute(sql)
    assert str(excinfo.value) == "Unsupported table: https://example.org/"


def test_html_table_fragment(mocker: MockerFixture) -> None:
    """
    Test the fragment used for the table index.
    """
    df = pd.DataFrame(  # noqa: F841  pylint: disable=unused-variable, invalid-name
        [
            {"index": 10, "temperature": 15.2, "site": "Diamond_St"},
            {"index": 11, "temperature": 13.1, "site": "Blacktail_Loop"},
            {"index": 12, "temperature": 13.3, "site": "Platinum_St"},
            {"index": 13, "temperature": 12.1, "site": "Kodiak_Trail"},
        ],
    )
    mock_pd = mocker.patch("shillelagh.adapters.api.html_table.pd")
    mock_pd.read_html.return_value = [None, df]

    connection = connect(":memory:", adapters=["htmltableapi"])
    cursor = connection.cursor()
    sql = 'SELECT * FROM "https://example.org/#1"'
    cursor.execute(sql)
    assert cursor.fetchall() == [
        (10, 15.2, "Diamond_St"),
        (11, 13.1, "Blacktail_Loop"),
        (12, 13.3, "Platinum_St"),
        (13, 12.1, "Kodiak_Trail"),
    ]

    mock_pd.read_html.return_value = [df]

    connection = connect(":memory:", adapters=["htmltableapi"])
    cursor = connection.cursor()
    sql = 'SELECT * FROM "https://example.org/#anchor"'
    cursor.execute(sql)
    assert cursor.fetchall() == [
        (10, 15.2, "Diamond_St"),
        (11, 13.1, "Blacktail_Loop"),
        (12, 13.3, "Platinum_St"),
        (13, 12.1, "Kodiak_Trail"),
    ]
