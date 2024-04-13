"""
Test the Pandas in-memory adapter.
"""

import pandas as pd
import pytest
from pytest_mock import MockerFixture

from shillelagh.adapters.memory.pandas import PandasMemory, find_dataframe
from shillelagh.backends.apsw.db import connect
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Order
from shillelagh.filters import Equal, IsNotNull, IsNull, NotEqual, Operator


def test_pandas() -> None:
    """
    Test basic operations with a dataframe.
    """
    mydf = pd.DataFrame(  # noqa: F841  pylint: disable=unused-variable
        [
            {"index": 10, "temperature": 15.2, "site": "Diamond_St"},
            {"index": 11, "temperature": 13.1, "site": "Blacktail_Loop"},
            {"index": 12, "temperature": 13.3, "site": "Platinum_St"},
            {"index": 13, "temperature": 12.1, "site": "Kodiak_Trail"},
        ],
    )

    connection = connect(":memory:")
    cursor = connection.cursor()

    sql = "SELECT * FROM mydf"
    cursor.execute(sql)
    assert cursor.fetchall() == [
        (10, 15.2, "Diamond_St"),
        (11, 13.1, "Blacktail_Loop"),
        (12, 13.3, "Platinum_St"),
        (13, 12.1, "Kodiak_Trail"),
    ]

    sql = "SELECT * FROM mydf WHERE temperature > 10 AND temperature < 10"
    cursor.execute(sql)
    assert cursor.fetchall() == []

    sql = "SELECT * FROM mydf WHERE temperature > 13 ORDER BY site DESC"
    cursor.execute(sql)
    assert cursor.fetchall() == [
        (12, 13.3, "Platinum_St"),
        (10, 15.2, "Diamond_St"),
        (11, 13.1, "Blacktail_Loop"),
    ]

    sql = "SELECT * FROM mydf WHERE temperature < 13"
    cursor.execute(sql)
    assert cursor.fetchall() == [
        (13, 12.1, "Kodiak_Trail"),
    ]

    sql = "SELECT * FROM mydf WHERE site = 'Platinum_St'"
    cursor.execute(sql)
    assert cursor.fetchall() == [
        (12, 13.3, "Platinum_St"),
    ]

    sql = """INSERT INTO mydf ("index", temperature, site) VALUES (14, 10.1, 'New_Site')"""
    cursor.execute(sql)
    sql = 'SELECT * FROM mydf WHERE "index" > 11'
    cursor.execute(sql)
    assert cursor.fetchall() == [
        (12, 13.3, "Platinum_St"),
        (13, 12.1, "Kodiak_Trail"),
        (14, 10.1, "New_Site"),
    ]

    sql = "DELETE FROM mydf WHERE site = 'New_Site'"
    cursor.execute(sql)
    sql = 'SELECT * FROM mydf WHERE "index" > 11'
    cursor.execute(sql)
    assert cursor.fetchall() == [
        (12, 13.3, "Platinum_St"),
        (13, 12.1, "Kodiak_Trail"),
    ]

    sql = "UPDATE mydf SET temperature = temperature * 2"
    cursor.execute(sql)
    sql = "SELECT * FROM mydf"
    cursor.execute(sql)
    assert cursor.fetchall() == [
        (10, 30.4, "Diamond_St"),
        (11, 26.2, "Blacktail_Loop"),
        (12, 26.6, "Platinum_St"),
        (13, 24.2, "Kodiak_Trail"),
    ]

    sql = "UPDATE mydf SET temperature = temperature / 2"
    cursor.execute(sql)


def test_adapter(mocker: MockerFixture) -> None:
    """
    Test additional operations on the adapter.
    """
    mydf = pd.DataFrame(
        [
            {"index": 10, "temperature": 15.2, "site": "Diamond_St"},
            {"index": 11, "temperature": 13.1, "site": "Blacktail_Loop"},
            {"index": 12, "temperature": 13.3, "site": "Platinum_St"},
            {"index": 13, "temperature": 12.1, "site": "Kodiak_Trail"},
        ],
    )

    mock_find_dataframe = mocker.patch(
        "shillelagh.adapters.memory.pandas.find_dataframe",
    )
    mock_find_dataframe.return_value = mydf

    adapter = PandasMemory("mydf")

    assert list(adapter.get_data({"index": Equal(10)}, [])) == [
        {"rowid": 0, "index": 10, "site": "Diamond_St", "temperature": 15.2},
    ]

    assert list(adapter.get_data({"index": NotEqual(10)}, [])) == [
        {"rowid": 1, "index": 11, "temperature": 13.1, "site": "Blacktail_Loop"},
        {"rowid": 2, "index": 12, "temperature": 13.3, "site": "Platinum_St"},
        {"rowid": 3, "index": 13, "temperature": 12.1, "site": "Kodiak_Trail"},
    ]

    adapter.update_data(
        3,
        {"rowid": 5, "index": 13, "temperature": 12.1, "site": "Kodiak_Trail"},
    )
    assert list(adapter.get_data({}, [])) == [
        {"index": 10, "rowid": 0, "site": "Diamond_St", "temperature": 15.2},
        {"index": 11, "rowid": 1, "site": "Blacktail_Loop", "temperature": 13.1},
        {"index": 12, "rowid": 2, "site": "Platinum_St", "temperature": 13.3},
        {"index": 13, "rowid": 5, "site": "Kodiak_Trail", "temperature": 12.1},
    ]

    adapter.insert_data(
        {"rowid": 7, "index": 14, "temperature": 10.1, "site": "New_Site"},
    )
    assert list(adapter.get_data({}, [])) == [
        {"index": 10, "rowid": 0, "site": "Diamond_St", "temperature": 15.2},
        {"index": 11, "rowid": 1, "site": "Blacktail_Loop", "temperature": 13.1},
        {"index": 12, "rowid": 2, "site": "Platinum_St", "temperature": 13.3},
        {"index": 13, "rowid": 5, "site": "Kodiak_Trail", "temperature": 12.1},
        {"index": 14, "rowid": 7, "site": "New_Site", "temperature": 10.1},
    ]

    adapter.update_data(
        5,
        {"rowid": 3, "index": 13, "temperature": 12.1, "site": "Kodiak_Trail"},
    )
    adapter.delete_data(7)
    assert list(adapter.get_data({}, [])) == [
        {"index": 10, "rowid": 0, "site": "Diamond_St", "temperature": 15.2},
        {"index": 11, "rowid": 1, "site": "Blacktail_Loop", "temperature": 13.1},
        {"index": 12, "rowid": 2, "site": "Platinum_St", "temperature": 13.3},
        {"index": 13, "rowid": 3, "site": "Kodiak_Trail", "temperature": 12.1},
    ]

    with pytest.raises(ProgrammingError) as excinfo:
        list(adapter.get_data({"a": [1, 2, 3]}, []))  # type: ignore
    assert str(excinfo.value) == "Invalid filter: [1, 2, 3]"


def test_adapter_nulls(mocker: MockerFixture) -> None:
    """
    Test operations with nulls on the adapter.
    """
    mydf = pd.DataFrame([{"a": None, "b": 10}, {"a": 20, "b": None}])

    mock_find_dataframe = mocker.patch(
        "shillelagh.adapters.memory.pandas.find_dataframe",
    )
    mock_find_dataframe.return_value = mydf

    adapter = PandasMemory("mydf")

    rows = list(adapter.get_data({"a": IsNull()}, []))
    assert len(rows) == 1
    row = rows[0]
    assert row["a"] != row["a"]  # NaN
    assert row["b"] == 10.0
    assert row["rowid"] == 0

    rows = list(adapter.get_data({"a": IsNotNull()}, []))
    assert len(rows) == 1
    row = rows[0]
    assert row["a"] == 20.0
    assert row["b"] != row["b"]  # NaN
    assert row["rowid"] == 1


outer_df = pd.DataFrame()


def test_find_dataframe() -> None:
    """
    Test that we can find the dataframe.
    """
    mydf = pd.DataFrame()

    def inner_scope() -> pd.DataFrame:
        return find_dataframe("mydf")

    assert inner_scope() is mydf

    assert find_dataframe("outer_df") is outer_df

    assert find_dataframe("foobar") is None
    with pytest.raises(ProgrammingError) as excinfo:
        PandasMemory("foobar")
    assert str(excinfo.value) == "Could not find dataframe"


def test_get_cost(mocker: MockerFixture) -> None:
    """
    Test cost estimation.
    """
    mydf = pd.DataFrame(
        [
            {"index": 10, "temperature": 15.2, "site": "Diamond_St"},
            {"index": 11, "temperature": 13.1, "site": "Blacktail_Loop"},
            {"index": 12, "temperature": 13.3, "site": "Platinum_St"},
            {"index": 13, "temperature": 12.1, "site": "Kodiak_Trail"},
        ],
    )

    mock_find_dataframe = mocker.patch(
        "shillelagh.adapters.memory.pandas.find_dataframe",
    )
    mock_find_dataframe.return_value = mydf

    adapter = PandasMemory(mydf)
    assert adapter.get_cost([], []) == 0

    # linear filtering cost
    assert adapter.get_cost([("one", Operator.EQ)], []) == 1000
    assert adapter.get_cost([("one", Operator.EQ), ("two", Operator.EQ)], []) == 2000

    # linear sorting cost
    assert (
        adapter.get_cost(
            [("one", Operator.EQ), ("two", Operator.EQ)],
            [("one", Order.ASCENDING)],
        )
        == 11965
    )
    assert (
        adapter.get_cost(
            [("one", Operator.EQ), ("two", Operator.EQ)],
            [("one", Order.ASCENDING), ("two", Order.DESCENDING)],
        )
        == 21931
    )


def test_integer_column_names() -> None:
    """
    Test dataframes with column names that are integers.
    """
    mydf = pd.DataFrame(  # noqa: F841  pylint: disable=unused-variable
        [
            [10, 15.2, "Diamond_St"],
            [11, 13.1, "Blacktail_Loop"],
            [12, 13.3, "Platinum_St"],
            [13, 12.1, "Kodiak_Trail"],
        ],
    )

    connection = connect(":memory:")
    cursor = connection.cursor()

    sql = 'SELECT "0", "1", "2" FROM mydf'
    cursor.execute(sql)
    assert cursor.fetchall() == [
        (10, 15.2, "Diamond_St"),
        (11, 13.1, "Blacktail_Loop"),
        (12, 13.3, "Platinum_St"),
        (13, 12.1, "Kodiak_Trail"),
    ]


def test_empty_dataframe() -> None:
    """
    Test that empty dataframes work.
    """
    emptydf = pd.DataFrame({"a": []})  # noqa: F841  pylint: disable=unused-variable

    connection = connect(":memory:")
    cursor = connection.cursor()

    sql = "SELECT * FROM emptydf"
    cursor.execute(sql)
    assert cursor.fetchall() == []
