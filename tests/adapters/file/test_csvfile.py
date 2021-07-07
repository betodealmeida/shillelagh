from unittest.mock import mock_open

import apsw
import pytest

from ...fakes import FakeEntryPoint
from shillelagh.adapters.base import Adapter
from shillelagh.adapters.file.csvfile import CSVFile
from shillelagh.adapters.file.csvfile import RowTracker
from shillelagh.backends.apsw.db import connect
from shillelagh.backends.apsw.vt import VTModule
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Float
from shillelagh.fields import Order
from shillelagh.fields import String
from shillelagh.filters import Impossible
from shillelagh.filters import Range


contents = """"index","temperature","site"
10,15.2,"Diamond_St"
11,13.1,"Blacktail_Loop"
12,13.3,"Platinum_St"
13,12.1,"Kodiak_Trail"
"""


def test_csvfile_get_columns(mocker):
    mocker.patch("builtins.open", mock_open(read_data=contents))

    adapter = CSVFile("test.csv")

    assert adapter.get_columns() == {
        "index": Float(filters=[Range], order=Order.ASCENDING, exact=True),
        "temperature": Float(filters=[Range], order=Order.NONE, exact=True),
        "site": String(filters=[Range], order=Order.NONE, exact=True),
    }


def test_csvfile_different_types(mocker):
    contents = '''"a"
1
2.0
"test"'''
    mocker.patch("builtins.open", mock_open(read_data=contents))

    adapter = CSVFile("test.csv")

    assert adapter.get_columns() == {
        "a": String(filters=[Range], order=Order.NONE, exact=True),
    }


def test_csvfile_empty(mocker):
    """
    Test empty file on instantiation.
    """
    mocker.patch("builtins.open", mock_open(read_data=""))

    with pytest.raises(ProgrammingError) as excinfo:
        CSVFile("test.csv")
    assert str(excinfo.value) == "The file has no rows"


def test_csvfile_empty_get_data(mocker):
    """
    Test empty file on `get_data`.

    This is unlikely to happen, requiring the file to be modified
    externally during the connection.
    """
    mock_files = [
        mock_open(read_data=contents).return_value,
        mock_open(read_data="").return_value,
    ]
    mock_opener = mock_open()
    mock_opener.side_effect = mock_files
    mocker.patch("builtins.open", mock_opener)

    adapter = CSVFile("test.csv")
    with pytest.raises(ProgrammingError) as excinfo:
        list(adapter.get_data({}, []))
    assert str(excinfo.value) == "The file has no rows"


def test_csvfile_unordered(mocker):
    contents = """"a"
1
2
1"""
    mocker.patch("builtins.open", mock_open(read_data=contents))

    adapter = CSVFile("test.csv")

    assert adapter.get_columns() == {
        "a": Float(filters=[Range], order=Order.NONE, exact=True),
    }


def test_csvfile_get_data(mocker):
    mocker.patch("builtins.open", mock_open(read_data=contents))

    adapter = CSVFile("test.csv")

    assert list(adapter.get_data({}, [])) == [
        {"rowid": 0, "index": 10.0, "temperature": 15.2, "site": "Diamond_St"},
        {"rowid": 1, "index": 11.0, "temperature": 13.1, "site": "Blacktail_Loop"},
        {"rowid": 2, "index": 12.0, "temperature": 13.3, "site": "Platinum_St"},
        {"rowid": 3, "index": 13.0, "temperature": 12.1, "site": "Kodiak_Trail"},
    ]

    assert list(adapter.get_data({"index": Range(11, None, False, False)}, [])) == [
        {"rowid": 2, "index": 12.0, "temperature": 13.3, "site": "Platinum_St"},
        {"rowid": 3, "index": 13.0, "temperature": 12.1, "site": "Kodiak_Trail"},
    ]

    assert list(adapter.get_data({"index": Range(None, 11, False, True)}, [])) == [
        {"rowid": 0, "index": 10.0, "temperature": 15.2, "site": "Diamond_St"},
        {"rowid": 1, "index": 11.0, "temperature": 13.1, "site": "Blacktail_Loop"},
    ]

    assert (
        list(
            adapter.get_data(
                {
                    "index": Range(None, 11, False, True),
                    "temperature": Range(14, None, False, False),
                },
                [],
            ),
        )
        == [{"rowid": 0, "index": 10.0, "temperature": 15.2, "site": "Diamond_St"}]
    )


def test_csvfile_get_data_impossible_filter(mocker):
    mocker.patch("builtins.open", mock_open(read_data=contents))

    adapter = CSVFile("test.csv")
    assert list(adapter.get_data({"index": Impossible()}, [])) == []


def test_csvfile(fs):
    with open("test.csv", "w") as fp:
        fp.write(contents)

    connection = apsw.Connection(":memory:")
    cursor = connection.cursor()
    connection.createmodule("csvfile", VTModule(CSVFile))
    cursor.execute("""CREATE VIRTUAL TABLE test USING csvfile('"test.csv"')""")

    sql = 'SELECT * FROM test WHERE "index" > 11'
    data = list(cursor.execute(sql))
    assert data == [(12.0, 13.3, "Platinum_St"), (13.0, 12.1, "Kodiak_Trail")]

    sql = """INSERT INTO test ("index", temperature, site) VALUES (14, 10.1, 'New_Site')"""
    cursor.execute(sql)
    sql = 'SELECT * FROM test WHERE "index" > 11'
    data = list(cursor.execute(sql))
    assert data == [
        (12.0, 13.3, "Platinum_St"),
        (13.0, 12.1, "Kodiak_Trail"),
        (14.0, 10.1, "New_Site"),
    ]

    sql = "DELETE FROM test WHERE site = 'Kodiak_Trail'"
    cursor.execute(sql)
    sql = 'SELECT * FROM test WHERE "index" > 11'
    data = list(cursor.execute(sql))
    assert data == [
        (12.0, 13.3, "Platinum_St"),
        (14.0, 10.1, "New_Site"),
    ]

    connection.close()

    # test garbage collection
    with open("test.csv") as fp:
        updated_contents = fp.read()
    assert (
        updated_contents
        == """"index","temperature","site"
10.0,15.2,"Diamond_St"
11.0,13.1,"Blacktail_Loop"
12.0,13.3,"Platinum_St"
14.0,10.1,"New_Site"
"""
    )


def test_dispatch(mocker, fs):
    entry_points = [FakeEntryPoint("csvfile", CSVFile)]
    mocker.patch(
        "shillelagh.backends.apsw.db.iter_entry_points",
        return_value=entry_points,
    )

    with open("test.csv", "w") as fp:
        fp.write(contents)

    connection = connect(":memory:", ["csvfile"])
    cursor = connection.cursor()

    sql = """SELECT * FROM "csv://test.csv" WHERE "index" > 11"""
    data = list(cursor.execute(sql))
    assert data == [(12.0, 13.3, "Platinum_St"), (13.0, 12.1, "Kodiak_Trail")]


def test_row_tracker():
    rows = [{"col0_": 1}, {"col0_": 2}]
    row_tracker = RowTracker(iter(rows))
    assert next(row_tracker) == {"col0_": 1}
    assert next(row_tracker) == {"col0_": 2}
