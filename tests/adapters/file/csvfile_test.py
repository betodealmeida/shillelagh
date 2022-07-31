# pylint: disable=c-extension-no-member, invalid-name, unused-argument
"""
Tests for shillelagh.adapters.file.csvfile.
"""
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import mock_open

import apsw
import pytest
from freezegun import freeze_time
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from shillelagh.adapters.file.csvfile import CSVFile, RowTracker
from shillelagh.backends.apsw.db import connect
from shillelagh.backends.apsw.vt import VTModule
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Float, Order, String
from shillelagh.filters import (
    Equal,
    Impossible,
    IsNotNull,
    IsNull,
    NotEqual,
    Operator,
    Range,
)
from shillelagh.lib import serialize

CONTENTS = """"index","temperature","site"
10,15.2,"Diamond_St"
11,13.1,"Blacktail_Loop"
12,13.3,"Platinum_St"
13,12.1,"Kodiak_Trail"
"""


def test_csvfile_get_columns(mocker: MockerFixture) -> None:
    """
    Test that columns are returned correctly.
    """
    mocker.patch("builtins.open", mock_open(read_data=CONTENTS))

    adapter = CSVFile("test.csv")

    assert adapter.get_columns() == {
        "index": Float(
            filters=[Range, Equal, NotEqual, IsNull, IsNotNull],
            order=Order.ASCENDING,
            exact=True,
        ),
        "temperature": Float(
            filters=[Range, Equal, NotEqual, IsNull, IsNotNull],
            order=Order.NONE,
            exact=True,
        ),
        "site": String(
            filters=[Range, Equal, NotEqual, IsNull, IsNotNull],
            order=Order.NONE,
            exact=True,
        ),
    }


def test_csvfile_get_cost(mocker: MockerFixture) -> None:
    """
    Test cost estimation.
    """
    mocker.patch("builtins.open", mock_open(read_data=CONTENTS))

    adapter = CSVFile("test.csv")
    assert adapter.get_cost([], []) == 0

    # constant filtering cost
    assert adapter.get_cost([("one", Operator.EQ)], []) == 1000
    assert adapter.get_cost([("one", Operator.EQ), ("two", Operator.EQ)], []) == 1000

    # linear sorting cost
    assert (
        adapter.get_cost(
            [("one", Operator.EQ), ("two", Operator.EQ)],
            [("one", Order.ASCENDING)],
        )
        == 11000
    )
    assert (
        adapter.get_cost(
            [("one", Operator.EQ), ("two", Operator.EQ)],
            [("one", Order.ASCENDING), ("two", Order.DESCENDING)],
        )
        == 21000
    )


def test_csvfile_different_types(mocker: MockerFixture) -> None:
    """
    Test type coercion when a column has different types.
    """
    contents = '''"a"
1
2.0
"test"'''
    mocker.patch("builtins.open", mock_open(read_data=contents))

    adapter = CSVFile("test.csv")

    assert adapter.get_columns() == {
        "a": String(
            filters=[Range, Equal, NotEqual, IsNull, IsNotNull],
            order=Order.NONE,
            exact=True,
        ),
    }


def test_csvfile_empty(mocker: MockerFixture) -> None:
    """
    Test empty file on instantiation.
    """
    mocker.patch("builtins.open", mock_open(read_data=""))

    with pytest.raises(ProgrammingError) as excinfo:
        CSVFile("test.csv")
    assert str(excinfo.value) == "The file has no rows"


def test_csvfile_empty_get_data(mocker: MockerFixture) -> None:
    """
    Test empty file on `get_data`.

    This is unlikely to happen, requiring the file to be modified
    externally during the connection.
    """
    mock_files = [
        mock_open(read_data=CONTENTS).return_value,
        mock_open(read_data="").return_value,
    ]
    mock_opener = mock_open()
    mock_opener.side_effect = mock_files
    mocker.patch("builtins.open", mock_opener)

    adapter = CSVFile("test.csv")
    with pytest.raises(ProgrammingError) as excinfo:
        list(adapter.get_data({}, []))
    assert str(excinfo.value) == "The file has no rows"


def test_csvfile_unordered(mocker: MockerFixture) -> None:
    """
    Test order return when data is not sorted.
    """
    contents = """"a"
1
2
1"""
    mocker.patch("builtins.open", mock_open(read_data=contents))

    adapter = CSVFile("test.csv")

    assert adapter.get_columns() == {
        "a": Float(
            filters=[Range, Equal, NotEqual, IsNull, IsNotNull],
            order=Order.NONE,
            exact=True,
        ),
    }


def test_csvfile_single_row_of_data(mocker: MockerFixture) -> None:
    """
    Test adapter when we have only 1 row of data.

    In this case, order cannot be determined.
    """
    contents = """"a","b"
1,2"""
    mocker.patch("builtins.open", mock_open(read_data=contents))

    adapter = CSVFile("test.csv")

    assert adapter.get_columns() == {
        "a": Float(
            filters=[Range, Equal, NotEqual, IsNull, IsNotNull],
            order=Order.NONE,
            exact=True,
        ),
        "b": Float(
            filters=[Range, Equal, NotEqual, IsNull, IsNotNull],
            order=Order.NONE,
            exact=True,
        ),
    }
    assert list(adapter.get_data({}, [])) == [{"a": 1.0, "b": 2.0, "rowid": 0}]


def test_csvfile_get_data(mocker: MockerFixture) -> None:
    """
    Test ``get_data``.
    """
    mocker.patch("builtins.open", mock_open(read_data=CONTENTS))

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


def test_csvfile_get_data_impossible_filter(mocker: MockerFixture) -> None:
    """
    Test that impossible conditions return no data.
    """
    mocker.patch("builtins.open", mock_open(read_data=CONTENTS))

    adapter = CSVFile("test.csv")
    assert list(adapter.get_data({"index": Impossible()}, [])) == []


def test_csvfile(fs: FakeFilesystem) -> None:
    """
    Test the whole workflow.
    """
    with open("test.csv", "w", encoding="utf-8") as fp:
        fp.write(CONTENTS)

    connection = apsw.Connection(":memory:")
    cursor = connection.cursor()
    connection.createmodule("csvfile", VTModule(CSVFile))
    cursor.execute(
        f"""CREATE VIRTUAL TABLE test USING csvfile('{serialize('test.csv')}')""",
    )

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
    with open("test.csv", encoding="utf-8") as fp:
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


def test_csvfile_close_not_modified(fs: FakeFilesystem) -> None:
    """
    Test closing the file when it hasn't been modified.
    """
    with freeze_time("2022-01-01T00:00:00Z"):
        with open("test.csv", "w", encoding="utf-8") as fp:
            fp.write(CONTENTS)

    connection = apsw.Connection(":memory:")
    cursor = connection.cursor()
    connection.createmodule("csvfile", VTModule(CSVFile))
    cursor.execute(
        f"""CREATE VIRTUAL TABLE test USING csvfile('{serialize('test.csv')}')""",
    )

    sql = 'SELECT * FROM test WHERE "index" > 11'
    data = list(cursor.execute(sql))
    assert data == [(12.0, 13.3, "Platinum_St"), (13.0, 12.1, "Kodiak_Trail")]

    with freeze_time("2022-01-02T00:00:00Z"):
        connection.close()

    path = Path("test.csv")
    assert path.stat().st_mtime == datetime(2022, 1, 1, tzinfo=timezone.utc).timestamp()


def test_dispatch() -> None:
    """
    Test the URI dispatcher.
    """
    with open("test.csv", "w", encoding="utf-8") as fp:
        fp.write(CONTENTS)

    connection = connect(":memory:", ["csvfile"])
    cursor = connection.cursor()

    sql = """SELECT * FROM "test.csv" WHERE "index" > 11"""
    data = list(cursor.execute(sql))
    assert data == [(12.0, 13.3, "Platinum_St"), (13.0, 12.1, "Kodiak_Trail")]


def test_drop_table() -> None:
    """
    Test that dropping the table removes the file.
    """
    with open("test.csv", "w", encoding="utf-8") as fp:
        fp.write(CONTENTS)

    connection = connect(":memory:", ["csvfile"])
    cursor = connection.cursor()

    sql = 'DROP TABLE "test.csv"'
    cursor.execute(sql)
    assert not Path("test.csv").exists()


def test_row_tracker() -> None:
    """
    Test the RowTracker.
    """
    rows = [{"col0_": 1}, {"col0_": 2}]
    row_tracker = RowTracker(iter(rows))
    assert next(row_tracker) == {"col0_": 1}
    assert next(row_tracker) == {"col0_": 2}
