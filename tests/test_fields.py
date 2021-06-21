import datetime

import pytest
from shillelagh.fields import Blob
from shillelagh.fields import Boolean
from shillelagh.fields import Date
from shillelagh.fields import DateTime
from shillelagh.fields import Field
from shillelagh.fields import Float
from shillelagh.fields import Integer
from shillelagh.fields import Order
from shillelagh.fields import String
from shillelagh.fields import Time
from shillelagh.filters import Equal
from shillelagh.types import BINARY
from shillelagh.types import DATETIME
from shillelagh.types import NUMBER
from shillelagh.types import STRING


def test_comparison():
    field1 = Field(filters=[Equal], order=Order.ASCENDING, exact=True)
    field2 = Field(filters=[Equal], order=Order.ASCENDING, exact=True)
    field3 = Field(filters=[Equal], order=Order.ASCENDING, exact=False)

    assert field1 == field2
    assert field1 != field3
    assert field1 != 42


def test_integer():
    assert Integer.parse(1) == 1
    assert Integer.parse("1") == 1
    assert Integer.parse(None) is None
    assert Integer.quote(1) == "1"


def test_blob():
    assert Blob.parse(1) == 1
    assert Blob.parse("test") == "test"
    assert Blob.parse(b"test") == b"test"
    assert Blob.parse(None) is None
    assert Blob.quote(b"\x00") == "X'00'"


def test_date():
    assert Date.parse("2020-01-01") == datetime.date(2020, 1, 1)
    assert Date.parse("2020-01-01T00:00+00:00") == datetime.date(
        2020,
        1,
        1,
    )
    assert Date.parse(None) is None
    assert Date.parse("invalid") is None
    assert Date.quote(datetime.date(2020, 1, 1)) == "'2020-01-01'"


def test_time():
    assert Time.parse("12:00+00:00") == datetime.time(
        12,
        0,
        tzinfo=datetime.timezone.utc,
    )
    assert Time.parse("12:00") == datetime.time(
        12,
        0,
        tzinfo=datetime.timezone.utc,
    )
    assert Time.parse(None) is None
    assert Time.parse("invalid") is None
    assert (
        Time.quote(datetime.time(12, 0, tzinfo=datetime.timezone.utc))
        == "'12:00:00+00:00'"
    )


def test_datetime():
    assert DateTime.parse("2020-01-01T12:00+00:00") == datetime.datetime(
        2020,
        1,
        1,
        12,
        0,
        0,
        tzinfo=datetime.timezone.utc,
    )
    assert DateTime.parse("2020-01-01T12:00") == datetime.datetime(
        2020,
        1,
        1,
        12,
        0,
        0,
        tzinfo=datetime.timezone.utc,
    )
    assert DateTime.parse(None) is None
    assert DateTime.parse("invalid") is None
    assert (
        DateTime.quote(
            datetime.datetime(2020, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc),
        )
        == "'2020-01-01T12:00:00+00:00'"
    )


def test_boolean():
    assert Boolean.parse(True) is True
    assert Boolean.parse(False) is False
    assert Boolean.parse("true") is True
    assert Boolean.parse(0) is False
    assert Boolean.quote(True) == "TRUE"
    assert Boolean.quote(False) == "FALSE"


def test_type_code():
    assert Integer == NUMBER
    assert Float == NUMBER
    assert String == STRING
    assert Date == DATETIME
    assert Time == DATETIME
    assert DateTime == DATETIME
    assert Blob == BINARY
    assert Boolean == NUMBER

    assert not NUMBER == 1


def test_quote():
    assert Integer.quote(1) == "1"
    assert Float.quote(1.0) == "1.0"
    assert String.quote("one") == "'one'"
    assert (
        DateTime.quote(
            datetime.datetime(2021, 6, 3, 7, 0, tzinfo=datetime.timezone.utc),
        )
        == "'2021-06-03T07:00:00+00:00'"
    )
