import datetime

from shillelagh.fields import Blob
from shillelagh.fields import Boolean
from shillelagh.fields import Date
from shillelagh.fields import Field
from shillelagh.fields import Integer
from shillelagh.fields import Order
from shillelagh.fields import Time
from shillelagh.filters import Equal


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


def test_blob():
    assert Blob.parse(1) == b"\x00"
    assert Blob.parse("test") == b"test"
    assert Blob.parse(b"test") == b"test"


def test_date():
    assert Date.parse("2020-01-01") == datetime.date(2020, 1, 1)


def test_time():
    assert Time.parse("12:00+00:00") == datetime.time(
        12,
        0,
        tzinfo=datetime.timezone.utc,
    )


def test_boolean():
    assert Boolean.parse(True) is True
    assert Boolean.parse(False) is False
    assert Boolean.parse("true") is True
    assert Boolean.parse(0) is False
