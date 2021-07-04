import datetime

import dateutil.tz

from shillelagh.adapters.api.gsheets.fields import GSheetsBoolean
from shillelagh.adapters.api.gsheets.fields import GSheetsDate
from shillelagh.adapters.api.gsheets.fields import GSheetsDateTime
from shillelagh.adapters.api.gsheets.fields import GSheetsTime
from shillelagh.fields import ISODateTime
from shillelagh.types import Order


def test_GSheetsDateTime():
    assert GSheetsDateTime().parse("Date(2018,8,9,0,0,0)") == datetime.datetime(
        2018,
        9,
        9,
        0,
        0,
    )
    assert GSheetsDateTime().parse(None) is None
    assert (
        GSheetsDateTime().format(datetime.datetime(2018, 9, 9, 0, 0))
        == "09/09/2018 00:00:00"
    )
    assert GSheetsDateTime().format(None) is None
    assert (
        GSheetsDateTime().quote("09/09/2018 00:00:00")
        == "datetime '2018-09-09 00:00:00'"
    )
    assert GSheetsDateTime().quote(None) == "NULL"


def test_GSheetsDateTime_timezone():
    tz = dateutil.tz.gettz("America/Los_Angeles")
    assert (
        GSheetsDateTime(timezone=tz).parse(
            "Date(2018,8,9,0,0,0)",
        )
        == datetime.datetime(2018, 9, 9, 0, 0, tzinfo=tz)
    )
    assert (
        GSheetsDateTime(timezone=tz).format(
            datetime.datetime(2018, 1, 1, 0, 0, tzinfo=datetime.timezone.utc),
        )
        == "12/31/2017 16:00:00"
    )


def test_GSheetsDate():
    assert GSheetsDate().parse("Date(2018,0,1)") == datetime.date(2018, 1, 1)
    assert GSheetsDate().parse(None) is None
    assert GSheetsDate().format(datetime.date(2018, 1, 1)) == "2018-01-01"
    assert GSheetsDate().format(None) is None
    assert GSheetsDate().quote("2018-01-01") == "date '2018-01-01'"
    assert GSheetsDate().quote(None) == "NULL"


def test_GSheetsTime():
    assert GSheetsTime().parse([17, 0, 0, 0]) == datetime.time(
        17,
        0,
    )
    assert GSheetsTime().parse(None) is None
    assert (
        GSheetsTime().format(datetime.time(17, 0, tzinfo=datetime.timezone.utc))
        == "17:00:00+00:00"
    )
    assert GSheetsTime().format(None) is None
    assert (
        GSheetsTime().quote(datetime.time(17, 0, tzinfo=datetime.timezone.utc))
        == "timeofday '17:00:00+00:00'"
    )
    assert GSheetsTime().quote(None) == "NULL"


def test_GSheetsBoolean():
    assert GSheetsBoolean().parse("TRUE") is True
    assert GSheetsBoolean().parse("FALSE") is False
    assert GSheetsBoolean().parse(None) is None
    assert GSheetsBoolean().format(True) == "TRUE"
    assert GSheetsBoolean().format(False) == "FALSE"
    assert GSheetsBoolean().format(None) is None
    assert GSheetsBoolean().quote("TRUE") == "true"
    assert GSheetsBoolean().quote("FALSE") == "false"
    assert GSheetsBoolean().quote(None) == "NULL"


def test_comparison():
    assert GSheetsDateTime([], Order.NONE, True) != ISODateTime([], Order.NONE, True)
