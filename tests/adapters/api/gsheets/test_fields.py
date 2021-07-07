import datetime

import dateutil.tz

from shillelagh.adapters.api.gsheets.fields import GSheetsBoolean
from shillelagh.adapters.api.gsheets.fields import GSheetsDate
from shillelagh.adapters.api.gsheets.fields import GSheetsDateTime
from shillelagh.adapters.api.gsheets.fields import GSheetsTime
from shillelagh.fields import ISODateTime
from shillelagh.fields import Order


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


def test_GSheetsDateTime_to_unformatted():
    assert (
        GSheetsDateTime(timezone=datetime.timezone.utc).to_unformatted(
            datetime.datetime(2018, 9, 1, tzinfo=datetime.timezone.utc),
        )
        == 43344
    )
    assert (
        GSheetsDateTime(timezone=datetime.timezone.utc).to_unformatted(
            datetime.datetime(2018, 1, 1, tzinfo=datetime.timezone.utc),
        )
        == 43101
    )
    assert GSheetsDateTime().to_unformatted(None) == ""


def test_GSheetsDateTime_from_unformatted():
    assert (
        GSheetsDateTime(timezone=datetime.timezone.utc).from_unformatted(
            43344,
        )
        == datetime.datetime(2018, 9, 1, tzinfo=datetime.timezone.utc)
    )

    assert (
        GSheetsDateTime(timezone=datetime.timezone.utc).from_unformatted(
            43101,
        )
        == datetime.datetime(2018, 1, 1, tzinfo=datetime.timezone.utc)
    )
    assert GSheetsDateTime().from_unformatted("") is None


def test_GSheetsDate_to_unformatted():
    assert GSheetsDate().to_unformatted(datetime.date(2018, 9, 1)) == 43344
    assert GSheetsDate().to_unformatted(datetime.date(2018, 1, 1)) == 43101
    assert GSheetsDate().to_unformatted(None) == ""


def test_GSheetsDate_from_unformatted():
    assert GSheetsDate().from_unformatted(43344) == datetime.date(2018, 9, 1)
    assert GSheetsDate().from_unformatted(43101) == datetime.date(2018, 1, 1)
    assert GSheetsDate().from_unformatted("") is None


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
        == "5:00:00 PM"
    )
    assert GSheetsTime().format(None) is None
    assert GSheetsTime().quote("5:00:00 PM") == "timeofday '17:00:00'"
    assert GSheetsTime().quote(None) == "NULL"


def test_GSheetsBoolean():
    assert GSheetsBoolean().parse(True) is True
    assert GSheetsBoolean().parse(False) is False
    assert GSheetsBoolean().parse(None) is None
    assert GSheetsBoolean().format(True) is True
    assert GSheetsBoolean().format(False) is False
    assert GSheetsBoolean().format(None) is None
    assert GSheetsBoolean().quote(True) == "true"
    assert GSheetsBoolean().quote(False) == "false"
    assert GSheetsBoolean().quote(None) == "NULL"


def test_comparison():
    assert GSheetsDateTime([], Order.NONE, True) != ISODateTime([], Order.NONE, True)
