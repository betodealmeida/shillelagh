import datetime

import dateutil.tz
from shillelagh.adapters.api.gsheets.fields import GSheetsBoolean
from shillelagh.adapters.api.gsheets.fields import GSheetsDate
from shillelagh.adapters.api.gsheets.fields import GSheetsDateTime
from shillelagh.adapters.api.gsheets.fields import GSheetsTime
from shillelagh.fields import DateTime
from shillelagh.types import Order


def test_fields():
    assert GSheetsDateTime().parse(None) is None
    assert GSheetsDateTime().parse("Date(2018,8,9,0,0,0)") == datetime.datetime(
        2018,
        9,
        9,
        0,
        0,
    )
    assert (
        GSheetsDateTime().quote(
            datetime.datetime(2018, 9, 9, 0, 0, tzinfo=datetime.timezone.utc),
        )
        == "datetime '2018-09-09 00:00:00+00:00'"
    )

    assert GSheetsDate().parse(None) is None
    assert GSheetsDate().parse("Date(2018,0,1)") == datetime.date(2018, 1, 1)
    assert GSheetsDate().quote(datetime.date(2018, 1, 1)) == "date '2018-01-01'"

    assert GSheetsTime().parse(None) is None
    assert GSheetsTime().parse([17, 0, 0, 0]) == datetime.time(
        17,
        0,
    )
    assert (
        GSheetsTime().quote(datetime.time(17, 0, tzinfo=datetime.timezone.utc))
        == "timeofday '17:00:00+00:00'"
    )

    assert GSheetsBoolean().parse(None) is None
    assert GSheetsBoolean().parse("TRUE")
    assert not GSheetsBoolean().parse("FALSE")
    assert GSheetsBoolean().quote(True) == "true"
    assert GSheetsBoolean().quote(False) == "false"

    assert GSheetsDateTime().format(None) is None
    assert (
        GSheetsDateTime().format(
            datetime.datetime(2018, 1, 1, 0, 0, tzinfo=datetime.timezone.utc),
        )
        == "01/01/2018 00:00:00"
    )
    tz = dateutil.tz.gettz("America/Los_Angeles")
    assert (
        GSheetsDateTime(timezone=tz).format(
            datetime.datetime(2018, 1, 1, 0, 0, tzinfo=datetime.timezone.utc),
        )
        == "12/31/2017 16:00:00"
    )

    print(GSheetsDateTime([], Order.NONE, True) != DateTime([], Order.NONE, True))
    assert GSheetsDateTime([], Order.NONE, True) != DateTime([], Order.NONE, True)
