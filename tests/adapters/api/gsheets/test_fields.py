# pylint: disable=invalid-name
"""
Tests for shilellagh.adapters.api.gsheets.fields.
"""
import datetime

import dateutil.tz
import pytest

from shillelagh.adapters.api.gsheets.fields import convert_pattern_to_format
from shillelagh.adapters.api.gsheets.fields import GSheetsBoolean
from shillelagh.adapters.api.gsheets.fields import GSheetsDate
from shillelagh.adapters.api.gsheets.fields import GSheetsDateTime
from shillelagh.adapters.api.gsheets.fields import GSheetsNumber
from shillelagh.adapters.api.gsheets.fields import GSheetsString
from shillelagh.adapters.api.gsheets.fields import GSheetsTime
from shillelagh.fields import ISODateTime
from shillelagh.fields import Order


def test_convert_pattern_to_format():
    """
    Test pattern conversion from ICU to strftime.
    """
    assert convert_pattern_to_format("M/d/yyyy H:mm:ss") == "%m/%d/%Y %H:%M:%S"
    assert convert_pattern_to_format("M/d/yyyy") == "%m/%d/%Y"
    assert convert_pattern_to_format("h:mm:ss am/pm") == "%I:%M:%S %p"

    with pytest.raises(NotImplementedError) as excinfo:
        convert_pattern_to_format("hh:mm:ss am/pm")
    assert str(excinfo.value) == (
        'Unknown pattern "hh:mm:ss am/pm". Please file a ticket at '
        "https://github.com/betodealmeida/shillelagh/issues."
    )


def test_comparison():
    """
    Test that a GSheets field is different from a standard field.
    """
    assert GSheetsDateTime([], Order.NONE, True) != ISODateTime([], Order.NONE, True)
    assert GSheetsDateTime([], Order.NONE, True) == GSheetsDateTime(
        [],
        Order.NONE,
        True,
    )


def test_GSheetsDateTime():
    """
    Test GSheetsDateTime.
    """
    assert GSheetsDateTime().parse(None) is None
    assert GSheetsDateTime().parse("") is None

    assert GSheetsDateTime(pattern="M/d/yyyy H:mm:ss").parse(
        "12/31/2020 12:34:56",
    ) == datetime.datetime(
        2020,
        12,
        31,
        12,
        34,
        56,
    )

    assert GSheetsDateTime().format(None) == ""
    assert (
        GSheetsDateTime(pattern="M/d/yyyy H:mm:ss").format(
            datetime.datetime(2020, 12, 31, 12, 34, 56),
        )
        == "12/31/2020 12:34:56"
    )

    assert GSheetsDateTime().quote(None) == "null"
    assert GSheetsDateTime().quote("") == "null"
    assert (
        GSheetsDateTime(pattern="M/d/yyyy H:mm:ss").quote("12/31/2020 12:34:56")
        == "datetime '2020-12-31 12:34:56'"
    )


def test_GSheetsDateTime_timezone():
    """
    Test GSheetsDateTime when timezone is set.
    """
    timezone = dateutil.tz.gettz("America/Los_Angeles")

    assert (
        GSheetsDateTime(pattern="M/d/yyyy H:mm:ss", timezone=timezone).parse(
            "12/31/2020 12:34:56",
        )
        == datetime.datetime(2020, 12, 31, 12, 34, 56, tzinfo=timezone)
    )

    assert (
        GSheetsDateTime(pattern="M/d/yyyy H:mm:ss", timezone=timezone).format(
            datetime.datetime(2020, 12, 31, 12, 34, 56, tzinfo=datetime.timezone.utc),
        )
        == "12/31/2020 04:34:56"
    )


def test_GSheetsDate():
    """
    Test GSheetsDate.
    """
    assert GSheetsDate().parse(None) is None
    assert GSheetsDate().parse("") is None

    assert GSheetsDate(pattern="M/d/yyyy").parse("12/31/2020") == datetime.date(
        2020,
        12,
        31,
    )

    assert GSheetsDate().format(None) == ""
    assert (
        GSheetsDate(pattern="M/d/yyyy").format(datetime.date(2020, 12, 31))
        == "12/31/2020"
    )

    assert GSheetsDate().quote(None) == "null"
    assert GSheetsDate().quote("") == "null"
    assert GSheetsDate(pattern="M/d/yyyy").quote("12/31/2020") == "date '2020-12-31'"


def test_GSheetsTime():
    """
    Test GSheetsTime.
    """
    assert GSheetsTime().parse(None) is None
    assert GSheetsTime().parse("") is None

    assert GSheetsTime(pattern="h:mm:ss am/pm").parse("12:34:56 AM") == datetime.time(
        0,
        34,
        56,
    )

    assert GSheetsTime().format(None) == ""
    assert (
        GSheetsTime(pattern="h:mm:ss am/pm").format(datetime.time(12, 34, 56))
        == "12:34:56 PM"
    )

    assert GSheetsTime().quote(None) == "null"
    assert GSheetsTime().quote("") == "null"
    assert (
        GSheetsTime(pattern="h:mm:ss am/pm").quote("12:34:56 AM")
        == "timeofday '00:34:56'"
    )


def test_GSheetsBoolean():
    """
    Test GSheetsBoolean.
    """
    assert GSheetsBoolean().parse(None) is None
    assert GSheetsBoolean().parse("") is None
    assert GSheetsBoolean().parse("TRUE") is True
    assert GSheetsBoolean().parse("FALSE") is False

    assert GSheetsBoolean().format(None) == ""
    assert GSheetsBoolean().format(True) == "TRUE"
    assert GSheetsBoolean().format(False) == "FALSE"

    assert GSheetsBoolean().quote(None) == "null"
    assert GSheetsBoolean().quote("") == "null"
    assert GSheetsBoolean().quote("TRUE") == "true"
    assert GSheetsBoolean().quote("FALSE") == "false"


def test_GSheetsNumber():
    """
    Test GSheetsNumber.
    """
    assert GSheetsNumber().parse(None) is None
    assert GSheetsNumber().parse("") is None
    assert GSheetsNumber().parse("1") == 1.0
    assert GSheetsNumber().parse("1.0") == 1.0

    assert isinstance(GSheetsNumber().parse("1"), int)
    assert isinstance(GSheetsNumber().parse("1.0"), float)

    assert GSheetsNumber().format(None) == ""
    assert GSheetsNumber().format(1) == "1"
    assert GSheetsNumber().format(1.0) == "1.0"

    assert GSheetsNumber().quote(None) == "null"
    assert GSheetsNumber().quote("") == "null"
    assert GSheetsNumber().quote(1) == "1"
    assert GSheetsNumber().quote(1.0) == "1.0"


def test_GSheetsString():
    """
    Test GSheetsString.
    """
    assert GSheetsString().parse(None) is None
    assert GSheetsString().parse("") is None
    assert GSheetsString().parse("test") == "test"

    assert GSheetsString().format(None) == ""
    assert GSheetsString().format("test") == "test"

    assert GSheetsString().quote(None) == "null"
    assert GSheetsString().quote("") == "null"
    assert GSheetsString().quote("test") == "'test'"
