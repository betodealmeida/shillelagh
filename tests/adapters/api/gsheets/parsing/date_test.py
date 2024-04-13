"""
Test the date/time pattern handling (parsing and formatting).
"""

# pylint: disable=protected-access
from datetime import date, datetime, time, timedelta
from typing import cast

import pytest

from shillelagh.adapters.api.gsheets.parsing.base import LITERAL, Token
from shillelagh.adapters.api.gsheets.parsing.date import (
    AMPM,
    AP,
    DD,
    DDD,
    MM,
    MMM,
    MMMM,
    MMMMM,
    SS,
    YY,
    YYYY,
    ZERO,
    D,
    DDDDPlus,
    H,
    HHPlus,
    HPlusDuration,
    M,
    Meridiem,
    MPlusDuration,
    S,
    SPlusDuration,
    format_date_time_pattern,
    infer_column_type,
    parse_date_time_pattern,
    tokenize,
)

classes = [
    H,
    HHPlus,
    M,
    MM,
    MMM,
    MMMM,
    MMMMM,
    S,
    SS,
    HPlusDuration,
    MPlusDuration,
    SPlusDuration,
    D,
    DD,
    DDD,
    DDDDPlus,
    YY,
    YYYY,
    AP,
    AMPM,
    ZERO,
    LITERAL,
]


def test_implementation() -> None:
    """
    Test the examples from the reference.

    https://developers.google.com/sheets/api/guides/formats?hl=en
    """
    timestamp = datetime(2016, 4, 5, 16, 8, 53, 528000)

    assert format_date_time_pattern(timestamp, "h:mm:ss.00 a/p") == "4:08:53.53 p"
    assert parse_date_time_pattern("4:08:53.53 p", "h:mm:ss.00 a/p", time) == time(
        16,
        8,
        53,
        530000,
    )

    assert format_date_time_pattern(timestamp, 'hh:mm A/P".M."') == "04:08 P.M."
    assert parse_date_time_pattern("04:08 P.M.", 'hh:mm A/P".M."', time) == time(16, 8)

    assert format_date_time_pattern(timestamp, "yyyy-mm-dd") == "2016-04-05"
    assert parse_date_time_pattern("2016-04-05", "yyyy-mm-dd", date) == date(2016, 4, 5)

    # unsupported parse
    assert (
        format_date_time_pattern(timestamp, r"mmmm d \[dddd\]") == "April 5 [Tuesday]"
    )
    assert format_date_time_pattern(timestamp, "h PM, ddd mmm dd") == "4 PM, Tue Apr 05"

    assert (
        format_date_time_pattern(timestamp, "dddd, m/d/yy at h:mm")
        == "Tuesday, 4/5/16 at 16:08"
    )
    assert parse_date_time_pattern(
        "Tuesday, 4/5/16 at 16:08",
        "dddd, m/d/yy at h:mm",
        datetime,
    ) == datetime(2016, 4, 5, 16, 8)

    duration = timedelta(hours=3, minutes=13, seconds=41, microseconds=255000)

    assert format_date_time_pattern(duration, "[hh]:[mm]:[ss].000") == "03:13:41.255"
    assert (
        parse_date_time_pattern("03:13:41.255", "[hh]:[mm]:[ss].000", timedelta)
        == duration
    )

    assert format_date_time_pattern(duration, "[mmmm]:[ss].000") == "0193:41.255"
    assert (
        parse_date_time_pattern("0193:41.255", "[mmmm]:[ss].000", timedelta) == duration
    )


def test_token() -> None:
    """
    General tests for tokens.
    """
    assert H("h") == H("h")
    assert H("h") != M("m")


def test_h_token() -> None:
    """
    Test the h token.
    """
    token = H("h")

    assert H.match("h:mm:ss", [])
    assert not H.match("hh:mm:ss", [])
    assert not H.match("s", [])

    assert H.consume("h:mm:ss", []) == (token, ":mm:ss")
    with pytest.raises(Exception) as excinfo:
        H.consume("hh:mm:ss", [])
    assert str(excinfo.value) == "Token could not find match"

    tokens = list(tokenize("h:mm:ss", classes))
    assert token.format(datetime(2021, 11, 12, 13, 14, 15, 16), tokens) == "13"
    assert token.format(datetime(2021, 11, 12, 3, 14, 15, 16), tokens) == "3"
    tokens = list(tokenize("h:mm:ss a/p", classes))
    assert token.format(datetime(2021, 11, 12, 13, 14, 15, 16), tokens) == "1"
    tokens = list(tokenize('h:mm:ss "PM"', classes))
    assert token.format(datetime(2021, 11, 12, 13, 14, 15, 16), tokens) == "1"

    tokens = list(tokenize("h:mm:ss", classes))
    assert token.parse("123", tokens) == ({"hour": 12}, "3")
    assert token.parse("303", tokens) == ({"hour": 3}, "03")
    with pytest.raises(Exception) as excinfo:
        token.parse("invalid", tokens)
    assert str(excinfo.value) == "Cannot parse value: invalid"


def test_hhplus_token() -> None:
    """
    Test the hhh+ token.
    """
    token = HHPlus("hhh")

    assert HHPlus.match("hhh:mm:ss", [])
    assert HHPlus.match("hhhh:mm:ss", [])
    assert not HHPlus.match("h:mm:ss", [])

    tokens = list(tokenize("hhh:mm:ss", classes))
    assert token.format(datetime(2021, 11, 12, 13, 14, 15, 16), tokens) == "13"
    assert token.format(datetime(2021, 11, 12, 3, 14, 15, 16), tokens) == "03"
    tokens = list(tokenize("hhh:mm:ss a/p", classes))
    assert token.format(datetime(2021, 11, 12, 13, 14, 15, 16), tokens) == "01"

    tokens = list(tokenize("hhh:mm:ss", classes))
    assert token.parse("123", tokens) == ({"hour": 12}, "3")
    assert token.parse("303", tokens) == ({"hour": 30}, "3")


def test_m_token() -> None:
    """
    Test the m token.
    """
    token: Token = M("m")

    assert M.match("m/d/y", [])
    assert not M.match("mm/dd/yyyy", [])
    assert not M.match("M/d/y", [])

    assert M.consume("m/d/y", []) == (token, "/d/y")

    tokens = list(tokenize("m/d/y", classes))
    token = cast(M, tokens[0])
    assert token._is_minute(tokens) is False
    tokens = list(tokenize("h//m", classes))
    token = cast(M, tokens[2])
    assert token._is_minute(tokens) is True
    tokens = list(tokenize("m//s", classes))
    token = cast(M, tokens[0])
    assert token._is_minute(tokens) is True

    tokens = list(tokenize("m/d/y", classes))
    with pytest.raises(Exception) as excinfo:
        token.format(datetime(2021, 11, 12, 13, 14, 15, 16), tokens)
    assert str(excinfo.value) == "Token is not present in list of tokens"
    token = tokens[0]
    assert token.format(datetime(2021, 11, 12, 13, 14, 15, 16), tokens) == "11"
    tokens = list(tokenize("h:m:s", classes))
    token = tokens[2]
    assert token.format(datetime(2021, 11, 12, 13, 14, 15, 16), tokens) == "14"
    tokens = list(tokenize("m/d/y", classes))
    token = tokens[0]
    with pytest.raises(Exception) as excinfo:
        token.format(time(13, 14, 15, 16), tokens)
    assert str(excinfo.value) == "Cannot format value: 13:14:15.000016"

    tokens = list(tokenize("m/d/y", classes))
    token = tokens[0]
    assert token.parse("123", tokens) == ({"month": 12}, "3")
    assert token.parse("303", tokens) == ({"month": 3}, "03")
    tokens = list(tokenize("h:m:s", classes))
    token = tokens[2]
    assert token.parse("14:15", tokens) == ({"minute": 14}, ":15")
    with pytest.raises(Exception) as excinfo:
        token.parse("invalid", tokens)
    assert str(excinfo.value) == "Cannot parse value: invalid"


def test_mm_token() -> None:
    """
    Test the mm token.
    """
    token: Token = MM("mm")

    assert MM.match("mm/dd/yyy", [])
    assert not MM.match("mmm/dd/yyy", [])

    assert MM.consume("mm/dd/yyyy", []) == (token, "/dd/yyyy")

    tokens = list(tokenize("mm/dd/yyy", classes))
    token = tokens[0]
    assert token.format(datetime(2021, 11, 12, 13, 14, 15, 16), tokens) == "11"
    assert token.parse("123", tokens) == ({"month": 12}, "3")


def test_mmm_token() -> None:
    """
    Test the mmm token.
    """
    token = MMM("mmm")

    assert MMM.match("mmm/dd/yyy", [])
    assert not MMM.match("mm/dd/yyy", [])

    assert MMM.consume("mmm/dd/yyyy", []) == (token, "/dd/yyyy")

    tokens = list(tokenize("mmm/dd/yyy", classes))
    assert token.format(datetime(2021, 11, 12, 13, 14, 15, 16), tokens) == "Nov"
    assert token.parse("Mar 1st", tokens) == ({"month": 3}, " 1st")


def test_mmmm_token() -> None:
    """
    Test the mmm token.
    """
    token = MMMM("mmmm")

    assert MMMM.match("mmmm/dd/yyy", [])
    assert not MMMM.match("mmm/dd/yyy", [])

    assert MMMM.consume("mmmm/dd/yyyy", []) == (token, "/dd/yyyy")

    tokens = list(tokenize("mmmm/dd/yyy", classes))
    assert token.format(datetime(2021, 11, 12, 13, 14, 15, 16), tokens) == "November"
    assert token.parse("March 1st", tokens) == ({"month": 3}, " 1st")


def test_mmmmm_token() -> None:
    """
    Test the mmmmm token.
    """
    token = MMMMM("mmmmm")

    assert MMMMM.match("mmmmm/dd/yyy", [])
    assert not MMMMM.match("mmm/dd/yyy", [])

    assert MMMMM.consume("mmmmm/dd/yyyy", []) == (token, "/dd/yyyy")

    tokens = list(tokenize("mmmmm/dd/yyy", classes))
    assert token.format(datetime(2021, 11, 12, 13, 14, 15, 16), tokens) == "N"
    assert token.parse("F 1st", tokens) == ({"month": 2}, " 1st")
    with pytest.raises(Exception) as excinfo:
        token.parse("Z 1st", tokens)
    assert str(excinfo.value) == "Unable to find month letter: Z"
    with pytest.raises(Exception) as excinfo:
        token.parse("M 1st", tokens)
    assert str(excinfo.value) == "Unable to parse month letter unambiguously: M"


def test_s_token() -> None:
    """
    Test the s token.
    """
    token = S("s")

    assert S.match("s.00", [])
    assert not S.match("ss.00", [])

    assert S.consume("s.00", []) == (token, ".00")

    tokens = list(tokenize("h:m:s", classes))
    assert token.format(datetime(2021, 11, 12, 13, 14, 5, 16), tokens) == "5"
    assert token.format(datetime(2021, 11, 12, 13, 14, 5, 16), tokens) == "5"
    assert token.format(datetime(2021, 11, 12, 13, 14, 15, 16), tokens) == "15"
    assert token.parse("59.123", tokens) == ({"second": 59}, ".123")
    assert token.parse("60.123", tokens) == ({"second": 60}, ".123")
    assert token.parse("61.123", tokens) == ({"second": 61}, ".123")
    assert token.parse("62.123", tokens) == ({"second": 6}, "2.123")
    with pytest.raises(Exception) as excinfo:
        token.parse("invalid", tokens)
    assert str(excinfo.value) == "Cannot parse value: invalid"


def test_ss_token() -> None:
    """
    Test the ss token.
    """
    token = SS("ss")

    assert SS.match("ss.00", [])
    assert not SS.match("s.00", [])

    assert SS.consume("ss.00", []) == (token, ".00")

    tokens = list(tokenize("hh:mm:ss", classes))
    assert token.format(datetime(2021, 11, 12, 13, 14, 15, 16), tokens) == "15"
    assert token.format(datetime(2021, 11, 12, 13, 14, 5, 16), tokens) == "05"
    assert token.parse("59.123", tokens) == ({"second": 59}, ".123")


def test_hplusduration_token() -> None:
    """
    Test the [h+] token.
    """
    token = HPlusDuration("[hh]")

    assert HPlusDuration.match("[h]", [])
    assert HPlusDuration.match("[hh]", [])
    assert not HPlusDuration.match("hh", [])

    assert HPlusDuration.consume("[hh]:[mm]:[ss].000", []) == (token, ":[mm]:[ss].000")

    tokens = list(tokenize("[hh]:[mm]:[ss].000", classes))
    assert (
        token.format(
            timedelta(days=1, hours=2, minutes=3, seconds=4, microseconds=500000),
            tokens,
        )
        == "26"
    )
    assert (
        token.format(
            timedelta(hours=2, minutes=3, seconds=4, microseconds=500000),
            tokens,
        )
        == "02"
    )
    assert token.parse("26:03:04.500", []) == (
        {"hours": 26},
        ":03:04.500",
    )

    # should never happen
    with pytest.raises(Exception) as excinfo:
        token.parse("invalid", [])
    assert str(excinfo.value) == "Cannot parse value: invalid"


def test_mplusduration_token() -> None:
    """
    Test the [m+] token.
    """
    token = MPlusDuration("[mm]")

    assert MPlusDuration.match("[m]", [])
    assert MPlusDuration.match("[mm]", [])
    assert not MPlusDuration.match("mm", [])

    assert MPlusDuration.consume("[mm]:[ss].000", []) == (token, ":[ss].000")

    tokens = list(tokenize("[hh]:[mm]:[ss].000", classes))
    assert (
        token.format(
            timedelta(days=1, hours=2, minutes=3, seconds=4, microseconds=500000),
            tokens,
        )
        == "03"
    )

    token = MPlusDuration("[mmmm]")
    tokens = list(tokenize("[mmmm]:[ss].000", classes))
    assert (
        token.format(
            timedelta(hours=2, minutes=3, seconds=4, microseconds=500000),
            tokens,
        )
        == "0123"
    )

    token = MPlusDuration("[mm]")
    tokens = list(tokenize("[hh]:[mm]:[ss].000", classes))
    assert token.parse("03:04.500", []) == (
        {"minutes": 3},
        ":04.500",
    )

    # should never happen
    with pytest.raises(Exception) as excinfo:
        token.parse("invalid", [])
    assert str(excinfo.value) == "Cannot parse value: invalid"


def test_splusduration_token() -> None:
    """
    Test the [s+] token.
    """
    token = SPlusDuration("[ss]")

    assert SPlusDuration.match("[s]", [])
    assert SPlusDuration.match("[ss]", [])
    assert not SPlusDuration.match("ss", [])

    assert SPlusDuration.consume("[ss].000", []) == (token, ".000")

    # should never happen
    with pytest.raises(Exception) as excinfo:
        SPlusDuration.consume("invalid", [])
    assert str(excinfo.value) == "Token could not find match"

    tokens = list(tokenize("[hh]:[mm]:[ss].000", classes))
    assert (
        token.format(
            timedelta(days=1, hours=2, minutes=3, seconds=4, microseconds=500000),
            tokens,
        )
        == "04"
    )

    tokens = list(tokenize("[ss].000", classes))
    assert (
        token.format(
            timedelta(hours=2, minutes=3, seconds=4, microseconds=500000),
            tokens,
        )
        == "7384"
    )

    tokens = list(tokenize("[hh]:[mm]:[ss].000", classes))
    assert token.parse("04.500", []) == (
        {"seconds": 4},
        ".500",
    )

    # should never happen
    with pytest.raises(Exception) as excinfo:
        token.parse("invalid", [])
    assert str(excinfo.value) == "Cannot parse value: invalid"


def test_d_token() -> None:
    """
    Test the d token.
    """
    token = D("d")

    assert D.match("d", [])
    assert not D.match("dd", [])

    assert D.consume("d/m/y", []) == (token, "/m/y")

    tokens = list(tokenize("d/m/y", classes))
    assert token.format(datetime(2021, 11, 12, 13, 14, 15, 16), tokens) == "12"
    assert token.format(datetime(2021, 11, 2, 13, 14, 15, 16), tokens) == "2"
    assert token.parse("12/11/21", tokens) == ({"day": 12}, "/11/21")
    with pytest.raises(Exception) as excinfo:
        token.parse("invalid", tokens)
    assert str(excinfo.value) == "Cannot parse value: invalid"


def test_dd_token() -> None:
    """
    Test the dd token.
    """
    token = DD("dd")

    assert DD.match("dd", [])
    assert not DD.match("d", [])
    assert not DD.match("ddd", [])

    assert DD.consume("dd/mm/yyyy", []) == (token, "/mm/yyyy")

    tokens = list(tokenize("dd/mm/yyyy", classes))
    assert token.format(datetime(2021, 11, 12, 13, 14, 15, 16), tokens) == "12"
    assert token.format(datetime(2021, 11, 2, 13, 14, 15, 16), tokens) == "02"
    assert token.parse("12/11/2021", tokens) == ({"day": 12}, "/11/2021")


def test_ddd_token() -> None:
    """
    Test the ddd token.
    """
    token = DDD("ddd")

    assert DDD.match("ddd", [])
    assert not DDD.match("dd", [])
    assert not DDD.match("dddd", [])

    assert DDD.consume("ddd, dd/mm/yyyy", []) == (token, ", dd/mm/yyyy")

    tokens = list(tokenize("ddd, dd/mm/yyyy", classes))
    assert token.format(datetime(2021, 11, 12, 13, 14, 15, 16), tokens) == "Fri"
    assert token.parse("Fri, 12/11/2021", tokens) == ({"weekday": 0}, ", 12/11/2021")


def test_ddddplus_token() -> None:
    """
    Test the dddd+ token.
    """
    token = DDDDPlus("dddd")

    assert DDDDPlus.match("dddd", [])
    assert DDDDPlus.match("ddddd", [])
    assert not DDDDPlus.match("ddd", [])

    assert DDDDPlus.consume("dddd, dd/mm/yyyy", []) == (token, ", dd/mm/yyyy")

    tokens = list(tokenize("dddd, dd/mm/yyyy", classes))
    assert token.format(datetime(2021, 11, 12, 13, 14, 15, 16), tokens) == "Friday"
    assert token.parse("Friday, 12/11/2021", tokens) == ({"weekday": 0}, ", 12/11/2021")


def test_yy_token() -> None:
    """
    Test the yy token.
    """
    token = YY("yy")

    assert YY.match("y", [])
    assert YY.match("yy", [])
    assert not YY.match("yyy", [])

    assert YY.consume("yy/mm/dd", []) == (token, "/mm/dd")

    tokens = list(tokenize("yy/mm/dd", classes))
    assert token.format(datetime(2021, 11, 12, 13, 14, 15, 16), tokens) == "21"
    assert token.parse("21/11/12", tokens) == ({"year": 2021}, "/11/12")


def test_yyyy_token() -> None:
    """
    Test the yyyy token.
    """
    token = YYYY("yyyy")

    assert YYYY.match("yyy", [])
    assert YYYY.match("yyyy", [])
    assert not YYYY.match("yy", [])

    assert YYYY.consume("yyyy/mm/dd", []) == (token, "/mm/dd")

    tokens = list(tokenize("yyyy/mm/dd", classes))
    assert token.format(datetime(2021, 11, 12, 13, 14, 15, 16), tokens) == "2021"
    assert token.parse("2021/11/12", tokens) == ({"year": 2021}, "/11/12")


def test_zero_token() -> None:
    """
    Test the 00 token.
    """
    token = ZERO("00")

    assert ZERO.match("0", [])
    assert ZERO.match("00", [])
    assert ZERO.match("000", [])
    assert not ZERO.match("0000", [])

    assert ZERO.consume("00", []) == (token, "")

    tokens = list(tokenize("hh:mm:ss.000", classes))
    assert token.format(datetime(2021, 11, 12, 13, 14, 15, 167000), tokens) == "17"

    token = ZERO("000")
    assert token.format(datetime(2021, 11, 12, 13, 14, 15, 167000), tokens) == "167"

    assert token.parse("123", tokens) == ({"microsecond": 123000}, "")


def test_ap_token() -> None:
    """
    Test the a/p token.
    """
    token = AP("a/p")

    assert AP.match("a/p", [])
    assert AP.match("A/P", [])
    assert not AP.match("AM", [])

    assert AP.consume("a/p hh:mm", []) == (token, " hh:mm")

    tokens = list(tokenize("a/p hh:mm", classes))
    assert token.format(datetime(2021, 11, 12, 13, 14, 15, 16), tokens) == "p"
    assert token.format(datetime(2021, 11, 12, 1, 14, 15, 16), tokens) == "a"

    tokens = list(tokenize("A/P hh:mm", classes))
    token = AP("A/P")
    assert token.format(datetime(2021, 11, 12, 13, 14, 15, 16), tokens) == "P"

    assert token.parse("P 09:00", tokens) == ({"meridiem": Meridiem.PM}, " 09:00")
    assert token.parse("A 09:00", tokens) == ({"meridiem": Meridiem.AM}, " 09:00")


def test_ampm_token() -> None:
    """
    Test the am/pm token.
    """
    token = AMPM("am/pm")

    assert AMPM.match("am/pm", [])
    assert not AMPM.match("AM/PM", [])
    assert not AMPM.match("AM", [])

    assert AMPM.consume("am/pm hh:mm", []) == (token, " hh:mm")

    tokens = list(tokenize("am/pm hh:mm", classes))
    assert token.format(datetime(2021, 11, 12, 13, 14, 15, 16), tokens) == "PM"
    assert token.format(datetime(2021, 11, 12, 1, 14, 15, 16), tokens) == "AM"

    assert token.parse("PM 09:00", tokens) == ({"meridiem": Meridiem.PM}, " 09:00")
    assert token.parse("AM 09:00", tokens) == ({"meridiem": Meridiem.AM}, " 09:00")


def test_parse_date_time_pattern() -> None:
    """
    Test the parse_date_time_pattern function.
    """
    assert parse_date_time_pattern("0:38:19", "[h]:mm:ss", timedelta) == timedelta(
        hours=0,
        minutes=38,
        seconds=19,
    )

    assert parse_date_time_pattern(
        "2021/11/12 13:14:15.167",
        "yyyy/mm/dd hh:mm:ss.000",
        datetime,
    ) == datetime(2021, 11, 12, 13, 14, 15, 167000)

    assert parse_date_time_pattern(
        "2021/11/12 01:14:15.167 PM",
        "yyyy/mm/dd hh:mm:ss.000 am/pm",
        datetime,
    ) == datetime(2021, 11, 12, 13, 14, 15, 167000)

    # test that weekday is ignored
    assert parse_date_time_pattern(
        "2021/11/12 Fri, 01:14:15.167 PM",
        "yyyy/mm/dd ddd, hh:mm:ss.000 am/pm",
        datetime,
    ) == datetime(2021, 11, 12, 13, 14, 15, 167000)

    assert parse_date_time_pattern("60.123", "[ss].000", timedelta) == timedelta(
        seconds=60,
        microseconds=123000,
    )

    with pytest.raises(Exception) as excinfo:
        parse_date_time_pattern("60.123", "[ss].000", datetime)
    assert str(excinfo.value) == "Unsupported format"


def test_format_date_time_pattern() -> None:
    """
    Test the format_date_time_pattern function.
    """
    assert (
        format_date_time_pattern(
            datetime(2021, 11, 12, 13, 14, 15, 16),
            "yyyy/mm/dd hh/mm/ss.000",
        )
        == "2021/11/12 13/14/15.000"
    )


def test_parse_date_time_pattern_with_quotes() -> None:
    """
    Test parsing a timestamp with quotes.
    """
    parsed = parse_date_time_pattern(
        "1/1/2021",
        'm"/"d"/"yyyy',
        date,
    )
    assert parsed == date(2021, 1, 1)


def test_parse_date_time_with_meridiem() -> None:
    """
    Test parsing a timestamp with AM/PM in the hour.
    """
    parsed = parse_date_time_pattern(
        "12:34:56 AM",
        "h:mm:ss am/pm",
        time,
    )
    assert parsed == time(0, 34, 56)

    parsed = parse_date_time_pattern(
        "12:34:56 PM",
        "h:mm:ss am/pm",
        time,
    )
    assert parsed == time(12, 34, 56)


def test_parse_date_time_without_meridiem() -> None:
    """
    Test parsing a timestamp without AM/PM in the hour.
    """
    parsed = parse_date_time_pattern(
        "12/31/2020 12:34:56",
        "m/d/yyyy h:mm:ss",
        datetime,
    )
    assert parsed == datetime(2020, 12, 31, 12, 34, 56)


def test_format_date_time_with_meridiem() -> None:
    """
    Test formatting a timestamp with AM/pM in the hour.
    """
    assert (
        format_date_time_pattern(
            time(12, 34, 56),
            "h:mm:ss am/pm",
        )
        == "12:34:56 PM"
    )


def test_infer_column_type() -> None:
    """
    Test type inferring via patterns.
    """
    assert infer_column_type("h:mm:ss am/pm") == "timeofday"
    assert infer_column_type("[h]:mm:ss") == "duration"
