"""
Test number parsing.
"""

import pytest

from shillelagh.adapters.api.gsheets.parsing.base import tokenize
from shillelagh.adapters.api.gsheets.parsing.number import (
    AT,
    COLOR,
    COMMA,
    DIGITS,
    FRACTION,
    LITERAL,
    PERCENT,
    PERIOD,
    STAR,
    UNDERSCORE,
    E,
    InvalidValue,
    condition_matches,
    fix_periods,
    format_number_pattern,
    get_fraction,
    has_condition,
    parse_number_pattern,
)


def test_digits_token() -> None:
    """
    Test the digits token.
    """
    token = DIGITS("00")
    assert token.format(123, [token]) == "123"
    operation, rest = token.parse("123", [token])
    assert operation["operation"](0) == 123
    assert rest == ""

    token = DIGITS("0000")
    assert token.format(123, [token]) == "0123"
    operation, rest = token.parse("0123", [token])
    assert operation["operation"](0) == 123
    assert rest == ""

    token = DIGITS("00")
    assert token.format(3, [token]) == "03"
    operation, rest = token.parse("03", [token])
    assert operation["operation"](0) == 3
    assert rest == ""

    token = DIGITS("#0")
    assert token.format(3, [token]) == "3"
    operation, rest = token.parse("3", [token])
    assert operation["operation"](0) == 3
    assert rest == ""

    token = DIGITS("?0")
    assert token.format(3, [token]) == " 3"
    operation, rest = token.parse("3", [token])
    assert operation["operation"](0) == 3
    assert rest == ""

    token = DIGITS("00")
    assert token.format(0.456, [DIGITS("0"), PERIOD("."), token]) == "45"
    operation, rest = token.parse("45", [DIGITS("0"), PERIOD("."), token])
    assert operation["operation"](0) == 0.45
    assert rest == ""

    token = DIGITS("#,###")
    assert token.format(12000, [token]) == "12,000"
    operation, rest = token.parse("12,000", [token])
    assert operation["operation"](0) == 12000
    assert rest == ""

    token = DIGITS("#,##0")
    assert token.format(1234513400, [token]) == "1,234,513,400"
    operation, rest = token.parse("1,234,513,400", [token])
    assert operation["operation"](0) == 1234513400
    assert rest == ""

    token = DIGITS("???")
    operation, rest = token.parse("  1", [token])
    assert operation["operation"](0) == 1
    assert rest == ""


def test_digits_errors() -> None:
    """
    Test errors with the digits token.
    """
    token = DIGITS("00")
    with pytest.raises(InvalidValue) as excinfo:
        token.parse("invalid", [token])
    assert str(excinfo.value) == "invalid"

    token = DIGITS("invalid")
    with pytest.raises(Exception) as excinfo:
        token.format(123, [token])
    assert str(excinfo.value) == "Invalid token: d"

    token = DIGITS("invalid")
    with pytest.raises(Exception) as excinfo:
        token.format(0.456, [DIGITS("0"), PERIOD("."), token])
    assert str(excinfo.value) == "Invalid token: i"


def test_period_token() -> None:
    """
    Test the period token.
    """
    token = PERIOD(".")
    assert token.format(123, [token]) == "."
    operation, rest = token.parse(".123", [token])
    assert operation == {}
    assert rest == "123"

    with pytest.raises(InvalidValue) as excinfo:
        token.parse("invalid", [token])
    assert str(excinfo.value) == "invalid"


def test_multiple_periods() -> None:
    """
    Test that only the first period is tokenized to a ``PERIOD``.
    """
    tokens = list(fix_periods(tokenize("#.#...", [DIGITS, PERIOD, LITERAL])))
    assert tokens == [
        DIGITS("#"),
        PERIOD("."),
        DIGITS("#"),
        LITERAL("."),
        LITERAL("."),
        LITERAL("."),
    ]


def test_percent_token() -> None:
    """
    Test the percent token.
    """
    token = PERCENT("%")
    assert token.format(123, [token]) == "%"
    operation, rest = token.parse("%", [token])
    assert operation["operation"](80) == 0.8
    assert rest == ""

    with pytest.raises(InvalidValue) as excinfo:
        token.parse("invalid", [token])
    assert str(excinfo.value) == "invalid"


def test_comma_token() -> None:
    """
    Test the comma token.
    """
    token = COMMA(",,")
    assert token.format(123456789, [token]) == ""
    operation, rest = token.parse("", [token])
    assert operation["operation"](1.2) == 1200000
    assert rest == ""


def test_e_token() -> None:
    """
    Test the scientific notation token.
    """
    token = E("e+00")
    assert token.format(123456789, [token]) == "e+08"
    operation, rest = token.parse("e+08", [token])
    assert operation["operation"](1.2) == 120000000.0
    assert rest == ""

    assert token.format(0.123456789, [token]) == "e-01"
    operation, rest = token.parse("e-01", [token])
    assert operation["operation"](1.2) == 0.12
    assert rest == ""

    token = E("E-##")
    assert token.format(123456789, [token]) == "E8"
    operation, rest = token.parse("E8", [token])
    assert operation["operation"](1.2) == 120000000.0
    assert rest == ""

    assert token.format(0.123456789, [token]) == "E-1"
    operation, rest = token.parse("E-1", [token])
    assert operation["operation"](1.2) == 0.12
    assert rest == ""

    token = E("invalid")
    with pytest.raises(Exception) as excinfo:
        token.format(123, [token])
    assert str(excinfo.value) == "You are likely to be eaten by a grue."


def test_fraction_token() -> None:
    """
    Test the fraction token.
    """
    token = FRACTION("#/#")
    assert token.format(5.25, [token]) == "1/4"
    operation, rest = token.parse("1/4", [token])
    assert operation["operation"](5) == 5.25
    assert rest == ""

    token = FRACTION("#/8")
    assert token.format(5.25, [token]) == "2/8"
    assert operation["operation"](5) == 5.25
    assert rest == ""

    token = FRACTION("???/???")
    operation, rest = token.parse("  1/8", [token])
    assert operation["operation"](5) == 5.125
    assert rest == ""

    token = FRACTION("invalid")
    with pytest.raises(Exception) as excinfo:
        token.format(123, [token])
    assert str(excinfo.value) == "You are likely to be eaten by a grue."


def test_star_token() -> None:
    """
    Test the star token.
    """
    token = STAR("*")
    assert token.format(123, [token]) == ""
    operation, rest = token.parse("1", [token])
    assert operation == {}
    assert rest == "1"


def test_underscore_token() -> None:
    """
    Test the underscore token.
    """
    token = UNDERSCORE("_)")
    assert token.format(123, [token]) == " "
    operation, rest = token.parse(" ", [token])
    assert operation == {}
    assert rest == ""

    token = UNDERSCORE("_)")
    with pytest.raises(Exception) as excinfo:
        token.parse("A", [token])
    assert str(excinfo.value) == "A"


def test_at_token() -> None:
    """
    Test the at token.
    """
    token = AT("@")
    assert token.format("123", [token]) == "123"
    operation, rest = token.parse("123", [token])
    assert operation["operation"](0) == "123"
    assert rest == ""

    # test lookahead
    token = AT("@")
    operation, rest = token.parse("123 ", [token, UNDERSCORE("_)")])
    assert operation["operation"](0) == "123"
    assert rest == " "

    with pytest.raises(InvalidValue) as excinfo:
        token.parse("123$", [token, UNDERSCORE("_)")])
    assert str(excinfo.value) == "123$"


def test_color_token() -> None:
    """
    Test the color token.
    """
    token = COLOR("[Black]")
    assert token.format(123, [token]) == ""
    operation, rest = token.parse("123", [token])
    assert operation == {}
    assert rest == "123"


def test_get_fraction() -> None:
    """
    Test ``get_fraction``.
    """
    assert get_fraction("9") == 0.9
    assert get_fraction("123") == 0.123
    assert get_fraction("1") == 0.1
    assert get_fraction("001") == 0.001
    assert get_fraction("10") == 0.1
    assert get_fraction("0") == 0

    with pytest.raises(Exception) as excinfo:
        get_fraction("-1")
    assert str(excinfo.value) == "Number should be a positive integer"


def test_has_condition() -> None:
    """
    Test ``has_condition``.
    """
    assert has_condition('###0.000;"TEXT: "_(@_)') is False
    assert has_condition("[Blue]#,##0;[Red]#,##0;[Green]0.0;[Magenta]_(@_)") is False
    assert has_condition('[>1000]"HIGH";[Color43][<=200]"LOW";0000') is True


def test_condition_matches() -> None:
    """
    Test ``condition_matches``.
    """
    assert condition_matches(1005, '[>1000]"HIGH"') is True
    assert condition_matches(32, '[>1000]"HIGH"') is False
    assert condition_matches(527, '[>1000]"HIGH"') is False

    assert condition_matches(1005, '[Color43][<=200]"LOW"') is False
    assert condition_matches(32, '[Color43][<=200]"LOW"') is True
    assert condition_matches(527, '[Color43][<=200]"LOW"') is False

    assert condition_matches(1005, "0000") is True
    assert condition_matches(32, "0000") is True
    assert condition_matches(527, "0000") is True


def test_parse_number_pattern() -> None:
    """
    Test ``parse_number_pattern``.
    """
    # examples from the spec
    assert parse_number_pattern("12345.1", "####.#") == 12345.1
    assert parse_number_pattern("012.3400", "000.0000") == 12.34
    assert parse_number_pattern("12.0", "#.0#") == 12
    assert parse_number_pattern("5   1/8  ", "# ???/???") == 5.125
    assert parse_number_pattern("23 1/4", "0 #/#") == 23.25
    assert parse_number_pattern("23 2/8", "0 #/8") == 23.25
    assert parse_number_pattern("23 ", "0 #/3") == 23
    assert parse_number_pattern("12,000", "#,###") == 12000
    assert parse_number_pattern("1.2M", '0.0,,"M"') == 1200000
    assert parse_number_pattern("1.23e+09", "0.00e+00") == 1230000000
    assert parse_number_pattern("1,234,513,400.00", "#,##0.00") == 1234513400.0

    # additional examples
    assert parse_number_pattern("-12345.1", "####.#") == -12345.1
    assert parse_number_pattern("80%", "##%") == 0.8
    assert parse_number_pattern("80.00", "#,##0.00") == 80.0
    assert parse_number_pattern("12.2", "#0.0,,") == 12200000
    assert parse_number_pattern("12.2", "*#0.0,,") == 12200000
    assert parse_number_pattern("$12.2", "$#0.0,,") == 12200000
    assert parse_number_pattern("12.2,", r"#0.0\,") == 12.2
    assert parse_number_pattern("dollars: 12.0", "dollars: #.0#") == 12
    assert parse_number_pattern("1.23e-02", "0.00e+00") == 0.0123
    assert parse_number_pattern("0123.45", "#0000.00") == 123.45

    # regressions
    assert parse_number_pattern("1.001", "#,##0.000") == 1.001

    # tests for conditions
    assert (
        parse_number_pattern("0527", '[>1000]"HIGH";[Color43][<=200]"LOW";0000') == 527
    )

    # some really complicated examples
    assert parse_number_pattern("123.00 ", "#,##0.00_);[Red](#,##0.00)") == 123
    assert parse_number_pattern("(123.00)", "#,##0.00_);[Red](#,##0.00)") == -123
    assert parse_number_pattern("TEXT:  MyText ", '###0.000;"TEXT: "_(@_)') == "MyText"
    assert (
        parse_number_pattern(
            " $ 123.00 ",
            '_($* #,##0.00_);_($* (#,##0.00);_($* "-"??_);_(@_)',
        )
        == 123
    )

    # corner cases
    assert parse_number_pattern("$12.2", "$#0.0,,") == 12200000.0
    assert parse_number_pattern("-$12.2", "$#0.0,,") == -12200000.0
    with pytest.raises(Exception) as excinfo:
        assert parse_number_pattern("-$12.2", "#")
    assert str(excinfo.value) == "Unable to parse value -$12.2 with pattern #"


def test_parse_number_pattern_errors() -> None:
    """
    Test errors in ``parse_number_pattern``.
    """
    # we fallback to int/float parsing when the format is not correct
    assert parse_number_pattern("80", "##%") == 80
    assert parse_number_pattern("1.23", "0.00e+00") == 1.23

    with pytest.raises(Exception) as excinfo:
        parse_number_pattern("abc", "##%")
    assert str(excinfo.value) == "Unable to parse value abc with pattern ##%"


def test_format_number_pattern() -> None:
    """
    Test ``format_number_pattern``.
    """
    # examples from the spec
    assert format_number_pattern(12345.1, "####.#") == "12345.1"
    assert format_number_pattern(12.34, "000.0000") == "012.3400"
    assert format_number_pattern(12, "#.0#") == "12.0"
    assert format_number_pattern(5.125, "# ???/???") == "5   1/8  "
    assert format_number_pattern(23.25, "0 #/#") == "23 1/4"
    assert format_number_pattern(23.25, "0 #/8") == "23 2/8"
    assert format_number_pattern(23.1, "0 #/3") == "23 "
    assert format_number_pattern(12000, "#,###") == "12,000"
    assert format_number_pattern(1200000, '0.0,,"M"') == "1.2M"
    assert format_number_pattern(1230000000, "0.00e+00") == "1.23e+09"
    assert format_number_pattern(1234513400.0, "#,##0.00") == "1,234,513,400.00"

    # additional examples
    assert format_number_pattern(-12345.1, "####.#") == "-12345.1"
    assert format_number_pattern(0.8, "##%") == "80%"
    assert format_number_pattern(80.0, "#,##0.00") == "80.00"
    assert format_number_pattern(12200000, "#0.0,,") == "12.2"
    assert format_number_pattern(12200000, "*#0.0,,") == "12.2"
    assert format_number_pattern(12200000, "$#0.0,,") == "$12.2"
    assert format_number_pattern(12.2, r"#0.0\,") == "12.2,"
    assert format_number_pattern(12, "dollars: #0.0#") == "dollars: 12.0"
    assert format_number_pattern(0.0123, "0.00e+00") == "1.23e-02"
    assert format_number_pattern(123.456, "#0000.00") == "0123.45"

    # tests for conditions
    assert (
        format_number_pattern(1005, '[>1000]"HIGH";[Color43][<=200]"LOW";0000')
        == "HIGH"
    )
    assert (
        format_number_pattern(32, '[>1000]"HIGH";[Color43][<=200]"LOW";0000') == "LOW"
    )
    assert (
        format_number_pattern(527, '[>1000]"HIGH";[Color43][<=200]"LOW";0000') == "0527"
    )
    assert (
        format_number_pattern(527, '[>1000]"HIGH";[Color43][<=200]"LOW"') == "########"
    )

    # some really complicated examples
    assert format_number_pattern(123, "#,##0.00_);[Red](#,##0.00)") == "123.00 "
    assert format_number_pattern(-123, "#,##0.00_);[Red](#,##0.00)") == "(123.00)"
    assert format_number_pattern("MyText", '###0.000;"TEXT: "_(@_)') == "TEXT:  MyText "
    assert format_number_pattern(0, "#;(#);ZERO") == "ZERO"


def test_format_number_pattern_errors() -> None:
    """
    Test errors in ``format_number_pattern``.
    """
    with pytest.raises(Exception) as excinfo:
        format_number_pattern(123, "")
    assert str(excinfo.value) == "Empty pattern!"

    with pytest.raises(Exception) as excinfo:
        format_number_pattern("MyText", "###0.000")
    assert str(excinfo.value) == "No text format found for string value"
