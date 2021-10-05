"""
Test number parsing.
"""
import pytest

from shillelagh.adapters.api.gsheets.parsing.number import get_fraction
from shillelagh.adapters.api.gsheets.parsing.number import parse_number_pattern


def test_get_fraction() -> None:
    """
    Test ``get_fraction``.
    """
    assert get_fraction(9) == 0.9
    assert get_fraction(123) == 0.123
    assert get_fraction(1) == 0.1
    assert get_fraction(10) == 0.1
    assert get_fraction(0) == 0

    with pytest.raises(Exception) as excinfo:
        get_fraction(-1)
    assert str(excinfo.value) == "Number should be a positive integer"


def test_parse_number_pattern() -> None:
    """
    Test ``parse_number_pattern``.
    """
    # examples from the spec
    assert parse_number_pattern("12345.1", "####.#") == 12345.1
    assert parse_number_pattern("012.3400", "000.0000") == 12.34
    assert parse_number_pattern("12.0", "#.0#") == 12
    assert parse_number_pattern("5 1/8", "# ???/???") == 5.125
    assert parse_number_pattern("23 1/4", "0 #/#") == 23.25
    assert parse_number_pattern("23 2/8", "0 #/8") == 23.25
    assert parse_number_pattern("23", "0 #/3") == 23
    assert parse_number_pattern("12,000", "#,###") == 12000
    assert parse_number_pattern("1.2M", '0.0,,"M"') == 1200000
    assert parse_number_pattern("1.23e+09", "0.00e+00") == 1230000000

    # additional examples
    assert parse_number_pattern("80%", "##%") == 0.8
    assert parse_number_pattern("80.00", "#,##0.00") == 80.0
    assert parse_number_pattern("12.2", "#0.0,,") == 12200000
    assert parse_number_pattern("12.2", "*#0.0,,") == 12200000
    assert parse_number_pattern("$12.2", "$#0.0,,") == 12200000
    assert parse_number_pattern("12.2,", r"#0.0\,") == 12.2
    assert parse_number_pattern("dollars: 12.0", "dollars: #.0#") == 12

    # some really complicated examples
    assert parse_number_pattern("123.00", "#,##0.00_);[Red](#,##0.00)") == 123
    assert parse_number_pattern("(123.00)", "#,##0.00_);[Red](#,##0.00)") == -123


def test_parse_number_pattern_errors() -> None:
    """
    Test errors in ``parse_number_pattern``.
    """
    with pytest.raises(Exception) as excinfo:
        parse_number_pattern("80", "##%")
    assert str(excinfo.value) == "Unable to parse value 80 with pattern ##%"

    with pytest.raises(Exception) as excinfo:
        parse_number_pattern("1.23", "0.00e+00")
    assert str(excinfo.value) == "Unable to parse value 1.23 with pattern 0.00e+00"
