"""
Tests for shillelagh.filters.
"""

import pytest

from shillelagh.filters import (
    Endpoint,
    Equal,
    Impossible,
    IsNotNull,
    IsNull,
    Like,
    NotEqual,
    Operator,
    Range,
    Side,
)


def test_equal() -> None:
    """
    Test ``Equal``.
    """
    operations = {(Operator.EQ, 10)}
    filter_ = Equal.build(operations)
    assert isinstance(filter_, Equal)
    assert filter_.value == 10


def test_equal_multiple_value() -> None:
    """
    Test multiple operations.
    """
    operations = {
        (Operator.EQ, 10),
        (Operator.EQ, 10),
    }
    filter_ = Equal.build(operations)
    assert isinstance(filter_, Equal)
    assert filter_.value == 10


def test_equal_check() -> None:
    """
    Test ``_check``.
    """
    operations = {
        (Operator.EQ, 10),
        (Operator.EQ, 10),
    }
    filter_ = Equal.build(operations)
    assert filter_.check(10)
    assert not filter_.check(20)


def test_equal_impossible() -> None:
    """
    Test impossible operations.
    """
    operations = {
        (Operator.EQ, 10),
        (Operator.EQ, 20),
    }
    filter_ = Equal.build(operations)
    assert isinstance(filter_, Impossible)


def test_not_equal() -> None:
    """
    Test ``NotEqual``.
    """
    operations = {(Operator.NE, 10)}
    filter_ = NotEqual.build(operations)
    assert isinstance(filter_, NotEqual)
    assert filter_.value == 10


def test_not_equal_multiple_value() -> None:
    """
    Test multiple operations.
    """
    operations = {
        (Operator.NE, 10),
        (Operator.NE, 10),
    }
    filter_ = NotEqual.build(operations)
    assert isinstance(filter_, NotEqual)
    assert filter_.value == 10


def test_not_equal_check() -> None:
    """
    Test ``_check``.
    """
    operations = {
        (Operator.NE, 10),
        (Operator.NE, 10),
    }
    filter_ = NotEqual.build(operations)
    assert filter_.check(20)
    assert not filter_.check(10)


def test_not_equal_impossible() -> None:
    """
    Test impossible operations.
    """
    operations = {
        (Operator.NE, 10),
        (Operator.NE, 20),
    }
    filter_ = NotEqual.build(operations)
    assert isinstance(filter_, Impossible)


def test_like() -> None:
    """
    Test ``Like``.
    """
    operations = {(Operator.LIKE, "%test%")}
    filter_ = Like.build(operations)
    assert isinstance(filter_, Like)
    assert filter_.value == "%test%"


def test_like_multiple_value() -> None:
    """
    Test multiple operations.
    """
    operations = {
        (Operator.LIKE, "%test%"),
        (Operator.LIKE, "%test%"),
    }
    filter_ = Like.build(operations)
    assert isinstance(filter_, Like)
    assert filter_.value == "%test%"


def test_like_check() -> None:
    """
    Test ``_check``.
    """
    operations = {
        (Operator.LIKE, "%test%"),
        (Operator.LIKE, "%test%"),
    }
    filter_ = Like.build(operations)
    assert filter_.check("this is a test")
    assert not filter_.check("this is not")


def test_like_impossible() -> None:
    """
    Test impossible operations.
    """
    operations = {
        (Operator.LIKE, "%foo%"),
        (Operator.LIKE, "%bar%"),
    }
    filter_ = Like.build(operations)
    assert isinstance(filter_, Impossible)


def test_range() -> None:
    """
    Test ``Range``.
    """
    operations = {
        (Operator.GT, 0),
        (Operator.LT, 10),
        (Operator.GT, 2),
        (Operator.LE, 4),
        (Operator.GE, 2),
    }
    filter_ = Range.build(operations)
    assert isinstance(filter_, Range)
    assert filter_.start == 2
    assert filter_.end == 4
    assert not filter_.include_start
    assert filter_.include_end
    assert str(filter_) == ">2,<=4"


def test_range_equal() -> None:
    """
    Test ``Range`` collapsed to an equality.
    """
    operations = {
        (Operator.GT, 0),
        (Operator.EQ, 3),
        (Operator.LT, 10),
        (Operator.GT, 2),
        (Operator.LE, 4),
        (Operator.GE, 2),
    }
    filter_ = Range.build(operations)
    assert isinstance(filter_, Range)
    assert filter_.start == 3
    assert filter_.end == 3
    assert filter_.include_start
    assert filter_.include_end
    assert str(filter_) == "==3"


def test_range_equal_impossible() -> None:
    """
    Test an impossible range.
    """
    operations = {
        (Operator.GT, 0),
        (Operator.LT, -1),
    }
    filter_ = Range.build(operations)
    assert isinstance(filter_, Impossible)

    operations = {
        (Operator.LT, -1),
        (Operator.GT, 0),
    }
    filter_ = Range.build(operations)
    assert isinstance(filter_, Impossible)


def test_range_include() -> None:
    """
    Test operations with different includes.
    """
    operations = {
        (Operator.GE, 2),
        (Operator.GT, 2),
        (Operator.LE, 4),
        (Operator.LT, 4),
    }
    filter_ = Range.build(operations)
    assert isinstance(filter_, Range)
    assert filter_.start == 2
    assert filter_.end == 4
    assert not filter_.include_start
    assert not filter_.include_end


def test_range_check() -> None:
    """
    Test ``_check`` in different ranges.
    """
    filter_ = Range(2, 4, False, True)
    assert not filter_.check(2)
    assert filter_.check(3)
    assert filter_.check(4)
    assert str(filter_) == ">2,<=4"

    filter_ = Range(2, None, True, True)
    assert not filter_.check(1)
    assert filter_.check(2)
    assert str(filter_) == ">=2"

    filter_ = Range(2, None, False, True)
    assert not filter_.check(2)
    assert str(filter_) == ">2"

    filter_ = Range(None, 4, True, True)
    assert filter_.check(4)
    assert not filter_.check(5)
    assert str(filter_) == "<=4"

    filter_ = Range(None, 4, True, False)
    assert not filter_.check(4)
    assert str(filter_) == "<4"


def test_range_invalid_operator() -> None:
    """
    Test that ``build`` raises an exception on invalid operators.
    """
    operations = {(-1, 0)}
    with pytest.raises(Exception) as excinfo:
        Range.build(operations)  # type: ignore

    assert str(excinfo.value) == "Invalid operator: -1"


def test_combine_ranges() -> None:
    """
    Test combining ranges.
    """
    assert (Range(0, 1, False, False) == 1) is False
    with pytest.raises(TypeError) as excinfo:
        _ = Range(0, 1, False, False) + 1
    assert str(excinfo.value) == "unsupported operand type(s) for +: 'Range' and 'int'"

    range1 = Range(1, 10, False, False)
    range2 = Range(2, 9, True, True)
    assert range1 + range2 == range2
    assert range2 + range1 == range2
    assert range1 + range1 == range1

    range3 = Range(None, 9, True, True)
    assert range1 + range3 == Range(1, 9, False, True)
    assert range3 + range1 == Range(1, 9, False, True)

    range4 = Range(2, None, True, True)
    assert range1 + range4 == Range(2, 10, True, False)
    assert range4 + range1 == Range(2, 10, True, False)

    range5 = Range(None, -10, True, True)
    range6 = Range(10, None, True, True)
    assert range5 + range6 == Impossible()


def test_impossible() -> None:
    """
    Test ``Impossible``.
    """
    assert Impossible.build([]) == Impossible()  # type: ignore
    assert Impossible().check(10) is False
    assert Impossible() != 0


def build_endpoint(representation: str) -> Endpoint:
    """
    Function to build an endpoint.
    """
    if representation[0] in {"(", "["}:
        return Endpoint(
            value=int(representation[1:]),
            include=representation[0] == "[",
            side=Side.LEFT,
        )
    return Endpoint(
        value=int(representation[:-1]),
        include=representation[-1] == "]",
        side=Side.RIGHT,
    )


def test_endpoints() -> None:
    """
    Test building endpoints.
    """
    start = build_endpoint("(0")
    assert start == Endpoint(0, False, Side.LEFT)
    assert str(start) == "(0"

    assert build_endpoint("(-10") < start
    assert build_endpoint("(0") == start
    assert build_endpoint("[0") < start
    assert build_endpoint("0]") < start
    assert build_endpoint("0)") < start

    end = build_endpoint("0]")
    assert end == Endpoint(0, True, Side.RIGHT)
    assert str(end) == "0]"

    assert build_endpoint("10)") > end
    assert build_endpoint("0)") < end
    assert build_endpoint("0]") == end
    assert build_endpoint("(0") > end
    assert build_endpoint("[0") == end

    # 0] < (0
    assert (Endpoint(0, True, Side.RIGHT) > Endpoint(0, False, Side.LEFT)) is False

    assert end != 1
    with pytest.raises(TypeError) as excinfo:
        end > 1  # pylint: disable=pointless-statement
    assert (
        str(excinfo.value)
        == "'>' not supported between instances of 'Endpoint' and 'int'"
    )


def test_is_null() -> None:
    """
    Test ``IsNull``.
    """
    assert IsNull.build([]) == IsNull()  # type: ignore
    assert IsNull().check(None) is True
    assert IsNull() != 0


def test_is_not_null() -> None:
    """
    Test ``IsNotNull``.
    """
    assert IsNotNull.build([]) == IsNotNull()  # type: ignore
    assert IsNotNull().check(None) is False
    assert IsNotNull() != 0
