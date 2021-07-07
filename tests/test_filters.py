import pytest

from shillelagh.filters import Endpoint
from shillelagh.filters import Equal
from shillelagh.filters import Impossible
from shillelagh.filters import Operator
from shillelagh.filters import Range
from shillelagh.filters import Side


def test_equal():
    operations = {(Operator.EQ, 10)}
    filter_ = Equal.build(operations)
    assert filter_.value == 10


def test_equal_multiple_value():
    operations = [
        (Operator.EQ, 10),
        (Operator.EQ, 10),
    ]
    filter_ = Equal.build(operations)
    assert filter_.value == 10


def test_equal_check():
    operations = [
        (Operator.EQ, 10),
        (Operator.EQ, 10),
    ]
    filter_ = Equal.build(operations)
    assert filter_.check(10)
    assert not filter_.check(20)


def test_equal_impossible():
    operations = [
        (Operator.EQ, 10),
        (Operator.EQ, 20),
    ]
    filter_ = Equal.build(operations)
    assert isinstance(filter_, Impossible)


def test_range():
    operations = [
        (Operator.GT, 0),
        (Operator.LT, 10),
        (Operator.GT, 2),
        (Operator.LE, 4),
        (Operator.GE, 2),
    ]
    filter_ = Range.build(operations)
    assert filter_.start == 2
    assert filter_.end == 4
    assert not filter_.include_start
    assert filter_.include_end


def test_range_equal():
    operations = [
        (Operator.GT, 0),
        (Operator.EQ, 3),
        (Operator.LT, 10),
        (Operator.GT, 2),
        (Operator.LE, 4),
        (Operator.GE, 2),
    ]
    filter_ = Range.build(operations)
    assert filter_.start == 3
    assert filter_.end == 3
    assert filter_.include_start
    assert filter_.include_end


def test_range_equal_impossible():
    operations = [
        (Operator.GT, 0),
        (Operator.LT, -1),
    ]
    filter_ = Range.build(operations)
    assert isinstance(filter_, Impossible)

    operations = [
        (Operator.LT, -1),
        (Operator.GT, 0),
    ]
    filter_ = Range.build(operations)
    assert isinstance(filter_, Impossible)


def test_range_include():
    operations = [
        (Operator.GE, 2),
        (Operator.GT, 2),
        (Operator.LE, 4),
        (Operator.LT, 4),
    ]
    filter_ = Range.build(operations)
    assert filter_.start == 2
    assert filter_.end == 4
    assert not filter_.include_start
    assert not filter_.include_end


def test_range_check():
    filter_ = Range(2, 4, False, True)
    assert not filter_.check(2)
    assert filter_.check(3)
    assert filter_.check(4)

    filter_ = Range(2, None, True, True)
    assert not filter_.check(1)
    assert filter_.check(2)

    filter_ = Range(2, None, False, True)
    assert not filter_.check(2)

    filter_ = Range(None, 4, True, True)
    assert filter_.check(4)
    assert not filter_.check(5)

    filter_ = Range(None, 4, True, False)
    assert not filter_.check(4)


def test_range_invalid_operator():
    operations = {(-1, 0)}
    with pytest.raises(Exception) as excinfo:
        Range.build(operations)

    assert str(excinfo.value) == "Invalid operator: -1"


def test_combine_ranges():
    assert not Range(0, 1, False, False) == 1
    with pytest.raises(TypeError) as excinfo:
        Range(0, 1, False, False) + 1
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


def test_impossible():
    assert Impossible.build([]) == Impossible()
    assert Impossible().check(10) is False
    assert Impossible() != 0


def build_endpoint(representation: str) -> Endpoint:
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


def test_endpoints():
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
    assert not Endpoint(0, True, Side.RIGHT) > Endpoint(0, False, Side.LEFT)

    assert end != 1
    with pytest.raises(TypeError) as excinfo:
        end > 1
    assert (
        str(excinfo.value)
        == "'>' not supported between instances of 'Endpoint' and 'int'"
    )
