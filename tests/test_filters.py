import apsw
import pytest
from shillelagh.filters import Equal
from shillelagh.filters import Impossible
from shillelagh.filters import Range


def test_equal():
    operations = {(apsw.SQLITE_INDEX_CONSTRAINT_EQ, 10)}
    filter_ = Equal.build(operations)
    assert filter_.value == 10


def test_equal_multiple_value():
    operations = {
        (apsw.SQLITE_INDEX_CONSTRAINT_EQ, 10),
        (apsw.SQLITE_INDEX_CONSTRAINT_EQ, 10),
    }
    filter_ = Equal.build(operations)
    assert filter_.value == 10


def test_equal_check():
    operations = {
        (apsw.SQLITE_INDEX_CONSTRAINT_EQ, 10),
        (apsw.SQLITE_INDEX_CONSTRAINT_EQ, 10),
    }
    filter_ = Equal.build(operations)
    assert filter_.check(10)
    assert not filter_.check(20)


def test_equal_impossible():
    operations = {
        (apsw.SQLITE_INDEX_CONSTRAINT_EQ, 10),
        (apsw.SQLITE_INDEX_CONSTRAINT_EQ, 20),
    }
    filter_ = Equal.build(operations)
    assert isinstance(filter_, Impossible)


def test_range():
    operations = {
        (apsw.SQLITE_INDEX_CONSTRAINT_GT, 0),
        (apsw.SQLITE_INDEX_CONSTRAINT_LT, 10),
        (apsw.SQLITE_INDEX_CONSTRAINT_GT, 2),
        (apsw.SQLITE_INDEX_CONSTRAINT_LE, 4),
        (apsw.SQLITE_INDEX_CONSTRAINT_GE, 2),
    }
    filter_ = Range.build(operations)
    assert filter_.start == 2
    assert filter_.end == 4
    assert not filter_.include_start
    assert filter_.include_end


def test_range_equal():
    operations = {
        (apsw.SQLITE_INDEX_CONSTRAINT_GT, 0),
        (apsw.SQLITE_INDEX_CONSTRAINT_EQ, 3),
        (apsw.SQLITE_INDEX_CONSTRAINT_LT, 10),
        (apsw.SQLITE_INDEX_CONSTRAINT_GT, 2),
        (apsw.SQLITE_INDEX_CONSTRAINT_LE, 4),
        (apsw.SQLITE_INDEX_CONSTRAINT_GE, 2),
    }
    filter_ = Range.build(operations)
    assert filter_.start == 3
    assert filter_.end == 3
    assert filter_.include_start
    assert filter_.include_end


def test_range_equal_impossible():
    operations = {
        (apsw.SQLITE_INDEX_CONSTRAINT_GT, 0),
        (apsw.SQLITE_INDEX_CONSTRAINT_EQ, 13),
        (apsw.SQLITE_INDEX_CONSTRAINT_LT, 10),
        (apsw.SQLITE_INDEX_CONSTRAINT_GT, 2),
        (apsw.SQLITE_INDEX_CONSTRAINT_LE, 4),
        (apsw.SQLITE_INDEX_CONSTRAINT_GE, 2),
    }
    filter_ = Range.build(operations)
    assert isinstance(filter_, Impossible)


def test_range_include():
    # use list instead of set to define order
    operations = [
        (apsw.SQLITE_INDEX_CONSTRAINT_GE, 2),
        (apsw.SQLITE_INDEX_CONSTRAINT_GT, 2),
        (apsw.SQLITE_INDEX_CONSTRAINT_LE, 4),
        (apsw.SQLITE_INDEX_CONSTRAINT_LT, 4),
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
