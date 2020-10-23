from typing import Any
from typing import Optional
from typing import Set
from typing import Tuple

import apsw


class Filter:

    operators: Set[int] = set()

    @classmethod
    def build(cls, operations: Set[Tuple[int, Any]]) -> "Filter":
        raise NotImplementedError("Subclass must implement `build`")


class Impossible(Filter):
    """Custom Filter return when impossible conditions are passed."""

    pass


class Equal(Filter):

    operators: Set[int] = {
        apsw.SQLITE_INDEX_CONSTRAINT_EQ,
    }

    def __init__(self, value: Any):
        self.value = value

    @classmethod
    def build(cls, operations: Set[Tuple[int, Any]]) -> Filter:
        values = {value for operator, value in operations}
        if len(values) != 1:
            return Impossible()

        return cls(values.pop())


class Range(Filter):
    def __init__(
        self,
        start: Optional[Any],
        end: Optional[Any],
        include_start: bool,
        include_end: bool,
    ):
        self.start = start
        self.end = end
        self.include_start = include_start
        self.include_end = include_end

    operators: Set[int] = {
        apsw.SQLITE_INDEX_CONSTRAINT_EQ,
        apsw.SQLITE_INDEX_CONSTRAINT_GE,
        apsw.SQLITE_INDEX_CONSTRAINT_GT,
        apsw.SQLITE_INDEX_CONSTRAINT_LE,
        apsw.SQLITE_INDEX_CONSTRAINT_LT,
    }

    @classmethod
    def build(cls, operations: Set[Tuple[int, Any]]) -> Filter:
        start = end = None
        include_start = include_end = False

        for operator, value in operations:
            new_start = start
            new_end = end
            new_include_start = include_start
            new_include_end = include_end

            if operator == apsw.SQLITE_INDEX_CONSTRAINT_EQ:
                new_start = new_end = value
                new_include_start = new_include_end = True
            elif operator == apsw.SQLITE_INDEX_CONSTRAINT_GE:
                new_start = value
                new_include_start = True
            elif operator == apsw.SQLITE_INDEX_CONSTRAINT_GT:
                new_start = value
                new_include_start = False
            elif operator == apsw.SQLITE_INDEX_CONSTRAINT_LE:
                new_end = value
                new_include_end = True
            elif operator == apsw.SQLITE_INDEX_CONSTRAINT_LT:
                new_end = value
                new_include_end = False

            if (end is not None and new_start is not None and new_start > end) or (
                start is not None and new_end is not None and new_end < start
            ):
                return Impossible()

            # update start and end by tightening up range
            if start is None or new_start >= start:
                if new_start == start and include_start and not new_include_start:
                    include_start = False
                else:
                    include_start = new_include_start
                start = new_start

            if end is None or new_end <= end:
                if new_end == end and include_end and not new_include_end:
                    include_end = False
                else:
                    include_end = new_include_end
                end = new_end

        return cls(start, end, include_start, include_end)
