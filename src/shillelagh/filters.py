from enum import Enum
from typing import Any
from typing import Optional
from typing import Set
from typing import Tuple


class Operator(Enum):
    EQ = "=="
    GE = ">="
    GT = ">"
    LE = "<="
    LT = "<"


class Filter:

    operators: Set[Operator] = set()

    @classmethod
    def build(cls, operations: Set[Tuple[Operator, Any]]) -> "Filter":
        raise NotImplementedError("Subclass must implement `build`")

    def check(self, value: Any) -> bool:
        raise NotImplementedError("Subclass must implement `check`")


class Impossible(Filter):
    """Custom Filter returned when impossible conditions are passed."""

    pass


class Equal(Filter):

    operators: Set[Operator] = {
        Operator.EQ,
    }

    def __init__(self, value: Any):
        self.value = value

    @classmethod
    def build(cls, operations: Set[Tuple[Operator, Any]]) -> Filter:
        values = {value for operator, value in operations}
        if len(values) != 1:
            return Impossible()

        return cls(values.pop())

    def check(self, value: Any) -> bool:
        return bool(value == self.value)

    def __repr__(self) -> str:
        return f"== {self.value}"


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

    operators: Set[Operator] = {
        Operator.EQ,
        Operator.GE,
        Operator.GT,
        Operator.LE,
        Operator.LT,
    }

    def __eq__(self, other: Any):
        if not isinstance(other, Range):
            return NotImplemented

        return (
            self.start == other.start
            and self.end == other.end
            and self.include_start == other.include_start
            and self.include_end == other.include_end
        )

    def __add__(self, other: Any) -> "Range":
        if not isinstance(other, Range):
            return NotImplemented

        if self.start is None:
            start = other.start
            include_start = other.include_start
        elif other.start is None:
            start = self.start
            include_start = self.include_start
        elif self.start > other.start:
            start = self.start
            include_start = self.include_start
        elif self.start < other.start:
            start = other.start
            include_start = other.include_start
        else:
            start = self.start
            include_start = self.include_start and other.include_start

        if self.end is None:
            end = other.end
            include_end = other.include_end
        elif other.end is None:
            end = self.end
            include_end = self.include_end
        elif self.end < other.end:
            end = self.end
            include_end = self.include_end
        elif self.end > other.end:
            end = other.end
            include_end = other.include_end
        else:
            end = self.end
            include_end = self.include_end and other.include_end

        return Range(start, end, include_start, include_end)

    @classmethod
    def build(cls, operations: Set[Tuple[Operator, Any]]) -> Filter:
        start = end = None
        include_start = include_end = False

        for operator, value in operations:
            new_start = start
            new_end = end
            new_include_start = include_start
            new_include_end = include_end

            if operator == Operator.EQ:
                new_start = new_end = value
                new_include_start = new_include_end = True
            elif operator == Operator.GE:
                new_start = value
                new_include_start = True
            elif operator == Operator.GT:
                new_start = value
                new_include_start = False
            elif operator == Operator.LE:
                new_end = value
                new_include_end = True
            elif operator == Operator.LT:
                new_end = value
                new_include_end = False
            else:
                raise Exception(f"Invalid operator: {operator}")

            if (
                end is not None
                and new_start is not None
                and (
                    new_start > end
                    or (new_start >= end and (not new_include_start or not include_end))
                )
            ):
                return Impossible()
            if (
                start is not None
                and new_end is not None
                and (
                    new_end < start
                    or (new_end <= start and (not include_start or not new_include_end))
                )
            ):
                return Impossible()

            # update start and end by tightening up range
            if start is None or new_start >= start:
                if new_start == start:
                    if include_start and not new_include_start:
                        include_start = False
                else:
                    include_start = new_include_start
                start = new_start

            if end is None or new_end <= end:
                if new_end == end:
                    if include_end and not new_include_end:
                        include_end = False
                else:
                    include_end = new_include_end
                end = new_end

        return cls(start, end, include_start, include_end)

    def check(self, value: Any) -> bool:
        if self.start is not None:
            if self.include_start and value < self.start:
                return False
            if not self.include_start and value <= self.start:
                return False

        if self.end is not None:
            if self.include_end and value > self.end:
                return False
            if not self.include_end and value >= self.end:
                return False

        return True

    def __repr__(self) -> str:
        if self.start == self.end and self.include_start and self.include_end:
            return f"== {self.start}"

        comparisons = []
        if self.start is not None:
            op = ">=" if self.include_start else ">"
            comparisons.append(f"{op}{self.start}")
        if self.end is not None:
            op = "<=" if self.include_end else "<"
            comparisons.append(f"{op}{self.end}")
        return ",".join(comparisons)
