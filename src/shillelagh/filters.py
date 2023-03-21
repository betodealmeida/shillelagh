"""
Filters for representing SQL predicates.
"""
import re
from enum import Enum
from typing import Any, Optional, Set, Tuple


class Operator(Enum):
    """
    Enum representing support comparisons.
    """

    EQ = "=="
    NE = "!="
    GE = ">="
    GT = ">"
    LE = "<="
    LT = "<"
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"
    LIKE = "LIKE"
    LIMIT = "LIMIT"
    OFFSET = "OFFSET"


class Side(Enum):
    """Define the side of an interval endpoint."""

    LEFT = "LEFT"
    RIGHT = "RIGHT"


class Endpoint:
    """
    One of the two endpoints of a ``Range``.

    Used to compare ranges. Eg, the range ``>10`` can be represented by:

        >>> start = Endpoint(10, False, Side.LEFT)
        >>> end = Endpoint(None, True, Side.RIGHT)
        >>> print(f'{start},{end}')
        (10,∞]

    The first endpoint represents the value 10 at the left side, in an open
    interval. The second endpoint represents infinity in this case.
    """

    def __init__(self, value: Any, include: bool, side: Side):
        self.value = value
        self.include = include
        self.side = side

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Endpoint):
            return NotImplemented

        return self.value == other.value and self.include == other.include

    def __gt__(self, other: Any) -> bool:  # pylint: disable=too-many-return-statements
        if not isinstance(other, Endpoint):
            return NotImplemented

        if self.value is None:
            return self.side == Side.RIGHT

        if other.value is None:
            return other.side == Side.LEFT

        if self.value == other.value:
            if self.side == Side.LEFT:
                if other.side == Side.LEFT:
                    return not self.include and other.include
                return not self.include

            # self.side = Side.RIGHT
            if other.side == Side.RIGHT:
                return not other.include and self.include
            return False

        return bool(self.value > other.value)

    # needed for ``max()``
    def __lt__(self, other: Any) -> bool:
        return not self > other

    def __repr__(self) -> str:
        """
        Representation of an endpoint.

            >>> print(Endpoint(10, False, Side.LEFT))
            (10

        """
        if self.side == Side.LEFT:
            symbol = "[" if self.include else "("
            value = "-∞" if self.value is None else self.value
            return f"{symbol}{value}"

        symbol = "]" if self.include else ")"
        value = "∞" if self.value is None else self.value
        return f"{value}{symbol}"


def get_endpoints_from_operation(
    operator: Operator,
    value: Any,
) -> Tuple[Endpoint, Endpoint]:
    """
    Returns endpoints from an operation.
    """
    if operator == Operator.EQ:
        return Endpoint(value, True, Side.LEFT), Endpoint(value, True, Side.RIGHT)
    if operator == Operator.GE:
        return Endpoint(value, True, Side.LEFT), Endpoint(None, True, Side.RIGHT)
    if operator == Operator.GT:
        return Endpoint(value, False, Side.LEFT), Endpoint(None, True, Side.RIGHT)
    if operator == Operator.LE:
        return Endpoint(None, True, Side.LEFT), Endpoint(value, True, Side.RIGHT)
    if operator == Operator.LT:
        return Endpoint(None, True, Side.LEFT), Endpoint(value, False, Side.RIGHT)

    # pylint: disable=broad-exception-raised
    raise Exception(f"Invalid operator: {operator}")


class Filter:
    """
    A filter representing a SQL predicate.
    """

    operators: Set[Operator] = set()

    @classmethod
    def build(cls, operations: Set[Tuple[Operator, Any]]) -> "Filter":
        """
        Given a set of operations, build a filter:

            >>> operations = [(Operator.GT, 10), (Operator.GT, 20)]
            >>> print(Range.build(operations))
            >20

        """
        raise NotImplementedError("Subclass must implement ``build``")

    def check(self, value: Any) -> bool:
        """
        Test if a given filter matches a value:

            >>> operations = [(Operator.GT, 10), (Operator.GT, 20)]
            >>> filter_ = Range.build(operations)
            >>> filter_.check(10)
            False
            >>> filter_.check(30)
            True

        """
        raise NotImplementedError("Subclass must implement ``check``")


class Impossible(Filter):
    """
    Custom Filter returned when impossible conditions are passed.
    """

    @classmethod
    def build(cls, operations: Set[Tuple[Operator, Any]]) -> Filter:
        return Impossible()

    def check(self, value: Any) -> bool:
        return False

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Impossible):
            return NotImplemented

        return True

    def __repr__(self) -> str:
        return "1 = 0"


class IsNull(Filter):
    """
    Filter for ``IS NULL``.
    """

    operators: Set[Operator] = {Operator.IS_NULL}

    @classmethod
    def build(cls, operations: Set[Tuple[Operator, Any]]) -> Filter:
        return IsNull()

    def check(self, value: Any) -> bool:
        return value is None

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, IsNull):
            return NotImplemented

        return True

    def __repr__(self) -> str:
        return "IS NULL"


class IsNotNull(Filter):
    """
    Filter for ``IS NOT NULL``.
    """

    operators: Set[Operator] = {Operator.IS_NOT_NULL}

    @classmethod
    def build(cls, operations: Set[Tuple[Operator, Any]]) -> Filter:
        return IsNotNull()

    def check(self, value: Any) -> bool:
        return value is not None

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, IsNotNull):
            return NotImplemented

        return True

    def __repr__(self) -> str:
        return "IS NOT NULL"


class Equal(Filter):
    """
    Equality comparison.
    """

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
        return f"=={self.value}"


class NotEqual(Filter):
    """
    Inequality comparison.
    """

    operators: Set[Operator] = {
        Operator.NE,
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
        return bool(value != self.value)

    def __repr__(self) -> str:
        return f"!={self.value}"


class Like(Filter):
    """
    Substring searches.
    """

    operators: Set[Operator] = {
        Operator.LIKE,
    }

    def __init__(self, value: Any):
        self.value = value
        self.regex = re.compile(
            self.value.replace("_", ".").replace("%", ".*"),
            re.IGNORECASE,
        )

    @classmethod
    def build(cls, operations: Set[Tuple[Operator, Any]]) -> Filter:
        # we only accept a single value
        values = {value for operator, value in operations}
        if len(values) != 1:
            return Impossible()

        return cls(values.pop())

    def check(self, value: Any) -> bool:
        return bool(self.regex.match(value))

    def __repr__(self) -> str:
        return f"LIKE {self.value}"


class Range(Filter):
    """
    A range comparison.

    This filter represents a range, with an optional start and an
    optional end. Start and end can be inclusive or exclusive.

    Ranges can be combined by adding them:

        >>> range1 = Range(start=10)
        >>> range2 = Range(start=20)
        >>> print(range1 + range2)
        >20
        >>> range3 = Range(end=40)
        >>> print(range2 + range3)
        >20,<40

    """

    def __init__(
        self,
        start: Optional[Any] = None,
        end: Optional[Any] = None,
        include_start: bool = False,
        include_end: bool = False,
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

    def __add__(self, other: Any) -> Filter:
        if not isinstance(other, Range):
            return NotImplemented

        start = Endpoint(self.start, self.include_start, Side.LEFT)
        end = Endpoint(self.end, self.include_end, Side.RIGHT)

        new_start = Endpoint(other.start, other.include_start, Side.LEFT)
        new_end = Endpoint(other.end, other.include_end, Side.RIGHT)

        start = max(start, new_start)
        end = min(end, new_end)

        if start > end:
            return Impossible()

        return Range(start.value, end.value, start.include, end.include)

    @classmethod
    def build(cls, operations: Set[Tuple[Operator, Any]]) -> Filter:
        start = Endpoint(None, True, Side.LEFT)
        end = Endpoint(None, True, Side.RIGHT)

        for operator, value in operations:
            new_start, new_end = get_endpoints_from_operation(operator, value)

            start = max(start, new_start)
            end = min(end, new_end)

            if start > end:
                return Impossible()

        return cls(start.value, end.value, start.include, end.include)

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
            return f"=={self.start}"

        comparisons = []
        if self.start is not None:
            operator = ">=" if self.include_start else ">"
            comparisons.append(f"{operator}{self.start}")
        if self.end is not None:
            operator = "<=" if self.include_end else "<"
            comparisons.append(f"{operator}{self.end}")
        return ",".join(comparisons)
