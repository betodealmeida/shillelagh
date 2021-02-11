from datetime import datetime
from enum import Enum
from typing import Any
from typing import Callable
from typing import cast
from typing import List
from typing import Optional
from typing import Type

import dateutil.parser
from shillelagh.filters import Filter


class Order(Enum):
    ASCENDING = "ascending"
    DESCENDING = "descending"
    NONE = "none"


class Field:

    type: Optional[str] = None

    def __init__(
        self,
        filters: Optional[List[Type[Filter]]] = None,
        order: Order = Order.NONE,
        exact: bool = False,
    ):
        self.filters = filters or []
        self.order = order
        self.exact = exact

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Field):
            return NotImplemented

        return (
            self.filters == other.filters
            and self.order == other.order
            and self.exact == other.exact
        )

    @staticmethod
    def parse(value: Any) -> Any:
        raise NotImplementedError("Subclasses must implement `parse`")


class Integer(Field):
    type = "INTEGER"

    @staticmethod
    def parse(value: Any) -> int:
        return int(value)


class Float(Field):
    type = "REAL"

    @staticmethod
    def parse(value: Any) -> float:
        return float(value)


class String(Field):
    type = "TEXT"

    @staticmethod
    def parse(value: Any) -> str:
        return str(value)


class DateTime(Field):
    type = "TIMESTAMP"

    @staticmethod
    def parse(value: Any) -> datetime:
        return dateutil.parser.parse(value)
