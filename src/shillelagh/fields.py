from enum import Enum
from typing import List
from typing import Optional

import dateutil.parser
from shillelagh.filters import Filter


class Order(Enum):
    ASCENDING = "ascending"
    DESCENDING = "descending"
    NONE = "none"


class Field:
    def __init__(
        self,
        filters: Optional[List[Filter]] = None,
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


class Integer(Field):
    type = "INTEGER"
    parse = int


class Float(Field):
    type = "REAL"
    parse = float


class String(Field):
    type = "TEXT"
    parse = str


class DateTime(Field):
    type = "TIMESTAMP"

    @staticmethod
    def parse(value):
        return dateutil.parser.parse(value)
