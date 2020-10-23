from enum import Enum
from typing import Any, Dict, List, Tuple, Optional, Union

import dateutil.parser

from shillelagh.filters import Filter


# A value corresponding to a constraint is one of:
#     None
#         This means you have no index for that constraint. SQLite will have to
#         iterate over every row for it.
#     integer
#         This is the argument number for the constraintargs being passed
#         into the Filter() function of your cursor.
#     (integer, boolean)
#         By default SQLite will check what you return. If you set the boolean
#         to False then SQLite won’t do that double checking.
Constraint = Union[None, int, Tuple[int, bool]]

# A row of data
Row = Dict[str, Any]


class Order(Enum):
    ASCENDING = "ascending"
    DESCENDING = "descending"
    NONE = "none"


class Type:
    def __init__(
        self,
        filters: Optional[List[Filter]] = None,
        order: Order = Order.NONE,
        exact: bool = False,
    ):
        self.filters = filters or []
        self.order = order
        self.exact = exact


class Float(Type):
    type = "REAL"

    @staticmethod
    def parse(value):
        return float(value)


class DateTime(Type):
    type = "TIMESTAMP"

    @staticmethod
    def parse(value):
        return dateutil.parser.parse(value)
