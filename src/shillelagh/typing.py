"""Custom types for Shillelagh."""

from typing import Any, Optional, Union

from typing_extensions import Literal

from shillelagh.fields import Field, Order

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
Constraint = Union[None, int, tuple[int, bool]]
SQLiteConstraint = int

# A row of data
Row = dict[str, Any]

# An index is a tuple with a column index and an operator to filter it
Index = tuple[int, SQLiteConstraint]

OrderBy = tuple[int, bool]
RequestedOrder = Union[Literal[Order.ASCENDING], Literal[Order.DESCENDING]]

SQLiteValidType = Union[None, int, float, str, bytes]


# Cursor description
ColumnDescription = tuple[
    str,
    type[Field],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[bool],
]
Description = Optional[list[ColumnDescription]]

MaybeType = Optional[bool]
Maybe = None  # pylint: disable=invalid-name
