"""Helper functions for Shillelagh."""
import inspect
import itertools
import json
import operator
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple
from typing import Type

from shillelagh.exceptions import ImpossibleFilterError
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Field
from shillelagh.fields import Float
from shillelagh.fields import Integer
from shillelagh.fields import Order
from shillelagh.fields import String
from shillelagh.filters import Equal
from shillelagh.filters import Filter
from shillelagh.filters import Impossible
from shillelagh.filters import Range
from shillelagh.typing import RequestedOrder
from shillelagh.typing import Row

DELETED = range(-1, 0)


class RowIDManager:
    """
    A row ID manager that tracks insert and deletes.

    The `RowIDManager` should be used with an append-only table structure.
    It assigns a row ID to each row. When a new row is appended it will
    automatically receive a new ID. And when rows are deleted their ID
    gets changed to -1 to indicate the deletion.

    An example:

        >>> data = ["zero", "one", "two"]
        >>> manager = RowIDManager([range(len(data))])

    To insert data:

        >>> data.append("three")
        >>> manager.insert()
        3
        >>> data.append("four")
        >>> manager.insert(10)  # you can specify a row ID
        10
        >>> for row_id, value in zip(manager, data):
        ...     if row_id != -1:
        ...         print(row_id, value)
        0 zero
        1 one
        2 two
        3 three
        10 four

    To delete data:

        >>> manager.delete(data.index("two"))
        >>> print(data)
        ['zero', 'one', 'two', 'three', 'four']
        >>> for row_id, value in zip(manager, data):
        ...     if row_id != -1:
        ...         print(row_id, value)
        0 zero
        1 one
        3 three
        10 four

    """

    def __init__(self, ranges: List[range]):
        if not ranges:
            raise Exception("Argument `ranges` cannot be empty")

        self.ranges = ranges

    def __iter__(self):
        yield from itertools.chain(*self.ranges)

    def get_max_row_id(self) -> int:
        """
        Find the maximum row ID.
        """
        return max((r.stop - 1) for r in self.ranges)

    def check_row_id(self, row_id: int) -> None:
        """
        Check if a provided row ID is not being used.
        """
        for range_ in self.ranges:
            if range_.start <= row_id < range_.stop:
                raise Exception(f"Row ID {row_id} already present")

    def insert(self, row_id: Optional[int] = None) -> int:
        """
        Insert a new row ID.
        """
        if row_id is None:
            max_row_id = self.get_max_row_id()
            row_id = max_row_id + 1
        else:
            self.check_row_id(row_id)

        last = self.ranges[-1]
        if last.stop == row_id:
            self.ranges[-1] = range(last.start, row_id + 1)
        else:
            self.ranges.append(range(row_id, row_id + 1))
        return row_id

    def delete(self, row_id: int) -> None:
        """Mark a given row ID as deleted."""
        for i, range_ in enumerate(self.ranges):
            if range_.start <= row_id < range_.stop:
                if range_.start == range_.stop - 1:
                    self.ranges[i] = DELETED
                elif row_id == range_.start:
                    self.ranges[i] = range(range_.start + 1, range_.stop)
                    self.ranges.insert(i, DELETED)
                elif row_id == range_.stop - 1:
                    self.ranges[i] = range(range_.start, range_.stop - 1)
                    self.ranges.insert(i + 1, DELETED)
                else:
                    self.ranges[i] = range(range_.start, row_id)
                    self.ranges.insert(i + 1, range(row_id + 1, range_.stop))
                    self.ranges.insert(i + 1, DELETED)

                return

        raise Exception(f"Row ID {row_id} not found")


def analyze(
    data: Iterator[Row],
) -> Tuple[int, Dict[str, Order], Dict[str, Type[Field]]]:
    """
    Compute number of rows, order, and types from a stream of rows.
    """
    order: Dict[str, Order] = {}
    types: Dict[str, Type[Field]] = {}

    previous_row: Row = {}
    i = 0
    for i, row in enumerate(data):
        for column_name, value in row.items():
            # determine order
            if i > 0:
                previous = previous_row[column_name]
                order[column_name] = update_order(
                    current_order=order.get(column_name, Order.NONE),
                    previous=previous,
                    current=value,
                    num_rows=i + 1,
                )

            # determine types
            if types.get(column_name) == String:
                continue
            if isinstance(value, str):
                types[column_name] = String
            elif types.get(column_name) == Float:
                continue
            elif isinstance(value, float):
                types[column_name] = Float
            elif types.get(column_name) == Integer:
                continue
            else:
                types[column_name] = Integer

        previous_row = row

    num_rows = i + 1

    return num_rows, order, types


def update_order(
    current_order: Order,
    previous: Any,
    current: Any,
    num_rows: int,
) -> Order:
    """
    Update the stored order of a given column.

    This is used to analyze the order of columns, by traversing the
    results and checking if their are sorted in any way.
    """
    if num_rows < 2 or previous is None:
        return Order.NONE

    try:
        if num_rows == 2:
            return Order.ASCENDING if current >= previous else Order.DESCENDING
        if (
            current_order == Order.NONE
            or (current_order == Order.ASCENDING and current < previous)
            or (current_order == Order.DESCENDING and current > previous)
        ):
            return Order.NONE
    except TypeError:
        return Order.NONE

    return current_order


def escape(value: str) -> str:
    """Escape single quotes."""
    return value.replace("'", "''")


def unescape(value: str) -> str:
    """Unescape single quotes."""
    return value.replace("''", "'")


def serialize(value: Any) -> str:
    """
    Serialize adapter arguments.

    This function is used with the SQLite backend, in order to serialize
    the arguments needed to instantiate an adapter via a virtual table.
    """
    return f"'{escape(json.dumps(value))}'"


def deserialize(value: str) -> Any:
    """
    Deserialize adapter arguments.

    This function is used by the SQLite backend, in order to deserialize
    the virtual table definition and instantiate an adapter.
    """
    return json.loads(unescape(value[1:-1]))


def build_sql(
    columns: Dict[str, Field],
    bounds: Dict[str, Filter],
    order: List[Tuple[str, RequestedOrder]],
    column_map: Optional[Dict[str, str]] = None,
    offset: int = 0,
) -> str:
    """
    Build a SQL query.

    This is used by the GSheets and Socrata adapters, which use a simplified
    SQL dialect to fetch data. For GSheets a column map is required, since the
    SQL references columns by label ("A", "B", etc.) instead of name.
    """
    sql = "SELECT *"

    conditions = []
    for column_name, filter_ in bounds.items():
        field = columns[column_name]
        id_ = column_map[column_name] if column_map else column_name
        if isinstance(filter_, Impossible):
            raise ImpossibleFilterError()
        if isinstance(filter_, Equal):
            conditions.append(f"{id_} = {field.quote(filter_.value)}")
        elif isinstance(filter_, Range):
            if filter_.start is not None:
                operator_ = ">=" if filter_.include_start else ">"
                conditions.append(f"{id_} {operator_} {field.quote(filter_.start)}")
            if filter_.end is not None:
                operator_ = "<=" if filter_.include_end else "<"
                conditions.append(f"{id_} {operator_} {field.quote(filter_.end)}")
        else:
            raise ProgrammingError(f"Invalid filter: {filter_}")
    if conditions:
        sql = f"{sql} WHERE {' AND '.join(conditions)}"

    column_order: List[str] = []
    for column_name, requested_order in order:
        id_ = column_map[column_name] if column_map else column_name
        desc = " DESC" if requested_order == Order.DESCENDING else ""
        column_order.append(f"{id_}{desc}")
    if column_order:
        sql = f"{sql} ORDER BY {', '.join(column_order)}"
    if offset > 0:
        sql = f"{sql} OFFSET {offset}"

    return sql


def combine_args_kwargs(
    func: Callable[..., Any], *args: Any, **kwargs: Any
) -> Tuple[Any, ...]:
    """
    Combine args and kwargs into args.

    This is needed because we allow users to pass custom kwargs to adapters,
    but when creating the virtual table we serialize only args.
    """
    signature = inspect.signature(func)
    bound_args = signature.bind(*args, **kwargs)
    bound_args.apply_defaults()
    return bound_args.args


def filter_data(
    data: Iterator[Row],
    bounds: Dict[str, Filter],
    order: List[Tuple[str, RequestedOrder]],
) -> Iterator[Row]:
    """
    Apply filtering and sorting to a stream of rows.

    This is used mostly as an exercise. It's probably much more efficient to
    simply declare fields without any filtering/sorting and let the backend
    (SQLite, eg) handle it.
    """
    for column_name, filter_ in bounds.items():

        def apply_filter(
            data: Iterator[Row],
            operator_: Callable[[Any, Any], bool],
            column_name: str,
            value: Any,
        ) -> Iterator[Row]:
            """
            Apply a given filter to an iterator of rows.

            This method is needed because Python uses lazy bindings in
            generator expressions, so we need to create a new scope in
            order to apply several filters that reuse the same variable
            names.
            """
            return (row for row in data if operator_(row[column_name], value))

        if isinstance(filter_, Impossible):
            return
        if isinstance(filter_, Equal):
            data = apply_filter(data, operator.eq, column_name, filter_.value)
        if isinstance(filter_, Range):
            if filter_.start is not None:
                operator_ = operator.ge if filter_.include_start else operator.gt
                data = apply_filter(data, operator_, column_name, filter_.start)
            if filter_.end is not None:
                operator_ = operator.le if filter_.include_end else operator.lt
                data = apply_filter(data, operator_, column_name, filter_.end)

    if order:
        # in order to sort we need to consume the iterator and load it into
        # memory :(
        rows = list(data)
        for column_name, requested_order in order:
            reverse = requested_order == Order.DESCENDING
            rows.sort(key=operator.itemgetter(column_name), reverse=reverse)
        data = iter(rows)

    yield from data
