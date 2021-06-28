import inspect
import itertools
import json
import operator
from functools import reduce
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
from shillelagh.types import RequestedOrder
from shillelagh.types import Row

DELETED = range(-1, 0)


class RowIDManager:
    def __init__(self, ranges: List[range]):
        if not ranges:
            raise Exception("Argument `ranges` cannot be empty")

        self.ranges = ranges

    def __iter__(self):
        yield from itertools.chain(*self.ranges)

    def get_max_row_id(self) -> int:
        return max((r.stop - 1) for r in self.ranges)

    def check_row_id(self, row_id: int) -> None:
        for r in self.ranges:
            if r.start <= row_id < r.stop:
                raise Exception(f"Row ID {row_id} already present")

    def insert(self, row_id: Optional[int] = None) -> int:
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
        for i, r in enumerate(self.ranges):
            if r.start <= row_id < r.stop:
                if r.start == r.stop - 1:
                    self.ranges[i] = DELETED
                elif row_id == r.start:
                    self.ranges[i] = range(r.start + 1, r.stop)
                    self.ranges.insert(i, DELETED)
                elif row_id == r.stop - 1:
                    self.ranges[i] = range(r.start, r.stop - 1)
                    self.ranges.insert(i + 1, DELETED)
                else:
                    self.ranges[i] = range(r.start, row_id)
                    self.ranges.insert(i + 1, range(row_id + 1, r.stop))
                    self.ranges.insert(i + 1, DELETED)

                return

        raise Exception(f"Row ID {row_id} not found")


def analyse(
    data: Iterator[Row],
) -> Tuple[int, Dict[str, Order], Dict[str, Type[Field]]]:
    """Compute number of rows, order and types."""
    order: Dict[str, Order] = {}
    types: Dict[str, Type[Field]] = {}

    previous_row: Optional[Row] = None
    i = 0
    for i, row in enumerate(data):
        for column_name, value in row.items():
            # determine order
            if previous_row is not None:
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
            elif type(value) == str:
                types[column_name] = String
            elif types.get(column_name) == Float:
                continue
            elif type(value) == float:
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
    if num_rows < 2 or previous is None:
        return Order.NONE

    try:
        if num_rows == 2:
            return Order.ASCENDING if current >= previous else Order.DESCENDING
        elif (
            current_order == Order.NONE
            or (current_order == Order.ASCENDING and current < previous)
            or (current_order == Order.DESCENDING and current > previous)
        ):
            return Order.NONE
    except TypeError:
        return Order.NONE

    return current_order


def quote(value: str) -> str:
    return value.replace("'", "''")


def unquote(value: str) -> str:
    return value.replace("''", "'")


def serialize(value: Any) -> str:
    return f"'{quote(json.dumps(value))}'"


def deserialize(value: str) -> Any:
    return json.loads(unquote(value[1:-1]))


def build_sql(
    columns: Dict[str, Field],
    bounds: Dict[str, Filter],
    order: List[Tuple[str, RequestedOrder]],
    column_map: Optional[Dict[str, str]] = None,
    offset: int = 0,
) -> str:
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
                op = ">=" if filter_.include_start else ">"
                conditions.append(f"{id_} {op} {field.quote(filter_.start)}")
            if filter_.end is not None:
                op = "<=" if filter_.include_end else "<"
                conditions.append(f"{id_} {op} {field.quote(filter_.end)}")
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
    for column_name, filter_ in bounds.items():

        def apply_filter(
            data: Iterator[Row],
            op: Callable[[Any, Any], bool],
            column_name: str,
            value: Any,
        ) -> Iterator[Row]:
            return (row for row in data if op(row[column_name], value))

        if isinstance(filter_, Impossible):
            return
        if isinstance(filter_, Equal):
            data = apply_filter(data, operator.eq, column_name, filter_.value)
        if isinstance(filter_, Range):
            if filter_.start is not None:
                op = operator.ge if filter_.include_start else operator.gt
                data = apply_filter(data, op, column_name, filter_.start)
            if filter_.end is not None:
                op = operator.le if filter_.include_end else operator.lt
                data = apply_filter(data, op, column_name, filter_.end)

    if order:
        rows = list(data)  # :(
        for column_name, requested_order in order:
            reverse = requested_order == Order.DESCENDING
            rows.sort(key=operator.itemgetter(column_name), reverse=reverse)
        data = iter(rows)

    yield from data
