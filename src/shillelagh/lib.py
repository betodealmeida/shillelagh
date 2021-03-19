import itertools
import json
from typing import Any
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple
from typing import Type

from shillelagh.fields import Field
from shillelagh.fields import Float
from shillelagh.fields import Integer
from shillelagh.fields import Order
from shillelagh.fields import String
from shillelagh.types import Row


class RowIDManager:
    def __init__(self, ranges: List[range]):
        if not ranges:
            raise Exception("Argument `ranges` cannot be empty")

        self.ranges = ranges
        self._max_id = ranges[-1].stop - 1

    def __iter__(self):
        ids = itertools.chain(*self.ranges)
        first = next(ids)
        for i in range(first):
            yield -1
        yield first
        prev = first
        for id_ in ids:
            for i in range(id_ - prev - 1):
                yield -1
            yield id_
            prev = id_
        for i in range(self._max_id - prev):
            yield -1

    def _check_row_id(self, row_id: int) -> None:
        for r in self.ranges:
            if r.start <= row_id < r.stop:
                raise Exception(f"Row ID {row_id} already present")

    def insert(self, row_id: Optional[int] = None) -> int:
        if row_id is None:
            row_id = self._max_id + 1
        else:
            self._check_row_id(row_id)
        self._max_id = max(self._max_id, row_id)

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
                    self.ranges.pop(i)
                elif row_id == r.start:
                    self.ranges[i] = range(r.start + 1, r.stop)
                elif row_id == r.stop - 1:
                    self.ranges[i] = range(r.start, r.stop - 1)
                else:
                    self.ranges[i] = range(r.start, row_id)
                    self.ranges.insert(i + 1, range(row_id + 1, r.stop))

                return

        raise Exception(f"Row ID {row_id} not found")


def analyse(
    data: Iterator[Row],
) -> Tuple[int, Dict[str, Order], Dict[str, Type[Field]]]:
    """Compute number of rows, order and types."""
    order: Dict[str, Order] = {}
    types: Dict[str, Type[Field]] = {}

    previous_row = None
    for i, row in enumerate(data):
        for column_name, value in row.items():
            # determine order
            if previous_row:
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
