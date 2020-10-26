import itertools
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple

from shillelagh.fields import Field
from shillelagh.fields import Float
from shillelagh.fields import Integer
from shillelagh.fields import Order
from shillelagh.fields import String
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
            if not r.start <= row_id < r.stop:
                continue

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


def analyse(data: Iterator[Row]) -> Tuple[int, Dict[str, Order], Dict[str, Field]]:
    """Compute number of rows, order and types."""
    order: Dict[str, Order] = {}
    types: Dict[str, Field] = {}

    previous_row = None
    for i, row in enumerate(data):
        for column_name, value in row.items():
            # determine order
            if previous_row:
                previous = previous_row[column_name]
                try:
                    if i == 1:
                        order[column_name] = (
                            Order.ASCENDING if value >= previous else Order.DESCENDING
                        )
                    elif order[column_name] == Order.NONE:
                        pass
                    elif order[column_name] == Order.ASCENDING and value < previous:
                        order[column_name] = Order.NONE
                    elif order[column_name] == Order.DESCENDING and value > previous:
                        order[column_name] = Order.NONE
                except TypeError:
                    order[column_name] = Order.NONE

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
