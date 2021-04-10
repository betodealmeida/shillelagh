import csv
import operator
import os
import urllib.parse
from functools import reduce
from pathlib import Path
from typing import cast
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple

from shillelagh.adapters.base import Adapter
from shillelagh.fields import Field
from shillelagh.filters import Filter
from shillelagh.filters import Range
from shillelagh.lib import analyse
from shillelagh.lib import RowIDManager
from shillelagh.lib import update_order
from shillelagh.types import RequestedOrder
from shillelagh.types import Row


class RowTracker:
    def __init__(self, iterable: Iterator[Row]):
        self.iterable = iterable
        self.last_row: Optional[Row] = None

    def __iter__(self) -> Iterator[Row]:
        for row in self.iterable:
            self.last_row = row
            yield row

    def __next__(self) -> Row:
        return self.iterable.__next__()


class CSVFile(Adapter):

    safe = False

    @staticmethod
    def supports(uri: str) -> bool:
        parsed = urllib.parse.urlparse(uri)
        return parsed.scheme == "csv"

    @staticmethod
    def parse_uri(uri: str) -> Tuple[str]:
        parsed = urllib.parse.urlparse(uri)

        # netloc is populated for relative paths, path for absolute
        return (parsed.path or parsed.netloc,)

    def __init__(self, path: str):
        self.path = Path(path)

        with open(self.path) as fp:
            reader = csv.reader(fp, quoting=csv.QUOTE_NONNUMERIC)
            column_names = next(reader)
            data = (dict(zip(column_names, row)) for row in reader)
            row_tracker = RowTracker(data)
            num_rows, order, types = analyse(row_tracker)

        self.columns = {
            column_name: types[column_name](
                filters=[Range],
                order=order[column_name],
                exact=True,
            )
            for column_name in column_names
        }
        self.last_row = row_tracker.last_row
        self.num_rows = num_rows
        self.row_id_manager = RowIDManager([range(0, num_rows + 1)])

    def get_columns(self) -> Dict[str, Field]:
        return self.columns

    def get_data(
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
    ) -> Iterator[Row]:
        with open(self.path) as fp:
            reader = csv.reader(fp, quoting=csv.QUOTE_NONNUMERIC)
            column_names = ["rowid"] + next(reader)
            data = ([i, *row] for i, row in zip(self.row_id_manager, reader) if i != -1)

            filters = []
            for column_name, filter_ in bounds.items():
                if not isinstance(filter_, Range):
                    raise Exception("Invalid filter")

                column_index = column_names.index(column_name)

                if filter_.start is not None:
                    start = filter_.start
                    op = operator.ge if filter_.include_start else operator.gt
                    filters.append(
                        lambda row, value=start, i=column_index, op=op: op(
                            row[i],
                            value,
                        ),
                    )

                if filter_.end is not None:
                    end = filter_.end
                    op = operator.le if filter_.include_end else operator.lt
                    filters.append(
                        lambda row, value=end, i=column_index, op=op: op(
                            row[i],
                            value,
                        ),
                    )

            def combined_filters(row):
                return reduce(lambda f1, f2: f1 and f2(row), filters, True)

            for row in filter(combined_filters, data):
                yield {col: value for col, value in zip(column_names, row)}

    def insert_row(self, row: Row) -> int:
        row_id: Optional[int] = row.pop("rowid")
        row_id = cast(int, self.row_id_manager.insert(row_id))

        # append row
        column_names = list(self.get_columns().keys())
        with open(self.path, "a") as fp:
            writer = csv.writer(fp, quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow([row[column_name] for column_name in column_names])
        self.num_rows += 1

        # update order
        for column_name, column_type in self.columns.items():
            column_type.order = update_order(
                current_order=column_type.order,
                previous=self.last_row[column_name] if self.last_row else None,
                current=row[column_name],
                num_rows=self.num_rows,
            )
        self.last_row = row

        return row_id

    def delete_row(self, row_id: int) -> None:
        # mark row as deleted
        self.row_id_manager.delete(row_id)
        self.num_rows -= 1

    def close(self) -> None:
        # garbage collect
        with open(self.path) as fp:
            reader = csv.reader(fp, quoting=csv.QUOTE_NONNUMERIC)
            column_names = next(reader)
            data = (row for i, row in zip(self.row_id_manager, reader) if i != -1)

            with open(self.path.with_suffix(".csv.bak"), "w") as copy:
                writer = csv.writer(copy, quoting=csv.QUOTE_NONNUMERIC)
                writer.writerow(column_names)
                writer.writerows(data)

        os.replace(self.path.with_suffix(".csv.bak"), self.path)
