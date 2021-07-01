import atexit
import csv
import os
import urllib.parse
from pathlib import Path
from typing import Any
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
from shillelagh.lib import filter_data
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
    def supports(uri: str, **kwargs: Any) -> bool:
        parsed = urllib.parse.urlparse(uri)
        return parsed.scheme == "csv"

    @staticmethod
    def parse_uri(uri: str) -> Tuple[str]:
        parsed = urllib.parse.urlparse(uri)

        # netloc is populated for relative paths, path for absolute
        return (parsed.path or parsed.netloc,)

    def __init__(self, path: str):
        # ensure we do GC on the file if we exit without closing the
        # connection
        self.modified = False
        atexit.register(self.close)

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
            column_names = ["rowid", *next(reader)]
            rows = ([i, *row] for i, row in zip(self.row_id_manager, reader) if i != -1)
            data = (
                {column_name: value for column_name, value in zip(column_names, row)}
                for row in rows
            )
            yield from filter_data(data, bounds, order)

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
        self.modified = True

        return row_id

    def delete_row(self, row_id: int) -> None:
        # mark row as deleted
        self.row_id_manager.delete(row_id)
        self.num_rows -= 1
        self.modified = True

    def close(self) -> None:
        if not self.modified:
            return

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
        self.modified = False
