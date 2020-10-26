import csv
import operator
import os
from functools import reduce
from pathlib import Path
from typing import Dict
from typing import Iterator
from typing import Optional

from shillelagh.fields import Field
from shillelagh.fields import Float
from shillelagh.fields import Integer
from shillelagh.fields import Order
from shillelagh.fields import String
from shillelagh.filters import Filter
from shillelagh.filters import Range
from shillelagh.lib import RowIDManager
from shillelagh.table import VirtualTable
from shillelagh.types import Row


class CSVFile(VirtualTable):
    def __init__(self, path: str):
        self.path = Path(path)

        self.analyse()

    def analyse(self) -> None:
        with open(self.path) as fp:
            reader = csv.reader(fp, quoting=csv.QUOTE_NONNUMERIC)
            column_names = next(reader)
            order = {column_name: Order.NONE for column_name in column_names}
            types = {column_name: Float for column_name in column_names}

            previous_row = None
            for i, row in enumerate(reader):
                # determine types
                for column_name, value in zip(column_names, row):
                    if types[column_name] == String:
                        continue
                    elif type(value) == str:
                        types[column_name] = String

                # determine order
                if previous_row:
                    for column_name, previous, current in zip(
                        column_names, previous_row, row,
                    ):
                        try:
                            # on the 2nd row we can determine the potential order
                            if i == 1:
                                order[column_name] = (
                                    Order.ASCENDING
                                    if current >= previous
                                    else Order.DESCENDING
                                )
                            elif order[column_name] == Order.NONE:
                                continue
                            elif (
                                order[column_name] == Order.ASCENDING
                                and current < previous
                            ):
                                order[column_name] = Order.NONE
                            elif (
                                order[column_name] == Order.DESCENDING
                                and current > previous
                            ):
                                order[column_name] = Order.NONE
                        except TypeError:
                            order[column_name] = Order.NONE

                previous_row = row

        num_rows = i + 1
        self.row_id_manager = RowIDManager([range(0, num_rows + 1)])
        self.columns = {
            column_name: types[column_name](
                filters=[Range], order=order[column_name], exact=True,
            )
            for column_name in column_names
        }

    def get_columns(self) -> Dict[str, Field]:
        return self.columns

    def get_data(self, bounds: Dict[str, Filter]) -> Iterator[Row]:
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
                    op = operator.ge if filter_.include_start else operator.gt
                    filters.append(
                        lambda row, value=filter_.start, i=column_index, op=op: op(
                            row[i], value,
                        ),
                    )

                if filter_.end is not None:
                    op = operator.le if filter_.include_end else operator.lt
                    filters.append(
                        lambda row, value=filter_.end, i=column_index, op=op: op(
                            row[i], value,
                        ),
                    )

            def combined_filters(row):
                return reduce(lambda f1, f2: f1 and f2(row), filters, True)

            for row in filter(combined_filters, data):
                yield {col: value for col, value in zip(column_names, row)}

    def insert_row(self, row: Row) -> int:
        row_id: Optional[int] = row.pop("rowid")
        row_id = self.row_id_manager.add(row_id)

        column_names = list(self.get_columns().keys())
        with open(self.path, "a") as fp:
            writer = csv.writer(fp, quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow([row[column_name] for column_name in column_names])

        return row_id

    def delete_row(self, row_id: int) -> None:
        self.row_id_manager.delete(row_id)

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
