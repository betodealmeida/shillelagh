"""
An adapter for CSV files.

This adapter treats a CSV file as a table, allowing rows to be inserted,
deleted, and updated. It's not very practical since it requires the data
to be written with the ``QUOTE_NONNUMERIC`` format option, with strings
explicitly quoted. It's also not very efficient, since it implements the
filtering and sorting in Python, instead of relying on the backend.
"""
import csv
import logging
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
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Field
from shillelagh.filters import Filter
from shillelagh.filters import Range
from shillelagh.lib import analyze
from shillelagh.lib import filter_data
from shillelagh.lib import RowIDManager
from shillelagh.lib import update_order
from shillelagh.typing import RequestedOrder
from shillelagh.typing import Row

_logger = logging.getLogger(__name__)


class RowTracker:
    """An iterator that keeps track of the last yielded row."""

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

    r"""
    An adapter to CSV files.

    The files must be written with the ``QUOTE_NONNUMERIC`` format option, with
    strings explicitly quoted::

        "index","temperature","site"
        10.0,15.2,"Diamond_St"
        11.0,13.1,"Blacktail_Loop"
        12.0,13.3,"Platinum_St"
        13.0,12.1,"Kodiak_Trail"

    The adapter will first scan the whole file to determine number of rows, as
    well as the type and order of each column.

    The adapter has no index. When data is ``SELECT``\ed the adapter will stream
    over all the rows in the file, filtering them on the fly. If a specific
    order is requests the resulting rows will be loaded into memory so they
    can be sorted.

    Inserted rows are appended to the end of the file. Deleted rows simply
    have their row ID marked as deleted (-1), and are ignored when the data is
    scanned for results. When the adapter is closed deleted rows will be
    garbage collected.

    Updates are handled with a delete followed by an insert.
    """

    # the adapter is not safe, since it could be used to read files from
    # the filesystem, or potentially overwrite existing files
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
        super().__init__()

        self.path = Path(path)
        self.modified = False

        _logger.info("Opening file CSV file %s to load metadata", self.path)
        with open(self.path) as csvfile:
            reader = csv.reader(csvfile, quoting=csv.QUOTE_NONNUMERIC)
            try:
                column_names = next(reader)
            except StopIteration as ex:
                raise ProgrammingError("The file has no rows") from ex
            data = (dict(zip(column_names, row)) for row in reader)

            # put data in a ``RowTracker``, so we can monitor the last row
            # and keep track of the column order
            row_tracker = RowTracker(data)

            # analyze data to determine number of rows, as well as the order
            # and type of each column
            num_rows, order, types = analyze(row_tracker)
            _logger.debug("Read %d rows", num_rows)

        self.columns = {
            column_name: types[column_name](
                filters=[Range],
                order=order[column_name],
                exact=True,
            )
            for column_name in column_names
        }

        # the row ID manager is used to keep track of insertions and deletions
        self.row_id_manager = RowIDManager([range(0, num_rows + 1)])

        self.last_row = row_tracker.last_row
        self.num_rows = num_rows

    def get_columns(self) -> Dict[str, Field]:
        return self.columns

    def get_data(
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
    ) -> Iterator[Row]:
        _logger.info("Opening file CSV file %s to load data", self.path)
        with open(self.path) as csvfile:
            reader = csv.reader(csvfile, quoting=csv.QUOTE_NONNUMERIC)

            try:
                header = next(reader)
            except StopIteration as ex:
                raise ProgrammingError("The file has no rows") from ex
            column_names = ["rowid", *header]

            rows = ([i, *row] for i, row in zip(self.row_id_manager, reader) if i != -1)
            data = (dict(zip(column_names, row)) for row in rows)

            # Filter and sort the data. It would probably be more efficient to simply
            # declare the columns as having no filter and no sort order, and let the
            # backend handle this; but it's nice to have an example of how to do this.
            for row in filter_data(data, bounds, order):
                yield row
                _logger.debug(row)

    def insert_data(self, row: Row) -> int:
        row_id: Optional[int] = row.pop("rowid")
        row_id = cast(int, self.row_id_manager.insert(row_id))

        # append row
        column_names = list(self.get_columns().keys())
        _logger.info("Appending row with ID %d to CSV file %s", row_id, self.path)
        _logger.debug(row)
        with open(self.path, "a") as csvfile:
            writer = csv.writer(csvfile, quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow([row[column_name] for column_name in column_names])
        self.num_rows += 1

        # update order, in case it has changed
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

    def delete_data(self, row_id: int) -> None:
        _logger.info("Deleting row with ID %d from CSV file %s", row_id, self.path)
        # on ``DELETE``\s we simply mark the row as deleted, so that it will be ignored
        # on ``SELECT``\s
        self.row_id_manager.delete(row_id)
        self.num_rows -= 1
        self.modified = True

    def close(self) -> None:
        """
        Garbage collect the file.

        This method will get rid of deleted rows in the files.
        """
        if not self.modified:
            return

        # garbage collect -- should we sort the data according to the initial sort
        # order when writing to the new file?
        with open(self.path) as csvfile:
            reader = csv.reader(csvfile, quoting=csv.QUOTE_NONNUMERIC)
            column_names = next(reader)
            data = (row for i, row in zip(self.row_id_manager, reader) if i != -1)

            with open(self.path.with_suffix(".csv.bak"), "w") as copy:
                writer = csv.writer(copy, quoting=csv.QUOTE_NONNUMERIC)
                writer.writerow(column_names)
                writer.writerows(data)

        os.replace(self.path.with_suffix(".csv.bak"), self.path)
        self.modified = False
