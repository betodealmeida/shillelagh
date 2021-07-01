import inspect
from typing import Any
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple
from typing import Type
from typing import TypeVar

from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Field
from shillelagh.fields import Integer
from shillelagh.filters import Filter
from shillelagh.types import RequestedOrder
from shillelagh.types import Row


T = TypeVar("T", bound="Adapter")


class Adapter:

    # disable "unsafe" adapters that write to disk
    safe = False

    def __init__(self, *args: Any, **kwargs: Any):
        pass  # pragma: no cover

    @staticmethod
    def supports(uri: str, **kwargs: Any) -> bool:
        raise NotImplementedError("Subclasses must implement `supports`")

    @staticmethod
    def parse_uri(uri: str) -> Tuple[Any, ...]:
        raise NotImplementedError("Subclasses must implement `parse_uri`")

    def get_metadata(self) -> Dict[str, Any]:
        return {}

    def get_columns(self) -> Dict[str, Field]:
        """This method is called for every query, so make sure it's cheap."""
        return dict(
            inspect.getmembers(self, lambda attribute: isinstance(attribute, Field)),
        )

    def get_data(
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
    ) -> Iterator[Row]:
        """
        Yield rows as DB-specific types.

        This method expects rows to be in the storage format. Eg, for the CSV adapter
        datetime columns would be stored (and yielded) as strings. The `get_rows`
        method will use the adapter fields to convert these values into native Python
        types (in this case, a proper `datetime.datetime`).
        """
        raise NotImplementedError("Subclasses must implement `get_data`")

    def get_rows(
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
    ) -> Iterator[Row]:
        """
        Yield rows as native Python types.
        """
        columns = self.get_columns()
        parsers = {column_name: field.parse for column_name, field in columns.items()}
        parsers["rowid"] = Integer().parse

        for row in self.get_data(bounds, order):
            yield {
                column_name: parsers[column_name](value)
                for column_name, value in row.items()
            }

    def insert_data(self, row: Row) -> int:
        raise NotImplementedError("Subclasses must implement `insert_row`")

    def insert_row(self, row: Row) -> int:
        """
        Convert native Python to DB-specific types.
        """
        columns = self.get_columns().copy()
        columns["rowid"] = Integer()
        row = {
            column_name: columns[column_name].format(value)
            for column_name, value in row.items()
        }
        return self.insert_data(row)

    def delete_data(self, row_id: int) -> None:
        raise NotImplementedError("Subclasses must implement `delete_row`")

    def delete_row(self, row_id: int) -> None:
        return self.delete_data(row_id)

    def update_data(self, row_id: int, row: Row) -> None:
        # Subclasses are free to implement inplace updates
        self.delete_data(row_id)
        self.insert_data(row)

    def update_row(self, row_id: int, row: Row) -> None:
        # Subclasses are free to implement inplace updates
        columns = self.get_columns().copy()
        columns["rowid"] = Integer()
        row = {
            column_name: columns[column_name].format(value)
            for column_name, value in row.items()
        }
        self.update_data(row_id, row)

    def close(self) -> None:
        pass
