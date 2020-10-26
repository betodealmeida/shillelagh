import inspect
from typing import Dict
from typing import Iterator

from shillelagh.fields import Field
from shillelagh.filters import Filter
from shillelagh.types import Row


class Adapter:
    def get_columns(self) -> Dict[str, Field]:
        return dict(
            inspect.getmembers(self, lambda attribute: isinstance(attribute, Field)),
        )

    def get_data(self, bounds: Dict[str, Filter]) -> Iterator[Row]:
        raise NotImplementedError("Subclasses must implement `get_data`")

    def insert_row(self, row: Row) -> int:
        raise NotImplementedError("Subclasses must implement `insert_row`")

    def delete_row(self, row_id: int) -> None:
        raise NotImplementedError("Subclasses must implement `delete_row`")

    def update_row(self, row_id: int, row: Row) -> None:
        # Subclasses are free to implement inplace updates
        self.delete_row(row_id)
        self.insert_row(row)

    def close(self) -> None:
        pass
