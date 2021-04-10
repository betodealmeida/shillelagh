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
from shillelagh.filters import Filter
from shillelagh.types import RequestedOrder
from shillelagh.types import Row


T = TypeVar("T", bound="Adapter")


class Adapter:

    # disable "unsafe" adapters that write to disk
    safe = False

    @staticmethod
    def supports(uri: str) -> bool:
        raise NotImplementedError("Subclasses must implement `supports`")

    @classmethod
    def from_uri(cls: Type[T], uri: str) -> T:
        args = cls.parse_uri(uri)
        return cls(*args)

    @staticmethod
    def parse_uri(uri: str) -> Tuple[Any, ...]:
        raise NotImplementedError("Subclasses must implement `parse_uri`")

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
