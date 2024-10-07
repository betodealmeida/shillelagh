"""
An FDW.
"""

from collections import defaultdict
from collections.abc import Iterator
from typing import Any, DefaultDict, Optional, TypedDict

from multicorn import ForeignDataWrapper, Qual, SortKey

from shillelagh.adapters.registry import registry
from shillelagh.fields import Order
from shillelagh.filters import Operator
from shillelagh.lib import deserialize, get_bounds
from shillelagh.typing import RequestedOrder, Row

operator_map = {
    "=": Operator.EQ,
    ">": Operator.GT,
    "<": Operator.LT,
    ">=": Operator.GE,
    "<=": Operator.LE,
}


def get_all_bounds(quals: list[Qual]) -> DefaultDict[str, set[tuple[Operator, Any]]]:
    """
    Convert list of ``Qual`` into a set of operators for each column.
    """
    all_bounds: DefaultDict[str, set[tuple[Operator, Any]]] = defaultdict(set)
    for qual in quals:
        if operator := operator_map.get(qual.operator):
            all_bounds[qual.field_name].add((operator, qual.value))

    return all_bounds


class OptionsType(TypedDict):
    """
    Type for OPTIONS.
    """

    adapter: str
    args: str


class MulticornForeignDataWrapper(ForeignDataWrapper):
    """
    A FDW that dispatches queries to adapters.
    """

    def __init__(self, options: OptionsType, columns: dict[str, str]):
        super().__init__(options, columns)

        deserialized_args = deserialize(options["args"])
        self.adapter = registry.load(options["adapter"])(*deserialized_args)
        self.columns = self.adapter.get_columns()

    def execute(
        self,
        quals: list[Qual],
        columns: list[str],
        sortkeys: Optional[list[SortKey]] = None,
    ) -> Iterator[Row]:
        """
        Execute a query.
        """
        all_bounds = get_all_bounds(quals)
        bounds = get_bounds(self.columns, all_bounds)

        order: list[tuple[str, RequestedOrder]] = [
            (key.attname, Order.DESCENDING if key.is_reversed else Order.ASCENDING)
            for key in sortkeys or []
        ]

        kwargs = (
            {"requested_columns": columns}
            if self.adapter.supports_requested_columns
            else {}
        )

        return self.adapter.get_rows(bounds, order, **kwargs)

    def can_sort(self, sortkeys: list[SortKey]) -> list[SortKey]:
        """
        Return a list of sorts the adapter can perform.
        """

        def is_sortable(key: SortKey) -> bool:
            """
            Return if a given sort key can be enforced by the adapter.
            """
            if key.attname not in self.columns:
                return False

            order = self.columns[key.attname].order
            return (
                order == Order.ANY
                or (order == Order.ASCENDING and not key.is_reversed)
                or (order == Order.DESCENDING and key.is_reversed)
            )

        return [key for key in sortkeys if is_sortable(key)]

    def insert(self, values: Row) -> Row:
        rowid = self.adapter.insert_row(values)
        values["rowid"] = rowid
        return values

    def delete(self, oldvalues: Row) -> None:
        rowid = oldvalues["rowid"]
        self.adapter.delete_row(rowid)

    def update(self, oldvalues: Row, newvalues: Row) -> Row:
        rowid = newvalues["rowid"]
        self.adapter.update_row(rowid, newvalues)
        return newvalues

    @property
    def rowid_column(self):
        return "rowid"

    def get_rel_size(self, quals: list[Qual], columns: list[str]) -> tuple[int, int]:
        """
        Estimate query cost.
        """
        all_bounds = get_all_bounds(quals)
        filtered_columns = [
            (column, operator[0])
            for column, operators in all_bounds.items()
            for operator in operators
        ]

        # the adapter returns an arbitrary cost that takes in consideration filtering and
        # sorting; let's use that as an approximation for rows
        rows = int(self.adapter.get_cost(filtered_columns, []))

        # same assumption as the parent class
        row_width = len(columns) * 100

        return (rows, row_width)

    @classmethod
    def import_schema(  # pylint: disable=too-many-arguments, too-many-positional-arguments
        cls,
        schema: str,
        srv_options: dict[str, str],
        options: dict[str, str],
        restriction_type: Optional[str],
        restricts: list[str],
    ):
        return []
