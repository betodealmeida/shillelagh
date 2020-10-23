import inspect
import json
from collections import defaultdict
from typing import Any, DefaultDict, Dict, Iterator, List, Tuple, Type

import apsw

from shillelagh.filters import Filter
from shillelagh.types import Constraint, Order, Row, Type


class VirtualTable:
    @classmethod
    def create(
        cls,
        connection: apsw.Connection,
        modulename: str,
        dbname: str,
        tablename: str,
        *args: str,
    ) -> Tuple[str, "VirtualTable"]:
        instance = cls(*args)
        create_table: str = instance.get_create_table(tablename)
        return create_table, instance

    def get_columns(self) -> Dict[str, Type]:
        return dict(
            inspect.getmembers(self, lambda attribute: isinstance(attribute, Type))
        )

    def get_create_table(self, tablename: str) -> str:
        columns = self.get_columns()
        formatted_columns = ", ".join(f"{k} {v.type}" for (k, v) in columns.items())
        return f"CREATE TABLE {tablename} ({formatted_columns})"

    def best_index(
        self, constraints: Tuple[int, int], orderbys: Tuple[int, bool]
    ) -> Tuple[List[Constraint], int, str, bool, int]:
        column_types = list(self.get_columns().values())

        index_number = 42
        estimated_cost = 666

        # indexes is a list of pairs of column index and the operation used to filter
        # it, eg, (2, apsw.SQLITE_INDEX_CONSTRAINT_GT)
        indexes: List[Tuple[int, int]] = []

        constraints_used: List[Constraint] = []
        filter_index = 0
        for column_index, operator in constraints:
            column_type = column_types[column_index]
            for class_ in column_type.filters:
                if operator in class_.operators:
                    constraints_used.append((filter_index, column_type.exact))
                    filter_index += 1
                    indexes.append((column_index, operator))
                    break
            else:
                # no indexes supported in this column
                constraints_used.append(None)

        # serialize the indexes to str so it can be used later when filtering the data
        index_name = json.dumps(indexes)

        # is the data being returned in the requested order? if not, SQLite will have
        # to sort it
        orderby_consumed = True
        for column_index, descending in orderbys:
            requested_order = Order.DESCENDING if descending else Order.ASCENDING
            column_type = column_types[column_index]
            if column_type.order != requested_order:
                orderby_consumed = False
                break

        return (
            constraints_used,
            index_number,
            index_name,
            orderby_consumed,
            estimated_cost,
        )

    def open(self) -> "Cursor":
        return Cursor(self)

    def disconnect(self) -> None:
        pass

    # apsw expects these method names
    Create = Connect = create
    BestIndex = best_index
    Open = open
    Disconnect = Destroy = disconnect

    def get_data(bounds: Dict[str, Filter]) -> Iterator[Row]:
        raise NotImplementedError("Subclasses must implement `get_data`")


class Cursor:
    def __init__(self, table: VirtualTable):
        self.table = table

    def filter(
        self, indexnumber: int, indexname: str, constraintargs: List[Any]
    ) -> None:
        columns: Dict[str, Type] = self.table.get_columns()
        column_names: List[str] = list(columns.keys())
        indexes: List[Tuple[int, int]] = json.loads(indexname)

        all_bounds: DefaultDict[str, Set[Tuple[int, Any]]] = defaultdict(set)
        for (column_index, operator), constraint in zip(indexes, constraintargs):
            column_name = column_names[column_index]
            column_type = columns[column_name]
            value = column_type.parse(constraint)
            all_bounds[column_name].add((operator, value))

        # find the filter that works with all the operations and build it
        bounds: Dict[str, Filter] = {}
        for column_name, operations in all_bounds.items():
            column_type = columns[column_name]
            operators = {operation[0] for operation in operations}
            for class_ in column_type.filters:
                if all(operator in class_.operators for operator in operators):
                    break
            else:
                raise Exception("No valid filter found")
            bounds[column_name] = class_.build(operations)

        self.data = (
            tuple(row[name] for name in ["rowid"] + column_names)
            for row in self.table.get_data(bounds)
        )
        self.next()

    def eof(self) -> bool:
        return self.at_eof

    def rowid(self) -> int:
        return self.current_row[0]

    def column(self, col) -> Any:
        return self.current_row[1 + col]

    def next(self) -> None:
        try:
            self.current_row = next(self.data)
            self.at_eof = False
        except StopIteration:
            self.at_eof = True

    def close(self) -> None:
        pass

    # apsw expects these method names
    Filter = filter
    Eof = eof
    Rowid = rowid
    Column = column
    Next = next
    Close = close
