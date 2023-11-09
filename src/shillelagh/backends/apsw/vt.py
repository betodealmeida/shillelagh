# pylint: disable=c-extension-no-member, invalid-name
"""
A SQLite virtual table.

This module implements a SQLite virtual table that delegates data requests
to adapters. The main goal is to make the interface easier to use, to
simplify the work of writing new adapters.
"""
import json
import logging
from collections import defaultdict
from typing import (
    Any,
    DefaultDict,
    Dict,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    cast,
)

import apsw

from shillelagh.adapters.base import Adapter
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import (
    Blob,
    FastISODateTime,
    Field,
    Float,
    IntBoolean,
    ISODate,
    ISOTime,
    Order,
    RowID,
    String,
    StringDecimal,
    StringDuration,
    StringInteger,
)
from shillelagh.filters import Filter, Operator
from shillelagh.lib import best_index_object_available, deserialize
from shillelagh.typing import (
    Constraint,
    Index,
    OrderBy,
    RequestedOrder,
    Row,
    SQLiteConstraint,
    SQLiteValidType,
)

if best_index_object_available():  # pragma: no cover
    from apsw.ext import index_info_to_dict
else:  # pragma: no cover
    apsw.IndexInfo = Any  # for type annotation

    # pylint: disable=unused-argument
    def index_info_to_dict(index_info: apsw.IndexInfo) -> None:
        """
        Dummy function for testing.
        """


_logger = logging.getLogger(__name__)


# map between APSW operators and the ``Operator`` enum
operator_map = {
    apsw.SQLITE_INDEX_CONSTRAINT_EQ: Operator.EQ,
    apsw.SQLITE_INDEX_CONSTRAINT_GE: Operator.GE,
    apsw.SQLITE_INDEX_CONSTRAINT_GT: Operator.GT,
    apsw.SQLITE_INDEX_CONSTRAINT_LE: Operator.LE,
    apsw.SQLITE_INDEX_CONSTRAINT_LT: Operator.LT,
}


def _add_sqlite_constraint(constant_name: str, operator: Operator) -> None:
    if hasattr(apsw, constant_name):
        operator_map[getattr(apsw, constant_name)] = operator


# SQLITE_INDEX_CONSTRAINT_LIKE, >=3.10.0
_add_sqlite_constraint("SQLITE_INDEX_CONSTRAINT_LIKE", Operator.LIKE)
# SQLITE_INDEX_CONSTRAINT_NE, >=3.21.0
_add_sqlite_constraint("SQLITE_INDEX_CONSTRAINT_NE", Operator.NE)
# SQLITE_INDEX_CONSTRAINT_ISNULL, >=3.21.0
_add_sqlite_constraint("SQLITE_INDEX_CONSTRAINT_ISNULL", Operator.IS_NULL)
# SQLITE_INDEX_CONSTRAINT_ISNOTNULL, >=3.21.0
_add_sqlite_constraint("SQLITE_INDEX_CONSTRAINT_ISNOTNULL", Operator.IS_NOT_NULL)
# SQLITE_INDEX_CONSTRAINT_LIMIT, >=3.38.0
_add_sqlite_constraint("SQLITE_INDEX_CONSTRAINT_LIMIT", Operator.LIMIT)
# SQLITE_INDEX_CONSTRAINT_OFFSET, >=3.38.0
_add_sqlite_constraint("SQLITE_INDEX_CONSTRAINT_OFFSET", Operator.OFFSET)

# limit and offset are special constraints without an associated column index
LIMIT_OFFSET_INDEX = -1

# map for converting between Python native types (boolean, datetime, etc.)
# and types understood by SQLite (integers, strings, etc.)
type_map: Dict[str, Type[Field]] = {
    field.type: field  # type: ignore
    for field in [
        Blob,
        FastISODateTime,
        Float,
        ISODate,
        ISOTime,
        IntBoolean,
        String,
        StringDecimal,
        StringDuration,
        StringInteger,
    ]
}


# a row with only SQLite-valid types
SQLiteRow = Dict[str, SQLiteValidType]


def convert_rows_to_sqlite(
    columns: Dict[str, Field],
    rows: Iterator[Row],
) -> Iterator[SQLiteRow]:
    """
    Convert values from native Python types to SQLite types.

    Native Python types like ``datetime.datetime`` are not supported by SQLite; instead
    we need to cast them to strings or numbers. We use the original fields to handle
    the conversion (not the adapter fields).
    """
    converters = {
        column_name: type_map[column_field.type]().format
        for column_name, column_field in columns.items()
    }
    converters["rowid"] = RowID().format
    for row in rows:
        yield {
            column_name: converters[column_name](value)
            for column_name, value in row.items()
        }


def convert_rows_from_sqlite(
    columns: Dict[str, Field],
    rows: Iterator[SQLiteRow],
) -> Iterator[Row]:
    """
    Convert values from SQLite types to native Python types.

    Native Python types like ``datetime.datetime`` are not supported by SQLite; instead
    we need to cast them to strings or numbers. We use the original fields to handle
    the conversion (not the adapter fields).
    """
    converters = {
        column_name: type_map[column_field.type]().parse
        for column_name, column_field in columns.items()
    }
    converters["rowid"] = RowID().parse
    for row in rows:
        yield {
            column_name: converters[column_name](value)
            for column_name, value in row.items()
        }


def get_all_bounds(
    indexes: List[Index],
    constraintargs: List[Any],
    columns: Dict[str, Field],
) -> DefaultDict[str, Set[Tuple[Operator, Any]]]:
    """
    Convert indexes and constraints to operations on each column.
    """
    column_names = list(columns.keys())

    all_bounds: DefaultDict[str, Set[Tuple[Operator, Any]]] = defaultdict(set)
    for (column_index, sqlite_index_constraint), constraint in zip(
        indexes,
        constraintargs,
    ):
        if sqlite_index_constraint not in operator_map:
            # pylint: disable=broad-exception-raised
            raise Exception(f"Invalid constraint passed: {sqlite_index_constraint}")
        if column_index == LIMIT_OFFSET_INDEX:
            continue
        operator = operator_map[sqlite_index_constraint]
        column_name = column_names[column_index]
        column_type = columns[column_name]

        # convert constraint to native Python type, then to DB specific type
        constraint = type_map[column_type.type]().parse(constraint)
        value = column_type.format(constraint)

        all_bounds[column_name].add((operator, value))

    return all_bounds


def get_limit_offset(
    indexes: List[Index],
    constraintargs: List[Any],
) -> Tuple[Optional[int], Optional[int]]:
    """
    Extract limit and offset.
    """
    limit = offset = None

    for (column_index, sqlite_index_constraint), constraint in zip(
        indexes,
        constraintargs,
    ):
        if sqlite_index_constraint not in operator_map:
            # pylint: disable=broad-exception-raised
            raise Exception(f"Invalid constraint passed: {sqlite_index_constraint}")
        if column_index != LIMIT_OFFSET_INDEX:
            continue
        operator = operator_map[sqlite_index_constraint]
        if operator == Operator.LIMIT:
            limit = constraint
        elif operator == Operator.OFFSET:
            offset = constraint

    return limit, offset


def get_order(
    orderbys: List[OrderBy],
    column_names: List[str],
) -> List[Tuple[str, RequestedOrder]]:
    """
    Return a list of column names and sort order from a SQLite orderbys.
    """
    return [
        (
            column_names[column_index],
            Order.DESCENDING if descending else Order.ASCENDING,
        )
        for column_index, descending in orderbys
    ]


def get_bounds(
    columns: Dict[str, Field],
    all_bounds: DefaultDict[str, Set[Tuple[Operator, Any]]],
) -> Dict[str, Filter]:
    """
    Combine all filters that apply to each column.
    """
    bounds: Dict[str, Filter] = {}
    for column_name, operations in all_bounds.items():
        column_type = columns[column_name]
        operators = {operation[0] for operation in operations}
        for class_ in column_type.filters:
            if all(operator in class_.operators for operator in operators):
                bounds[column_name] = class_.build(operations)
                break

    return bounds


class VTModule:  # pylint: disable=too-few-public-methods

    """
    A module used to create SQLite virtual tables.

    This implementation delegates data requests to a Shillelagh adapter, simplifying
    the work needed to support new data sources.
    """

    def __init__(self, adapter: Type[Adapter]):
        self.adapter = adapter

    def Create(  # pylint: disable=unused-argument
        self,
        connection: apsw.Connection,
        modulename: str,
        dbname: str,
        tablename: str,
        *args: str,
    ) -> Tuple[str, "VTTable"]:
        """
        Called when a table is first created on a connection.
        """
        deserialized_args = [deserialize(arg[1:-1]) for arg in args]
        _logger.debug(
            "Instantiating adapter with deserialized arguments: %s",
            deserialized_args,
        )
        adapter = self.adapter(*deserialized_args)
        table = VTTable(adapter)
        create_table = table.get_create_table(tablename)
        return create_table, table

    Connect = Create


class VTTable:
    """
    A SQLite virtual table.

    The VTTable object contains knowledge of the indices, makes cursors and can
    perform transactions.

    A virtual table is structured as a series of rows, each of which has the same
    columns. The value in a column must be one of the 5 supported types, but the
    type can be different between rows for the same column. The virtual table
    routines identify the columns by number, starting at zero.

    Each row has a unique 64 bit integer rowid with the Cursor routines operating
    on this number, as well as some of the Table routines such as UpdateChangeRow.
    """

    def __init__(self, adapter: Adapter):
        self.adapter = adapter

    def get_create_table(self, tablename: str) -> str:
        """
        Return the table's ``CREATE TABLE`` statement.
        """
        columns = self.adapter.get_columns()
        if not columns:
            raise ProgrammingError(f"Virtual table {tablename} has no columns")

        quoted_columns = {k.replace('"', '""'): v for k, v in columns.items()}
        formatted_columns = ", ".join(
            f'"{k}" {v.type}' for (k, v) in quoted_columns.items()
        )
        return f'CREATE TABLE "{tablename}" ({formatted_columns})'

    def _build_index(  # pylint: disable=too-many-locals
        self,
        constraints: List[Tuple[int, SQLiteConstraint]],
        orderbys: List[OrderBy],
    ) -> Tuple[List[Constraint], int, List[Index], List[OrderBy], bool, float]:
        """
        Helper function to build index.
        """
        columns = self.adapter.get_columns()
        column_names = list(columns.keys())
        column_types = list(columns.values())

        # currently the index number is not used for anything; instead, we encode the
        # the index as JSON in ``index_name``
        index_number = 42

        indexes: List[Index] = []
        constraints_used: List[Constraint] = []
        filter_index = 0
        filtered_columns: List[Tuple[str, Operator]] = []
        for column_index, sqlite_index_constraint in constraints:
            operator = operator_map.get(sqlite_index_constraint)

            # LIMIT/OFFSET
            if (operator is Operator.LIMIT and self.adapter.supports_limit) or (
                operator is Operator.OFFSET and self.adapter.supports_offset
            ):
                constraints_used.append((filter_index, True))
                filter_index += 1
                indexes.append((LIMIT_OFFSET_INDEX, sqlite_index_constraint))
            # column operator
            elif column_index >= 0:
                column_name = column_names[column_index]
                column_type = column_types[column_index]
                for class_ in column_type.filters:
                    if operator in class_.operators:
                        filtered_columns.append((column_name, operator))
                        constraints_used.append((filter_index, column_type.exact))
                        filter_index += 1
                        indexes.append((column_index, sqlite_index_constraint))
                        break
                else:
                    constraints_used.append(None)

        # estimate query cost
        order = get_order(orderbys, column_names)
        estimated_cost = self.adapter.get_cost(filtered_columns, order)

        # is the data being returned in the requested order? if not, SQLite will have
        # to sort it
        orderby_consumed = True
        orderbys_to_process: List[OrderBy] = []
        for column_index, descending in orderbys:
            requested_order = Order.DESCENDING if descending else Order.ASCENDING
            column_type = column_types[column_index]
            if column_type.order == Order.ANY:
                orderbys_to_process.append((column_index, descending))
            elif column_type.order != requested_order:
                orderby_consumed = False
                break

        return (
            constraints_used,
            index_number,
            indexes,
            orderbys_to_process,
            orderby_consumed,
            estimated_cost,
        )

    def BestIndex(  # pylint: disable=too-many-locals
        self,
        constraints: List[Tuple[int, SQLiteConstraint]],
        orderbys: List[OrderBy],
    ) -> Tuple[List[Constraint], int, str, bool, float]:
        """
        Build an index for a given set of constraints and order bys.

        The purpose of this method is to ask if you have the ability to determine if
        a row meets certain constraints that doesn’t involve visiting every row.
        """
        (
            constraints_used,
            index_number,
            indexes,
            orderbys_to_process,
            orderby_consumed,
            estimated_cost,
        ) = self._build_index(constraints, orderbys)

        index_name = json.dumps(
            {"indexes": indexes, "orderbys_to_process": orderbys_to_process},
        )

        return (
            constraints_used,
            index_number,
            index_name,
            orderby_consumed,
            estimated_cost,
        )

    def BestIndexObject(  # pylint: disable=too-many-locals
        self,
        index_info: apsw.IndexInfo,
    ) -> bool:
        """
        Alternative to ``BestIndex`` that allows returning only selected columns.
        """
        columns = self.adapter.get_columns()
        column_names = list(columns.keys())

        index_info_dict = index_info_to_dict(index_info)
        constraints = [
            (constraint.get("iColumn", -1), constraint["op"])
            for constraint in index_info_dict["aConstraint"]
        ]
        orderbys = [
            (orderby["iColumn"], orderby["desc"])
            for orderby in index_info_dict["aOrderBy"]
        ]
        (
            constraints_used,
            index_number,
            indexes,
            orderbys_to_process,
            orderby_consumed,
            estimated_cost,
        ) = self._build_index(constraints, orderbys)

        requested_columns = sorted({column_names[i] for i in index_info.colUsed})
        index_name = json.dumps(
            {
                "indexes": indexes,
                "orderbys_to_process": orderbys_to_process,
                "requested_columns": requested_columns,
            },
        )

        for i, constraint in enumerate(constraints_used):
            if isinstance(constraint, tuple):
                index_info.set_aConstraintUsage_argvIndex(i, constraint[0] + 1)
                index_info.set_aConstraintUsage_omit(i, constraint[1])
        index_info.idxNum = index_number
        index_info.idxStr = index_name
        index_info.orderByConsumed = orderby_consumed
        index_info.estimatedCost = estimated_cost

        return True

    def Open(self) -> "VTCursor":
        """
        Returns a cursor object.
        """
        return VTCursor(self.adapter)

    def Disconnect(self) -> None:
        """
        The opposite of VTModule.Connect().

        This method is called when a reference to a virtual table is no longer used,
        but VTTable.Destroy() will be called when the table is no longer used.
        """
        self.adapter.close()

    Destroy = Disconnect

    def UpdateInsertRow(self, rowid: Optional[int], fields: Tuple[Any, ...]) -> int:
        """
        Insert a row with the specified rowid.
        """
        columns = self.adapter.get_columns()

        row = {column_name: field for field, column_name in zip(fields, columns.keys())}
        row["rowid"] = rowid
        row = next(convert_rows_from_sqlite(columns, iter([row])))

        return cast(int, self.adapter.insert_row(row))

    def UpdateDeleteRow(self, rowid: int) -> None:
        """
        Delete the row with the specified rowid.
        """
        self.adapter.delete_row(rowid)

    def UpdateChangeRow(
        self,
        rowid: int,
        newrowid: int,
        fields: Tuple[Any, ...],
    ) -> None:
        """
        Change an existing row.

        Note that the row ID can be modified.
        """
        columns = self.adapter.get_columns()

        row = {column_name: field for field, column_name in zip(fields, columns.keys())}
        row["rowid"] = newrowid
        row = next(convert_rows_from_sqlite(columns, iter([row])))

        self.adapter.update_row(rowid, row)


class VTCursor:
    """
    An object for iterating over a table.
    """

    def __init__(self, adapter: Adapter):
        self.adapter = adapter

        self.data: Iterator[Tuple[Any, ...]]
        self.current_row: Tuple[Any, ...]
        self.eof = False

    def Filter(  # pylint: disable=too-many-locals
        self,
        indexnumber: int,  # pylint: disable=unused-argument
        indexname: str,
        constraintargs: List[Any],
    ) -> None:
        """
        Filter and sort data according to constraints.

        This method converts the ``indexname`` (containing which columns to filter
        and the order to sort the results) and ``constraintargs`` into a pair of
        ``bounds`` and ``order``. These are then passed to the ``get_rows`` method of
        the adapter, to filter and sort the data.
        """
        columns: Dict[str, Field] = self.adapter.get_columns()
        column_names: List[str] = list(columns.keys())
        index = json.loads(indexname)
        indexes: List[Index] = index["indexes"]
        orderbys: List[OrderBy] = index["orderbys_to_process"]

        # compute bounds for each column
        all_bounds = get_all_bounds(indexes, constraintargs, columns)
        limit, offset = get_limit_offset(indexes, constraintargs)
        bounds = get_bounds(columns, all_bounds)

        # compute requested order
        order = get_order(orderbys, column_names)

        # limit and offset were introduced in 1.1, and not all adapters support it
        kwargs: Dict[str, Any] = {}
        if self.adapter.supports_limit:
            kwargs["limit"] = limit
        if self.adapter.supports_offset:
            kwargs["offset"] = offset
        if "requested_columns" in index:
            kwargs["requested_columns"] = set(index["requested_columns"])

        rows = self.adapter.get_rows(bounds, order, **kwargs)
        rows = convert_rows_to_sqlite(columns, rows)

        # if a given column is not present, replace it with ``None``
        self.data = (
            tuple(row.get(name) for name in ["rowid", *column_names]) for row in rows
        )
        self.Next()

    def Eof(self) -> bool:
        """
        Called to ask if we are at the end of the table.
        """
        return self.eof

    def Rowid(self) -> int:
        """
        Return the current rowid.
        """
        return cast(int, self.current_row[0])

    def Column(self, col) -> Any:
        """
        Requests the value of the specified column number of the current row.
        """
        return self.current_row[1 + col]

    def Next(self) -> None:
        """
        Move the cursor to the next row.
        """
        try:
            self.current_row = next(self.data)
            self.eof = False
        except StopIteration:
            self.eof = True

    def Close(self) -> None:
        """
        This is the destructor for the cursor.
        """
