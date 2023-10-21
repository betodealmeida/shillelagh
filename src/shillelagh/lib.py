"""Helper functions for Shillelagh."""
import base64
import inspect
import itertools
import json
import marshal
import math
import operator
from datetime import timedelta
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
)

import apsw
import requests_cache
from packaging.version import Version

from shillelagh.adapters.base import Adapter
from shillelagh.exceptions import ImpossibleFilterError, ProgrammingError
from shillelagh.fields import Boolean, Field, Float, Integer, Order, String
from shillelagh.filters import (
    Equal,
    Filter,
    Impossible,
    IsNotNull,
    IsNull,
    Like,
    NotEqual,
    Operator,
    Range,
)
from shillelagh.typing import RequestedOrder, Row

DELETED = range(-1, 0)
CACHE_EXPIRATION = timedelta(minutes=3)


class RowIDManager:
    """
    A row ID manager that tracks insert and deletes.

    The ``RowIDManager`` should be used with an append-only table structure.
    It assigns a row ID to each row. When a new row is appended it will
    automatically receive a new ID. And when rows are deleted their ID
    gets changed to -1 to indicate the deletion.

    An example:

        >>> data = ["zero", "one", "two"]
        >>> manager = RowIDManager([range(len(data))])

    To insert data:

        >>> data.append("three")
        >>> manager.insert()
        3
        >>> data.append("four")
        >>> manager.insert(10)  # you can specify a row ID
        10
        >>> for row_id, value in zip(manager, data):
        ...     if row_id != -1:
        ...         print(row_id, value)
        0 zero
        1 one
        2 two
        3 three
        10 four

    To delete data:

        >>> manager.delete(data.index("two"))
        >>> print(data)
        ['zero', 'one', 'two', 'three', 'four']
        >>> for row_id, value in zip(manager, data):
        ...     if row_id != -1:
        ...         print(row_id, value)
        0 zero
        1 one
        3 three
        10 four

    """

    def __init__(self, ranges: List[range]):
        if not ranges:
            # pylint: disable=broad-exception-raised
            raise Exception("Argument ``ranges`` cannot be empty")

        self.ranges = ranges

    def __iter__(self):
        yield from itertools.chain(*self.ranges)

    def get_max_row_id(self) -> int:
        """
        Find the maximum row ID.
        """
        return max((r.stop - 1) for r in self.ranges)

    def check_row_id(self, row_id: int) -> None:
        """
        Check if a provided row ID is not being used.
        """
        for range_ in self.ranges:
            if range_.start <= row_id < range_.stop:
                # pylint: disable=broad-exception-raised
                raise Exception(f"Row ID {row_id} already present")

    def insert(self, row_id: Optional[int] = None) -> int:
        """
        Insert a new row ID.
        """
        if row_id is None:
            max_row_id = self.get_max_row_id()
            row_id = max_row_id + 1
        else:
            self.check_row_id(row_id)

        last = self.ranges[-1]
        if last.stop == row_id:
            self.ranges[-1] = range(last.start, row_id + 1)
        else:
            self.ranges.append(range(row_id, row_id + 1))
        return row_id

    def delete(self, row_id: int) -> None:
        """Mark a given row ID as deleted."""
        for i, range_ in enumerate(self.ranges):
            if range_.start <= row_id < range_.stop:
                if range_.start == range_.stop - 1:
                    self.ranges[i] = DELETED
                elif row_id == range_.start:
                    self.ranges[i] = range(range_.start + 1, range_.stop)
                    self.ranges.insert(i, DELETED)
                elif row_id == range_.stop - 1:
                    self.ranges[i] = range(range_.start, range_.stop - 1)
                    self.ranges.insert(i + 1, DELETED)
                else:
                    self.ranges[i] = range(range_.start, row_id)
                    self.ranges.insert(i + 1, range(row_id + 1, range_.stop))
                    self.ranges.insert(i + 1, DELETED)

                return

        # pylint: disable=broad-exception-raised
        raise Exception(f"Row ID {row_id} not found")


def analyze(  # pylint: disable=too-many-branches
    data: Iterator[Row],
) -> Tuple[int, Dict[str, Order], Dict[str, Type[Field]]]:
    """
    Compute number of rows, order, and types from a stream of rows.
    """
    order: Dict[str, Order] = {}
    types: Dict[str, Type[Field]] = {}

    previous_row: Row = {}
    row: Row = {}
    i = -1
    for i, row in enumerate(data):
        for column_name, value in row.items():
            # determine order
            if i > 0:
                previous = previous_row.get(column_name)
                order[column_name] = update_order(
                    current_order=order.get(column_name, Order.NONE),
                    previous=previous,
                    current=value,
                    num_rows=i + 1,
                )

            # determine types
            if types.get(column_name) == String:
                continue
            if isinstance(value, (str, list, dict)):
                types[column_name] = String
            elif types.get(column_name) == Float:
                continue
            elif isinstance(value, float):
                types[column_name] = Float
            elif types.get(column_name) == Integer:
                continue
            # ``isintance(True, int) == True`` :(
            elif isinstance(value, int) and not isinstance(value, bool):
                types[column_name] = Integer
            elif types.get(column_name) == Boolean:
                continue
            elif isinstance(value, bool):
                types[column_name] = Boolean
            else:
                # something weird, use string
                types[column_name] = String

        previous_row = row

    if row and not order:
        order = {column_name: Order.NONE for column_name in row.keys()}

    num_rows = i + 1

    return num_rows, order, types


def update_order(
    current_order: Order,
    previous: Any,
    current: Any,
    num_rows: int,
) -> Order:
    """
    Update the stored order of a given column.

    This is used to analyze the order of columns, by traversing the
    results and checking if their are sorted in any way.
    """
    if num_rows < 2 or previous is None:
        return Order.NONE

    try:
        if num_rows == 2:
            return Order.ASCENDING if current >= previous else Order.DESCENDING
        if (
            current_order == Order.NONE
            or (current_order == Order.ASCENDING and current < previous)
            or (current_order == Order.DESCENDING and current > previous)
        ):
            return Order.NONE
    except TypeError:
        return Order.NONE

    return current_order


def escape_string(value: str) -> str:
    """Escape single quotes."""
    return value.replace("'", "''")


def unescape_string(value: str) -> str:
    """Unescape single quotes."""
    return value.replace("''", "'")


def escape_identifier(value: str) -> str:
    """Escape double quotes."""
    return value.replace('"', '""')


def unescape_identifier(value: str) -> str:
    """Unescape double quotes."""
    return value.replace('""', '"')


def serialize(value: Any) -> str:
    """
    Serialize adapter arguments.

    This function is used with the SQLite backend, in order to serialize
    the arguments needed to instantiate an adapter via a virtual table.
    """
    try:
        serialized = marshal.dumps(value)
    except ValueError as ex:
        raise ProgrammingError(
            f"The argument {value} is not serializable because it has type "
            f"{type(value)}. Make sure only basic types (list, dicts, strings, "
            "numbers) are passed as arguments to adapters.",
        ) from ex

    return escape_string(base64.b64encode(serialized).decode())


def deserialize(value: str) -> Any:
    """
    Deserialize adapter arguments.

    This function is used by the SQLite backend, in order to deserialize
    the virtual table definition and instantiate an adapter.
    """
    return marshal.loads(base64.b64decode(unescape_string(value).encode()))


def build_sql(  # pylint: disable=too-many-locals, too-many-arguments, too-many-branches
    columns: Dict[str, Field],
    bounds: Dict[str, Filter],
    order: List[Tuple[str, RequestedOrder]],
    table: Optional[str] = None,
    column_map: Optional[Dict[str, str]] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    alias: Optional[str] = None,
) -> str:
    """
    Build a SQL query.

    This is used by adapters which use a simplified SQL dialect to fetch data. For
    GSheets a column map is required, since the SQL references columns by label
    ("A", "B", etc.) instead of name.
    """
    sql = "SELECT *"

    if table:
        sql = f"{sql} FROM {table}"
        if alias:
            sql = f"{sql} AS {alias}"

    conditions = []
    for column_name, filter_ in bounds.items():
        if (
            isinstance(filter_, Range)  # pylint: disable=too-many-boolean-expressions
            and filter_.start is not None
            and filter_.end is not None
            and filter_.start == filter_.end
            and filter_.include_start
            and filter_.include_end
        ):
            filter_ = Equal(filter_.start)

        field = columns[column_name]
        id_ = column_map[column_name] if column_map else column_name
        if alias:
            id_ = f"{alias}.{id_}"
        conditions.extend(get_conditions(id_, field, filter_))
    if conditions:
        sql = f"{sql} WHERE {' AND '.join(conditions)}"

    column_order: List[str] = []
    for column_name, requested_order in order:
        id_ = column_map[column_name] if column_map else column_name
        if alias:
            id_ = f"{alias}.{id_}"
        desc = " DESC" if requested_order == Order.DESCENDING else ""
        column_order.append(f"{id_}{desc}")
    if column_order:
        sql = f"{sql} ORDER BY {', '.join(column_order)}"
    if limit is not None:
        sql = f"{sql} LIMIT {limit}"
    if offset is not None:
        sql = f"{sql} OFFSET {offset}"

    return sql


def get_conditions(id_: str, field: Field, filter_: Filter) -> List[str]:
    """
    Build a SQL condition from a column ID and a filter.
    """
    if isinstance(filter_, Impossible):
        raise ImpossibleFilterError()

    if isinstance(filter_, Equal):
        return [f"{id_} = {field.quote(filter_.value)}"]
    if isinstance(filter_, NotEqual):
        return [f"{id_} != {field.quote(filter_.value)}"]
    if isinstance(filter_, Range):
        conditions = []
        if filter_.start is not None:
            operator_ = ">=" if filter_.include_start else ">"
            conditions.append(f"{id_} {operator_} {field.quote(filter_.start)}")
        if filter_.end is not None:
            operator_ = "<=" if filter_.include_end else "<"
            conditions.append(f"{id_} {operator_} {field.quote(filter_.end)}")
        return conditions
    if isinstance(filter_, Like):
        return [f"{id_} LIKE {field.quote(filter_.value)}"]
    if isinstance(filter_, IsNull):
        return [f"{id_} IS NULL"]
    if isinstance(filter_, IsNotNull):
        return [f"{id_} IS NOT NULL"]

    raise ProgrammingError(f"Invalid filter: {filter_}")


def combine_args_kwargs(
    func: Callable[..., Any], *args: Any, **kwargs: Any
) -> Tuple[Any, ...]:
    """
    Combine args and kwargs into args.

    This is needed because we allow users to pass custom kwargs to adapters,
    but when creating the virtual table we serialize only args.
    """
    signature = inspect.signature(func)
    bound_args = signature.bind(*args, **kwargs)
    bound_args.apply_defaults()
    return bound_args.args


def is_null(column: Any, _: Any) -> bool:
    """
    Operator for ``IS NULL``.
    """
    return column is None


def is_not_null(column: Any, _: Any) -> bool:
    """
    Operator for ``IS NOT NULL``.
    """
    return column is not None


def filter_data(  # pylint: disable=too-many-arguments
    data: Iterator[Row],
    bounds: Dict[str, Filter],
    order: List[Tuple[str, RequestedOrder]],
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    requested_columns: Optional[Set[str]] = None,
) -> Iterator[Row]:
    """
    Apply filtering and sorting to a stream of rows.

    This is used mostly as an exercise. It's probably much more efficient to
    simply declare fields without any filtering/sorting and let the backend
    (SQLite, eg) handle it.
    """
    data = (
        {
            k: v
            for k, v in row.items()
            if requested_columns is None or k in requested_columns
        }
        for row in data
    )

    for column_name, filter_ in bounds.items():

        def apply_filter(
            data: Iterator[Row],
            operator_: Callable[[Any, Any], bool],
            column_name: str,
            value: Any,
        ) -> Iterator[Row]:
            """
            Apply a given filter to an iterator of rows.

            This method is needed because Python uses lazy bindings in
            generator expressions, so we need to create a new scope in
            order to apply several filters that reuse the same variable
            names.
            """
            return (row for row in data if operator_(row[column_name], value))

        if isinstance(filter_, Impossible):
            return
        if isinstance(filter_, Equal):
            data = apply_filter(data, operator.eq, column_name, filter_.value)
        elif isinstance(filter_, NotEqual):
            data = apply_filter(data, operator.ne, column_name, filter_.value)
        elif isinstance(filter_, Range):
            if filter_.start is not None:
                operator_ = operator.ge if filter_.include_start else operator.gt
                data = apply_filter(data, operator_, column_name, filter_.start)
            if filter_.end is not None:
                operator_ = operator.le if filter_.include_end else operator.lt
                data = apply_filter(data, operator_, column_name, filter_.end)
        elif isinstance(filter_, IsNull):
            data = apply_filter(data, is_null, column_name, None)
        elif isinstance(filter_, IsNotNull):
            data = apply_filter(data, is_not_null, column_name, None)
        else:
            raise ProgrammingError(f"Invalid filter: {filter_}")

    if order:
        # in order to sort we need to consume the iterator and load it into
        # memory :(
        rows = list(data)
        for column_name, requested_order in order:
            reverse = requested_order == Order.DESCENDING
            rows.sort(key=operator.itemgetter(column_name), reverse=reverse)
        data = iter(rows)

    data = apply_limit_and_offset(data, limit, offset)

    yield from data


T = TypeVar("T")


def apply_limit_and_offset(
    rows: Iterator[T],
    limit: Optional[int] = None,
    offset: Optional[int] = None,
) -> Iterator[T]:
    """
    Apply limit/offset to a stream of rows.
    """
    if limit is not None or offset is not None:
        start = offset or 0
        end = None if limit is None else start + limit
        rows = itertools.islice(rows, start, end)
    return rows


def SimpleCostModel(rows: int, fixed_cost: int = 0):  # pylint: disable=invalid-name
    """
    A simple model for estimating query costs.

    The model assumes that each filtering operation is O(n), and each sorting
    operation is O(n log n), in addition to a fixed cost.
    """

    def method(
        obj: Any,  # pylint: disable=unused-argument
        filtered_columns: List[Tuple[str, Operator]],
        order: List[Tuple[str, RequestedOrder]],
    ) -> int:
        return int(
            fixed_cost
            + rows * len(filtered_columns)
            + rows
            * math.log2(rows)  # pylint: disable=c-extension-no-member
            * len(order),
        )

    return method


def NetworkAPICostModel(
    download_cost: int,
    fixed_cost: int = 0,
):  # pylint: disable=invalid-name
    """
    A cost model for adapters with network API calls.

    In this case, transferring less data and doing less connections is more efficient.
    """

    def method(
        obj: Any,  # pylint: disable=unused-argument
        filtered_columns: List[Tuple[str, Operator]],
        order: List[Tuple[str, RequestedOrder]],  # pylint: disable=unused-argument
    ) -> int:
        return fixed_cost + int(download_cost / (len(filtered_columns) + 1))

    return method


def find_adapter(
    uri: str,
    adapter_kwargs: Dict[str, Any],
    adapters: List[Type[Adapter]],
) -> Tuple[Type[Adapter], Tuple[Any, ...], Dict[str, Any]]:
    """
    Find an adapter that handles a given URI.

    This is done in 2 passes: first the ``supports`` method is called with ``fast=True``.
    If no adapter returns ``True`` we do a second pass on the plugins that returned
    ``None``, passing ``fast=False`` so they can do network requests to better inspect
    the URI.
    """
    candidates = set()

    for adapter in adapters:
        key = adapter.__name__.lower()
        kwargs = adapter_kwargs.get(key, {})
        supported: Optional[bool] = adapter.supports(uri, fast=True, **kwargs)
        if supported:
            args = adapter.parse_uri(uri)
            return adapter, args, kwargs
        if supported is None:
            candidates.add(adapter)

    for adapter in candidates:
        key = adapter.__name__.lower()
        kwargs = adapter_kwargs.get(key, {})
        if adapter.supports(uri, fast=False, **kwargs):
            args = adapter.parse_uri(uri)
            return adapter, args, kwargs

    raise ProgrammingError(f"Unsupported table: {uri}")


def flatten(row: Row) -> Row:
    """
    Function that converts JSON to strings, to flatten rows.
    """
    return {
        k: json.dumps(v) if isinstance(v, (list, dict)) else v for k, v in row.items()
    }


def best_index_object_available() -> bool:
    """
    Check if support for best index object is available.
    """
    return bool(Version(apsw.apswversion()) >= Version("3.41.0.0"))


def get_session(
    request_headers: Dict[str, str],
    cache_name: str,
    expire_after: timedelta = CACHE_EXPIRATION,
) -> requests_cache.CachedSession:  # E: line too long (81 > 79 characters)
    """
    Return a cached session.
    """
    session = requests_cache.CachedSession(
        cache_name=cache_name,
        backend="sqlite",
        expire_after=requests_cache.DO_NOT_CACHE
        if expire_after == timedelta(seconds=-1)
        else expire_after.total_seconds(),
    )
    session.headers.update(request_headers)

    return session
