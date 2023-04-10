"""
An adapter for in-memory Pandas dataframes.
"""

# pylint: disable=invalid-name

import inspect
import operator
from typing import Any, Dict, Iterator, List, Optional, Tuple, Type

import numpy as np
import pandas as pd

from shillelagh.adapters.base import Adapter
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Boolean, DateTime, Field, Float, Integer, Order, String
from shillelagh.filters import (
    Equal,
    Filter,
    Impossible,
    IsNotNull,
    IsNull,
    NotEqual,
    Range,
)
from shillelagh.lib import SimpleCostModel
from shillelagh.typing import RequestedOrder, Row

# this is just a wild guess; used to estimate query cost
AVERAGE_NUMBER_OF_ROWS = 1000

type_map: Dict[str, Tuple[Type[Field], List[Type[Filter]]]] = {
    "i": (Integer, [Range, Equal, NotEqual, IsNull, IsNotNull]),
    "b": (Boolean, [Equal, NotEqual, IsNull, IsNotNull]),
    "u": (Integer, [Range, Equal, NotEqual, IsNull, IsNotNull]),
    "f": (Float, [Range, Equal, NotEqual, IsNull, IsNotNull]),
    "M": (DateTime, [Range, Equal, NotEqual, IsNull, IsNotNull]),
    "S": (String, [Range, Equal, NotEqual, IsNull, IsNotNull]),
    "O": (String, [Range, Equal, NotEqual, IsNull, IsNotNull]),
}


def get_field(dtype: np.dtype) -> Field:
    """
    Return a Shillelagh `Field` from a Numpy dtype.
    """
    class_, filters = type_map[dtype.kind]
    return class_(
        filters=filters,
        order=Order.ANY,
        exact=True,
    )


def find_dataframe(uri: str) -> Optional[pd.DataFrame]:
    """
    Go up the stack, find the Pandas dataframe.
    """
    for level in inspect.stack():
        # search locals
        context_locals = dict(inspect.getmembers(level[0]))["f_locals"]
        if uri in context_locals and isinstance(context_locals[uri], pd.DataFrame):
            return context_locals[uri]

        # search globals
        context_globals = dict(inspect.getmembers(level[0]))["f_globals"]
        if uri in context_globals and isinstance(context_globals[uri], pd.DataFrame):
            return context_globals[uri]

    return None


def get_df_data(  # pylint: disable=too-many-arguments, too-many-branches
    df: pd.DataFrame,
    columns: Dict[str, Field],
    bounds: Dict[str, Filter],
    order: List[Tuple[str, RequestedOrder]],
    limit: Optional[int] = None,
    offset: Optional[int] = None,
) -> Iterator[Row]:
    """
    Apply the ``get_data`` method on a Pandas dataframe.
    """
    if df.empty:
        return

    # ensure column names are strings
    df = df.rename(columns={k: str(k) for k in df.columns})

    column_names = list(columns.keys())
    df = df[column_names]

    for column_name, filter_ in bounds.items():
        if isinstance(filter_, Impossible):
            return
        if isinstance(filter_, Equal):
            df = df[df[column_name] == filter_.value]
        elif isinstance(filter_, NotEqual):
            df = df[df[column_name] != filter_.value]
        elif isinstance(filter_, Range):
            if filter_.start is not None:
                operator_ = operator.ge if filter_.include_start else operator.gt
                df = df[operator_(df[column_name], filter_.start)]
            if filter_.end is not None:
                operator_ = operator.le if filter_.include_end else operator.lt
                df = df[operator_(df[column_name], filter_.end)]
        elif isinstance(filter_, IsNull):
            df = df[~df[column_name].notnull()]
        elif isinstance(filter_, IsNotNull):
            df = df[df[column_name].notnull()]
        else:
            raise ProgrammingError(f"Invalid filter: {filter_}")

    if order:
        by, requested_orders = list(zip(*order))
        ascending = [
            requested_order == Order.ASCENDING for requested_order in requested_orders
        ]
        df = df.sort_values(by=list(by), ascending=ascending)

    df = df[offset:]
    df = df[:limit]

    for row in df.itertuples(name=None):
        yield dict(zip(["rowid", *column_names], row))


def get_columns_from_df(df: pd.DataFrame) -> Dict[str, Field]:
    """
    Construct adapter columns from a Pandas dataframe.
    """
    return {
        # ensure column name is string
        str(column_name): get_field(dtype)
        for column_name, dtype in zip(df.columns, df.dtypes)
        if dtype.kind in type_map
    }


class PandasMemory(Adapter):

    """
    An adapter for in-memory Pandas dataframes.
    """

    safe = False

    supports_limit = True
    supports_offset = True

    @staticmethod
    def supports(uri: str, fast: bool = True, **kwargs: Any) -> Optional[bool]:
        return find_dataframe(uri) is not None

    @staticmethod
    def parse_uri(uri: str) -> Tuple[str]:
        return (uri,)

    def __init__(self, uri: str):
        super().__init__()
        df = find_dataframe(uri)
        if df is None:
            raise ProgrammingError("Could not find dataframe")

        self.df = df
        self.columns = get_columns_from_df(df)

    def get_columns(self) -> Dict[str, Field]:
        return self.columns

    get_cost = SimpleCostModel(AVERAGE_NUMBER_OF_ROWS)

    def get_data(
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        **kwargs: Any,
    ) -> Iterator[Row]:
        yield from get_df_data(self.df, self.columns, bounds, order, limit, offset)

    def insert_data(self, row: Row) -> int:
        row_id: Optional[int] = row.pop("rowid")
        if row_id is None:
            row_id = max(self.df.index) + 1

        self.df.loc[row_id] = row

        return row_id

    def delete_data(self, row_id: int) -> None:
        self.df.drop([row_id], inplace=True)

    def update_data(self, row_id: int, row: Row) -> None:
        # the row_id might change on an update
        new_row_id = row.pop("rowid")
        if new_row_id != row_id:
            self.df.drop([row_id], inplace=True)

        self.df.loc[new_row_id] = row.values()
