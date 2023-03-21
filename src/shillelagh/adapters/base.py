"""Base class for adapters."""
import atexit
import inspect
from typing import Any, Dict, Iterator, List, Optional, Tuple

from shillelagh.exceptions import NotSupportedError
from shillelagh.fields import Field, RowID
from shillelagh.filters import Filter, Operator
from shillelagh.typing import RequestedOrder, Row

FIXED_COST = 666


class Adapter:

    """
    An adapter to a table.

    Adapters provide an interface to resources, so they can be queried via
    SQL. An adapter instance represents a virtual table, and the adapter is
    responsible for fetching data and metadata from the resource, and
    possibly insert, delete, or update rows.

    In order to find an adapter responsible for a given table name, adapters
    need to be registered under the "shillelagh.adapter" entry point, eg::

        # setup.cfg
        [options.entry_points]
        shillelagh.adapter =
            custom_adapter = shillelagh.adapters.api.custom:CustomAdapter

    Adapters also need to implement the ``supports`` method. Given a table
    name, the method should return true if the table is supported by the
    adapter.
    """

    # An adapter is considered "safe" when it has no explicit access to the
    # local filesystem. Users can then use the ``shillelagh+safe://`` URI
    # in SQLAlchemy to load only safe adapters, as well as only adapters
    # explicitly listed:
    #
    #     >>> engine = create_engine("shillelagh+safe://", adapters=["gsheetsapi"])
    #
    safe = False

    # if true, a corresponding argument will be passed in the kwargs of
    # ``get_rows`` and ``get_data``
    supports_limit = False
    supports_offset = False

    # if true, the requested columns will be passed to ``get_rows`` and ``get_data``
    supports_requested_columns = False

    def __init__(self, *args: Any, **kwargs: Any):  # pylint: disable=unused-argument
        # ensure ``self.close`` gets called before GC
        atexit.register(self.close)

    @staticmethod
    def supports(uri: str, fast: bool = True, **kwargs: Any) -> Optional[bool]:
        """
        Return if a given table is supported by the adapter.

        The discovery is done in 2 passes. First all adapters have their methods
        called with ``fast=True``. On the first pass adapters should implement
        a cheap method, without any network calls.

        If no adapter returns ``True`` a second pass is made with ``fast=False``
        using only adapters that returned ``None`` on the first pass. In this
        second pass adapters can perform network requests to get more
        information about the URI.

        The method receives the table URI, as well as the adapter connection
        arguments, eg::

            >>> from shillelagh.backends.apsw.db import connect
            >>> connection = connect(
            ...     ':memory:',
            ...     adapter_kwargs={"gsheetsapi": {"catalog":
            ...         {"table": "https://docs.google.com/spreadsheets/d/1/"}}},
            ... )

        This would call all adapters in order to find which one should handle
        the table ``table``. The Gsheets adapter would be called with::

            >>> from shillelagh.adapters.api.gsheets.adapter import GSheetsAPI
            >>> GSheetsAPI.supports("table", fast=True,  # first pass
            ...     catalog={"table": "https://docs.google.com/spreadsheets/d/1"})
            True

        """
        raise NotImplementedError("Subclasses must implement ``supports``")

    @staticmethod
    def parse_uri(uri: str) -> Tuple[Any, ...]:
        """Parse table name, and return arguments to instantiate adapter."""
        raise NotImplementedError("Subclasses must implement ``parse_uri``")

    def get_metadata(self) -> Dict[str, Any]:
        """Return any extra metadata about the table."""
        return {}

    def get_columns(self) -> Dict[str, Field]:
        """
        Return the columns available in the table.

        This method is called for every query, so make sure it's cheap. For most
        (all?) tables this won't change, so you can store it in an instance
        attribute.
        """
        return dict(
            inspect.getmembers(self, lambda attribute: isinstance(attribute, Field)),
        )

    def get_cost(  # pylint: disable=unused-argument
        self,
        filtered_columns: List[Tuple[str, Operator]],
        order: List[Tuple[str, RequestedOrder]],
    ) -> float:
        """
        Estimate the query cost.

        The base adapter returns a fixed cost, and custom adapter can implement
        their own cost estimation.
        """
        return FIXED_COST

    def get_data(
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
        **kwargs: Any,
    ) -> Iterator[Row]:
        """
        Yield rows as adapter-specific types.

        This method expects rows to be in the storage format. Eg, for the CSV adapter
        datetime columns would be stored (and yielded) as strings. The ``get_rows``
        method will use the adapter fields to convert these values into native Python
        types (in this case, a proper ``datetime.datetime``).

        Missing values (NULLs) may be omitted from the dictionary; they will be
        replaced by ``None`` by the backend.
        """
        raise NotImplementedError("Subclasses must implement ``get_data``")

    def get_rows(
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
        **kwargs: Any,
    ) -> Iterator[Row]:
        """
        Yield rows as native Python types.
        """
        columns = self.get_columns()
        parsers = {column_name: field.parse for column_name, field in columns.items()}
        parsers["rowid"] = RowID().parse

        for row in self.get_data(bounds, order, **kwargs):
            yield {
                column_name: parsers[column_name](value)
                for column_name, value in row.items()
                if column_name in parsers
            }

    def insert_data(self, row: Row) -> int:
        """
        Insert a single row with adapter-specific types.

        The rows will be formatted according to the adapter fields. Eg, if an adapter
        represents timestamps as ISO strings, and timestamp values will be ISO strings.
        """
        raise NotSupportedError("Adapter does not support ``INSERT`` statements")

    def insert_row(self, row: Row) -> int:
        """
        Insert a single row with native Python types.

        The row types will be converted to the native adapter types, and passed to
        ``insert_data``.
        """
        columns = self.get_columns().copy()
        columns["rowid"] = RowID()
        row = {
            column_name: columns[column_name].format(value)
            for column_name, value in row.items()
        }
        return self.insert_data(row)

    def delete_data(self, row_id: int) -> None:
        """Delete a row from the table."""
        raise NotSupportedError("Adapter does not support ``DELETE`` statements")

    def delete_row(self, row_id: int) -> None:
        """
        Delete a row from the table.

        This method is identical to ``delete_data``, only here for symmetry.
        """
        return self.delete_data(row_id)

    def update_data(self, row_id: int, row: Row) -> None:
        """
        Update a single row with adapter-specific types.

        This method by default will call a delete followed by an insert.
        Adapters can implement their own more efficient methods.
        """
        try:
            self.delete_data(row_id)
            self.insert_data(row)
        except NotSupportedError as ex:
            raise NotSupportedError(
                "Adapter does not support ``UPDATE`` statements",
            ) from ex

    def update_row(self, row_id: int, row: Row) -> None:
        """
        Update a single row with native Python types.
        """
        columns = self.get_columns().copy()
        columns["rowid"] = RowID()
        row = {
            column_name: columns[column_name].format(value)
            for column_name, value in row.items()
        }
        self.update_data(row_id, row)

    def close(self) -> None:
        """
        Close the adapter.

        Adapters should use this method to perform any pending changes when the
        connection is closed.
        """

    def drop_table(self) -> None:
        """
        Drop a table.
        """
