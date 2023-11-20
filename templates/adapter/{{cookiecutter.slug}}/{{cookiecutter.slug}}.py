# pylint: disable=abstract-method
"""
{{ cookiecutter.description }}
"""
import math
from typing import Any, Dict, Iterator, List, Optional, Tuple

import requests_cache

from shillelagh.adapters.base import Adapter
from shillelagh.fields import Field
from shillelagh.filters import Filter, Operator

# from shillelagh.lib import SimpleCostModel
from shillelagh.typing import RequestedOrder, Row

AVERAGE_NUMBER_OF_ROWS = 1000
FIXED_COST = 0


class {{ cookiecutter.adapter_name|replace(' ', '') }}{{ cookiecutter.adapter_type }}(Adapter):

    """
    {{ cookiecutter.description }}
    """

    # Set this to ``True`` if the adapter doesn't access the filesystem.
    safe = False

    # If this is true, the adapter will receive a ``limit`` argument in the
    # ``get_data`` method, and will be responsible for limiting the number of
    # rows returned.
    supports_limit = False

    # Similarly, if this is true the adapter will receive an ``offset`` argument
    # in the ``get_data`` method, and will be responsible for offsetting the
    # rows that are returned.
    supports_offset = False

    # If this is true, the adapter will receive a ``requested_columns`` argument
    # in the ``get_data`` method, and will be responsible for returning only the
    # requested columns. Otherwise the adapter will have no indication of which
    # columns were requested, and should return all columns always.
    supports_requested_columns = False

    # This method is used to determine which URIs your adapter will handle. For
    # example, if your adapter interfaces with an API at api.example.com you
    # can do:
    #
    #     parsed = urllib.parse.urlparse(uri)
    #     return parsed.netloc == "api.example.com"
    #
    # The method will be called 1 or 2 times. On the first call ``fast`` will be
    # true, and the adapter should return as soon as possible. This means that if
    # you need to do a network request to determine if the URI should be handled
    # by your adapter, when ``fast=True`` you should return ``None``.
    #
    # On the second call, ``fast`` will be false, and the adapter can make network
    # requests to introspect URIs. The adapter should then return either true or
    # false on the second request.
    @staticmethod
    def supports(uri: str, fast: bool = True, **kwargs: Any) -> Optional[bool]:
        return False

    # This method parses the URI into arguments that are passed to initialize the
    # adapter class. The simplest implementation returns the URI unmodified, but
    # adapters might process the URL and return only relevant parts of the URI,
    # like an ID or path.
    @staticmethod
    def parse_uri(uri: str) -> Tuple[str]:
        return (uri,)

    def __init__(self, uri: str):
        super().__init__()
        self.uri = uri

        # If the adapter needs to do API requests it's useful to use a cache for
        # the requests. If you're not doing network requests you get delete this
        # session object.
        self._session = requests_cache.CachedSession(
            cache_name="{{ cookiecutter.slug }}_cache",
            backend="sqlite",
            expore_after=180,
        )

        # For adapters with dynamic columns (ie, number, names, and types of
        # columns depend on the URI) it's good practice to set the columns when
        # the class is initialized.
        self._set_columns()

    # When defining columns it's important to know which columns can be used to
    # filter the URI. For example, if the API accepts a time range the adapter
    # probably needs a temporal column that is filterable:
    #
    #     from shillelagh.fields import DateTime
    #     from shillelagh.filters import Range
    #
    #     self.columns["time"] = DateTime(filters=[Range])
    #
    # It's then up to the ``get_data`` method to translate temporal filters into
    # the corresponding API calls.
    #
    # The column definition should also specify if the column has a natural order,
    # or if the adapter can handle any requested order (and the adapter will be
    # responsible for ordering the data):
    #
    #     from shillelagh.fields import Integer
    #     from shillelagh.fields import Order
    #
    #     self.columns["prime_numbers"] = Integer(
    #         filters=[Range],
    #         order=Order.ASCENDING,
    #     )
    #
    # Finally, columns can return data that is not filtered perfectly — eg, hourly
    # data filtered only down to the daily granularity. In that case the column
    # should be declared as inexact:
    #
    #     self.columns["time"] = DateTime(filters=[Range], exact=False)
    #
    def _set_columns(self) -> None:
        self.columns: Dict[str, Field] = {}

    # If you have static columns you can get rid of the ``get_columns`` method,
    # get rid of ``_set_columns``, and simply define the columns as class
    # attributes:
    #
    # time = DateTime(filters=[Range])
    #
    # Then ``get_columns`` from the parent class will find these attributes and
    # use them as the dictionary of columns.
    def get_columns(self) -> Dict[str, Field]:
        return self.columns

    # This method is used to return any extra metadata associated with the URI.
    # You can delete it instead of returning an empty dictionary, since that's
    # the exact implementation of the parent method.
    def get_metadata(self) -> Dict[str, Any]:
        return {}

    # A method for estimating the cost of a query, used by the query planner. The
    # model receives the name of the columns and the operators filtering them, as
    # well as the columns that should be sorted and how.
    def get_cost(
        self,
        filtered_columns: List[Tuple[str, Operator]],
        order: List[Tuple[str, RequestedOrder]],
    ) -> float:
        # A simple model for estimating query costs.
        #
        # The model assumes that each filtering operation is O(n), and each
        # sorting operation is O(n log n), in addition to a fixed cost.
        return int(
            FIXED_COST
            + AVERAGE_NUMBER_OF_ROWS * len(filtered_columns)
            + AVERAGE_NUMBER_OF_ROWS * math.log2(AVERAGE_NUMBER_OF_ROWS) * len(order)
        )

    # If you're using the cost model above unmodified you can simply point the
    # method to the ``SimpleCostModel`` class. Don't forget to uncomment the import
    # at the top of the file.
    # get_cost = SimpleCostModel(AVERAGE_NUMBER_OF_ROWS, FIXED_COST)

    # This method yields rows of data, each row a dictionary. If any columns are
    # declared as filterable there might be a corresponding ``Filter`` object in
    # the ``bounds`` argument that must be used to filter the column (unless the
    # column was declared as inexact).
    #
    # Note that, in addition to the actual columns, each row should also have a
    # column called ``rowid``, with an integer value. If the adapter is read-only
    # this can be any number; for adapters that implement DML the rowid is used
    # in ``INSERT``, ``UPDATE``, and ``DELETE`` operations.
    def get_data(
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
        # if ``supports_limit`` is true uncomment below:
        # limit: Optional[int] = None,
        # if ``supports_offset` is true uncomment below:
        # offset: Optional[int] = None,
        # if ``supports_requested_columns`` is true uncomment below:
        # requested_columns: Optional[Set[str]] = None,
        **kwargs: Any,
    ) -> Iterator[Row]:
        pass

    # For adapters that support ``INSERT`` statements, this method will be called
    # for every row that is inserted. Note that ``row`` will have a special column
    # called ``rowid``.
    #
    # The CSV adapter is a good example of how to handle row IDs. Each row in the
    # file is assigned a sequential row ID, starting from zero, and the CSV adapter
    # uses a ``RowIDManager`` to keep track of inserts and deletes.
    def insert_data(self, row: Row) -> int:
        pass

    # Delete a row given its ID.
    def delete_data(self, row_id: int) -> None:
        pass

    # Method for updating data. If this method is not implemented the base method
    # will perform a delete followed by an insert.
    def update_data(self, row_id: int, row: Row) -> None:
        pass

    # Close the file. This can be used for garbage collection. For example, the
    # CSV adapter uses this method to effectively delete rows marked as deleted;
    # the GSheets adapter uses this to synchronize the local copy of the sheet back
    # to the actual sheet.
    def close(self) -> None:
        pass

    # This is called when the user runs ``DROP TABLE`` on a table.
    def drop_table(self) -> None:
        pass
