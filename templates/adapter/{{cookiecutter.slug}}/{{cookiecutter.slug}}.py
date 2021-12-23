# pylint: disable=abstract-method
"""
{{ cookiecutter.description }}
"""
from typing import Any, Dict, Iterator, List, Optional, Tuple

import requests_cache

from shillelagh.adapters.base import Adapter
from shillelagh.fields import Field
from shillelagh.filters import Filter
from shillelagh.typing import RequestedOrder, Row


class {{ cookiecutter.adapter_name|replace(' ', '') }}{{ cookiecutter.adapter_type }}(Adapter):

    """
    {{ cookiecutter.description }}
    """

    # Set this to ``True`` if the adapter doesn't access the filesystem.
    safe = False

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
    # adapter class. The simplest implementation returns the URI unmodified.
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
    # It's then up to the ``get_rows`` method to translate temporal filters into
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

    # This method yields rows of data, each row a dictionary. If any columns are
    # declared as filterable there might be a corresponding ``Filter`` object in
    # the ``bounds`` argument that must be used to filter the column (unless the
    # column was declared as inexact).
    def get_rows(
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
    ) -> Iterator[Row]:
        pass
