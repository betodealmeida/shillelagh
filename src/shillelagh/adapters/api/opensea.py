# pylint: disable=abstract-method
"""
Write SQL to explore opensea.io data
"""
import logging
from re import T
import urllib.parse

from typing import Any
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple

import requests_cache

from shillelagh.adapters.base import Adapter
from shillelagh.fields import Field
from shillelagh.filters import Filter
from shillelagh.typing import RequestedOrder
from shillelagh.typing import Row
from shillelagh.fields import String

from requests import Request

_logger = logging.getLogger(__name__)
class OpenseaAPI(Adapter):

    """
    Write SQL to explore opensea.io data
    """

    permalink = String()

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
        parsed = urllib.parse.urlparse(uri)
        query_string = urllib.parse.parse_qs(parsed.query)
        return (
            parsed.netloc == "api.opensea.io"
        )

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
            cache_name="opensea_cache",
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
        self.columns: Dict[str, Field] = {
            'event_type': String(),
            'approved_account': String(),
            'asset_bundle': String(),
            'auction_type': String(),
            'bid_amount': String(),
            'collection_slug': String(),
            'contract_address': String(),
            'created_date': String(),
            'custom_event_name': String(),
            'dev_fee_payment_event': String(),
            'duration': String(),
            'ending_price': String(),
            'quantity': String(),
            'seller': String(),
            'starting_price': String(),
            'to_account': String(),
            'total_price': String(),
            'transaction': String(),
            'winner_account': String(),

            # assets dict
            'asset_id': String(),
            'asset_token_id': String(),
            'asset_num_sales': String(),
            'asset_background_color': String(),
            'asset_image_url': String(),
            'asset_image_preview_url': String(),
            'asset_image_thumbnail_url': String(),
            'asset_image_original_url': String(),
            'asset_animation_url': String(),
            'asset_animation_original_url': String(),
            'asset_name': String(),
            'asset_description': String(),
            'asset_external_link': String(),
            'asset_permalink': String(),
            'asset_decimals': String(),
            'asset_token_metadata': String(),
            
                # asset nested fields
                # 'asset_asset_contract': String(),
                # 'asset_owner: String(),
                # 'asset_collection': String(),
                # 'asset_contract': String(),

            # nested 
            # 'from_account': String(),
            # 'id': String(),
            # 'is_private': String(),
            # 'owner_account': String(),
            # 'payment_token': String(),
        }

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

        url = f"https://api.opensea.io/api/v1/events?only_opensea=false&offset=0&limit=20"
        headers = {}
        prepared = Request(
            "GET",
            url,
            params={},
            headers=headers,
        ).prepare()
        
        _logger.info("GET %s", prepared.url)
        response = self._session.send(prepared)
        payload = response.json()

        for record in payload['asset_events']:
            asset_data = record.get('asset')
            asset_row = {f"asset_{column}": asset_data[column] for column in asset_data.keys()}
            record.update(asset_row)
            row = {column: record[column] for column in self.get_columns()}
            yield row
