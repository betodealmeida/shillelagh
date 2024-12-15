"""
An adapter to CPEs API. https://nvd.nist.gov/developers/vulnerabilities
"""
import logging
import urllib.parse
from collections import namedtuple
from typing import Any, Dict, Iterator, List, Optional, Tuple, cast

import dateutil.parser
import dateutil.tz
import requests_cache

from shillelagh.adapters.base import Adapter
from shillelagh.exceptions import ImpossibleFilterError
from shillelagh.fields import DateTime, String
from shillelagh.filters import Equal, Filter, Operator
from shillelagh.typing import RequestedOrder, Row

_logger = logging.getLogger(__name__)

INITIAL_COST = 0
FETCHING_COST = 1000
PAGE_SIZE = 100

CpeItem = namedtuple(
    "CpeItem",
    [
        "vendor",
        "product",
        "version",
        "update",
    ],
)


def get_cpe_from_items(
    vendor: str = "*",
    product: str = "*",
    version: str = "*",
    update: str = "*",
) -> str:
    """
    Generate a CPE from a CPE item.

    The CPE is a string with the following format:

    cpe:2.3:a:vendor:product:version:update:edition:language:sw_edition:target_sw:target_hw:other

    This function generates a CPE from a CPE items.
    """
    return f"cpe:2.3:a:{vendor}:{product}:{version}:{update}:*:*"


def get_cpe_item_from_cpe(cpe: str) -> CpeItem:
    """
    Split a CPE into its components.

    The CPE is a string with the following format:

    cpe:2.3:a:vendor:product:version:update:edition:language:sw_edition:target_sw:target_hw:other

    This function splits the CPE into its components and returns a tuple with the
    components.
    """
    parts = cpe.split(":")
    return CpeItem(parts[3], parts[4], parts[5], parts[6])


def parse_raw_cpe_item(cpe_item: Dict[str, Any]) -> Row:
    """
    Helper method to parse a RAW cpe_item from the API into a row
    """
    row: Row = {}
    row["cpe_name"] = cpe_item["cpeName"]
    row["cpe_name_id"] = cpe_item["cpeNameId"]
    row["last_modified"] = dateutil.parser.parse(cpe_item["lastModified"]).replace(
        tzinfo=dateutil.tz.UTC,
    )
    row["created"] = dateutil.parser.parse(cpe_item["created"]).replace(
        tzinfo=dateutil.tz.UTC,
    )
    row["title"] = cpe_item["titles"][0]["title"]
    row["vendor"], row["product"], _, _ = get_cpe_item_from_cpe(
        cpe_item["cpeName"],
    )

    return row


class CpesAPI(Adapter):

    """
    An adapter for NVD CPES REST Api (https://services.nvd.nist.gov/rest/json/).

    The adapter expects an URL like::

        cpes://
    """

    safe = True

    supports_limit = False
    supports_offset = False

    cpe_name = String()
    cpe_name_id = String()
    last_modified = DateTime()
    created = DateTime()
    title = String()
    vendor = String(filters=[Equal], exact=True)
    product = String(filters=[Equal], exact=True)
    version = String()
    update = String()

    @staticmethod
    def supports(uri: str, fast: bool = True, **kwargs: Any) -> Optional[bool]:
        """cpes://"""
        parsed = urllib.parse.urlparse(uri)
        return parsed.scheme == "cpes"

    @staticmethod
    def parse_uri(uri: str) -> Tuple[str, ...]:
        parsed = urllib.parse.urlparse(uri)
        query_string = urllib.parse.parse_qs(parsed.query)
        # key can be passed in the URL or via connection arguments
        if "api_key" in query_string:
            return (query_string["api_key"][0],)
        return tuple()

    def __init__(self, api_key: Optional[str] = None, window: int = PAGE_SIZE):
        super().__init__()

        self._url = "https://services.nvd.nist.gov/rest/json/cpes/2.0"
        self.api_key = api_key
        self.window = window

        # use a cache, since the adapter does a lot of similar API requests,
        # and the data should rarely (never?) change
        self._session = requests_cache.CachedSession(
            cache_name="cpes_cache",
            backend="sqlite",
            expire_after=1800,
        )

    def get_cost(
        self,
        filtered_columns: List[Tuple[str, Operator]],
        order: List[Tuple[str, RequestedOrder]],
    ) -> float:
        cost = INITIAL_COST

        # if the operator is ``Operator.EQ`` we only need to fetch 1 day of data;
        # otherwise we potentially need to fetch "window" days of data
        for _, operator in filtered_columns:
            weight = 1 if operator == Operator.EQ else self.window
            cost += FETCHING_COST * weight

        return cost

    def _build_url_params(self, bounds: Dict[str, Filter], offset=0) -> Dict[str, str]:
        # Handle pagination
        params = {"resultsPerPage": self.window, "startIndex": offset}
        # handles Filters
        vendor = (
            cast(Equal, bounds.get("vendor", Equal)).value
            if "vendor" in bounds
            else "*"
        )
        product = (
            cast(Equal, bounds.get("product", Equal)).value
            if "product" in bounds
            else "*"
        )
        params["cpeMatchString"] = get_cpe_from_items(vendor, product)
        return params

    def get_data(
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
        **kwargs: Any,
    ) -> Iterator[Row]:
        offset = 0
        total_results = None

        while True:
            try:
                params = self._build_url_params(bounds, offset)
            except ImpossibleFilterError:
                return

            query_string = urllib.parse.urlencode(params)
            _logger.info("GET %s?%s", self._url, query_string)

            response = self._session.get(self._url, params=params)
            if not response.ok:
                return
            payload = response.json()
            for cpe_item in payload["products"]:
                yield parse_raw_cpe_item(cpe_item["cpe"])

            offset += self.window
            if total_results is not None and offset >= total_results:
                break
            if total_results is None and len(payload["products"]) < self.window:
                break
