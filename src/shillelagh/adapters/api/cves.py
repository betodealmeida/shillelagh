"""
An adapter to CVEs API. https://nvd.nist.gov/developers/vulnerabilities
"""
import logging
import urllib.parse
from typing import Any, Dict, Iterator, List, Optional, Tuple, cast

import dateutil.parser
import dateutil.tz
import requests_cache

from shillelagh.adapters.api.cpes import get_cpe_from_items, get_cpe_item_from_cpe
from shillelagh.adapters.base import Adapter
from shillelagh.exceptions import ImpossibleFilterError
from shillelagh.fields import DateTime, Float, String
from shillelagh.filters import Equal, Filter, Operator
from shillelagh.typing import RequestedOrder, Row

_logger = logging.getLogger(__name__)

INITIAL_COST = 0
FETCHING_COST = 1000
PAGE_SIZE = 1000


def parse_raw_cve_item(cve_item: Dict[str, Any]) -> Row:
    """
    Helper method to parse a RAW cve_item from the API into a row
    """
    row: Row = {}

    # Converges both CVSS v2 and v3 into a single score and vector
    row["cvss_score"] = float(
        cve_item["metrics"]["cvssMetricV31"][0]["cvssData"]["baseScore"]
        if "cvssMetricV31" in cve_item["metrics"]
        else cve_item["metrics"]["cvssMetricV2"][0]["cvssData"]["baseScore"]
        if "cvssMetricV2" in cve_item["metrics"]
        else 0.0,
    )
    row["cvss_vector"] = (
        cve_item["metrics"]["cvssMetricV31"][0]["cvssData"]["vectorString"]
        if "cvssMetricV31" in cve_item["metrics"]
        else cve_item["metrics"]["cvssMetricV2"][0]["cvssData"]["vectorString"]
        if "cvssMetricV2" in cve_item["metrics"]
        else ""
    )
    row["cve_id"] = cve_item["id"]
    row["status"] = cve_item["vulnStatus"]
    row["description"] = cve_item["descriptions"][0]["value"]
    row["published_date"] = dateutil.parser.parse(cve_item["published"]).replace(
        tzinfo=dateutil.tz.UTC,
    )
    row["last_modified_date"] = dateutil.parser.parse(
        cve_item["lastModified"],
    ).replace(
        tzinfo=dateutil.tz.UTC,
    )
    row["cwe_id"] = cve_item["weaknesses"][0]["description"][0]["value"]
    if "configurations" in cve_item:
        cpe = cve_item["configurations"][0]["nodes"][0]["cpeMatch"][0]["criteria"]
        row["vendor"], row["product"], _, _ = get_cpe_item_from_cpe(
            cpe,
        )
    return row


class CvesAPI(Adapter):

    """
    An adapter for NVD CVES REST Api (https://services.nvd.nist.gov/rest/json/).

    The adapter expects an URL like::

        cves://
    """

    safe = True

    supports_limit = False
    supports_offset = False

    cve_id = String(filters=[Equal], exact=True)
    cwe_id = String(filters=[Equal], exact=True)
    cwe_name = String()
    cvss_score = Float()
    status = String()
    cvss_vector = String()
    description = String()
    published_date = DateTime()
    last_modified_date = DateTime()
    vendor = String(filters=[Equal], exact=True)
    product = String(filters=[Equal], exact=True)

    @staticmethod
    def supports(uri: str, fast: bool = True, **kwargs: Any) -> Optional[bool]:
        """cves://"""
        parsed = urllib.parse.urlparse(uri)
        return parsed.scheme == "cves"

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

        self._url = "https://services.nvd.nist.gov/rest/json/cves/2.0"
        self.api_key = api_key
        self.window = window

        # use a cache, since the adapter does a lot of similar API requests,
        # and the data should rarely (never?) change
        self._session = requests_cache.CachedSession(
            cache_name="cves_cache",
            backend="sqlite",
            expire_after=180,
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
        if "cve_id" in bounds:
            params["CveId"] = cast(Equal, bounds.get("cve_id", Equal)).value
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
        params["virtualMatchString"] = get_cpe_from_items(vendor, product)
        if "cwe_id" in bounds:
            params["cweID"] = cast(Equal, bounds.get("cwe_id", Equal)).value
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
            for cve_item in payload["vulnerabilities"]:
                yield parse_raw_cve_item(cve_item["cve"])

            offset += self.window
            if total_results is not None and offset >= total_results:
                break
            if total_results is None and len(payload["vulnerabilities"]) < self.window:
                break
