"""
An adapter for in-memory holidays.
"""

import datetime
from typing import Any, Iterator, List, Optional, Tuple, TypedDict

from holidays import country_holidays, list_supported_countries

from shillelagh.adapters.base import Adapter
from shillelagh.fields import Date, Order, String
from shillelagh.filters import Equal, Range
from shillelagh.lib import SimpleCostModel
from shillelagh.typing import RequestedOrder, Row

# this is just a wild guess; used to estimate query cost
AVERAGE_NUMBER_OF_ROWS = 1000


class BoundsType(TypedDict, total=False):
    """
    The type of the bounds parameter.
    """

    country: Equal
    date: Range


class HolidaysMemory(Adapter):

    """
    An adapter for in-memory holidays.
    """

    safe = True

    supports_limit = False
    supports_offset = False
    supports_requested_columns = False

    country = String(filters=[Equal], order=Order.NONE, exact=True)
    name = String(filters=[], order=Order.NONE, exact=False)
    date = Date(filters=[Range], order=Order.ASCENDING, exact=False)

    @staticmethod
    def supports(uri: str, fast: bool = True, **kwargs: Any) -> Optional[bool]:
        return uri == "holidays"

    @staticmethod
    def parse_uri(uri: str) -> Tuple[()]:
        return ()

    get_cost = SimpleCostModel(AVERAGE_NUMBER_OF_ROWS)

    def get_data(  # type: ignore
        self,
        bounds: BoundsType,
        order: List[Tuple[str, RequestedOrder]],
        **kwargs: Any,
    ) -> Iterator[Row]:
        countries = (
            [bounds["country"].value]
            if "country" in bounds
            else list_supported_countries()
        )

        default_start = default_end = datetime.date.today()
        time_range = bounds.get("date", Range(start=default_start, end=default_end))
        years = range(
            (time_range.start or default_start).year,
            (time_range.end or default_end).year + 1,
        )

        i = 0
        for country in countries:
            holidays = country_holidays(country, years=years)
            for date, name in sorted(holidays.items()):
                yield {
                    "rowid": i,
                    "country": country,
                    "name": name,
                    "date": date,
                }
                i += 1
