"""
An adapter for retrieving information on running processes and system utilization (CPU,
memory, disks, network, sensors).

See https://github.com/giampaolo/psutil for more information.
"""
import logging
import urllib.parse
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

import psutil

from shillelagh.adapters.base import Adapter
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import DateTime, Field, Float, Order
from shillelagh.filters import Filter
from shillelagh.typing import RequestedOrder, Row

_logger = logging.getLogger(__name__)

AVERAGE_NUMBER_OF_ROWS = 100


class SystemAPI(Adapter):

    """
    An adapter for retrieving system information.
    """

    safe = False

    @staticmethod
    def supports(uri: str, fast: bool = True, **kwargs: Any) -> Optional[bool]:
        parsed = urllib.parse.urlparse(uri)
        return parsed.scheme == "system"

    @staticmethod
    def parse_uri(uri: str) -> Union[Tuple[str], Tuple[str, float]]:
        parsed = urllib.parse.urlparse(uri)
        resource = parsed.netloc
        query_string = urllib.parse.parse_qs(parsed.query)

        if "interval" in query_string:
            return (resource, float(query_string["interval"][0]))
        return (resource,)

    def __init__(self, resource: str, interval: float = 1.0):
        super().__init__()

        self.resource = resource
        self.interval = interval

        self._set_columns()

    def _set_columns(self) -> None:
        self.columns: Dict[str, Field] = {
            "timestamp": DateTime(filters=None, order=Order.ASCENDING, exact=False),
        }

        if self.resource == "cpu":
            num_cpus = psutil.cpu_count()
            for i in range(num_cpus):
                self.columns[f"cpu{i}"] = Float(
                    filters=None,
                    order=Order.NONE,
                    exact=False,
                )
        else:
            raise ProgrammingError(f"Unknown resource: {self.resource}")

    def get_columns(self) -> Dict[str, Field]:
        return self.columns

    def get_data(
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
    ) -> Iterator[Row]:
        i = 0
        while True:
            try:
                values = psutil.cpu_percent(interval=self.interval, percpu=True)
            except KeyboardInterrupt:
                return

            row = {
                "rowid": i,
                "timestamp": datetime.now(timezone.utc),
            }
            for i, value in enumerate(values):
                row[f"cpu{i}"] = value / 100.0

            yield row
            _logger.debug(row)
            i += 1
