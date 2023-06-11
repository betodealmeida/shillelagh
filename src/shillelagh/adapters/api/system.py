"""
An adapter for retrieving information on running processes and system utilization (CPU,
memory, disks, network, sensors).

See https://github.com/giampaolo/psutil for more information.
"""
import logging
import time
import urllib.parse
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

import psutil

from shillelagh.adapters.base import Adapter
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import DateTime, Field, Float, Integer, Order
from shillelagh.filters import Filter
from shillelagh.typing import RequestedOrder, Row

_logger = logging.getLogger(__name__)

AVERAGE_NUMBER_OF_ROWS = 100
MEMORY_COLUMNS = [
    "total",
    "available",
    "percent",
    "used",
    "free",
    "active",
    "inactive",
    "wired",
]
SWAP_COLUMNS = ["total", "used", "free", "percent", "sin", "sout"]


class SystemAPI(Adapter):

    """
    An adapter for retrieving system information.
    """

    safe = False

    supports_limit = True
    supports_offset = True

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

    def _get_memory_column(self, column: str) -> Field:
        """
        Return a column field given its name.

        All memory columns are integers (bytes), except for ``percent``.
        """
        if column == "percent":
            return Float(filters=None, order=Order.NONE, exact=False)

        return Integer(filters=None, order=Order.NONE, exact=False)

    def _set_columns(self) -> None:
        self.columns: Dict[str, Field] = {
            "timestamp": DateTime(filters=None, order=Order.ASCENDING, exact=False),
        }

        if self.resource == "cpu":
            self.columns.update(
                {
                    f"cpu{i}": Float(
                        filters=None,
                        order=Order.NONE,
                        exact=False,
                    )
                    for i in range(psutil.cpu_count())
                },
            )
        elif self.resource == "memory":
            self.columns.update(
                {column: self._get_memory_column(column) for column in MEMORY_COLUMNS},
            )
        elif self.resource == "swap":
            self.columns.update(
                {column: self._get_memory_column(column) for column in SWAP_COLUMNS},
            )
        else:
            raise ProgrammingError(f"Unknown resource: {self.resource}")

        self.columns["timestamp"] = DateTime(
            filters=None,
            order=Order.ASCENDING,
            exact=False,
        )

    def get_columns(self) -> Dict[str, Field]:
        return self.columns

    def get_data(
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        **kwargs: Any,
    ) -> Iterator[Row]:
        rowid = 0
        while limit is None or rowid < limit:
            if offset is not None:
                time.sleep(self.interval * offset)

            if self.resource == "cpu":
                try:
                    row = {
                        f"cpu{i}": value / 100.0
                        for i, value in enumerate(
                            psutil.cpu_percent(interval=self.interval, percpu=True),
                        )
                    }
                except KeyboardInterrupt:
                    return
            elif self.resource == "memory":
                row = psutil.virtual_memory()._asdict()
                time.sleep(self.interval)
            elif self.resource == "swap":
                row = psutil.swap_memory()._asdict()
                time.sleep(self.interval)
            else:
                raise ProgrammingError(f"Unknown resource: {self.resource}")

            row.update({"rowid": rowid, "timestamp": datetime.now(timezone.utc)})

            _logger.debug(row)
            yield row
            rowid += 1
