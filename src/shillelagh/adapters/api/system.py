"""
An adapter for retrieving information on running processes and system utilization (CPU,
memory, disks, network, sensors).

See https://github.com/giampaolo/psutil for more information.
"""

import logging
import time
import urllib.parse
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple, Type, Union

import psutil

from shillelagh.adapters.base import Adapter
from shillelagh.fields import DateTime, Field, Float, Integer, Order
from shillelagh.filters import Filter
from shillelagh.typing import RequestedOrder, Row

_logger = logging.getLogger(__name__)

AVERAGE_NUMBER_OF_ROWS = 100


MEMORY_COLUMNS: Dict[str, Type[Field]] = {
    "total": Integer,
    "available": Integer,
    "percent": Float,
    "used": Integer,
    "free": Integer,
    "active": Integer,
    "inactive": Integer,
    "wired": Integer,
}
SWAP_COLUMNS: Dict[str, Type[Field]] = {
    "total": Integer,
    "used": Integer,
    "free": Integer,
    "percent": Float,
    "sin": Integer,
    "sout": Integer,
}


class ResourceType(str, Enum):
    """
    The type of resource to retrieve.
    """

    CPU = "cpu"
    MEMORY = "memory"
    SWAP = "swap"
    ALL = "all"


def get_columns(resource: ResourceType) -> Dict[str, Field]:
    """
    Build columns depending on the chosen resource.
    """
    memory_prefix = "virtual_" if resource == ResourceType.ALL else ""
    swap_prefix = "swap_" if resource == ResourceType.ALL else ""

    columns: Dict[str, Field] = {}

    if resource in {ResourceType.CPU, ResourceType.ALL}:
        columns.update(
            {
                f"cpu{i}": Float(
                    filters=None,
                    order=Order.NONE,
                    exact=False,
                )
                for i in range(psutil.cpu_count())
            },
        )
    if resource in {ResourceType.MEMORY, ResourceType.ALL}:
        columns.update(
            {
                f"{memory_prefix}{column}": field(
                    filters=None,
                    order=Order.NONE,
                    exact=False,
                )
                for column, field in MEMORY_COLUMNS.items()
            },
        )
    if resource in {ResourceType.SWAP, ResourceType.ALL}:
        columns.update(
            {
                f"{swap_prefix}{column}": field(
                    filters=None,
                    order=Order.NONE,
                    exact=False,
                )
                for column, field in SWAP_COLUMNS.items()
            },
        )

    return columns


class SystemAPI(Adapter):
    """
    An adapter for retrieving system information.
    """

    safe = False

    supports_limit = True
    supports_offset = True
    supports_requested_columns = True

    @staticmethod
    def supports(uri: str, fast: bool = True, **kwargs: Any) -> Optional[bool]:
        parsed = urllib.parse.urlparse(uri)
        return parsed.scheme == "system" and (
            # pylint: disable=protected-access
            parsed.netloc in ResourceType._value2member_map_ or parsed.netloc == ""
        )

    @staticmethod
    def parse_uri(uri: str) -> Union[Tuple[str], Tuple[str, float]]:
        parsed = urllib.parse.urlparse(uri)
        resource = parsed.netloc or "all"
        query_string = urllib.parse.parse_qs(parsed.query)

        if "interval" in query_string:
            return (resource, float(query_string["interval"][0]))
        return (resource,)

    def __init__(self, resource: str, interval: float = 1.0):
        super().__init__()

        self.resource = ResourceType(resource)
        self.interval = interval

        self._set_columns()

    def _set_columns(self) -> None:
        self.columns: Dict[str, Field] = {
            "timestamp": DateTime(filters=None, order=Order.ASCENDING, exact=False),
        }
        self.columns.update(get_columns(self.resource))

    def get_columns(self) -> Dict[str, Field]:
        return self.columns

    def get_data(  # pylint: disable=too-many-arguments
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        requested_columns: Optional[Set[str]] = None,
        **kwargs: Any,
    ) -> Iterator[Row]:
        requested_columns = requested_columns or set(self.columns.keys())

        rowid = 0
        while limit is None or rowid < limit:
            if offset is not None:
                time.sleep(self.interval * offset)

            row: Dict[str, Any] = {"rowid": rowid}
            if "timestamp" in requested_columns:
                row["timestamp"] = datetime.now(timezone.utc)

            needs_sleep = True

            # add CPU?
            if any(column.startswith("cpu") for column in requested_columns):
                try:
                    row.update(
                        {
                            f"cpu{i}": value / 100.0
                            for i, value in enumerate(
                                psutil.cpu_percent(interval=self.interval, percpu=True),
                            )
                            if f"cpu{i}" in requested_columns
                        },
                    )
                    needs_sleep = False
                except KeyboardInterrupt:
                    return

            # virtual memory
            if self.resource == ResourceType.ALL and any(
                column.startswith("virtual_") for column in requested_columns
            ):
                row.update(
                    {
                        f"virtual_{k}": v
                        for k, v in psutil.virtual_memory()._asdict().items()
                        if f"virtual_{k}" in requested_columns
                    },
                )
            elif self.resource == ResourceType.MEMORY and any(
                column in MEMORY_COLUMNS for column in requested_columns
            ):
                row.update(
                    {
                        k: v
                        for k, v in psutil.virtual_memory()._asdict().items()
                        if k in requested_columns
                    },
                )

            # swap
            if self.resource == ResourceType.ALL and any(
                column.startswith("swap_") for column in requested_columns
            ):
                row.update(
                    {
                        f"swap_{k}": v
                        for k, v in psutil.swap_memory()._asdict().items()
                        if f"swap_{k}" in requested_columns
                    },
                )
            elif self.resource == ResourceType.SWAP and any(
                column in SWAP_COLUMNS for column in requested_columns
            ):
                row.update(
                    {
                        k: v
                        for k, v in psutil.swap_memory()._asdict().items()
                        if k in requested_columns
                    },
                )

            _logger.debug(row)
            yield row
            rowid += 1

            if needs_sleep:
                time.sleep(self.interval)
