"""
Custom functions available to the SQL backend.
"""
import json
import time
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Type

import pkg_resources

from shillelagh.adapters.base import Adapter
from shillelagh.exceptions import ProgrammingError


__all__ = ["sleep", "get_metadata"]


def sleep(seconds: int) -> None:
    """
    Sleep for ``n`` seconds.

    This is useful for troubleshooting timeouts::

        sql> SELECT sleep(60);

    """
    time.sleep(seconds)


def get_metadata(
    adapter_kwargs: Dict[str, Dict[str, Any]],
    adapters: List[Type[Adapter]],
    uri: str,
) -> str:
    """
    Return metadata about a given table.

    Returns the name of the adapter that supports the table, as well
    as any extra metadata provided by the adapter::

        sql> SELECT GET_METADATA("https://docs.google.com/spreadsheets/d/1/edit#gid=0");
        GET_METADATA("https://docs.google.com/spreadsheets/d/1/edit#gid=0")
        -------------------------------------------------------------------
        {
            "extra": {
                "Spreadsheet title": "A title",
                "Sheet title": "Another title"
            },
            "adapter": "GSheetsAPI"
        }

    """
    adapter: Optional[Type[Adapter]] = None
    for adapter in adapters:
        key = adapter.__name__.lower()
        kwargs = adapter_kwargs.get(key, {})
        if adapter.supports(uri, **kwargs):
            break
    else:
        raise ProgrammingError(f"Unsupported table: {uri}")

    key = adapter.__name__.lower()
    args = adapter.parse_uri(uri)
    kwargs = adapter_kwargs.get(key, {})
    instance = adapter(*args, **kwargs)

    return json.dumps(
        {
            "extra": instance.get_metadata(),
            "adapter": adapter.__name__,
        },
    )


def version() -> str:
    """
    Return the current version of Shillelagh.

    As an example::

        sql> SELECT VERSION();
        VERSION()
        -----------
        0.7.4

    """
    return pkg_resources.get_distribution("shillelagh").version
