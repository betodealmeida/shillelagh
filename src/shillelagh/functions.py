"""
Custom functions available to the SQL backend.
"""
import json
import sys
import time
from typing import Any, Dict, List, Type

from shillelagh.adapters.base import Adapter
from shillelagh.lib import find_adapter

if sys.version_info < (3, 10):
    from importlib_metadata import distribution
else:
    from importlib.metadata import distribution

__all__ = ["sleep", "get_metadata", "version"]


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
    adapter, args, kwargs = find_adapter(uri, adapter_kwargs, adapters)
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
    return str(distribution("shillelagh").version)
