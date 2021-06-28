import json
import time
from typing import Any
from typing import Dict

from pkg_resources import iter_entry_points
from shillelagh.exceptions import ProgrammingError


__all__ = ["sleep", "get_metadata"]


def sleep(n: int) -> None:
    time.sleep(n)


def get_metadata(
    adapter_args: Dict[str, Any],
    adapter_kwargs: Dict[str, Any],
    uri: str,
) -> str:
    adapters = [
        entry_point.load() for entry_point in iter_entry_points("shillelagh.adapter")
    ]
    for adapter in adapters:
        if adapter.supports(uri):
            break
    else:
        raise ProgrammingError(f"Unsupported table: {uri}")

    key = adapter.__name__.lower()
    args = adapter.parse_uri(uri) + adapter_args.get(key, ())
    kwargs = adapter_kwargs.get(key, {})
    instance = adapter(*args, **kwargs)

    return json.dumps(
        {
            "extra": instance.get_metadata(),
            "adapter": adapter.__name__,
        },
    )
