import json
import time
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple
from typing import Type

import pkg_resources
from shillelagh.adapters.base import Adapter
from shillelagh.exceptions import ProgrammingError


__all__ = ["sleep", "get_metadata"]


def sleep(n: int) -> None:
    time.sleep(n)


def get_metadata(
    adapter_kwargs: Dict[str, Dict[str, Any]],
    adapters: List[Type[Adapter]],
    uri: str,
) -> str:
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
    return pkg_resources.get_distribution("shillelagh").version
