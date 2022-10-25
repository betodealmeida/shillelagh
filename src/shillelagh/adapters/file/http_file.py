"""
A base adapter for files over the network.
"""

import tempfile
import urllib.parse
from pathlib import Path
from typing import Any, Dict, Iterator, List, Tuple

import requests

from shillelagh.adapters.base import Adapter
from shillelagh.exceptions import ProgrammingError
from shillelagh.filters import Filter
from shillelagh.typing import Maybe, MaybeType, RequestedOrder, Row

SUPPORTED_PROTOCOLS = {"http", "https"}


class HTTPFileAdapter(Adapter):

    """
    Base class for adding network support to file adapters.
    """

    safe = False

    supports_limit = True
    supports_offset = True

    # must be set by child classes
    suffix = ""
    content_type = ""

    def __getattribute__(self, name: str) -> Any:
        try:
            local = object.__getattribute__(self, "local")
            if not local:
                if name in {"insert_data", "delete_data"}:
                    raise ProgrammingError("Cannot apply DML to a remote file")
                if name == "close":
                    self.path.unlink()
                    return None
        except AttributeError:
            pass

        return object.__getattribute__(self, name)

    @classmethod
    def supports(cls, uri: str, fast: bool = True, **kwargs: Any) -> MaybeType:
        # local file
        path = Path(uri)
        if path.suffix == cls.suffix and path.exists():
            return True

        # remote file
        parsed = urllib.parse.urlparse(uri)
        if parsed.scheme not in SUPPORTED_PROTOCOLS:
            return False

        # URLs ending in the suffix are a probably match
        if parsed.path.endswith(cls.suffix):
            return True

        # do a head request to get mimetype
        if fast:
            return Maybe

        response = requests.head(uri)
        return cls.content_type in response.headers["content-type"]

    @staticmethod
    def parse_uri(uri: str) -> Tuple[str]:
        return (uri,)

    def __init__(self, path_or_uri: str):
        super().__init__()

        path = Path(path_or_uri)
        if path.suffix == self.suffix and path.exists():
            self.local = True
        else:
            self.local = False

            with tempfile.NamedTemporaryFile(delete=False) as output:
                response = requests.get(path_or_uri)
                output.write(response.content)
            path = Path(output.name)

        self.path = path

    def get_data(
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
        **kwargs: Any,
    ) -> Iterator[Row]:
        raise NotImplementedError("Subclasses must implement ``get_data``")
