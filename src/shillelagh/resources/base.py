"""
Base class for network resource implementations.
"""

import atexit
from abc import abstractmethod
from typing import Any

from yarl import URL


class NetworkResourceImplementation:
    """Base class for network resource implementations."""

    def __init__(self, url: URL, **kwargs: Any) -> None:
        self._url = url
        self._kwargs = kwargs
        atexit.register(self.close)

    @abstractmethod
    def get_content_type(self) -> str:
        """Return content type."""
        raise NotImplementedError

    @abstractmethod
    def get_data(self) -> bytes:
        """Return data from network resource."""
        raise NotImplementedError

    def close(self) -> None:
        """
        Close the network resource.

        Network resource should use this method to perform any pending changes when the
        connection is closed.
        """
