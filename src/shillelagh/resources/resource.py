"""
Network resource gateway for Shillelagh.
"""

import sys
from typing import Any, Optional, Union

from yarl import URL

from shillelagh.exceptions import ProgrammingError
from shillelagh.resources.base import NetworkResourceImplementation

if sys.version_info < (3, 10):
    from importlib_metadata import EntryPoint, entry_points
else:
    from importlib.metadata import EntryPoint, entry_points

LOADED_PROTOCOLS: dict[str, EntryPoint] = {}


class NetworkResource:
    """
    A Network Resource is an entrypoint to getting files from different external network resources.
    """

    def __init__(
        self,
        uri: Union[str, URL],
        **kwargs: Any,
    ) -> None:
        if isinstance(uri, str):
            self._uri = URL(uri)
        else:
            self._uri = uri
        self._kwargs = kwargs
        self._content_type: Optional[str] = None

        _primary_resource = LOADED_PROTOCOLS.get(self._uri.scheme)
        if not _primary_resource:
            raise ProgrammingError("Protocol is not supported")

        self._resource: NetworkResourceImplementation = _primary_resource.load()(
            self._uri,
            **kwargs,
        )

    @staticmethod
    def is_protocol_supported(protocol: str) -> bool:
        """
        Returns if protocol is supported
        """
        return LOADED_PROTOCOLS.get(protocol) is not None

    def assert_content_type(self, content_type: str) -> bool:
        """
        Assert that the content type is of the expected type
        """
        if self._content_type:
            return content_type in self._content_type

        self._content_type = self._resource.get_content_type()
        return content_type in self._content_type

    def get_data(self) -> bytes:
        """
        Get data from network resource
        """
        return self._resource.get_data()


for entry_point in entry_points(group="shillelagh.network_resource"):
    LOADED_PROTOCOLS[entry_point.name] = entry_point
