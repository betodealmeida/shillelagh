"""
Class with implementation network resource for SFTP protocols
"""

import mimetypes
from io import BytesIO
from typing import Optional

import paramiko
from paramiko.sftp_client import SFTPClient
from yarl import URL

from shillelagh.exceptions import ProgrammingError
from shillelagh.resources.base import NetworkResourceImplementation


class SFTPNetworkResourceImplementation(NetworkResourceImplementation):
    """
    Implement logic for SFTP protocol.
    """

    def __init__(self, url: URL, **kwargs) -> None:
        super().__init__(url, **kwargs)

        self.__username = url.user
        self.__password = url.password
        self.host = url.host
        self.path = url.path
        self.port = url.port or 22

        try:
            self.__transport: paramiko.Transport = paramiko.Transport(
                (self.host, self.port),
            )
            self.__transport.connect(username=self.__username, password=self.__password)
        except Exception as ex:
            raise ProgrammingError(
                f"Error occurred while connecting to SFTP resource "
                f"({self.host}:{self.port}): {str(ex)}",
            ) from ex

        self.connection: Optional[SFTPClient] = paramiko.SFTPClient.from_transport(
            self.__transport,
        )
        if self.connection is None:
            raise ProgrammingError(
                f"Error occurred while creating SFTPClient from "
                f"({self.host}:{self.port})",
            )

    def get_content_type(self) -> str:
        """
        Get content type of file based on file extension in path
        """
        mime_type, _ = mimetypes.guess_type(self.path)
        return mime_type or ""

    def get_data(self) -> bytes:
        """
        Return data as a bytes
        """
        with BytesIO() as byte_stream:
            try:
                self.connection.getfo(self.path, byte_stream)  # type: ignore[union-attr]
            except Exception as ex:
                raise ProgrammingError(
                    f"Error occurred while getting {self.path} from SFTP resource "
                    f"({self.host}:{self.port}): {str(ex)}",
                ) from ex
            byte_stream.seek(0)
            return byte_stream.read()

    def close(self) -> None:
        """
        Quit and close SFTP connection
        """
        try:
            self.connection.close()  # type: ignore[union-attr]
        except AttributeError:
            pass

        try:
            self.__transport.close()
        except AttributeError:
            pass
