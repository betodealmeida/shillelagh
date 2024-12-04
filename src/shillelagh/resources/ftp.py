"""
Class with implementation network resource for FTP protocols
"""

import ftplib
import mimetypes

from yarl import URL

from shillelagh.exceptions import ProgrammingError
from shillelagh.resources.base import NetworkResourceImplementation


class FTPNetworkResourceImplementation(NetworkResourceImplementation):
    """
    Implement logic for FTP protocol.
    """

    def __init__(self, url: URL, **kwargs) -> None:
        super().__init__(url, **kwargs)

        self.__username = url.user
        self.__password = url.password
        self.host = url.host
        self.path = url.path
        self.port = url.port or 21

        self.connection = ftplib.FTP()
        try:
            self.connection.connect(host=self.host, port=self.port)
        except Exception as ex:
            raise ProgrammingError(
                f"Error occurred while connecting to FTP resource "
                f"({self.host}:{self.port}): {str(ex)}",
            ) from ex

        try:
            self.connection.login(self.__username, self.__password)
        except Exception as ex:
            raise ProgrammingError(
                f"Error occurred while authenticating on FTP resource "
                f"({self.host}:{self.port}): {str(ex)}",
            ) from ex

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
        data: bytes = b""

        def write_data(_data: bytes) -> None:
            """
            Callback to write received bytes
            """
            nonlocal data
            data = _data

        try:
            self.connection.retrbinary(f"RETR {self.path}", write_data)
        except Exception as ex:
            raise ProgrammingError(
                f"Error occurred while getting {self.path} from FTP resource "
                f"({self.host}): {str(ex)}",
            ) from ex

        return data

    def close(self) -> None:
        """
        Quit and close FTP connection
        """
        self.connection.quit()
