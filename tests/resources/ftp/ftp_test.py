# pylint: disable=c-extension-no-member, invalid-name, unused-argument
"""
Tests for FTP network resource.
"""

import pytest
from yarl import URL

from shillelagh.exceptions import ProgrammingError
from shillelagh.resources.ftp import FTPNetworkResourceImplementation


def test_init_connection_success(mock_ftp):
    """
    Test successful initialization of FTPNetworkResourceImplementation
    """
    url = URL("ftp://user:password@localhost:21/path")
    resource = FTPNetworkResourceImplementation(url)

    mock_ftp.connect.assert_called_once_with(host="localhost", port=21)
    mock_ftp.login.assert_called_once_with("user", "password")
    assert resource.host == "localhost"
    assert resource.path == "/path"
    assert resource.port == 21


def test_init_connection_failure(mocker):
    """
    Test connection failure during initialization
    """
    mock_ftp_class = mocker.patch("shillelagh.resources.ftp.ftplib.FTP")
    mock_ftp_instance = mock_ftp_class.return_value
    mock_ftp_instance.connect.side_effect = Exception("Connection error")

    url = URL("ftp://user:password@localhost:21/path")

    with pytest.raises(
        ProgrammingError,
        match="Error occurred while connecting to FTP resource",
    ):
        FTPNetworkResourceImplementation(url)


def test_init_authentication_failure(mocker):
    """
    Test authentication failure during initialization
    """
    mock_ftp_class = mocker.patch("shillelagh.resources.ftp.ftplib.FTP")
    mock_ftp_instance = mock_ftp_class.return_value
    mock_ftp_instance.login.side_effect = Exception("Authentication error")

    url = URL("ftp://user:password@localhost:21/path")

    with pytest.raises(
        ProgrammingError,
        match="Error occurred while authenticating on FTP resource",
    ):
        FTPNetworkResourceImplementation(url)


def test_get_content_type(mock_ftp):
    """
    Test get_content_type method
    """
    url = URL("ftp://user:password@localhost:21/path.csv")
    resource = FTPNetworkResourceImplementation(url)
    content_type = resource.get_content_type()

    assert content_type == "text/csv"


def test_get_data_success(mocker, mock_ftp):
    """
    Test get_data method success
    """

    def fake_retrbinary(cmd, callback):
        callback(b"file content")

    mock_ftp.retrbinary.side_effect = fake_retrbinary

    url = URL("ftp://user:password@localhost:21/path")
    resource = FTPNetworkResourceImplementation(url)
    data = resource.get_data()

    mock_ftp.retrbinary.assert_called_once_with("RETR /path", mocker.ANY)
    assert data == b"file content"


def test_get_data_failure(mock_ftp):
    """
    Test get_data method failure
    """
    mock_ftp.retrbinary.side_effect = Exception("Download error")

    url = URL("ftp://user:password@localhost:21/path")
    resource = FTPNetworkResourceImplementation(url)

    with pytest.raises(
        ProgrammingError,
        match="Error occurred while getting /path from FTP resource",
    ):
        resource.get_data()


def test_close(mock_ftp):
    """
    Test close method
    """
    url = URL("ftp://user:password@localhost:21/path")
    resource = FTPNetworkResourceImplementation(url)
    resource.close()

    mock_ftp.quit.assert_called_once()
