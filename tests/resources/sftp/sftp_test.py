# pylint: disable=c-extension-no-member, invalid-name, unused-argument, broad-exception-caught
"""
Tests for SFTP network resource.
"""

from unittest.mock import MagicMock

import pytest
from yarl import URL

from shillelagh.exceptions import ProgrammingError
from shillelagh.resources.sftp import SFTPNetworkResourceImplementation


def test_init_connection_success(mock_transport):
    """
    Test successful initialization of SFTPNetworkResourceImplementation.
    """
    transport_mock, sftp_client_mock = mock_transport

    url = URL("sftp://user:password@localhost:22/path")
    resource = SFTPNetworkResourceImplementation(url)

    transport_mock.connect.assert_called_once_with(username="user", password="password")
    assert resource.host == "localhost"
    assert resource.path == "/path"
    assert resource.port == 22
    assert resource.connection == sftp_client_mock


def test_init_connection_failure_from_transport(mocker):
    """
    Test connection failure during initialization.
    """
    url = URL("sftp://user:password@localhost:22/path")

    mock_transport_class = mocker.patch("shillelagh.resources.sftp.paramiko.Transport")
    mock_transport_instance = mock_transport_class.return_value
    mock_transport_instance.connect.side_effect = MagicMock()

    mock_sftp_class = mocker.patch(
        "shillelagh.resources.sftp.paramiko.SFTPClient.from_transport",
    )
    mock_sftp_class.return_value = None

    with pytest.raises(
        ProgrammingError,
        match="Error occurred while creating SFTPClient from",
    ):
        SFTPNetworkResourceImplementation(url)


def test_init_connection_failure_transport(mocker):
    """
    Test connection failure during initialization.
    """
    url = URL("sftp://user:password@localhost:22/path")

    mock_transport_class = mocker.patch("shillelagh.resources.sftp.paramiko.Transport")
    mock_transport_instance = mock_transport_class.return_value
    mock_transport_instance.connect.side_effect = Exception("Connection error")

    with pytest.raises(
        ProgrammingError,
        match="Error occurred while connecting to SFTP resource",
    ):
        SFTPNetworkResourceImplementation(url)


def test_get_content_type(mock_transport):
    """
    Test get_content_type method.
    """
    _, _ = mock_transport
    url = URL("sftp://user:password@localhost:22/test.txt")
    resource = SFTPNetworkResourceImplementation(url)

    content_type = resource.get_content_type()
    assert content_type == "text/plain"


def test_get_content_type_unknown_extension(mock_transport):
    """
    Test get_content_type method with unknown file extension.
    """
    _, _ = mock_transport
    url = URL("sftp://user:password@localhost:22/file.unknown")
    resource = SFTPNetworkResourceImplementation(url)

    content_type = resource.get_content_type()
    assert content_type == ""


def test_get_data_success(mock_transport, mocker):
    """
    Test get_data method success.
    """
    _, sftp_client_mock = mock_transport

    def fake_getfo(path, byte_stream):
        byte_stream.write(b"file content")

    sftp_client_mock.getfo.side_effect = fake_getfo

    url = URL("sftp://user:password@localhost:22/path")
    resource = SFTPNetworkResourceImplementation(url)
    data = resource.get_data()

    sftp_client_mock.getfo.assert_called_once_with("/path", mocker.ANY)
    assert data == b"file content"


def test_get_data_failure(mock_transport):
    """
    Test get_data method failure.
    """
    _, sftp_client_mock = mock_transport
    sftp_client_mock.getfo.side_effect = Exception("Download error")

    url = URL("sftp://user:password@localhost:22/path")
    resource = SFTPNetworkResourceImplementation(url)

    with pytest.raises(
        ProgrammingError,
        match="Error occurred while getting /path from SFTP resource",
    ):
        resource.get_data()


def test_close_success(mock_transport):
    """
    Test close method.
    """
    transport_mock, sftp_client_mock = mock_transport

    transport_mock.sock.closed = False
    sftp_client_mock.sock.closed = False

    url = URL("sftp://user:password@localhost:22/path")
    resource = SFTPNetworkResourceImplementation(url)

    resource.close()
    sftp_client_mock.close.assert_called_once()
    transport_mock.close.assert_called_once()


def test_close_with_exception(mock_transport):
    """
    Test close method with exceptions.
    """
    transport_mock, sftp_client_mock = mock_transport

    sftp_client_mock.close.side_effect = AttributeError("SFTP client close error")
    transport_mock.close.side_effect = AttributeError("Transport close error")

    url = URL("sftp://user:password@localhost:22/path")
    resource = SFTPNetworkResourceImplementation(url)

    try:
        resource.close()
    except Exception as e:
        pytest.fail(f"close() raised an exception: {e}")

    sftp_client_mock.close.assert_called_once()
    transport_mock.close.assert_called_once()
