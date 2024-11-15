"""
Fixtures for Shillelagh.
"""

import json
import logging
import os
from collections.abc import Iterator
from typing import Any
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture
from yarl import URL

from shillelagh.adapters.registry import AdapterLoader
from shillelagh.resources.sftp import SFTPNetworkResourceImplementation

_logger = logging.getLogger(__name__)


@pytest.fixture
def adapter_kwargs() -> dict[str, Any]:
    """
    Load adapter configuration.

    This fixture looks for the environment variable `SHILLELAGH_ADAPTER_KWARGS`.
    The configuration should be encoded as JSON.
    """
    kwargs = {}
    if "SHILLELAGH_ADAPTER_KWARGS" in os.environ:
        try:
            kwargs = json.loads(os.environ["SHILLELAGH_ADAPTER_KWARGS"])
        except Exception as ex:  # pylint: disable=broad-except
            _logger.warning('Unable to load "SHILLELAGH_ADAPTER_KWARGS": %s', ex)

    return kwargs


@pytest.fixture
def registry(mocker: MockerFixture) -> Iterator[AdapterLoader]:
    """
    Create a custom adapter registry.
    """
    custom_registry = AdapterLoader()
    mocker.patch("shillelagh.adapters.registry.registry", new=custom_registry)
    mocker.patch("shillelagh.backends.apsw.db.registry", new=custom_registry)
    yield custom_registry


@pytest.fixture
def mock_transport(mocker: MockerFixture) -> tuple[MagicMock, MagicMock]:
    """
    Fixture to mock paramiko.Transport and SFTPClient.
    """
    mock_transport_class = mocker.patch("shillelagh.resources.sftp.paramiko.Transport")
    mock_transport_instance = MagicMock()
    mock_transport_class.return_value = mock_transport_instance

    mock_sftp_client_class = mocker.patch(
        "shillelagh.resources.sftp.paramiko.SFTPClient",
    )
    mock_sftp_client_instance = MagicMock()
    mock_sftp_client_class.from_transport.return_value = mock_sftp_client_instance

    return mock_transport_instance, mock_sftp_client_instance


@pytest.fixture
def mock_session(mocker: MockerFixture):
    """
    Fixture to mock the session (either CachedSession or Session).
    """
    return mocker.patch(
        "shillelagh.resources.http.Session",
        autospec=True,
    )


@pytest.fixture
def mock_cached_session(mocker: MockerFixture):
    """
    Fixture to mock the CachedSession.
    """
    return mocker.patch("shillelagh.resources.http.get_session", autospec=True)


@pytest.fixture
def mock_ftp(mocker: MockerFixture) -> MagicMock:
    """
    Fixture to mock ftplib.FTP
    """
    mock_ftp_class = mocker.patch("shillelagh.resources.ftp.ftplib.FTP")
    mock_ftp_instance = MagicMock()
    mock_ftp_class.return_value = mock_ftp_instance
    return mock_ftp_instance


@pytest.fixture
def sftp_resource():
    """
    Fixture to create an instance of SFTPNetworkResourceImplementation
    """
    url = URL("sftp://shillelagh:shillelagh123@localhost:2222/test.csv")
    return SFTPNetworkResourceImplementation(url)
