# pylint: disable=c-extension-no-member, invalid-name, unused-argument, protected-access
"""
Tests for HTTP network resource.
"""

from datetime import timedelta
from unittest.mock import MagicMock

import pytest
from requests import Session
from requests_cache import CachedSession
from yarl import URL

from shillelagh.exceptions import ProgrammingError
from shillelagh.resources.http import HTTPNetworkResourceImplementation


def test_init_with_cache(mock_cached_session):
    """
    Test initialization with cache.
    """
    mock_session_instance = MagicMock(spec=CachedSession)
    mock_cached_session.return_value = mock_session_instance

    url = URL("https://example.com/resource")
    resource = HTTPNetworkResourceImplementation(
        url=url,
        cache_name="test_cache",
    )

    mock_cached_session.assert_called_once_with(
        {},
        "test_cache",
        timedelta(seconds=180),
    )
    assert resource._session == mock_session_instance


def test_init_without_cache(mock_session):
    """
    Test initialization without cache.
    """
    mock_session_instance = MagicMock(spec=Session)
    mock_session.return_value = mock_session_instance

    url = URL("https://example.com/resource")
    resource = HTTPNetworkResourceImplementation(
        url=url,
    )

    mock_session.assert_called_once()
    assert resource._session == mock_session_instance


def test_get_content_type(mock_session):
    """
    Test get_content_type method.
    """
    mock_session_instance = MagicMock()
    mock_session_instance.head.return_value.headers = {
        "content-type": "application/json",
    }
    mock_session.return_value = mock_session_instance

    url = URL("https://example.com/resource")
    resource = HTTPNetworkResourceImplementation(url=url)

    content_type = resource.get_content_type()
    assert content_type == "application/json"
    mock_session_instance.head.assert_called_once_with("https://example.com/resource")


def test_get_data_success(mock_session):
    """
    Test get_data method success.
    """
    mock_session_instance = MagicMock()
    mock_response = MagicMock()
    mock_response.ok = True
    mock_response.content = b"response content"
    mock_session_instance.get.return_value = mock_response
    mock_session.return_value = mock_session_instance

    url = URL("https://example.com/resource")
    resource = HTTPNetworkResourceImplementation(url=url)

    data = resource.get_data()
    assert data == b"response content"
    mock_session_instance.get.assert_called_once_with("https://example.com/resource")


def test_get_data_failure(mock_session):
    """
    Test get_data method failure.
    """
    mock_session_instance = MagicMock()
    mock_response = MagicMock()
    mock_response.ok = False
    mock_response.status_code = 404
    mock_response.content = b"Not Found"
    mock_session_instance.get.return_value = mock_response
    mock_session.return_value = mock_session_instance

    url = URL("https://example.com/resource")
    resource = HTTPNetworkResourceImplementation(url=url)

    with pytest.raises(
        ProgrammingError,
        match=r"Error while requesting HTTPS resource https://example.com/resource: "
        r"404 b'Not Found'",
    ):
        resource.get_data()

    mock_session_instance.get.assert_called_once_with("https://example.com/resource")
