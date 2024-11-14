# pylint: disable=c-extension-no-member, invalid-name, unused-argument
"""
Tests for shillelagh.resources.networkresource.
"""

import pytest
from requests_mock.mocker import Mocker

from shillelagh.exceptions import ProgrammingError
from shillelagh.resources.resource import NetworkResource


def test_networkresource_assert_content_type(requests_mock: Mocker) -> None:
    """
    Testing assertion content type
    """
    url = "https://example.com/csv/test"
    target_content_type = "text/csv"

    requests_mock.head(
        url,
        headers={"Content-type": target_content_type},
    )

    network_resource = NetworkResource(url)
    assert network_resource.assert_content_type(target_content_type)
    assert network_resource.assert_content_type(target_content_type)


def test_networkresource_init() -> None:
    """
    Testing NetworkResource init
    """
    with pytest.raises(ProgrammingError) as excinfo:
        NetworkResource("unknown_protocol://example.com/csv")
    assert str(excinfo.value) == "Protocol is not supported"
