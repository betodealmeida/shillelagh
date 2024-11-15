# pylint: disable=c-extension-no-member, invalid-name, unused-argument
"""
Integration tests for SFTP Network Resource.
"""

import pytest
from yarl import URL

from shillelagh.exceptions import ProgrammingError
from shillelagh.resources.sftp import SFTPNetworkResourceImplementation


def test_get_content_type_real(sftp_resource):
    """
    Test get_content_type() with real SFTP service
    """
    content_type = sftp_resource.get_content_type()
    assert content_type == "text/csv"


def test_get_data_real(sftp_resource):
    """
    Test get_data() with real SFTP service
    """
    data = sftp_resource.get_data()

    expected_data = (
        b'"index","temperature","site"\r\n10.0,15.2,'
        b'"Diamond_St"\r\n11.0,13.1,"Blacktail_Loop"\r\n12.0,13.3,'
        b'"Platinum_St"\r\n13.0,12.1,"Kodiak_Trail"\r\n'
    )
    assert data == expected_data


def test_get_data_no_file():
    """
    Test get_data() when the file is not found
    """
    invalid_url = URL("sftp://shillelagh:shillelagh123@localhost:2222/invalid.csv")
    sftp_resource = SFTPNetworkResourceImplementation(invalid_url)

    with pytest.raises(ProgrammingError, match="Error occurred while getting"):
        sftp_resource.get_data()


@pytest.mark.slow_integration_test
def test_invalid_credentials():
    """
    Test if an error is raised with invalid credentials
    """
    invalid_url = URL("sftp://shillelagh:wrongpass@localhost:2222/test.csv")

    with pytest.raises(
        ProgrammingError,
        match="Error occurred while connecting to SFTP resource",
    ):
        SFTPNetworkResourceImplementation(invalid_url)


@pytest.mark.slow_integration_test
def test_invalid_host():
    """
    Test if an error is raised with invalid credentials
    """
    invalid_url = URL("sftp://shillelagh:wrongpass@notexists:999/test.csv")

    with pytest.raises(
        ProgrammingError,
        match="Error occurred while connecting to SFTP resource",
    ):
        SFTPNetworkResourceImplementation(invalid_url)
