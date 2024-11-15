# pylint: disable=c-extension-no-member, invalid-name, unused-argument, protected-access, redefined-outer-name
"""
Integration tests for FTP Network Resource.
"""

from ftplib import FTP
from io import BytesIO

import pytest
from yarl import URL

from shillelagh.exceptions import ProgrammingError
from shillelagh.resources.ftp import FTPNetworkResourceImplementation


@pytest.fixture(scope="module")
def ftp_url():
    """
    FTP server details from the docker-compose.yml
    """
    return URL("ftp://shillelagh:shillelagh123@localhost:2121/test.csv")


@pytest.fixture(scope="module")
def ftp_resource(ftp_url):
    """
    Create an instance of FTPNetworkResourceImplementation
    """
    return FTPNetworkResourceImplementation(ftp_url)


@pytest.mark.slow_integration_test
def test_ftp_connection(ftp_resource):
    """
    Test if the FTP connection is established successfully
    """
    assert ftp_resource.connection.sock is not None


@pytest.mark.slow_integration_test
def test_get_content_type(ftp_resource):
    """
    Test if the FTP resource can correctly fetch the content type
    """
    content_type = ftp_resource.get_content_type()
    assert content_type == "text/csv"


@pytest.mark.slow_integration_test
def test_get_data(ftp_resource):
    """
    Test if the FTP resource can fetch the data
    """
    data = ftp_resource.get_data()
    assert data.startswith(b'"index"')


@pytest.mark.slow_integration_test
def test_get_data_invalid_path():
    """
    Test if an error is raised when an invalid file path is given
    """
    invalid_url = URL("ftp://shillelagh:shillelagh123@localhost:2121/invalid.csv")
    ftp_resource = FTPNetworkResourceImplementation(invalid_url)

    with pytest.raises(
        ProgrammingError,
        match="Error occurred while getting /invalid.csv",
    ):
        ftp_resource.get_data()


@pytest.mark.slow_integration_test
def test_invalid_credentials():
    """
    Test if an error is raised with invalid credentials
    """
    invalid_url = URL("ftp://wronguser:wrongpass@localhost:2121/test.csv")

    with pytest.raises(
        ProgrammingError,
        match="Error occurred while authenticating on FTP resource",
    ):
        FTPNetworkResourceImplementation(invalid_url)


@pytest.fixture(scope="module")
def ftp_server():
    """
    Setup FTP server connection to check the file contents
    """
    ftp = FTP()
    ftp.connect("localhost", 2121)
    ftp.login("shillelagh", "shillelagh123")
    yield ftp
    ftp.quit()


@pytest.mark.slow_integration_test
def test_ftp_file_download(ftp_server):
    """
    Test if the file exists and can be downloaded from the FTP server
    """
    with BytesIO() as file_content:
        ftp_server.retrbinary("RETR /test.csv", file_content.write)
        file_content.seek(0)
        content = file_content.read()

    expected_content = (
        b'"index","temperature","site"\r\n10.0,15.2,'
        b'"Diamond_St"\r\n11.0,13.1,"Blacktail_Loop"\r\n12.0,13.3,'
        b'"Platinum_St"\r\n13.0,12.1,"Kodiak_Trail"\r\n'
    )
    assert content == expected_content
