"""
Tests for the s3select adapter.
"""
# pylint: disable=unused-argument, redefined-outer-name, use-implicit-booleaness-not-comparison

from typing import cast
from unittest.mock import MagicMock
from urllib.parse import urlparse

import pytest
from botocore import UNSIGNED
from botocore.config import Config
from pytest_mock import MockerFixture

from shillelagh.adapters.api.s3select import (
    CSVSerializationType,
    S3SelectAPI,
    get_input_serialization,
)
from shillelagh.backends.apsw.db import connect
from shillelagh.exceptions import ProgrammingError
from shillelagh.filters import Impossible


def test_get_input_serialization() -> None:
    """
    Tests for ``get_input_serialization``.
    """
    input_serialization = get_input_serialization(urlparse("s3://bucket/sample.csv"))
    assert input_serialization == {
        "CompressionType": "NONE",
        "CSV": {"FileHeaderInfo": "USE"},
    }

    input_serialization = get_input_serialization(
        urlparse(r"s3://bucket/sample.csv?RecordDelimiter=\r\n"),
    )
    assert input_serialization == {
        "CompressionType": "NONE",
        "CSV": {"FileHeaderInfo": "USE", "RecordDelimiter": "\r\n"},
    }

    input_serialization = get_input_serialization(
        urlparse("s3://bucket/sample?format=csv&CompressionType=GZIP"),
    )
    assert input_serialization == {
        "CompressionType": "GZIP",
        "CSV": {"FileHeaderInfo": "USE"},
    }

    with pytest.raises(ProgrammingError) as excinfo:
        get_input_serialization(urlparse("s3://bucket/sample"))
    assert str(excinfo.value) == (
        "Unable to determine file format. You must declare the format in the "
        "SQLAlchemy URI using ``?format={csv,json,parquet}``."
    )

    input_serialization = get_input_serialization(
        urlparse("s3://bucket/sample?format=csv&FileHeaderInfo=IGNORE&foo=bar"),
    )
    assert input_serialization == {
        "CompressionType": "NONE",
        "CSV": {"FileHeaderInfo": "IGNORE"},
    }

    input_serialization = get_input_serialization(
        urlparse("s3://bucket/sample.json?Type=LINES"),
    )
    assert input_serialization == {
        "CompressionType": "NONE",
        "JSON": {"Type": "LINES"},
    }

    input_serialization = get_input_serialization(
        urlparse("s3://bucket/sample.parquet"),
    )
    assert input_serialization == {
        "CompressionType": "NONE",
        "Parquet": {},
    }

    with pytest.raises(ProgrammingError) as excinfo:
        get_input_serialization(urlparse("s3://bucket/sample.nc"))
    assert str(excinfo.value) == (
        'Invalid format "nc". Valid values: csv, json, parquet'
    )


@pytest.fixture
def boto3(mocker: MockerFixture) -> MagicMock:
    """
    Mock the boto3 client.
    """
    boto3 = mocker.patch("shillelagh.adapters.api.s3select.boto3")
    boto3.client().select_object_content.return_value = {
        "ResponseMetadata": {
            "RequestId": "VFC4GMDAHSX1EQAN",
            "HostId": (
                "jbrLapM/xnEetUcFCXIQNSN+QgBRG1CT6biXwIrr25kobqBHpjZUiFG5f6RY6Ao2IehDmJV"
                "VKtE="
            ),
            "HTTPStatusCode": 200,
            "HTTPHeaders": {
                "x-amz-id-2": (
                    "jbrLapM/xnEetUcFCXIQNSN+QgBRG1CT6biXwIrr25kobqBHpjZUiFG5f6RY6Ao2Ieh"
                    "DmJVVKtE="
                ),
                "x-amz-request-id": "VFC4GMDAHSX1EQAN",
                "date": "Wed, 13 Jul 2022 20:51:38 GMT",
                "transfer-encoding": "chunked",
                "server": "AmazonS3",
            },
            "RetryAttempts": 1,
        },
        "Payload": [
            {
                "Records": {"Payload": b'{"Name":"Sam",'},
            },
            {
                "Records": {
                    "Payload": (
                        b'"PhoneNumber":"(949) 555-1234","City":"Irvine",'
                        b'"Occupation":"Solutions Architect"}\n'
                    ),
                },
            },
            {
                "Stats": {
                    "Details": {
                        "BytesScanned": 624,
                        "BytesProcessed": 624,
                        "BytesReturned": 99,
                    },
                },
            },
            {"End": {}},
        ],
    }
    return cast(MagicMock, boto3)


def test_s3select(boto3: MagicMock) -> None:
    """
    Test the adapter.
    """
    connection = connect(":memory:")
    cursor = connection.cursor()

    sql = 'SELECT * FROM "s3://bucket/file.csv"'
    data = list(cursor.execute(sql))
    assert data == [("Sam", "(949) 555-1234", "Irvine", "Solutions Architect")]


def test_s3select_with_kwargs_auth(boto3: MagicMock) -> None:
    """
    Test the adapter when credentials are passed explicitly.
    """
    connection = connect(
        ":memory:",
        adapter_kwargs={
            "s3selectapi": {
                "aws_access_key_id": "XXX",
                "aws_secret_access_key": "YYY",
            },
        },
    )
    cursor = connection.cursor()

    sql = 'SELECT * FROM "s3://bucket/file.csv"'
    data = list(cursor.execute(sql))
    assert data == [("Sam", "(949) 555-1234", "Irvine", "Solutions Architect")]
    assert boto3.client.called_with(
        "s3",
        aws_access_key_id="XXX",
        aws_secret_access_key="YYY",
    )


def test_s3_select_environment_auth(boto3: MagicMock) -> None:
    """
    Test the adapter when credentials are found in the environment or config files.
    """
    boto3.session.Session().get_credentials.return_value = "CREDENTIALS"
    connection = connect(":memory:")
    cursor = connection.cursor()

    sql = 'SELECT * FROM "s3://bucket/file.csv"'
    data = list(cursor.execute(sql))
    assert data == [("Sam", "(949) 555-1234", "Irvine", "Solutions Architect")]
    assert boto3.client.called_with("s3")


def test_s3_select_no_auth(boto3: MagicMock) -> None:
    """
    Test the adapter when no credential are passed or found.
    """
    boto3.session.Session().get_credentials.return_value = None
    connection = connect(":memory:")
    cursor = connection.cursor()

    sql = 'SELECT * FROM "s3://bucket/file.csv"'
    data = list(cursor.execute(sql))
    assert data == [("Sam", "(949) 555-1234", "Irvine", "Solutions Architect")]
    assert boto3.client.called_with("s3", config=Config(signature_version=UNSIGNED))


def test_drop_table(boto3: MagicMock) -> None:
    """
    Test that dropping the table deletes the object from S3.
    """
    connection = connect(":memory:")
    cursor = connection.cursor()

    sql = 'DROP TABLE "s3://bucket/file.csv"'
    cursor.execute(sql)
    boto3.client().delete_object.assert_called_with(Bucket="bucket", Key="file.csv")


def test_impossible_condition(boto3: MagicMock) -> None:
    """
    Test for apsw 3.36 where an impossible condition is passed.
    """
    input_serialization: CSVSerializationType = {"CSV": {}, "CompressionType": "NONE"}
    adapter = S3SelectAPI("bucket", "file.csv", input_serialization)
    assert list(adapter.get_data({"City": Impossible()}, [])) == []
