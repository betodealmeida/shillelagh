"""
Tests for the s3select adapter.
"""
# pylint: disable=unused-argument, redefined-outer-name, use-implicit-booleaness-not-comparison

from urllib.parse import urlparse

import pytest
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
def boto3_client(mocker: MockerFixture) -> None:
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


def test_s3select(boto3_client: None) -> None:
    """
    Test the adapter.
    """
    connection = connect(":memory:")
    cursor = connection.cursor()

    sql = 'SELECT * FROM "s3://bucket/file.csv"'
    data = list(cursor.execute(sql))
    assert data == [("Sam", "(949) 555-1234", "Irvine", "Solutions Architect")]


def test_impossible_condition(boto3_client: None) -> None:
    """
    Test for apsw 3.36 where an impossible condition is passed.
    """
    input_serialization: CSVSerializationType = {"CSV": {}, "CompressionType": "NONE"}
    adapter = S3SelectAPI("bucket", "file.csv", input_serialization)
    assert list(adapter.get_data({"City": Impossible()}, [])) == []
