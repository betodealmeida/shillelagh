"""
An adapter to S3 files via S3Select.
"""

import json
import logging
import urllib.parse
from pathlib import Path
from typing import Any, Dict, Iterator, List, Literal, Optional, Tuple, Union, cast

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from typing_extensions import TypedDict

from shillelagh.adapters.base import Adapter
from shillelagh.exceptions import ImpossibleFilterError, ProgrammingError
from shillelagh.fields import Field, Order
from shillelagh.filters import Equal, Filter, IsNotNull, IsNull, NotEqual, Range
from shillelagh.lib import SimpleCostModel, analyze, build_sql, flatten
from shillelagh.typing import RequestedOrder, Row

_logger = logging.getLogger(__name__)

# this is just a wild guess; used to estimate query cost
AVERAGE_NUMBER_OF_ROWS = 1000


class CSVSerializationOptionsType(TypedDict, total=False):
    """
    Options for CSV input.
    """

    AllowQuotedRecordDelimiter: bool
    Comments: str
    FieldDelimiter: str
    FileHeaderInfo: Literal["USE", "IGNORE", "NONE"]
    QuoteCharacter: str
    QuoteEscapedCharacter: str
    RecordDelimiter: str


class CSVSerializationType(TypedDict):
    """
    CSV input serialization.
    """

    CompressionType: str
    CSV: CSVSerializationOptionsType


class JSONSerializationOptionsType(TypedDict, total=False):
    """
    Options for JSON input.
    """

    Type: Literal["DOCUMENT", "LINES"]


class JSONSerializationType(TypedDict):
    """
    CSV input serialization.
    """

    CompressionType: str
    JSON: JSONSerializationOptionsType


class ParquetSerializationOptionsType(TypedDict):
    """
    Options for Parquet input.
    """


class ParquetSerializationType(TypedDict):
    """
    CSV input serialization.
    """

    CompressionType: str
    Parquet: ParquetSerializationOptionsType


InputSerializationType = Union[
    CSVSerializationType,
    JSONSerializationType,
    ParquetSerializationType,
]


def unescape_backslash(value: str) -> str:
    r"""
    Unescape backslashes, converting ``\\n`` into ``\n``.

    For fields like ``RecordDelimiter`` where the user passes a value of "\r\n", the
    parameter should be convert to actual carriage return and new line so that S3
    understands it (ie, ``\r\n`` and not ``\\r\\n``).

    Apparently there's no easy way to do that in Python.
    """
    return value.encode("raw_unicode_escape").decode("unicode_escape")


def get_input_serialization(parsed: urllib.parse.ParseResult) -> InputSerializationType:
    """
    Build the input serialization object.
    """
    options: Dict[str, List[str]] = urllib.parse.parse_qs(parsed.query)

    # the serialization format can be passed explicitly or inferred from the file
    # extension
    if "format" in options:
        format_ = options["format"][-1].lower()
    else:
        key = parsed.path.lstrip("/")
        suffix = Path(key).suffix
        if not suffix:
            raise ProgrammingError(
                "Unable to determine file format. You must declare the format in "
                "the SQLAlchemy URI using ``?format={csv,json,parquet}``.",
            )
        format_ = suffix[1:].lower()

    input_serialization: Dict[str, Any] = {
        "CompressionType": options.get("CompressionType", ["NONE"])[-1],
    }

    if format_ == "csv":
        input_serialization["CSV"] = {
            k: unescape_backslash(v[-1])
            for k, v in options.items()
            if k
            in CSVSerializationOptionsType.__annotations__  # pylint: disable=no-member
        }
        if "FileHeaderInfo" not in input_serialization["CSV"]:
            input_serialization["CSV"]["FileHeaderInfo"] = "USE"

        return cast(CSVSerializationType, input_serialization)

    if format_ == "json":
        input_serialization["JSON"] = {
            k: v[-1]
            for k, v in options.items()
            if k
            in JSONSerializationOptionsType.__annotations__  # pylint: disable=no-member
        }

        return cast(JSONSerializationType, input_serialization)

    if format_ == "parquet":
        input_serialization["Parquet"] = {
            k: v[-1]
            for k, v in options.items()
            if k
            in ParquetSerializationOptionsType.__annotations__  # pylint: disable=no-member
        }

        return cast(ParquetSerializationType, input_serialization)

    raise ProgrammingError(
        f'Invalid format "{format_}". Valid values: csv, json, parquet',
    )


class S3SelectAPI(Adapter):

    """
    An adapter to S3 files via S3Select.

    Used to read data from an S3 bucket::

        s3://bucket-name/path/to/file.extension

    Supported formats for the file are CSV, JSON, and Parquet. They can be either
    inferred from the file extension or declared explicitly::

        s3://bucket-name/sample_data.csv
        s3://bucket-name/sample_data?format=csv

    Different formats have additional configuration options::

        <InputSerialization>
            <CompressionType>string</CompressionType>
            <CSV>
                <AllowQuotedRecordDelimiter>boolean</AllowQuotedRecordDelimiter>
                <Comments>string</Comments>
                <FieldDelimiter>string</FieldDelimiter>
                <FileHeaderInfo>string</FileHeaderInfo>
                <QuoteCharacter>string</QuoteCharacter>
                <QuoteEscapeCharacter>string</QuoteEscapeCharacter>
                <RecordDelimiter>string</RecordDelimiter>
            </CSV>
            <JSON>
                <Type>string</Type>
            </JSON>
            <Parquet>
            </Parquet>
        </InputSerialization>

    For example::

        s3://bucket-name/sample.csv?FileHeaderInfo=Use&CompressionType=NONE

    See https://docs.aws.amazon.com/AmazonS3/latest/API/API_SelectObjectContent.html for
    more info.

    """

    safe = True

    supports_limit = True
    supports_offset = False

    @staticmethod
    def supports(uri: str, fast: bool = True, **kwargs: Any) -> Optional[bool]:
        parsed = urllib.parse.urlparse(uri)
        return parsed.scheme == "s3"

    @staticmethod
    def parse_uri(uri: str) -> Tuple[str, str, InputSerializationType, str]:
        parsed = urllib.parse.urlparse(uri)
        path = urllib.parse.unquote(parsed.fragment) or "$"

        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        input_serialization = get_input_serialization(parsed)

        return (
            bucket,
            key,
            input_serialization,
            path,
        )

    def __init__(  # pylint: disable=too-many-arguments
        self,
        bucket: str,
        key: str,
        input_serialization: InputSerializationType,
        path: str = "$",
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        s3_endpoint_url: Optional[str] = None,
        s3_kwargs: Optional[Dict[str, Any]] = None,
    ):
        super().__init__()

        self.bucket = bucket
        self.key = key
        self.input_serialization = input_serialization
        self.table_name = path.replace("$", "S3Object")

        # if credentials were passed explicitly, use them
        if aws_access_key_id and aws_secret_access_key:
            self.s3_client = boto3.client(
                "s3",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                endpoint_url=s3_endpoint_url,
            )
        # if no credentials were passed, check if they're available
        elif boto3.session.Session().get_credentials():
            self.s3_client = boto3.client(
                "s3",
                endpoint_url=s3_endpoint_url,
            )
        # if no credentials were found, use an anonymous client to access public buckets
        else:
            self.s3_client = boto3.client(
                "s3",
                config=Config(signature_version=UNSIGNED),
                endpoint_url=s3_endpoint_url,
            )
        self.s3_kwargs = s3_kwargs or {}

        self._set_columns()

    def _set_columns(self) -> None:
        rows = list(self._run_query(f"SELECT * FROM {self.table_name} LIMIT 1"))
        column_names = list(rows[0].keys()) if rows else []
        types = analyze(iter(rows))[2]

        self.columns = {
            column_name: types[column_name](
                filters=[Range, Equal, NotEqual, IsNull, IsNotNull],
                order=Order.NONE,
                exact=True,
            )
            for column_name in column_names
        }

    def get_columns(self) -> Dict[str, Field]:
        return self.columns

    get_cost = SimpleCostModel(AVERAGE_NUMBER_OF_ROWS)

    def _run_query(self, sql: str) -> Iterator[Dict[str, Any]]:
        """
        Run a query and return rows.
        """
        response = self.s3_client.select_object_content(
            Bucket=self.bucket,
            Key=self.key,
            ExpressionType="SQL",
            Expression=sql,
            InputSerialization=self.input_serialization,
            OutputSerialization={"JSON": {}},
            **self.s3_kwargs,
        )

        # a JSON document might be split across different events
        leftover = ""
        for event in response["Payload"]:
            if "Records" in event:
                records = (
                    leftover + event["Records"]["Payload"].decode("utf-8")
                ).splitlines()
                leftover = ""
                for i, record in enumerate(records):
                    try:
                        row = json.loads(record)
                    except json.decoder.JSONDecodeError:
                        leftover = "\n".join(records[i:])
                        break

                    yield cast(Dict[str, Any], row)

    def get_data(
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
        limit: Optional[int] = None,
        **kwargs: Any,
    ) -> Iterator[Row]:
        try:
            sql = build_sql(
                self.columns,
                bounds,
                order,
                table=self.table_name,
                limit=limit,
                alias="s",
            )
        except ImpossibleFilterError:
            return

        rows = self._run_query(sql)
        for i, row in enumerate(rows):
            row["rowid"] = i
            _logger.debug(row)
            yield flatten(row)

    def drop_table(self) -> None:
        self.s3_client.delete_object(Bucket=self.bucket, Key=self.key, **self.s3_kwargs)
