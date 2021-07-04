import datetime
from distutils.util import strtobool
from typing import Any
from typing import cast
from typing import Generic
from typing import List
from typing import Optional
from typing import Type
from typing import TypeVar
from typing import Union

import dateutil.parser

from shillelagh.filters import Filter
from shillelagh.types import Order


Internal = TypeVar(
    "Internal",
    float,
    int,
    str,
    bool,
    datetime.date,
    datetime.time,
    datetime.datetime,
    bytes,
    # GSheets
    List[int],
    Union[str, List[int]],
)

External = TypeVar(
    "External",
    float,
    int,
    str,
    bool,
    datetime.date,
    datetime.time,
    datetime.datetime,
    bytes,
)


class Field(Generic[Internal, External]):

    """
    Represents a column in a table.
    """

    type = ""
    db_api_type = "DBAPIType"

    def __init__(
        self,
        filters: Optional[List[Type[Filter]]] = None,
        order: Order = Order.NONE,
        exact: bool = False,
    ):
        # a list of what kind of filters can be used on the column
        self.filters = filters or []

        # the ordering of the column
        self.order = order

        # are the results returned for the column exact or do they require
        # additional post-processing?
        self.exact = exact

    def __eq__(self, other: Any) -> bool:
        if other.__class__ != self.__class__:
            return NotImplemented

        return bool(
            self.filters == other.filters
            and self.order == other.order
            and self.exact == other.exact,
        )

    def parse(self, value: Optional[Internal]) -> Optional[External]:
        """
        Convert from a DB type to a native Python type.

        Some databases might represent booleans as integers, or timestamps
        as strings. To convert those values to native Python types we call
        the `parse` method in the field associated with the column.

        The default methods are compliant with SQLite types, with booleans
        represented as numbers, and time related types as strings. Custom
        adapters can defined their own derived fields to handle special
        formats.

        Eg, the Google Sheets API returns dates as strings in its response,
        using the format "Date(2018,0,1)" for "2018-01-01". A custom field
        allows the adapter to simply return the original value, and have it
        being automatically converted to a `datetime.date` object.
        """
        return cast(Optional[External], value)

    def format(self, value: Optional[External]) -> Optional[Internal]:
        """
        Convert from a native Python type to a DB type.

        This should be the opposite of `parse`.
        """
        return cast(Optional[Internal], value)

    def quote(self, value: Optional[Internal]) -> str:
        """
        Quote values.

        This method is used by some adapters to build a SQL expression.
        Eg, Google Sheets represents dates (and other time related types)
        with a prefix "date":

            SELECT A, B WHERE C = date '2018-01-01'

        In orded to handle that, the adapter defines its own time fields
        with custom `quote` methods.
        """
        if value is None:
            return "NULL"
        return str(value)


class Integer(Field[int, int]):
    type = "INTEGER"
    db_api_type = "NUMBER"


class RowID(Integer):
    db_api_type = "ROWID"


class Float(Field[float, float]):
    type = "REAL"
    db_api_type = "NUMBER"


class String(Field[str, str]):
    type = "TEXT"
    db_api_type = "STRING"

    def quote(self, value: Optional[str]) -> str:
        if value is None:
            return "NULL"
        escaped_value = value.replace("'", "''")
        return f"'{escaped_value}'"


class Date(Field[datetime.date, datetime.date]):
    type = "DATE"
    db_api_type = "DATETIME"

    def quote(self, value: Optional[datetime.date]) -> str:
        if value is None:
            return "NULL"
        return f"'{value.isoformat()}'"


class ISODate(Field[str, datetime.date]):
    type = "DATE"
    db_api_type = "DATETIME"

    def parse(self, value: Optional[str]) -> Optional[datetime.date]:
        if value is None:
            return None

        try:
            dt = dateutil.parser.parse(value)
        except dateutil.parser.ParserError:
            return None

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)

        return dt.astimezone(datetime.timezone.utc).date()

    def format(self, value: Optional[datetime.date]) -> Optional[str]:
        if value is None:
            return None
        return value.isoformat()

    def quote(self, value: Optional[str]) -> str:
        if value is None:
            return "NULL"
        return f"'{value}'"


class Time(Field[datetime.time, datetime.time]):
    type = "TIME"
    db_api_type = "DATETIME"

    def quote(self, value: Optional[datetime.time]) -> str:
        if value is None:
            return "NULL"
        return f"'{value.isoformat()}'"


class ISOTime(Field[str, datetime.time]):
    type = "TIME"
    db_api_type = "DATETIME"

    def parse(self, value: Optional[str]) -> Optional[datetime.time]:
        if value is None:
            return None

        try:
            dt = dateutil.parser.parse(value)
        except dateutil.parser.ParserError:
            return None

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)

        return dt.astimezone(datetime.timezone.utc).timetz()

    def format(self, value: Optional[datetime.time]) -> Optional[str]:
        if value is None:
            return None
        return value.isoformat()

    def quote(self, value: Optional[str]) -> str:
        if value is None:
            return "NULL"
        return f"'{value}'"


class DateTime(Field[datetime.datetime, datetime.datetime]):
    type = "TIMESTAMP"
    db_api_type = "DATETIME"

    def quote(self, value: Optional[datetime.datetime]) -> str:
        if value is None:
            return "NULL"
        return f"'{value.isoformat()}'"


class ISODateTime(Field[str, datetime.datetime]):
    type = "TIMESTAMP"
    db_api_type = "DATETIME"

    def parse(self, value: Optional[str]) -> Optional[datetime.datetime]:
        if value is None:
            return None

        try:
            dt = dateutil.parser.parse(value)
        except dateutil.parser.ParserError:
            return None

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)

        return dt.astimezone(datetime.timezone.utc)

    def format(self, value: Optional[datetime.datetime]) -> Optional[str]:
        if value is None:
            return None
        return value.isoformat()

    def quote(self, value: Optional[str]) -> str:
        if value is None:
            return "NULL"
        return f"'{value}'"


class Blob(Field[bytes, bytes]):
    type = "BLOB"
    db_api_type = "BINARY"

    def quote(self, value: Optional[bytes]) -> str:
        if value is None:
            return "NULL"
        return f"X'{value.hex()}'"


class StringBlob(Field[str, bytes]):
    type = "BLOB"
    db_api_type = "BINARY"

    def parse(self, value: Optional[str]) -> Optional[bytes]:
        if value is None:
            return None
        return bytes.fromhex(value)

    def format(self, value: Optional[bytes]) -> Optional[str]:
        if value is None:
            return None
        return cast(str, value.hex())

    def quote(self, value: Optional[str]) -> str:
        if value is None:
            return "NULL"
        return f"X'{value}'"


class Boolean(Field[bool, bool]):
    type = "BOOLEAN"
    db_api_type = "NUMBER"

    def quote(self, value: Optional[bool]) -> str:
        if value is None:
            return "NULL"
        return "TRUE" if value else "FALSE"


class StringBoolean(Field[str, bool]):
    type = "BOOLEAN"
    db_api_type = "NUMBER"

    def parse(self, value: Optional[str]) -> Optional[bool]:
        if value is None:
            return None
        return bool(strtobool(str(value)))

    def format(self, value: Optional[bool]) -> Optional[str]:
        if value is None:
            return None
        return "TRUE" if value else "FALSE"

    def quote(self, value: Optional[str]) -> str:
        if value is None:
            return "NULL"
        return value


class IntBoolean(Field[int, bool]):
    type = "BOOLEAN"
    db_api_type = "NUMBER"

    def parse(self, value: Optional[int]) -> Optional[bool]:
        if value is None:
            return None
        return bool(value)

    def format(self, value: Optional[bool]) -> Optional[int]:
        if value is None:
            return None
        return 1 if value else 0

    def quote(self, value: Optional[int]) -> str:
        if value is None:
            return "NULL"
        return str(value)
