import datetime
from distutils.util import strtobool
from enum import Enum
from typing import Any
from typing import Callable
from typing import cast
from typing import List
from typing import Optional
from typing import Type

import dateutil.parser
from shillelagh.filters import Filter


class Order(Enum):
    # Use ASCENDING/DESCENDING when you have static data with 1+
    # columns pre-sorted. All other columns should have Order.NONE.
    ASCENDING = "ascending"
    DESCENDING = "descending"

    # Use NONE when you can't or don't want to sort the data. Sqlite
    # will then sort the provided data according to the query.
    NONE = "none"

    # Use ANY when the column can be sorted in any order. Usually
    # all other columns will also have Order.ANY.
    ANY = "any"


class Field:

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
        if not isinstance(other, Field):
            return NotImplemented

        return (
            self.filters == other.filters
            and self.order == other.order
            and self.exact == other.exact
        )

    def parse(self, value: Any) -> Any:
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
        raise NotImplementedError("Subclasses must implement `parse`")

    def format(self, value: Any) -> Any:
        """
        Convert from a native Python type to a DB type.

        This should be the opposite of `parse`.
        """
        raise NotImplementedError("Subclasses must implement `format`")

    def quote(self, value: Any) -> str:
        """
        Quote values.

        This method is used by some adapters to build a SQL expression.
        Eg, Google Sheets represents dates (and other time related types)
        with a prefix "date":

            SELECT A, B WHERE C = date '2018-01-01'

        In orded to handle that, the adapter defines its own time fields
        with custom `quote` methods.
        """
        raise NotImplementedError("Subclasses must implement `quote`")


class Integer(Field):
    type = "INTEGER"
    db_api_type = "NUMBER"

    def parse(self, value: Any) -> Optional[int]:
        if value is None:
            return None

        return int(value)

    format = parse

    def quote(self, value: Any) -> str:
        return str(value)


class Float(Field):
    type = "REAL"
    db_api_type = "NUMBER"

    def parse(self, value: Any) -> Optional[float]:
        if value is None:
            return None

        return float(value)

    format = parse

    def quote(self, value: Any) -> str:
        return str(value)


class String(Field):
    type = "TEXT"
    db_api_type = "STRING"

    def parse(self, value: Any) -> Optional[str]:
        if value is None:
            return None

        return str(value)

    format = parse

    def quote(self, value: Any) -> str:
        escaped_value = value.replace("'", "''")
        return f"'{escaped_value}'"


class Date(Field):
    type = "DATE"
    db_api_type = "DATETIME"

    def parse(self, value: Any) -> Optional[datetime.date]:
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

    def quote(self, value: Any) -> str:
        return f"'{self.format(value)}'"


class Time(Field):
    type = "TIME"
    db_api_type = "DATETIME"

    def parse(self, value: Any) -> Optional[datetime.time]:
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

    def quote(self, value: Any) -> str:
        return f"'{self.format(value)}'"


class DateTime(Field):
    type = "TIMESTAMP"
    db_api_type = "DATETIME"

    def parse(self, value: Any) -> Optional[datetime.datetime]:
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

    def quote(self, value: Any) -> str:
        return f"'{self.format(value)}'"


class Blob(Field):
    type = "BLOB"
    db_api_type = "BINARY"

    def parse(self, value: Any) -> Any:
        if value is None:
            return None

        try:
            return bytes.fromhex(value)
        except Exception:
            return value

    def format(self, value: Any) -> Optional[str]:
        if value is None:
            return None

        if not isinstance(value, bytes):
            value = str(value).encode("utf-8")

        return cast(str, value.hex())

    def quote(self, value: bytes) -> str:
        return f"X'{self.format(value)}'"


class Boolean(Field):
    type = "BOOLEAN"
    db_api_type = "NUMBER"

    def parse(self, value: Any) -> Optional[bool]:
        if value is None:
            return None

        if isinstance(value, bool):
            return value
        return bool(strtobool(str(value)))

    def format(self, value: Optional[bool]) -> Optional[str]:
        if value is None:
            return None

        return "TRUE" if value else "FALSE"

    def quote(self, value: Any) -> str:
        return cast(str, self.format(value))


type_map = {
    field.type: field
    for field in {Integer, Float, String, Date, Time, DateTime, Blob, Boolean}
}
