import datetime
from distutils.util import strtobool
from enum import Enum
from typing import Any
from typing import Callable
from typing import cast
from typing import List
from typing import Optional
from typing import Type
from typing import TypeVar

import dateutil.parser
from shillelagh.filters import Filter

T = TypeVar("T")


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

    type = ""
    db_api_type = "DBAPIType"

    def __init__(
        self,
        filters: Optional[List[Type[Filter]]] = None,
        order: Order = Order.NONE,
        exact: bool = False,
    ):
        self.filters = filters or []
        self.order = order
        self.exact = exact

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Field):
            return NotImplemented

        return (
            self.filters == other.filters
            and self.order == other.order
            and self.exact == other.exact
        )

    @staticmethod
    def parse(value: Any) -> Any:
        raise NotImplementedError("Subclasses must implement `parse`")


class Integer(Field):
    type = "INTEGER"
    db_api_type = "NUMBER"

    @staticmethod
    def parse(value: Any) -> Optional[int]:
        if value is None:
            return None

        return int(value)


class Float(Field):
    type = "REAL"
    db_api_type = "NUMBER"

    @staticmethod
    def parse(value: Any) -> Optional[float]:
        if value is None:
            return None

        return float(value)


class String(Field):
    type = "TEXT"
    db_api_type = "STRING"

    @staticmethod
    def parse(value: Any) -> Optional[str]:
        if value is None:
            return None

        return str(value)


class Date(Field):
    type = "DATE"
    db_api_type = "DATETIME"

    @staticmethod
    def parse(value: Any) -> Optional[datetime.date]:
        if value is None:
            return None

        try:
            dt = dateutil.parser.parse(value)
        except dateutil.parser.ParserError:
            return None

        return dt.astimezone(datetime.timezone.utc).date()


class Time(Field):
    type = "TIME"
    db_api_type = "DATETIME"

    @staticmethod
    def parse(value: Any) -> Optional[datetime.time]:
        if value is None:
            return None

        try:
            dt = dateutil.parser.parse(value)
        except dateutil.parser.ParserError:
            return None

        return dt.astimezone(datetime.timezone.utc).timetz()


class DateTime(Field):
    type = "TIMESTAMP"
    db_api_type = "DATETIME"

    @staticmethod
    def parse(value: Any) -> Optional[datetime.datetime]:
        if value is None:
            return None

        try:
            dt = dateutil.parser.parse(value)
        except dateutil.parser.ParserError:
            return None

        return dt.astimezone(datetime.timezone.utc)


class Blob(Field):
    type = "BLOB"
    db_api_type = "BINARY"

    @staticmethod
    def parse(value: T) -> T:
        return value


class Boolean(Field):
    type = "BOOLEAN"
    db_api_type = "NUMBER"

    @staticmethod
    def parse(value: Any) -> Optional[bool]:
        if value is None:
            return None

        if isinstance(value, bool):
            return value
        return bool(strtobool(str(value)))


type_map = {
    field.type: field
    for field in {Integer, Float, String, Date, Time, DateTime, Blob, Boolean}
}
