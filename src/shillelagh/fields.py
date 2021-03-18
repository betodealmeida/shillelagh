import datetime
import inspect
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
from shillelagh.types import BINARY
from shillelagh.types import DATETIME
from shillelagh.types import DBAPIType
from shillelagh.types import NUMBER
from shillelagh.types import ROWID
from shillelagh.types import STRING

T = TypeVar("T")


class Order(Enum):
    ASCENDING = "ascending"
    DESCENDING = "descending"
    NONE = "none"
    ANY = "any"


class Field:

    type = ""
    db_api_type = DBAPIType

    def __init__(
        self,
        filters: Optional[List[Type[Filter]]] = None,
        order: Order = Order.NONE,
        exact: bool = False,
    ):
        self.filters = filters or []
        self.order = order
        self.exact = exact

    def __eq__(self, other: object) -> bool:
        # for DB API type_code comparison
        if inspect.isclass(other) and issubclass(other, DBAPIType):
            return self.db_api_type == other

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
    db_api_type = NUMBER

    @staticmethod
    def parse(value: Any) -> Optional[int]:
        if value is None:
            return None

        return int(value)


class Float(Field):
    type = "REAL"
    db_api_type = NUMBER

    @staticmethod
    def parse(value: Any) -> Optional[float]:
        if value is None:
            return None

        return float(value)


class String(Field):
    type = "TEXT"
    db_api_type = STRING

    @staticmethod
    def parse(value: Any) -> Optional[str]:
        if value is None:
            return None

        return str(value)


class Date(Field):
    type = "DATE"
    db_api_type = DATETIME

    @staticmethod
    def parse(value: Any) -> Optional[datetime.date]:
        if value is None:
            return None

        try:
            dt = dateutil.parser.parse(value)
        except dateutil.parser._parser.ParserError:
            return None

        return dt.astimezone(datetime.timezone.utc).date()


class Time(Field):
    type = "TIME"
    db_api_type = DATETIME

    @staticmethod
    def parse(value: Any) -> Optional[datetime.time]:
        if value is None:
            return None

        try:
            dt = dateutil.parser.parse(value)
        except dateutil.parser._parser.ParserError:
            return None

        return dt.astimezone(datetime.timezone.utc).timetz()


class DateTime(Field):
    type = "TIMESTAMP"
    db_api_type = DATETIME

    @staticmethod
    def parse(value: Any) -> Optional[datetime.datetime]:
        if value is None:
            return None

        try:
            dt = dateutil.parser.parse(value)
        except dateutil.parser._parser.ParserError:
            return None

        return dt.astimezone(datetime.timezone.utc)


class Blob(Field):
    type = "BLOB"
    db_api_type = BINARY

    @staticmethod
    def parse(value: T) -> T:
        return value


class Boolean(Field):
    type = "BOOLEAN"
    db_api_type = NUMBER

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
