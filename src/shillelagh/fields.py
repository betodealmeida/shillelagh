"""
Fields representing columns of different types and capabilities.
"""
import datetime
from distutils.util import strtobool
from enum import Enum
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


class Order(Enum):
    """An enum for different orders a field can have."""

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


class Field(Generic[Internal, External]):

    """
    Represents a column in a table.

    A field is probably the most important concept in Shillelagh. Fields are
    used to annotate types, indicate which columns are filterable/sortable,
    and convert data between different format.

    1. Type annotation

    The most basic use of field is to indicate the types of columns in a
    given resource. For example, an adapter that connects to a database with
    two columns, a string and an integer, could look like this:

        class SimpleAdapter(Adapter):

            string_col = String()
            integer_col = Integer()

    For dynamic resources the columns might be generated dynamically, but
    the idea is the same:

        class DynamicAdapter(Adapter):

            type_map = {
                "int": Integer(),
                "float": Float(),
                "string": String(),
            }

            def get_columns(self) -> Dict[str, Field]:
                columns = read_columns()
                return {
                    column.name: type_map[column.type]
                    for column in columns
                }

    2. Filterable/sortable columns

    Most adapters can perform some kind of filtering/sorting on the data,
    return less data to the backend in order to optimize queries. Adapters
    indicate this on the fields, eg:

        class FilteringAdapter(Adapter):

            timestamp_col = DateTime(
                filters=[Equal, Range],
                order=Order.ANY,
                exact=True,
            )
            values_col = Float()

    The adapter above declares a column called `timestamp_col` that can
    be filtered using either an equality (`== '2020-01-01T00:00:00'`) or a
    range (`>= '2020-01-01T00:00:00'`). Because of this declaration the
    backend will delegate the filtering to the adapter, which will be
    responsible for translating the filters into API/filesystem calls to
    fulfill them.

    Additionally, the timestamp column also declares an order of `ANY`, which
    means that the adapter can sort the data in any order requested by the
    backend. Fields can declare a static order (eg, `Order.ASCENDING`) or
    no order at all.

    Finally, the field also indicates that the filtering is exact, and no
    post-filtering is needed to be done by the backend. In some cases it's
    useful to have adapters perform an initial coarse filtering (say, at
    the daily level), and have the backend perform the final fine filtering
    (say, at the second level), to simplify the logic in the adapter.

    3. Data conversion

    Fields are also responsible for converting data between different
    formats, so it can flow through layers. For example, an adapter might
    return booleans as strings, and these need to be converted back and
    forth to Python booleans. The `StringBoolean` field should be used in
    that case, and it will automatically convert the data to the format
    the adapter understands.

    Similarly, the APSW backend only accepts types understood by SQLite:
    ints, floats, strings, and bytes. This means that the backend needs to
    convert between, eg, native Python booleans and integers. This is also
    done by using the `parse` and `format` methods from fields (`IntBoolean`
    in this case).

    When creating new fields, the base class should declare the type of
    the "internal" representation (used by the adapter) and the "external"
    representation (native Python types). For example, if we have an
    adapter that stores numbers as strings we could define a new type:

        class StringNumber(Field[str, float]):  # Field[internal, external]
            type = "REAL"
            db_api_type = "NUMBER"

            # internal -> external
            def parse(self, value: Optional[str]) -> Optional[float]:
                return value if value is None else float(value)

            # external -> internal
            def format(self, value: Optional[float]) -> Optional[str]:
                return value if value is None else str(value)

    Then the adapter can declare columns using that field:

        class SomeAdapter(Adapter):

            number_col = StringNumber()

    With this, it can simply return rows with the number as a string,
    without having to explicitly do the conversion:

        {"rowid": 0, "number_col": "1.0"}

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

    def parse(  # pylint: disable=no-self-use
        self,
        value: Optional[Internal],
    ) -> Optional[External]:
        """
        Convert from a DB type to a native Python type.

        Some databases might represent booleans as integers, or timestamps
        as strings. To convert those values to native Python types we call
        the `parse` method in the field associated with the column. Custom
        adapters can define their own derived fields to handle special
        formats.

        Eg, the Google Sheets API returns dates as strings in its response,
        using the format "Date(2018,0,1)" for "2018-01-01". A custom field
        allows the adapter to simply return the original value, and have it
        being automatically converted to a `datetime.date` object.

        This is not a staticmethod because some types need extra information
        in order to parse a value. Eg, GSheets takes into consideration the
        timezone of the sheet when parsing timestamps.
        """
        return cast(Optional[External], value)

    def format(  # pylint: disable=no-self-use
        self,
        value: Optional[External],
    ) -> Optional[Internal]:
        """
        Convert from a native Python type to a DB type.

        This should be the opposite of `parse`.
        """
        return cast(Optional[Internal], value)

    def quote(self, value: Optional[Internal]) -> str:  # pylint: disable=no-self-use
        """
        Quote values.

        This method is used by some adapters to build a SQL expression.
        Eg, GSheets represents dates (and other time related types) with
        the prefix "date":

            SELECT A, B WHERE C = date '2018-01-01'

        In orded to handle that, the adapter defines its own time fields
        with custom `quote` methods.
        """
        if value is None:
            return "NULL"
        return str(value)


class Integer(Field[int, int]):
    """An integer."""

    type = "INTEGER"
    db_api_type = "NUMBER"


class RowID(Integer):
    """
    Custom field for the row ID.

    All Shillelagh adapters return a special column for the row ID. In
    many cases it's just an increasing integer, but it's used for DML
    in adapters that support it.
    """

    db_api_type = "ROWID"


class Float(Field[float, float]):
    """A float."""

    type = "REAL"
    db_api_type = "NUMBER"


class String(Field[str, str]):
    """A string."""

    type = "TEXT"
    db_api_type = "STRING"

    def quote(self, value: Optional[str]) -> str:
        if value is None:
            return "NULL"
        escaped_value = value.replace("'", "''")
        return f"'{escaped_value}'"


class Date(Field[datetime.date, datetime.date]):
    """
    A date.

    This field is used in adapters that use `datetime.date` as the
    internal representation of dates.
    """

    type = "DATE"
    db_api_type = "DATETIME"

    def quote(self, value: Optional[datetime.date]) -> str:
        if value is None:
            return "NULL"
        return f"'{value.isoformat()}'"


class ISODate(Field[str, datetime.date]):
    """
    A date.

    This field is used in adapters that use an ISO string as the
    internal representation of dates. SQLite, for example, has no
    concept of `datetime.date` objects, so we need to convert between
    the object and an ISO string when the data flows through SQLite.
    """

    type = "DATE"
    db_api_type = "DATETIME"

    def parse(self, value: Optional[str]) -> Optional[datetime.date]:
        if value is None:
            return None

        try:
            date = dateutil.parser.parse(value)
        except dateutil.parser.ParserError:
            return None

        return date.date()

    def format(self, value: Optional[datetime.date]) -> Optional[str]:
        if value is None:
            return None
        return value.isoformat()

    def quote(self, value: Optional[str]) -> str:
        if value is None:
            return "NULL"
        return f"'{value}'"


class Time(Field[datetime.time, datetime.time]):
    """
    A time of the day.

    This field is used in adapters that use `datetime.time` as the
    internal representation of times of the day.
    """

    type = "TIME"
    db_api_type = "DATETIME"

    def quote(self, value: Optional[datetime.time]) -> str:
        if value is None:
            return "NULL"
        return f"'{value.isoformat()}'"


class ISOTime(Field[str, datetime.time]):
    """
    A time of the day.

    This field is used in adapters that use an ISO string as the
    internal representation of dates. SQLite, for example, has no
    concept of `datetime.time` objects, so we need to convert between
    the object and an ISO string when the data flows through SQLite.
    """

    type = "TIME"
    db_api_type = "DATETIME"

    def parse(self, value: Optional[str]) -> Optional[datetime.time]:
        if value is None:
            return None

        try:
            timestamp = dateutil.parser.parse(value)
        except dateutil.parser.ParserError:
            return None

        time = timestamp.time()

        # timezone is not preserved
        return time.replace(tzinfo=timestamp.tzinfo)

    def format(self, value: Optional[datetime.time]) -> Optional[str]:
        if value is None:
            return None
        return value.isoformat()

    def quote(self, value: Optional[str]) -> str:
        if value is None:
            return "NULL"
        return f"'{value}'"


class DateTime(Field[datetime.datetime, datetime.datetime]):
    """
    A timestamp.

    This field is used in adapters that use `datetime.datetime`
    as the internal representation of timestamps.
    """

    type = "TIMESTAMP"
    db_api_type = "DATETIME"

    def quote(self, value: Optional[datetime.datetime]) -> str:
        if value is None:
            return "NULL"
        return f"'{value.isoformat()}'"


class ISODateTime(Field[str, datetime.datetime]):
    """
    A timestamp.

    This field is used in adapters that use an ISO string as the
    internal representation of dates. SQLite, for example, has no
    concept of `datetime.datetime` objects, so we need to convert
    between the object and an ISO string when the data flows
    through SQLite.
    """

    type = "TIMESTAMP"
    db_api_type = "DATETIME"

    def parse(self, value: Optional[str]) -> Optional[datetime.datetime]:
        if value is None:
            return None

        try:
            timestamp = dateutil.parser.parse(value)
        except dateutil.parser.ParserError:
            return None

        # if the timestamp has a timezone change it to UTC, so that
        # timestamps in different timezones can be compared as strings
        if timestamp.tzinfo is not None:
            timestamp = timestamp.astimezone(datetime.timezone.utc)

        return timestamp

    def format(self, value: Optional[datetime.datetime]) -> Optional[str]:
        if value is None:
            return None

        # if the timestamp has a timezone change it to UTC, so that
        # timestamps in different timezones can be compared as strings
        if value.tzinfo is not None:
            value = value.astimezone(datetime.timezone.utc)

        return value.isoformat()

    def quote(self, value: Optional[str]) -> str:
        if value is None:
            return "NULL"
        return f"'{value}'"


class Blob(Field[bytes, bytes]):
    """
    A blob of bytes.

    This is used to represent binary data.
    """

    type = "BLOB"
    db_api_type = "BINARY"

    def quote(self, value: Optional[bytes]) -> str:
        if value is None:
            return "NULL"
        return f"X'{value.hex()}'"


class StringBlob(Field[str, bytes]):
    """
    A blob of bytes.

    This field is used in adapters that represent binary data as a
    string with the hexadecimal representation of the bytes.
    """

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
    """A boolean."""

    type = "BOOLEAN"
    db_api_type = "NUMBER"

    def quote(self, value: Optional[bool]) -> str:
        if value is None:
            return "NULL"
        return "TRUE" if value else "FALSE"


class StringBoolean(Field[str, bool]):
    """
    A boolean.

    This field is used in adapters that represent booleans as strings,
    eg, "TRUE" and "FALSE".
    """

    type = "BOOLEAN"
    db_api_type = "NUMBER"

    def parse(self, value: Optional[str]) -> Optional[bool]:
        if value is None:
            return None
        return bool(strtobool(value))

    def format(self, value: Optional[bool]) -> Optional[str]:
        if value is None:
            return None
        return "TRUE" if value else "FALSE"

    def quote(self, value: Optional[str]) -> str:
        if value is None:
            return "NULL"
        return value


class IntBoolean(Field[int, bool]):
    """
    A boolean.

    This field is used in adapters that represent booleans as an
    integer. SQLite, eg, has no boolean type, using 1 and 0 to
    represent true and false, respectively.
    """

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
