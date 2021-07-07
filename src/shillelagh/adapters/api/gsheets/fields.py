# pylint: disable=no-self-use
"""
Custom fields for the GSheets adapter.
"""
import datetime
from typing import Any
from typing import List
from typing import Optional
from typing import Type
from typing import Union

from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Boolean
from shillelagh.fields import Field
from shillelagh.fields import Float
from shillelagh.fields import Order
from shillelagh.fields import String
from shillelagh.filters import Filter


# Timestamp fprmat used to insert data into cells. Note that the timezone is not
# present, since GSheets assumes all timestamps are in the same timezone as the
# sheet.
DATETIME_CELL_FORMAT = "%m/%d/%Y %H:%M:%S"
DATE_CELL_FORMAT = "%m/%d/%Y"
TO_TIME_CELL_FORMAT = "%-I:%M:%S %p"
FROM_TIME_CELL_FORMAT = "%I:%M:%S %p"

# timestamp format used in SQL queries
DATETIME_SQL_QUOTE = "%Y-%m-%d %H:%M:%S"
TIME_SQL_QUOTE = "%H:%M:%S"

# GSheets uses this as the epoch for unformatted values
# https://developers.google.com/sheets/api/guides/formats?hl=en
LOTUS123_EPOCH = datetime.datetime(1899, 12, 30, tzinfo=datetime.timezone.utc)
SECONDS_1_DAY = datetime.timedelta(days=1).total_seconds()


class GSheetsDateTime(Field[str, datetime.datetime]):
    """
    A GSheets timestamp.

    The Google Chart API returns timestamps as a string in the following format:

        Date(2020,0,1,0,0,0) => '2020-01-01 00:00:00'

    (Note that the month is zero-indexed, probably to be compatible with
    Javascript.)

    There are no timezones; instead, there is a global timezone for the whole
    spreadsheet. The timezone can only be read if the user has set their
    credentials, since the Google Sheets API used to read metadata about the
    sheet requires authentication.

    When the timezone is present and read succesfully all timestamps are
    converted to it, both when fetching data as well as when inserting rows.

    When inserting data timestamps should be formatted differently, and also
    without a timezone:

        '2020-01-01 00:00:00'

    This field takes care of the conversion between `datetime.datetime` and the
    two formats.
    """

    type = "TIMESTAMP"
    db_api_type = "DATETIME"

    def __init__(
        self,
        filters: Optional[List[Type[Filter]]] = None,
        order: Order = Order.NONE,
        exact: bool = False,
        timezone: Optional[datetime.tzinfo] = None,
    ):
        super().__init__(filters, order, exact)
        self.timezone = timezone

    def __eq__(self, other: Any) -> bool:
        if other.__class__ != self.__class__:
            return NotImplemented

        return bool(
            self.filters == other.filters
            and self.order == other.order
            and self.exact == other.exact
            and self.timezone == other.timezone,
        )

    def parse(self, value: Optional[str]) -> Optional[datetime.datetime]:
        if value is None:
            return None

        args = [int(number) for number in value[len("Date(") : -1].split(",")]
        args[1] += 1  # month is zero indexed in the response
        return datetime.datetime(*args, tzinfo=self.timezone)  # type: ignore

    def format(self, value: Optional[datetime.datetime]) -> Optional[str]:
        if value is None:
            return None

        # Google Sheets does not support timezones in datetime values, so we
        # convert all timestamps to the sheet timezone
        if self.timezone:
            value = value.astimezone(self.timezone)

        return value.strftime(DATETIME_CELL_FORMAT) if value else None

    def quote(self, value: Optional[str]) -> str:
        if value is None:
            return "NULL"

        # On SQL queries the timestamp should be prefix by "datetime"
        value = datetime.datetime.strptime(value, DATETIME_CELL_FORMAT).strftime(
            DATETIME_SQL_QUOTE,
        )
        return f"datetime '{value}'"

    def to_unformatted(self, value: Optional[datetime.datetime]) -> Union[float, str]:
        """
        Return the number of days since 1899-12-30.
        """
        if value is None:
            return ""

        return (
            value.astimezone(self.timezone) - LOTUS123_EPOCH
        ).total_seconds() / SECONDS_1_DAY

    def from_unformatted(self, value: Union[float, str]) -> Optional[datetime.datetime]:
        """
        Convert to a native Python type.
        """
        if value == "":
            return None

        if isinstance(value, str):
            raise ProgrammingError("Invalid value found")

        return (LOTUS123_EPOCH + datetime.timedelta(days=value)).astimezone(
            self.timezone,
        )


class GSheetsDate(Field[str, datetime.date]):
    """
    A GSheets date.

    The Google Chart API returns timestamps as a string in the following format:

        Date(2020,0,1) => '2020-01-01'

    (Note that the month is zero-indexed, probably to be compatible with
    Javascript.)

    When inserting data timestamps should be formatted differently:

        '2020-01-01'

    This field takes care of the conversion between `datetime.date` and the
    two formats.
    """

    type = "DATE"
    db_api_type = "DATETIME"

    def parse(self, value: Optional[str]) -> Optional[datetime.date]:
        """Parse a string like 'Date(2018,0,1)'."""
        if value is None:
            return None

        args = [int(number) for number in value[len("Date(") : -1].split(",")]
        args[1] += 1  # month is zero indexed in the response (WTF, Google!?)
        return datetime.date(*args)

    def format(self, value: Optional[datetime.date]) -> Optional[str]:
        if value is None:
            return None
        return value.isoformat()

    def quote(self, value: Optional[str]) -> str:
        if value is None:
            return "NULL"

        # On SQL queries the timestamp should be prefix by "date"
        return f"date '{value}'"

    def to_unformatted(self, value: Optional[datetime.date]) -> Union[float, str]:
        """
        Return the number of days since 1899-12-30.
        """
        if value is None:
            return ""

        return (value - LOTUS123_EPOCH.date()).days

    def from_unformatted(self, value: Union[float, str]) -> Optional[datetime.date]:
        """
        Convert to a native Python type.
        """
        if value == "":
            return None

        if isinstance(value, str):
            raise ProgrammingError("Invalid value found")

        return (LOTUS123_EPOCH + datetime.timedelta(days=value)).date()


class GSheetsTime(Field[List[int], datetime.time]):
    """
    A GSheets time.

    The Google Chart API return time objects as a list of numbers, corresponding
    to hour, minute, and seconds.
    """

    type = "TIME"
    db_api_type = "DATETIME"

    def parse(self, value: Optional[List[int]]) -> Optional[datetime.time]:
        """
        Parse time of day as returned from the Google Chart API.
        """
        if value is None:
            return None

        return datetime.time(*value)  # type: ignore

    def format(self, value: Optional[datetime.time]) -> Optional[str]:  # type: ignore
        if value is None:
            return value
        return value.strftime(TO_TIME_CELL_FORMAT)

    def quote(self, value: Optional[str]) -> str:  # type: ignore
        if value is None:
            return "NULL"

        # On SQL queries the timestamp should be prefix by "timeofday"
        value = datetime.datetime.strptime(value, FROM_TIME_CELL_FORMAT).strftime(
            TIME_SQL_QUOTE,
        )
        return f"timeofday '{value}'"

    def to_unformatted(self, value: Optional[datetime.time]) -> Union[float, str]:
        """
        Return the fraction of the day.
        """
        if value is None:
            return ""

        return (
            value.hour * 60 * 60
            + value.minute * 60
            + value.second
            + value.microsecond / 1000
        ) / SECONDS_1_DAY

    def from_unformatted(self, value: Union[float, str]) -> Optional[datetime.time]:
        """
        Convert to a native Python type.
        """
        if value == "":
            return None

        if isinstance(value, str):
            raise ProgrammingError("Invalid value found")

        return (
            LOTUS123_EPOCH + datetime.timedelta(seconds=value * SECONDS_1_DAY)
        ).time()


class GSheetsBoolean(Boolean):
    """
    A GSheets boolean.
    """

    def quote(self, value: Optional[bool]) -> str:
        if value is None:
            return "NULL"
        return "true" if value else "false"

    def to_unformatted(self, value: Optional[bool]) -> Union[bool, str]:
        """
        Return an empty string if missing, else a boolean.
        """
        return "" if value is None else value

    def from_unformatted(self, value: Union[bool, str]) -> Optional[bool]:
        """
        Convert to a native Python type.
        """
        if value == "":
            return None

        if isinstance(value, str):
            raise ProgrammingError("Invalid value found")

        return value


class GSheetsFloat(Float):
    """
    A GSheets float.
    """

    def to_unformatted(self, value: Optional[bool]) -> Union[float, str]:
        """
        Return an empty string if missing, else a float.
        """
        return "" if value is None else value

    def from_unformatted(self, value: Union[float, str]) -> Optional[float]:
        """
        Convert to a native Python type.
        """
        if value == "":
            return None

        if isinstance(value, str):
            raise ProgrammingError("Invalid value found")

        return value


class GSheetsString(String):
    """
    A GSheets string.
    """

    def to_unformatted(self, value: Optional[str]) -> str:
        """
        Return an empty string if missing, else the original string.
        """
        return "" if value is None else value

    def from_unformatted(self, value: str) -> Optional[str]:
        """
        Convert to a native Python type.
        """
        return None if value == "" else value
