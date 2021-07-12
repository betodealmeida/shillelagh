"""
Custom fields for the GSheets adapter.
"""
import datetime
from distutils.util import strtobool
from typing import Any
from typing import List
from typing import Optional
from typing import Type

from shillelagh.fields import External
from shillelagh.fields import Field
from shillelagh.fields import Internal
from shillelagh.fields import Order
from shillelagh.filters import Filter


# Timestamp fprmat used to insert data into cells. Note that the timezone is not
# present, since GSheets assumes all timestamps are in the same timezone as the
# sheet.
DATETIME_CELL_FORMAT = "%m/%d/%Y %H:%M:%S"
DATE_CELL_FORMAT = "%m/%d/%Y"
TIME_CELL_FORMAT = "%I:%M:%S %p"

# timestamp format used in SQL queries
DATETIME_SQL_QUOTE = "%Y-%m-%d %H:%M:%S"
DATE_SQL_QUOTE = "%Y-%m-%d"
TIME_SQL_QUOTE = "%H:%M:%S"


def convert_pattern_to_format(pattern: Optional[str]) -> str:
    """
    Convert a Google Chart API pattern to a python time format.

    Reference: https://developers.google.com/sheets/api/guides/formats?hl=en
    """
    # Google Chart API uses ICU to represent patterns. In Python we can use
    # PyICU to parse the values given a pattern, but the library is difficult
    # to install and big. For now we have hardcoded values.
    formats = {
        "M/d/yyyy H:mm:ss": DATETIME_CELL_FORMAT,
        "M/d/yyyy": DATE_CELL_FORMAT,
        "h:mm:ss am/pm": TIME_CELL_FORMAT,
    }

    if pattern not in formats:
        raise NotImplementedError(
            f'Unknown pattern "{pattern}". Please file a ticket at '
            "https://github.com/betodealmeida/shillelagh/issues.",
        )

    return formats[pattern]


class GSheetsField(Field[Internal, External]):
    """
    A base class for GSheets fields.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        filters: Optional[List[Type[Filter]]] = None,
        order: Order = Order.NONE,
        exact: bool = False,
        pattern: Optional[str] = None,
        timezone: Optional[datetime.tzinfo] = None,
    ):
        super().__init__(filters, order, exact)
        self.pattern = pattern
        self.timezone = timezone

    def __eq__(self, other: Any) -> bool:
        if other.__class__ != self.__class__:
            return NotImplemented

        return bool(
            self.filters == other.filters
            and self.order == other.order
            and self.exact == other.exact
            and self.pattern == other.pattern
            and self.timezone == other.timezone,
        )


class GSheetsDateTime(GSheetsField[str, datetime.datetime]):
    """
    A GSheets timestamp.

    The Google Chart API returns timestamps as a string encoded using an ICU
    pattern. The default format is "M/d/yyyy H:mm:ss", and values look like
    this:

        "9/1/2018 0:00:00"

    There are no timezones; instead, there is a global timezone for the whole
    spreadsheet. The timezone can only be read if the user has set their
    credentials, since the Google Sheets API used to read metadata about the
    sheet requires authentication.

    When the timezone is present and read succesfully all timestamps are
    converted to it, both when fetching data as well as when inserting rows.
    """

    type = "TIMESTAMP"
    db_api_type = "DATETIME"

    def parse(self, value: Optional[str]) -> Optional[datetime.datetime]:
        # Google Chart API returns ``None`` for a NULL cell, while the Google
        # Sheets API returns an empty string
        if value is None or value == "":
            return None

        format_ = convert_pattern_to_format(self.pattern)
        timestamp = datetime.datetime.strptime(value, format_)

        # Set the timestamp to the spreadsheet timezone, if any.
        timestamp = timestamp.replace(tzinfo=self.timezone)

        return timestamp

    def format(self, value: Optional[datetime.datetime]) -> str:
        # This method is used only when inserting or updating rows, so we
        # encode NULLs as an empty string to match the Google Sheets API.
        if value is None:
            return ""

        # Google Sheets does not support timezones in datetime values, so we
        # convert all timestamps to the sheet timezone
        if self.timezone:
            value = value.astimezone(self.timezone)

        format_ = convert_pattern_to_format(self.pattern)
        return value.strftime(format_)

    def quote(self, value: Optional[str]) -> str:
        if value == "" or value is None:
            return "null"

        # On SQL queries the timestamp should be prefix by "datetime"
        format_ = convert_pattern_to_format(self.pattern)
        value = datetime.datetime.strptime(value, format_).strftime(
            DATETIME_SQL_QUOTE,
        )
        return f"datetime '{value}'"


class GSheetsDate(GSheetsField[str, datetime.date]):
    """
    A GSheets date.

    The Google Chart API returns dates as a string encoded using an ICU
    pattern. The default format is "M/d/yyyy", and values look like
    this:

        "9/1/2018"

    """

    type = "DATE"
    db_api_type = "DATETIME"

    def parse(self, value: Optional[str]) -> Optional[datetime.date]:
        # Google Chart API returns ``None`` for a NULL cell, while the Google
        # Sheets API returns an empty string
        if value is None or value == "":
            return None

        format_ = convert_pattern_to_format(self.pattern)
        return datetime.datetime.strptime(value, format_).date()

    def format(self, value: Optional[datetime.date]) -> str:
        if value is None:
            return ""

        format_ = convert_pattern_to_format(self.pattern)
        return value.strftime(format_)

    def quote(self, value: Optional[str]) -> str:
        if value == "" or value is None:
            return "null"

        # On SQL queries the timestamp should be prefix by "date"
        format_ = convert_pattern_to_format(self.pattern)
        value = datetime.datetime.strptime(value, format_).strftime(DATE_SQL_QUOTE)
        return f"date '{value}'"


class GSheetsTime(GSheetsField[str, datetime.time]):
    """
    A GSheets time.

    The Google Chart API returns times as a string encoded using an ICU
    pattern. The default format is "h:mm:ss am/pm", and values look like
    this:

        "5:00:00 PM"

    """

    type = "TIME"
    db_api_type = "DATETIME"

    def parse(self, value: Optional[str]) -> Optional[datetime.time]:
        """
        Parse time of day as returned from the Google Chart API.
        """
        # Google Chart API returns ``None`` for a NULL cell, while the Google
        # Sheets API returns an empty string
        if value is None or value == "":
            return None

        format_ = convert_pattern_to_format(self.pattern)
        return datetime.datetime.strptime(value, format_).time()

    def format(self, value: Optional[datetime.time]) -> str:
        if value is None:
            return ""

        format_ = convert_pattern_to_format(self.pattern)
        return value.strftime(format_)

    def quote(self, value: Optional[str]) -> str:
        if value == "" or value is None:
            return "null"

        # On SQL queries the timestamp should be prefix by "timeofday"
        format_ = convert_pattern_to_format(self.pattern)
        value = datetime.datetime.strptime(value, format_).strftime(
            TIME_SQL_QUOTE,
        )
        return f"timeofday '{value}'"


class GSheetsBoolean(GSheetsField[str, bool]):
    """
    A GSheets boolean.

    Booleans in the Google Chart API are return as a string, either "TRUE"
    of "FALSE".
    """

    type = "BOOLEAN"
    db_api_type = "NUMBER"

    def parse(self, value: Optional[str]) -> Optional[bool]:
        # Google Chart API returns ``None`` for a NULL cell, while the Google
        # Sheets API returns an empty string
        if value is None or value == "":
            return None

        return bool(strtobool(value))

    def format(self, value: Optional[bool]) -> str:
        if value is None:
            return ""

        return "TRUE" if value else "FALSE"

    def quote(self, value: Optional[str]) -> str:
        if value == "" or value is None:
            return "null"
        return value.lower()


class GSheetsNumber(GSheetsField[str, float]):
    """
    A GSheets number.

    The Google Chart/Sheets APIs return "numbers" only, encoded as strings.
    """

    type = "REAL"
    db_api_type = "NUMBER"

    def parse(self, value: Optional[str]) -> Optional[float]:
        if value is None or value == "":
            return None

        try:
            return int(value)
        except ValueError:
            pass

        return float(value)

    def format(self, value: Optional[float]) -> str:
        if value is None:
            return ""

        return str(value)

    def quote(self, value: Optional[str]) -> str:
        if value == "" or value is None:
            return "null"

        return str(value)


class GSheetsString(GSheetsField[str, str]):
    """
    A GSheets string.
    """

    type = "TEXT"
    db_api_type = "STRING"

    def parse(self, value: Optional[str]) -> Optional[str]:
        return None if value == "" else value

    def format(self, value: Optional[str]) -> str:
        return "" if value is None else value

    def quote(self, value: Optional[str]) -> str:
        if value == "" or value is None:
            return "null"

        return f"'{value}'"
