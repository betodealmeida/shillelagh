"""
Custom fields for the GSheets adapter.
"""

import datetime
import re
from collections.abc import Sequence
from typing import Any, Optional, Union, cast

from shillelagh.adapters.api.gsheets.parsing.date import (
    format_date_time_pattern,
    parse_date_time_pattern,
)
from shillelagh.adapters.api.gsheets.parsing.number import (
    format_number_pattern,
    parse_number_pattern,
)
from shillelagh.fields import External, Field, Internal, Order, StringBoolean
from shillelagh.filters import Filter

# timestamp format used in SQL queries
DATETIME_SQL_QUOTE = "%Y-%m-%d %H:%M:%S"
DATE_SQL_QUOTE = "%Y-%m-%d"
TIME_SQL_QUOTE = "%H:%M:%S"

# When filtering a sheet based on a duration we need to convert it into a datetime
# starting at 1899-12-30, for some reason. That is not documented anywhere, obviously.
DURATION_OFFSET = datetime.datetime(1899, 12, 30)

GVIZ_DATE_RE = re.compile(r"^Date\((?P<parts>\d+(?:,\d+)*)\)$")

GvizDateValue = Optional[str]
GvizTimeValue = Optional[Union[str, Sequence[int]]]


def parse_gviz_date(value: str) -> datetime.datetime:
    """Parse the locale-independent raw date value returned by GViz."""
    match = GVIZ_DATE_RE.match(value)
    if not match:
        raise ValueError(f"Invalid GViz date value: {value}")

    parts = [int(part) for part in match.group("parts").split(",")]
    parts.extend([0] * (7 - len(parts)))
    year, month, day, hour, minute, second, millisecond = parts
    return datetime.datetime(
        year,
        month + 1,  # GViz months are zero-based.
        day,
        hour,
        minute,
        second,
        millisecond * 1000,
    )


class GSheetsField(Field[Internal, External]):
    """
    A base class for GSheets fields.
    """

    # the default formats for date and datetime do not follow the same syntax
    # as user-configured formats (I suspect it uses ICU instead)
    pattern_substitutions = {
        "M/d/yyyy H:mm:ss": "m/d/yyyy h:mm:ss",
        "M/d/yyyy": "m/d/yyyy",
    }

    def __init__(  # pylint: disable=too-many-arguments, too-many-positional-arguments
        self,
        filters: Optional[list[type[Filter]]] = None,
        order: Order = Order.NONE,
        exact: bool = False,
        pattern: Optional[str] = None,
        timezone: Optional[datetime.tzinfo] = None,
    ):
        super().__init__(filters, order, exact)
        self.pattern: Optional[str] = (
            self.pattern_substitutions[pattern]
            if pattern in self.pattern_substitutions
            else pattern
        )
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

    When the timezone is present and read successfully all timestamps are
    converted to it, both when fetching data as well as when inserting rows.
    """

    type = "TIMESTAMP"
    db_api_type = "DATETIME"

    def parse(self, value: GvizDateValue) -> Optional[datetime.datetime]:
        # Google Chart API returns ``None`` for a NULL cell, while the Google
        # Sheets API returns an empty string
        if value is None or value == "":
            return None

        if self.pattern is None:
            timestamp = parse_gviz_date(value)
            return timestamp.replace(tzinfo=self.timezone)

        timestamp = parse_date_time_pattern(value, self.pattern, datetime.datetime)

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

        if self.pattern is None:
            # ISO output is deliberately shared by query-bound rendering and
            # Sheets API DML. With no effective display pattern/locale from
            # GViz, it is the only unambiguous string representation available.
            return value.strftime(DATETIME_SQL_QUOTE)
        return format_date_time_pattern(value, self.pattern)

    def quote(self, value: Optional[str]) -> str:
        if value == "" or value is None:
            return "null"

        if self.pattern is None:
            return f"datetime '{value}'"

        # On SQL queries the timestamp should be prefix by "datetime"
        value = parse_date_time_pattern(
            value,
            self.pattern,
            datetime.datetime,
        ).strftime(DATETIME_SQL_QUOTE)
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

    def parse(self, value: GvizDateValue) -> Optional[datetime.date]:
        # Google Chart API returns ``None`` for a NULL cell, while the Google
        # Sheets API returns an empty string
        if value is None or value == "":
            return None

        if self.pattern is None:
            return parse_gviz_date(value).date()

        return parse_date_time_pattern(value, self.pattern, datetime.date)

    def format(self, value: Optional[datetime.date]) -> str:
        if value is None:
            return ""
        if self.pattern is None:
            # See GSheetsDateTime.format: this value is also sent to the
            # Sheets API with valueInputOption=USER_ENTERED during DML.
            return value.strftime(DATE_SQL_QUOTE)
        return format_date_time_pattern(value, self.pattern)

    def quote(self, value: Optional[str]) -> str:
        if value == "" or value is None:
            return "null"

        if self.pattern is None:
            return f"date '{value}'"

        # On SQL queries the timestamp should be prefix by "date"
        value = parse_date_time_pattern(value, self.pattern, datetime.date).strftime(
            DATE_SQL_QUOTE,
        )
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

    def parse(self, value: GvizTimeValue) -> Optional[datetime.time]:
        """
        Parse time of day as returned from the Google Chart API.
        """
        # Google Chart API returns ``None`` for a NULL cell, while the Google
        # Sheets API returns an empty string
        if value is None or value == "":
            return None

        if self.pattern is None:
            hour, minute, second, *rest = cast(Sequence[int], value)
            return datetime.time(hour, minute, second, (rest[0] if rest else 0) * 1000)

        return parse_date_time_pattern(cast(str, value), self.pattern, datetime.time)

    def format(self, value: Optional[datetime.time]) -> str:
        if value is None:
            return ""
        if self.pattern is None:
            # See GSheetsDateTime.format: this value is also sent to the
            # Sheets API with valueInputOption=USER_ENTERED during DML.
            return value.strftime(TIME_SQL_QUOTE)
        return format_date_time_pattern(value, self.pattern)

    def quote(self, value: Optional[str]) -> str:
        if value == "" or value is None:
            return "null"

        if self.pattern is None:
            return f"timeofday '{value}'"

        # On SQL queries the timestamp should be prefix by "timeofday"
        value = parse_date_time_pattern(value, self.pattern, datetime.time).strftime(
            TIME_SQL_QUOTE,
        )
        return f"timeofday '{value}'"


class GSheetsDuration(GSheetsField[str, datetime.timedelta]):
    """
    A GSheets duration.
    """

    type = "DURATION"
    db_api_type = "DATETIME"

    def parse(self, value: Optional[str]) -> Optional[datetime.timedelta]:
        if self.pattern is None or value is None or value == "":
            return None

        return parse_date_time_pattern(value, self.pattern, datetime.timedelta)

    def format(self, value: Optional[datetime.timedelta]) -> str:
        # This method is used only when inserting or updating rows, so we
        # encode NULLs as an empty string to match the Google Sheets API.
        if self.pattern is None or value is None:
            return ""

        return format_date_time_pattern(value, self.pattern)

    def quote(self, value: Optional[str]) -> str:
        if self.pattern is None or value == "" or value is None:
            return "null"

        timestamp = DURATION_OFFSET + parse_date_time_pattern(
            value,
            self.pattern,
            datetime.timedelta,
        )
        return f"datetime '{timestamp}'"


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

        return StringBoolean.strtobool(value)

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

        if self.pattern is None or self.pattern == "General":
            try:
                return int(value)
            except ValueError:
                return float(value)

        return parse_number_pattern(value, self.pattern)

    def format(self, value: Optional[float]) -> str:
        if value is None:
            return ""

        if self.pattern is None or self.pattern == "General":
            return str(value)

        return format_number_pattern(value, self.pattern)

    def quote(self, value: Optional[Union[str, int, float]]) -> str:
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
        if value is None:
            return "null"

        return f"'{value}'"
