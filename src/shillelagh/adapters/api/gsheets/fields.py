"""
Custom fields for the GSheets adapter.
"""

import datetime
from typing import Any, Optional, Union

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


def _normalize_pattern(pattern: str) -> str:
    """
    Lowercase a date/time pattern outside of its literals.

    The Google Sheets API reports the *default* (non-user-configured) date and
    datetime formats using an ICU-style pattern with uppercase tokens, e.g.
    ``M/D/YYYY`` or ``M/d/yyyy H:mm:ss``. The token grammar in
    ``parsing.date`` is lowercase-only, so these patterns would otherwise fail
    to parse (or be silently mis-parsed as literals). Literal text is always
    quoted (``"..."``) or escaped (``\\x``) in patterns returned by the API, so
    lowercasing everything outside literals is safe and normalizes the
    uppercase default tokens into ones the parser handles.
    """
    result = []
    i = 0
    length = len(pattern)
    while i < length:
        char = pattern[i]
        if char == "\\" and i + 1 < length:
            # escaped literal: keep the backslash and the next char verbatim
            result.append(pattern[i : i + 2])
            i += 2
        elif char == '"':
            # quoted literal: keep verbatim up to and including the closing quote
            end = pattern.find('"', i + 1)
            if end == -1:
                end = length - 1
            result.append(pattern[i : end + 1])
            i = end + 1
        else:
            result.append(char.lower())
            i += 1
    return "".join(result)


class GSheetsField(Field[Internal, External]):
    """
    A base class for GSheets fields.
    """

    # Date/time fields set this to normalize the ICU-style pattern reported by
    # the API (see ``_normalize_pattern``). Number/boolean/string fields keep it
    # disabled, since e.g. the number format ``General`` must not be lowercased.
    normalize_pattern_case: bool = False

    def __init__(  # pylint: disable=too-many-arguments, too-many-positional-arguments
        self,
        filters: Optional[list[type[Filter]]] = None,
        order: Order = Order.NONE,
        exact: bool = False,
        pattern: Optional[str] = None,
        timezone: Optional[datetime.tzinfo] = None,
    ):
        super().__init__(filters, order, exact)
        if pattern is not None and self.normalize_pattern_case:
            pattern = _normalize_pattern(pattern)
        self.pattern: Optional[str] = pattern
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
    normalize_pattern_case = True

    def parse(self, value: Optional[str]) -> Optional[datetime.datetime]:
        # Google Chart API returns ``None`` for a NULL cell, while the Google
        # Sheets API returns an empty string
        if self.pattern is None or value is None or value == "":
            return None

        timestamp = parse_date_time_pattern(value, self.pattern, datetime.datetime)

        # Set the timestamp to the spreadsheet timezone, if any.
        timestamp = timestamp.replace(tzinfo=self.timezone)

        return timestamp

    def format(self, value: Optional[datetime.datetime]) -> str:
        # This method is used only when inserting or updating rows, so we
        # encode NULLs as an empty string to match the Google Sheets API.
        if self.pattern is None or value is None:
            return ""

        # Google Sheets does not support timezones in datetime values, so we
        # convert all timestamps to the sheet timezone
        if self.timezone:
            value = value.astimezone(self.timezone)

        return format_date_time_pattern(value, self.pattern)

    def quote(self, value: Optional[str]) -> str:
        if self.pattern is None or value == "" or value is None:
            return "null"

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
    normalize_pattern_case = True

    def parse(self, value: Optional[str]) -> Optional[datetime.date]:
        # Google Chart API returns ``None`` for a NULL cell, while the Google
        # Sheets API returns an empty string
        if self.pattern is None or value is None or value == "":
            return None

        return parse_date_time_pattern(value, self.pattern, datetime.date)

    def format(self, value: Optional[datetime.date]) -> str:
        if self.pattern is None or value is None:
            return ""

        return format_date_time_pattern(value, self.pattern)

    def quote(self, value: Optional[str]) -> str:
        if self.pattern is None or value == "" or value is None:
            return "null"

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
    normalize_pattern_case = True

    def parse(self, value: Optional[str]) -> Optional[datetime.time]:
        """
        Parse time of day as returned from the Google Chart API.
        """
        # Google Chart API returns ``None`` for a NULL cell, while the Google
        # Sheets API returns an empty string
        if self.pattern is None or value is None or value == "":
            return None

        return parse_date_time_pattern(value, self.pattern, datetime.time)

    def format(self, value: Optional[datetime.time]) -> str:
        if self.pattern is None or value is None:
            return ""

        return format_date_time_pattern(value, self.pattern)

    def quote(self, value: Optional[str]) -> str:
        if self.pattern is None or value == "" or value is None:
            return "null"

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
    normalize_pattern_case = True

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
