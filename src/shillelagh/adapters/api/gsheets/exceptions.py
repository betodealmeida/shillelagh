"""
Exceptions specific to the Google Sheets adapter.
"""

from shillelagh.exceptions import Error


class DateParseError(Error):
    """
    Raised when a date/time value cannot be parsed from a given pattern.
    """
