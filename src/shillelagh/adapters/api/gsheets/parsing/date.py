"""
Parse and format Google Sheet date/time patterns.

https://developers.google.com/sheets/api/guides/formats?hl=en#date_and_time_format_patterns
"""

# pylint: disable=invalid-name, fixme, broad-exception-raised

import calendar
import re
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from enum import Enum
from typing import Any, Dict, List, Tuple, Type, TypeVar, Union

from shillelagh.adapters.api.gsheets.parsing.base import LITERAL, Token, tokenize

DateTime = TypeVar("DateTime", datetime, date, time, timedelta)


class Meridiem(Enum):
    """
    Represent ante or post meridiem.
    """

    AM = "AM"
    PM = "PM"


class H(Token):
    """
    Hour of the day.

    Switches between 12 and 24 hour format depending on whether an am/pm
    indicator is present in the string.
    """

    regex = "h(?!h)"

    def format(self, value: Union[datetime, time], tokens: List[Token]) -> str:
        hour = value.hour

        if (
            any(token.__class__.__name__ in {"AP", "AMPM"} for token in tokens)
            and hour != 12
        ):
            hour %= 12

        # the 5th example in https://developers.google.com/sheets/api/guides/formats?hl=en
        # has a "PM" literal that switches to 12 hour format
        if (
            any(
                token.__class__.__name__ == "LITERAL"
                and ("AM" in token.token or "PM" in token.token)
                for token in tokens
            )
            and hour != 12
        ):
            hour %= 12

        return str(hour)

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        match = re.match(r"\d{1,2}", value)
        if not match:
            raise Exception(f"Cannot parse value: {value}")
        size = len(match.group()) if 0 <= int(match.group()) < 24 else 1

        return {"hour": int(value[:size])}, value[size:]


class HHPlus(H):
    """
    Same as previous, but with a leading 0 for 1-9.
    """

    regex = "hh+"

    def format(self, value: Union[datetime, time], tokens: List[Token]) -> str:
        return super().format(value, tokens).zfill(2)

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        return {"hour": int(value[:2])}, value[2:]


class M(Token):
    """
    If the previous non-literal token was hours or the subsequent one is
    seconds, then it represents minutes in the hour (no leading 0).
    Otherwise, it represents the month of the year as a number (no leading 0).
    """

    regex = "m(?!m)"

    def _is_minute(self, tokens: List[Token]) -> bool:
        """
        Return true if the token represents minutes, false if months.
        """
        is_minute = False
        i = -1
        for i, token in enumerate(tokens):
            if token is self:
                break
        else:
            raise Exception("Token is not present in list of tokens")

        for token in reversed(tokens[:i]):
            if token.__class__.__name__ == "LITERAL":
                continue
            if token.__class__.__name__ in {"H", "HHPlus"}:
                is_minute = True
            break

        for token in tokens[i + 1 :]:
            if token.__class__.__name__ == "LITERAL":
                continue
            if token.__class__.__name__ in {"S", "SS"}:
                is_minute = True
            break

        return is_minute

    def format(self, value: Union[date, datetime, time], tokens: List[Token]) -> str:
        if self._is_minute(tokens) and isinstance(value, (datetime, time)):
            return str(value.minute)

        if isinstance(value, (datetime, date)):
            return str(value.month)

        raise Exception(f"Cannot format value: {value}")

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        match = re.match(r"\d{1,2}", value)
        if not match:
            raise Exception(f"Cannot parse value: {value}")
        size = len(match.group()) if 1 <= int(match.group()) <= 24 else 1

        if self._is_minute(tokens):
            return {"minute": int(value[:size])}, value[size:]
        return {"month": int(value[:size])}, value[size:]


class MM(M):
    """
    As above, but with a leading 0 for both cases.
    """

    regex = "mm(?!m)"

    def format(self, value: Union[date, datetime, time], tokens: List[Token]) -> str:
        return super().format(value, tokens).zfill(2)

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        if self._is_minute(tokens):
            return {"minute": int(value[:2])}, value[2:]
        return {"month": int(value[:2])}, value[2:]


class MMM(Token):
    """
    Three letter month abbreviation (e.g., "Feb").
    """

    regex = "mmm(?!m)"

    def format(self, value: Union[date, datetime, time], tokens: List[Token]) -> str:
        return value.strftime("%b")

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        month = datetime.strptime(value[:3], "%b").month
        return {"month": month}, value[3:]


class MMMM(MMM):
    """
    Full month name. mmmmmm+ also matches this.
    """

    regex = "(mmmm(?!m))|(m{6,})"

    def format(self, value: Union[date, datetime, time], tokens: List[Token]) -> str:
        return value.strftime("%B")

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        token = re.split(r"\b", value, 2)[1]
        size = len(token)
        month = datetime.strptime(value[:size], "%B").month
        return {"month": month}, value[size:]


class MMMMM(MMM):
    """
    First letter of the month (e.g., "J" for June).
    """

    regex = "mmmmm"

    def format(self, value: Union[date, datetime, time], tokens: List[Token]) -> str:
        return value.strftime("%B")[0]

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        letter = value[0]

        mapping = defaultdict(list)
        for i in range(1, 13):
            mapping[calendar.month_name[i][0]].append(i)
        if len(mapping[letter]) == 0:
            raise Exception(f"Unable to find month letter: {letter}")
        if len(mapping[letter]) > 1:
            raise Exception(f"Unable to parse month letter unambiguously: {letter}")

        return {"month": mapping[letter][0]}, value[1:]


class S(Token):
    """
    Seconds in the minute without a leading 0.
    """

    regex = "s(?!s)"

    def format(self, value: Union[datetime, time], tokens: List[Token]) -> str:
        return str(value.second)

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        match = re.match(r"\d{1,2}", value)
        if not match:
            raise Exception(f"Cannot parse value: {value}")
        # leap seconds can be 60 or even 61
        size = len(match.group()) if 0 <= int(match.group()) <= 61 else 1

        return {"second": int(value[:size])}, value[size:]


class SS(S):
    """
    Seconds in the minute with a leading 0.
    """

    regex = "ss"

    def format(self, value: Union[datetime, time], tokens: List[Token]) -> str:
        return super().format(value, tokens).zfill(2)

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        return {"second": int(value[:2])}, value[2:]


class DurationToken(Token):  # pylint: disable=abstract-method
    """
    A token for durations.

    Durations are special because often only the first token is annotated. For example:

        - [h]:mm:ss
        - [ss].000

    But apparently it is valid to annotate subsequent tokens:

        - [hh]:[mm]:[ss].000

    Who knows?

    Because of this, their regexes are dynamic, and depend on the token history.
    """

    is_duration = True
    regexes: Tuple[str, str]

    @classmethod
    def match(
        cls,
        pattern: str,
        history: List[Token],
    ) -> bool:
        if any(isinstance(token, DurationToken) for token in history):
            regex = cls.regexes[1]
        else:
            regex = cls.regexes[0]

        return bool(re.match(regex, pattern))

    @classmethod
    def consume(
        cls,
        pattern: str,
        history: List[Token],
    ) -> Tuple[Token, str]:
        if any(isinstance(token, DurationToken) for token in history):
            regex = cls.regexes[1]
        else:
            regex = cls.regexes[0]

        match = re.match(regex, pattern)
        if not match:
            # pylint: disable=broad-exception-raised
            raise Exception("Token could not find match")
        token = match.group()
        return cls(token), pattern[len(token) :]


class HPlusDuration(DurationToken):
    """
    Number of elapsed hours in a time duration. Number of letters indicates
    minimum number of digits (adds leading 0s).
    """

    regexes = (r"\[h+\]", r"(h+)|(\[h+\])")

    def format(self, value: Union[timedelta], tokens: List[Token]) -> str:
        return str(int(value.total_seconds() // 3600)).zfill(len(self.token) - 2)

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        match = re.match(r"\d+", value)
        if not match:
            raise Exception(f"Cannot parse value: {value}")
        size = len(match.group())
        return {"hours": int(value[:size])}, value[size:]


class MPlusDuration(DurationToken):
    """
    Number of elapsed minutes in a time duration. Number of letters indicates
    minimum number of digits (adds leading 0s).
    """

    regexes = (r"\[m+\]", r"(m+)|(\[m+\])")

    def format(self, value: Union[timedelta], tokens: List[Token]) -> str:
        seconds = value.total_seconds()

        if any(token.__class__.__name__ == "HPlusDuration" for token in tokens):
            # ignore hours
            seconds %= 3600

        return str(int(seconds // 60)).zfill(len(self.token) - 2)

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        match = re.match(r"\d+", value)
        if not match:
            raise Exception(f"Cannot parse value: {value}")
        size = len(match.group())
        return {"minutes": int(value[:size])}, value[size:]


class SPlusDuration(DurationToken):
    """
    Number of elapsed seconds in a time duration. Number of letters indicates
    minimum number of digits (adds leading 0s).
    """

    regexes = (r"\[s+\]", r"(s+)|(\[s+\])")

    def format(self, value: Union[timedelta], tokens: List[Token]) -> str:
        seconds = value.total_seconds()

        if any(token.__class__.__name__ == "HPlusDuration" for token in tokens):
            # ignore hours
            seconds %= 3600

        if any(token.__class__.__name__ == "MPlusDuration" for token in tokens):
            # ignore minutes
            seconds %= 60

        return str(int(seconds)).zfill(len(self.token) - 2)

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        match = re.match(r"\d+", value)
        if not match:
            raise Exception(f"Cannot parse value: {value}")
        size = len(match.group())
        return {"seconds": int(value[:size])}, value[size:]


class D(Token):
    """
    Day of the month, no leading 0 for numbers less than 10.
    """

    regex = "d(?!d)"

    def format(self, value: Union[date, datetime], tokens: List[Token]) -> str:
        return str(value.day)

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        match = re.match(r"\d{1,2}", value)
        if not match:
            raise Exception(f"Cannot parse value: {value}")
        size = len(match.group()) if 1 <= int(match.group()) <= 31 else 1
        return {"day": int(value[:size])}, value[size:]


class DD(D):
    """
    Day of the month, with a leading 0 for numbers less than 10.
    """

    regex = "dd(?!d)"

    def format(self, value: Union[date, datetime], tokens: List[Token]) -> str:
        return value.strftime("%d")

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        return {"day": int(value[:2])}, value[2:]


class DDD(D):
    """
    Day of the week, three letter abbreviation (e.g., "Mon").
    """

    regex = "ddd(?!d)"

    def format(self, value: Union[date, datetime], tokens: List[Token]) -> str:
        return value.strftime("%a")

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        return {"weekday": datetime.strptime(value[:3], "%a").weekday()}, value[3:]


class DDDDPlus(D):
    """
    Day of the week, full name.
    """

    regex = "d{4,}"

    def format(self, value: Union[date, datetime], tokens: List[Token]) -> str:
        return value.strftime("%A")

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        token = re.split(r"\b", value, 2)[1]
        size = len(token)
        return {"weekday": datetime.strptime(value[:size], "%A").weekday()}, value[
            size:
        ]


class YY(Token):
    """
    2-digit year.
    """

    regex = "y{1,2}(?!y)"

    def format(self, value: Union[date, datetime], tokens: List[Token]) -> str:
        return value.strftime("%y")

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        # assume 00 == 2000
        year = int(value[:2]) + 2000

        return {"year": year}, value[2:]


class YYYY(Token):
    """
    4-digit year.
    """

    regex = "y{3,}"

    def format(self, value: Union[date, datetime], tokens: List[Token]) -> str:
        return value.strftime("%Y")

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        return {"year": int(value[:4])}, value[4:]


class ZERO(Token):
    """
    Tenths of seconds. You can increase the precision to 2 digits
    with 00 or 3 digits (milliseconds) with 000.
    """

    regex = "0{1,3}(?!0)"

    def format(
        self,
        value: Union[datetime, time, timedelta],
        tokens: List[Token],
    ) -> str:
        precision = len(self.token)
        us = value.microseconds if isinstance(value, timedelta) else value.microsecond
        rounded = round(us / 1e6, precision)
        return str(int(rounded * 10**precision)).zfill(precision)

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        size = len(self.token)

        # adjust precision
        token = value[:size]
        token += "0" * (6 - size)
        microsecond = int(token)

        return {"microsecond": microsecond}, value[size:]


class AP(Token):
    """
    Displays "a" for AM, and "p" for PM. Also changes hours to 12-hour
    format. If the token letter is capitalized, the output is as well.
    """

    regex = "(a/p)|(A/P)"

    def format(self, value: Union[datetime, time], tokens: List[Token]) -> str:
        output = "a" if value.hour < 12 else "p"
        if self.token == "A/P":
            output = output.upper()
        return output

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        letter = value[:1]
        meridiem = Meridiem.PM if letter.upper() == "P" else Meridiem.AM
        return {"meridiem": meridiem}, value[1:]


class AMPM(AP):
    """
    As above, but displays "AM" or "PM" instead and is always capitalized.
    """

    regex = "am/pm"

    def format(self, value: Union[datetime, time], tokens: List[Token]) -> str:
        return "AM" if value.hour < 12 else "PM"

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        letter = value[:2]
        meridiem = Meridiem.PM if letter.upper() == "PM" else Meridiem.AM
        return {"meridiem": meridiem}, value[2:]


def infer_column_type(pattern: str) -> str:
    """
    Infer the correct date-related type.

    GSheets returns ``datetime`` as the type for timestamps, but also for time of day and
    durations. We need to parse the pattern to figure out the exact type.

    This also handles a case where a timestamp (``1/2/24 14:41``) with a proper pattern
    (``M/D/YY h:mm``) was being returned as type ``timeofday``.
    """
    classes = [
        # durations should come first because they need to be modified
        # after the first capture
        HPlusDuration,
        MPlusDuration,
        SPlusDuration,
        # then the rest
        H,
        HHPlus,
        M,
        MM,
        MMM,
        MMMM,
        MMMMM,
        S,
        SS,
        D,
        DD,
        DDD,
        DDDDPlus,
        YY,
        YYYY,
        AP,
        AMPM,
        ZERO,
        LITERAL,
    ]

    tokens = list(tokenize(pattern, classes))

    if any(isinstance(token, DurationToken) for token in tokens):
        return "duration"

    datetime_tokens = (D, DD, DDD, DDDDPlus, YY, YYYY)
    if any(isinstance(token, datetime_tokens) for token in tokens):
        return "datetime"

    return "timeofday"


def parse_date_time_pattern(
    value: str,
    pattern: str,
    class_: Type[DateTime],
) -> DateTime:
    """
    Parse a value using a given pattern.

    See https://developers.google.com/sheets/api/guides/formats?hl=en.
    """
    classes = [
        # durations should come first because they need to be modified
        # after the first capture
        HPlusDuration,
        MPlusDuration,
        SPlusDuration,
        # then the rest
        H,
        HHPlus,
        M,
        MM,
        MMM,
        MMMM,
        MMMMM,
        S,
        SS,
        D,
        DD,
        DDD,
        DDDDPlus,
        YY,
        YYYY,
        AP,
        AMPM,
        ZERO,
        LITERAL,
    ]

    kwargs: Dict[str, Any] = {}
    tokens = list(tokenize(pattern, classes))
    for token in tokens:
        consumed, value = token.parse(value, tokens)
        kwargs.update(**consumed)

    # add PM offset
    if "hour" in kwargs:
        meridiem = kwargs.pop("meridiem", None)
        if meridiem == Meridiem.PM and kwargs["hour"] != 12:
            kwargs["hour"] += 12
        elif meridiem == Meridiem.AM and kwargs["hour"] == 12:
            kwargs["hour"] -= 12

    # we can't really do anything with ``weekday``
    if "weekday" in kwargs:
        del kwargs["weekday"]

    if "microsecond" in kwargs and class_ is timedelta:
        kwargs["microseconds"] = kwargs.pop("microsecond")

    try:
        return class_(**kwargs)
    except TypeError as ex:
        raise Exception("Unsupported format") from ex


def format_date_time_pattern(value: DateTime, pattern: str) -> str:
    """
    Format a date/time related object to a given pattern.

    See https://developers.google.com/sheets/api/guides/formats?hl=en.
    """
    classes = [
        # durations should come first because they need to be modified
        # after the first capture
        HPlusDuration,
        MPlusDuration,
        SPlusDuration,
        # then the rest
        H,
        HHPlus,
        M,
        MM,
        MMM,
        MMMM,
        MMMMM,
        S,
        SS,
        D,
        DD,
        DDD,
        DDDDPlus,
        YY,
        YYYY,
        AP,
        AMPM,
        ZERO,
        LITERAL,
    ]

    parts = []
    tokens = list(tokenize(pattern, classes))
    for token in tokens:
        parts.append(token.format(value, tokens))

    return "".join(parts)
