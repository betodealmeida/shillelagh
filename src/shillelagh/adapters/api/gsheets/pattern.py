# pylint: disable=invalid-name, no-self-use, fixme
"""
Parse and format Google Sheet date/time patterns.

https://developers.google.com/sheets/api/guides/formats?hl=en#date_and_time_format_patterns
"""
import re
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, Iterator, List, Type, Tuple, Union

DateTime = Union[datetime, date, time, timedelta]


class Token:
    """
    A token.
    """

    regex: str

    def __init__(self, token: str):
        self.token = token

    @classmethod
    def match(cls, pattern: str) -> bool:
        """
        Check if token handles the beginning of the pattern.
        """
        return re.match(cls.regex, pattern)

    @classmethod
    def consume(cls, pattern: str) -> Tuple["Token", str]:
        """
        Consume the pattern, returning the token and the remaining pattern.
        """
        token = cls.match(pattern).group()
        return cls(token), pattern[len(token) :]

    def format(self, value: DateTime, tokens: List["Token"]) -> str:
        """
        Format the value using the pattern.
        """
        raise NotImplementedError("Subclasses MUST implement ``format``")

    def parse(self, value: str, tokens: List["Token"]) -> Tuple[Dict[str, Any], str]:
        """
        Parse the value given a pattern.

        Returns the consumed parameter as an argument, and the rest of the value.
        """
        raise NotImplementedError("Subclasses MUST implement ``parse``")

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{self.token}')"


class H(Token):
    """
    Hour of the day.

    Switches between 12 and 24 hour format depending on whether an am/pm
    indicator is present in the string.
    """

    regex = "h(?!h)"

    def format(self, value: Union[datetime, time], tokens: List[Token]) -> str:
        hour = value.hour

        if any(token.__class__.__name__ in {"AP", "AMPM"} for token in tokens):
            hour %= 12

        # the 5th example in https://developers.google.com/sheets/api/guides/formats?hl=en
        # has a "PM" literal that switches to 12 hour format
        if any(
            token.__class__.__name__ == "LITERAL"
            and ("AM" in token.token or "PM" in token.token)
            for token in tokens
        ):
            hour %= 12

        return str(hour)

    def parse(self, value: str, tokens: List["Token"]) -> Tuple[Dict[str, Any], str]:
        match = re.match(r"\d{1,2}", value)
        size = len(match.group()) if 0 <= int(match.group()) < 24 else 1

        return {"hour": int(value[:size])}, value[size:]


class HHPlus(H):
    """
    Same as previous, but with a leading 0 for 1-9.
    """

    regex = "hh+"

    def format(self, value: Union[datetime, time], tokens: List[Token]) -> str:
        return super().format(value, tokens).zfill(2)

    def parse(self, value: str, tokens: List["Token"]) -> Tuple[Dict[str, Any], str]:
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
        i = tokens.index(self)

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
        if self._is_minute(tokens):
            return str(value.minute)

        return str(value.month)

    def parse(self, value: str, tokens: List["Token"]) -> Tuple[Dict[str, Any], str]:
        match = re.match(r"\d{1,2}", value)
        size = len(match.group()) if 1 <= int(match.group()) <= 12 else 1

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

    def parse(self, value: str, tokens: List["Token"]) -> Tuple[Dict[str, Any], str]:
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

    def parse(self, value: str, tokens: List["Token"]) -> Tuple[Dict[str, Any], str]:
        month = datetime.strptime(value[:3], "%b").month
        return {"month": month}, value[3:]


class MMMM(MMM):
    """
    Full month name. mmmmmm+ also matches this.
    """

    regex = "(mmmm(?!m))|(m{6,})"

    def format(self, value: Union[date, datetime, time], tokens: List[Token]) -> str:
        return value.strftime("%B")

    def parse(self, value: str, tokens: List["Token"]) -> Tuple[Dict[str, Any], str]:
        token = re.split(r"\b", value, 2)[1]
        size = len(token)
        month = datetime.strptime(value[:size], "%B").month
        return {"month": month}, value[size:]


class MMMMM(MMM):
    """
    First letter of the month (e.g., "J" for June).
    """

    regex = "mmmmm(?!m)"

    def format(self, value: Union[date, datetime, time], tokens: List[Token]) -> str:
        return value.strftime("%B")[0]

    def parse(self, value: str, tokens: List["Token"]) -> Tuple[Dict[str, Any], str]:
        raise Exception("Unable to parse a single month letter")


class S(Token):
    """
    Seconds in the minute without a leading 0.
    """

    regex = "s(?!s)"

    def format(self, value: Union[datetime, time], tokens: List[Token]) -> str:
        return str(value.second)

    def parse(self, value: str, tokens: List["Token"]) -> Tuple[Dict[str, Any], str]:
        match = re.match(r"\d{1,2}", value)
        size = len(match.group()) if 0 <= int(match.group()) < 60 else 1

        return {"second": int(value[:size])}, value[size:]


class SS(S):
    """
    Seconds in the minute with a leading 0.
    """

    regex = "ss"

    def format(self, value: Union[datetime, time], tokens: List[Token]) -> str:
        return super().format(value, tokens).zfill(2)

    def parse(self, value: str, tokens: List["Token"]) -> Tuple[Dict[str, Any], str]:
        return {"second": int(value[:2])}, value[2:]


class HPlusDuration(Token):
    """
    Number of elapsed hours in a time duration. Number of letters indicates
    minimum number of digits (adds leading 0s).
    """

    regex = r"\[h+\]"

    def format(self, value: Union[timedelta], tokens: List[Token]) -> str:
        return str(int(value.total_seconds() // 3600)).zfill(len(self.token) - 2)

    def parse(self, value: str, tokens: List["Token"]) -> Tuple[Dict[str, Any], str]:
        size = len(self.token) - 2
        return {"hours": int(value[:size])}, value[size:]


class MPlusDuration(Token):
    """
    Number of elapsed minutes in a time duration. Number of letters indicates
    minimum number of digits (adds leading 0s).
    """

    regex = r"\[m+\]"

    def format(self, value: Union[timedelta], tokens: List[Token]) -> str:
        seconds = value.total_seconds()

        if any(token.__class__.__name__ == "HPlusDuration" for token in tokens):
            # ignore hours
            seconds %= 3600

        return str(int(seconds // 60)).zfill(len(self.token) - 2)

    def parse(self, value: str, tokens: List["Token"]) -> Tuple[Dict[str, Any], str]:
        size = len(self.token) - 2
        return {"minutes": int(value[:size])}, value[size:]


class SPlusDuration(Token):
    """
    Number of elapsed seconds in a time duration. Number of letters indicates
    minimum number of digits (adds leading 0s).
    """

    regex = r"\[s+\]"

    def format(self, value: Union[timedelta], tokens: List[Token]) -> str:
        seconds = value.total_seconds()

        if any(token.__class__.__name__ == "HPlusDuration" for token in tokens):
            # ignore hours
            seconds %= 3600

        if any(token.__class__.__name__ == "MPlusDuration" for token in tokens):
            # ignore minutes
            seconds %= 60

        return str(int(seconds)).zfill(len(self.token) - 2)

    def parse(self, value: str, tokens: List["Token"]) -> Tuple[Dict[str, Any], str]:
        size = len(self.token) - 2
        return {"seconds": int(value[:size])}, value[size:]


class D(Token):
    """
    Day of the month, no leading 0 for numbers less than 10.
    """

    regex = "d(?!d)"

    def format(self, value: Union[date, datetime], tokens: List[Token]) -> str:
        return str(value.day)

    def parse(self, value: str, tokens: List["Token"]) -> Tuple[Dict[str, Any], str]:
        match = re.match(r"\d{1,2}", value)
        size = len(match.group()) if 1 <= int(match.group()) <= 31 else 1
        return {"day": int(value[:size])}, value[size:]


class DD(D):
    """
    Day of the month, with a leading 0 for numbers less than 10.
    """

    regex = "dd(?!d)"

    def format(self, value: Union[date, datetime], tokens: List[Token]) -> str:
        return value.strftime("%d")

    def parse(self, value: str, tokens: List["Token"]) -> Tuple[Dict[str, Any], str]:
        return {"day": int(value[:2])}, value[2:]


class DDD(D):
    """
    Day of the week, three letter abbreviation (e.g., "Mon").
    """

    regex = "ddd(?!d)"

    def format(self, value: Union[date, datetime], tokens: List[Token]) -> str:
        return value.strftime("%a")

    def parse(self, value: str, tokens: List["Token"]) -> Tuple[Dict[str, Any], str]:
        return {"weekday": datetime.strptime(value[:3], "%a").weekday()}, value[3:]


class DDDDPlus(D):
    """
    Day of the week, full name.
    """

    regex = "d{4,}"

    def format(self, value: Union[date, datetime], tokens: List[Token]) -> str:
        return value.strftime("%A")

    def parse(self, value: str, tokens: List["Token"]) -> Tuple[Dict[str, Any], str]:
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

    def parse(self, value: str, tokens: List["Token"]) -> Tuple[Dict[str, Any], str]:
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

    def parse(self, value: str, tokens: List["Token"]) -> Tuple[Dict[str, Any], str]:
        return {"year": int(value[:4])}, value[4:]


class Zero(Token):
    """
    Tenths of seconds. You can increase the precision to 2 digits
    with 00 or 3 digits (milliseconds) with 000.
    """

    regex = "0{1,3}"

    def format(
        self, value: Union[datetime, time, timedelta], tokens: List[Token]
    ) -> str:
        precision = len(self.token)
        us = value.microseconds if isinstance(value, timedelta) else value.microsecond
        rounded = round(us / 1e6, precision)
        return str(int(rounded * 10 ** precision))

    def parse(self, value: str, tokens: List["Token"]) -> Tuple[Dict[str, Any], str]:
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

    def format(self, value: Union[date, datetime, time], tokens: List[Token]) -> str:
        output = "a" if value.hour < 12 else "p"
        if self.token == "A/P":
            output = output.upper()
        return output

    def parse(self, value: str, tokens: List["Token"]) -> Tuple[Dict[str, Any], str]:
        letter = value[:1]
        hour_offset = 12 if letter.upper() == "P" else 0
        return {"hour_offset": hour_offset}, value[1:]


class AMPM(AP):
    """
    As above, but displays "AM" or "PM" instead and is always capitalized.
    """

    regex = "am/pm"

    def format(self, value: Union[date, datetime, time], tokens: List[Token]) -> str:
        return "AM" if value.hour < 12 else "PM"

    def parse(self, value: str, tokens: List["Token"]) -> Tuple[Dict[str, Any], str]:
        letter = value[:1]
        hour_offset = 12 if letter.upper() == "PM" else 0
        return {"hour_offset": hour_offset}, value[1:]


class LITERAL(Token):
    r"""
    A literal.

    Can be defined in different ways:

        \       Treats the next character as a literal value and not any special
                meaning it might have.
        "text"  Displays whatever text is inside the quotation marks as a literal.

    It's also a catchall.
    """

    regex = r'(\\.)|(".*?")|(.)'

    def format(self, value: Union[date, datetime, time], tokens: List[Token]) -> str:
        if self.token.startswith("\\"):
            return self.token[1:]
        if self.token.startswith('"'):
            return self.token[1:-1]
        return self.token

    def parse(self, value: str, tokens: List["Token"]) -> Tuple[Dict[str, Any], str]:
        if self.token.startswith("\\"):
            size = 1
        else:
            size = len(self.token)
        return {}, value[size:]


def tokenize(pattern: str) -> Iterator[Token]:
    """
    Tokenize a pattern.
    """
    classes = [
        H,
        HHPlus,
        M,
        MM,
        MMM,
        MMMM,
        MMMMM,
        S,
        SS,
        HPlusDuration,
        MPlusDuration,
        SPlusDuration,
        D,
        DD,
        DDD,
        DDDDPlus,
        YY,
        YYYY,
        AP,
        AMPM,
        Zero,
        LITERAL,
    ]

    tokens = []
    while pattern:
        for class_ in classes:
            if class_.match(pattern):
                token, pattern = class_.consume(pattern)
                tokens.append(token)

    # combine unescaped literals
    while tokens:
        token = tokens.pop(0)
        if is_unescaped_literal(token):
            acc = [token.token]
            while tokens and is_unescaped_literal(tokens[0]):
                next_ = tokens.pop(0)
                acc.append(next_.token)
            yield LITERAL("".join(acc))
        else:
            yield token


def is_unescaped_literal(token: Token) -> bool:
    """
    Return true if the token is an unescaped literal.
    """
    return isinstance(token, LITERAL) and not (
        token.token.startswith('"') or token.token.startswith("\\")
    )


def parse_date_time_pattern(
    value: str, pattern: str, class_: Type[DateTime]
) -> DateTime:
    """
    Parse a value using a given pattern.

    See https://developers.google.com/sheets/api/guides/formats?hl=en.
    """
    kwargs: Dict[str, Any] = {}
    tokens = list(tokenize(pattern))
    for token in tokens:
        consumed, value = token.parse(value, tokens)
        kwargs.update(**consumed)

    # add PM offset
    if "hour" in kwargs:
        hour_offset = kwargs.pop("hour_offset", 0)
        kwargs["hour"] += hour_offset

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
    parts = []
    tokens = list(tokenize(pattern))
    for token in tokens:
        parts.append(token.format(value, tokens))

    return "".join(parts)


if __name__ == "__main__":
    dt = datetime(2016, 4, 5, 16, 8, 53, 528000)

    assert format_date_time_pattern(dt, "h:mm:ss.00 a/p") == "4:08:53.53 p"
    assert parse_date_time_pattern("4:08:53.53 p", "h:mm:ss.00 a/p", time) == time(
        16, 8, 53, 530000
    )

    assert format_date_time_pattern(dt, 'hh:mm A/P".M."') == "04:08 P.M."
    assert parse_date_time_pattern("04:08 P.M.", 'hh:mm A/P".M."', time) == time(16, 8)

    assert format_date_time_pattern(dt, "yyyy-mm-dd") == "2016-04-05"
    assert parse_date_time_pattern("2016-04-05", "yyyy-mm-dd", date) == date(2016, 4, 5)

    # unsupported parse
    assert format_date_time_pattern(dt, r"mmmm d \[dddd\]") == "April 5 [Tuesday]"
    assert format_date_time_pattern(dt, "h PM, ddd mmm dd") == "4 PM, Tue Apr 05"

    assert (
        format_date_time_pattern(dt, "dddd, m/d/yy at h:mm")
        == "Tuesday, 4/5/16 at 16:08"
    )
    assert parse_date_time_pattern(
        "Tuesday, 4/5/16 at 16:08", "dddd, m/d/yy at h:mm", datetime
    ) == datetime(2016, 4, 5, 16, 8)

    td = timedelta(hours=3, minutes=13, seconds=41, microseconds=255000)

    assert format_date_time_pattern(td, "[hh]:[mm]:[ss].000") == "03:13:41.255"
    assert (
        parse_date_time_pattern("03:13:41.255", "[hh]:[mm]:[ss].000", timedelta) == td
    )

    assert format_date_time_pattern(td, "[mmmm]:[ss].000") == "0193:41.255"
    assert parse_date_time_pattern("0193:41.255", "[mmmm]:[ss].000", timedelta) == td
