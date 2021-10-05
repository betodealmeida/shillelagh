"""
Parse and format Google Sheet number formats.

https://developers.google.com/sheets/api/guides/formats#number_format_tokens
"""
# pylint: disable=c-extension-no-member
import re
from datetime import datetime
from datetime import time
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple
from typing import Union

from shillelagh.adapters.api.gsheets.parsing.base import InvalidValue
from shillelagh.adapters.api.gsheets.parsing.base import LITERAL
from shillelagh.adapters.api.gsheets.parsing.base import Token
from shillelagh.adapters.api.gsheets.parsing.base import tokenize


class ZERO(Token):
    """
    A digit in the number.

    If the digit is an insignificant 0, it is rendered as 0.
    """

    regex = "0+"

    def format(self, value: Union[datetime, time], tokens: List[Token]) -> str:
        raise NotImplementedError()

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        match = re.match(r"\d+", value)
        if not match:
            return {}, value

        size = len(match.group())

        # is this a fractional part?
        seen_self = False
        for token in tokens:
            if token is self:
                seen_self = True
            elif token.__class__.__name__ == "PERIOD" and not seen_self:
                part = get_fraction(int(value[:size]))
                break
        else:
            part = int(value[:size])

        return {"operation": lambda number: number + part}, value[size:]


class HASH(ZERO):
    """
    A digit in the number.

    If the digit is an insignificant 0, it is not rendered.
    """

    regex = "#+"

    def format(self, value: Union[datetime, time], tokens: List[Token]) -> str:
        raise NotImplementedError()


class QUESTION(ZERO):
    """
    A digit in the number.

    If the digit is an insignificant 0, it is rendered as a space.
    """

    regex = r"\?+"

    def format(self, value: Union[datetime, time], tokens: List[Token]) -> str:
        raise NotImplementedError()


class PERIOD(Token):
    """
    The decimal point in the number.

    The first period represents the decimal point in the number. Subsequent
    periods are rendered as literals. If you include a decimal point in the
    format, it will always be rendered, even for whole numbers.
    """

    regex = r"\."

    def format(self, value: Union[datetime, time], tokens: List[Token]) -> str:
        raise NotImplementedError()

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        match = re.match(r"\.", value)
        if not match:
            raise InvalidValue(value)

        return {}, value[1:]


class PERCENT(Token):
    """
    Literal percent sign.

    Appears as a literal but also causes existing numbers to be multiplied
    by 100 before being rendered, in order to make percentages more readable.
    """

    regex = "%"

    def format(self, value: Union[datetime, time], tokens: List[Token]) -> str:
        raise NotImplementedError()

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        match = re.match("%", value)
        if not match:
            raise InvalidValue(value)

        return {"operation": lambda number: number / 100.0}, value[1:]


class COMMA(Token):
    """
    Group separator or multiplier.

    If it appears between two digit characters (0, # or ?), then it renders
    the entire number with grouping separators (grouping by the thousands).
    If it follows the digit characters, it scales the digits by one thousand
    per comma.
    """

    regex = ",+"

    def format(self, value: Union[datetime, time], tokens: List[Token]) -> str:
        raise NotImplementedError()

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        seen_self = False
        is_multiplier = True
        for token in tokens:
            if token is self:
                seen_self = True
            elif token.__class__.__name__ in {"ZERO", "HASH", "QUESTION"} and seen_self:
                is_multiplier = False

        if is_multiplier:
            size = len(self.token)
        else:
            match = re.match(",*", value)
            size = len(match.group()) if match else 0

        return {"operation": lambda number: number * 1000 ** size}, value[size:]


class E(Token):  # pylint: disable=invalid-name
    """
    Scientific format.

    Renders the number in scientific format, with the formatting to the left of the E
    used for the non-exponent portion and the formatting to the right of the E used for
    the exponent portion. E+ will show a + sign for positive exponents. E- will only
    show a sign for negative exponents. If lowercase is used, the output e is
    lowercased as well.
    """

    regex = r"(E|e)(-|\+)"

    def format(self, value: Union[datetime, time], tokens: List[Token]) -> str:
        raise NotImplementedError()

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        match = re.match(r"(E|e)((\+|-)?\d+)", value)
        if not match:
            raise InvalidValue(value)

        exponent = int(match.group(2))
        size = len(match.group())

        return {"operation": lambda number: number * 10 ** exponent}, value[size:]


class FRACTION(Token):
    """
    Fractional format.

    If it appears between two digit characters (0, # or ?), then it treats those digit
    groups as a fractional format. For example, the number format 0 #/# renders the
    number 23.25 as 23 1/4. The denominator can also be a literal integer, in which case
    it will enforce that integer as the denominator. The number format 0 #/8 displays
    the number 23.25 as 23 2/8. The fraction part is not rendered at all if the
    numerator would become 0. The number 23.1 with the number format 0 #/3 renders as
    just 23 (because the 0.1 rounded to 0/3). / is not compatible with scientific format
    or a format with a decimal point in it.
    """

    regex = r"(0+/0+)|(#+/#+)|(\?+/\?+)|(0+/\d+)|(#+/\d+)|(\?+/\d+)"

    def format(self, value: Union[datetime, time], tokens: List[Token]) -> str:
        raise NotImplementedError()

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        match = re.match(r"(\d+)/(\d+)", value)
        if not match:
            # no numbers due to rounding
            return {}, value

        numerator = int(match.group(1))
        denominator = int(match.group(2))
        size = len(match.group())

        return {"operation": lambda number: number + (numerator / denominator)}, value[
            size:
        ]


class STAR(Token):
    """
    This is included for compatibility with Excel number formats. It is currently ignored.
    """

    regex = r"\*"

    def format(self, value: Union[datetime, time], tokens: List[Token]) -> str:
        raise NotImplementedError()

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        return {}, value


class UNDERSCORE(Token):
    """
    Skips the next character and renders a space.

    This is used to line up number formats where the negative value is surrounded
    by parenthesis.
    """

    regex = "_."

    def format(self, value: Union[datetime, time], tokens: List[Token]) -> str:
        raise NotImplementedError()

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        return {}, value[1:]


class AT(Token):
    """
    Inserts the raw text for the cell, if the cell has text input.

    Not compatible with any of the other special characters and won’t display for
    numeric values (which are displayed as general format).
    """

    regex = "@"

    def format(self, value: Union[datetime, time], tokens: List[Token]) -> str:
        raise NotImplementedError()

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        raise NotImplementedError()


class COLOR(Token):
    """
    Causes any value that is rendered by this sub-format to appear with the given text color.

    Valid values for Color are Black, Blue, Cyan, Green, Magenta, Red, White or Yellow.
    Valid values for the "#" in Color# are 0 - 56 (this color palette shows a list of the
    colors that correspond to each number). Number format colors will override any
    user-entered colors on the cell, but will not override colors set by conditional
    formatting.
    """

    regex = r"\[(Black|Blue|Cyan|Green|Magenta|Red|White|Yellow|\d{1,2})\]"

    def format(self, value: Union[datetime, time], tokens: List[Token]) -> str:
        raise NotImplementedError()

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        return {}, value


def get_fraction(number: int) -> float:
    """
    Return the fraction associated with a fractional part.

        >>> get_fraction(9)
        0.9
        >>> get_fraction(123)
        0.123
        >>> get_fraction(1)
        0.1

    """
    if number < 0:
        raise Exception("Number should be a positive integer")

    # we could do this analytically, but there are too many edge
    # cases (0 and 10^n, eg)
    result = float(number)
    while result >= 1:
        result /= 10

    return result


def parse_number_pattern(value: str, pattern: str) -> float:
    """
    Parse a value using a given pattern.
    """
    formats = pattern.split(";")

    i = -1
    for i, format_ in enumerate(formats):
        try:
            number = parse_number_format(value, format_)
            break
        except InvalidValue:
            pass
    else:
        raise Exception(f"Unable to parse value {value} with pattern {pattern}")

    # is negative?
    if i == 1 and (len(formats) > 2 or (len(formats) == 2 and "@" not in formats[1])):
        return number * -1

    return number


def parse_number_format(value: str, format_: str) -> float:
    """
    Parse a value using a given format pattern.
    """
    classes = [
        FRACTION,  # should come first
        ZERO,
        HASH,
        QUESTION,
        PERIOD,
        PERCENT,
        COMMA,
        E,
        STAR,
        UNDERSCORE,
        AT,
        COLOR,
        LITERAL,
    ]

    number = 0

    tokens = list(tokenize(format_, classes))
    for token in tokens:
        consumed, value = token.parse(value, tokens)
        if "operation" in consumed:
            number = consumed["operation"](number)

    return number
