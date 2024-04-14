"""
Parse and format Google Sheet number formats.

https://developers.google.com/sheets/api/guides/formats#number_format_tokens
"""

# pylint: disable=c-extension-no-member, broad-exception-raised
import math
import operator
import re
from itertools import zip_longest
from typing import Any, Dict, Iterator, List, Tuple, Union, cast

from shillelagh.adapters.api.gsheets.parsing.base import (
    LITERAL,
    InvalidValue,
    Token,
    tokenize,
)


def adjust_value(value: Union[int, float], tokens: List[Token]) -> Union[int, float]:
    """
    Adjust value applying percent, scientific notation, and comma multiplier.
    """
    for token in tokens:
        if token.__class__.__name__ == "PERCENT":
            value *= 100
        elif token.__class__.__name__ == "COMMA":
            value /= 1000 ** len(token.token)
        elif token.__class__.__name__ == "E":
            while value > 10:
                value /= 10
            while value < 1:
                value *= 10

    return value


class DIGITS(Token):
    """
    One or more digits in the number.

    Each token renders digits differently:

    - 0: if the digit is an insignificant 0, it is rendered as 0.
    - #: if the digit is an insignificant 0, it is not rendered.
    - ?: if the digit is an insignificant 0, it is rendered as a space.

    If there's a comma between tokens it renders the entire number with grouping
    separators (grouping by the thousands)
    """

    regex = r"(0|#|\?)+(,(0|#|\?){3})?"

    def format(  # pylint: disable=too-many-branches
        self,
        value: Union[int, float],
        tokens: List[Token],
    ) -> str:
        value = adjust_value(value, tokens)
        number = str(value)

        formatted: List[str] = []
        if is_fractional(self, tokens):
            number = number.split(".")[1] if "." in number else "0"
            for token, digit in zip_longest(self.token, number):
                if token is None:
                    break
                if token == "0":
                    formatted.append(digit or "0")
                elif token == "#":
                    formatted.append(digit or "")
                elif token == "?":
                    formatted.append(digit or " ")
                else:
                    raise Exception(f"Invalid token: {token}")
            return "".join(formatted)

        number = number.split(".", maxsplit=1)[0]
        has_comma = "," in self.token
        token = self.token.replace(",", "")

        for i, (token, digit) in enumerate(zip_longest(token[::-1], number[::-1])):
            if token is None:
                formatted.append(digit)
            elif token == "0":
                formatted.append(digit or "0")
            elif token == "#":
                formatted.append(digit or "")
            elif token == "?":
                formatted.append(digit or " ")
            else:
                raise Exception(f"Invalid token: {token}")

            # group by thousands
            if has_comma and (i + 1) % 3 == 0:
                formatted.append(",")

        return "".join(formatted[::-1]).strip(",")

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        match = re.match(r"\s*(\+|-)?\d+(,\d+)*", value)
        if not match:
            raise InvalidValue(value)

        digits = match.group().replace(",", "")
        if is_fractional(self, tokens):
            return (
                {
                    "operation": lambda number: math.copysign(
                        abs(number) + get_fraction(digits),
                        number,
                    ),
                },
                value[len(match.group()) :],
            )

        return (
            {"operation": lambda number: number + int(digits)},
            value[len(match.group()) :],
        )


class PERIOD(Token):
    """
    The decimal point in the number.

    The first period represents the decimal point in the number. Subsequent
    periods are rendered as literals. If you include a decimal point in the
    format, it will always be rendered, even for whole numbers.
    """

    regex = r"\."

    def format(self, value: Union[int, float], tokens: List[Token]) -> str:
        return "."

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

    def format(self, value: Union[int, float], tokens: List[Token]) -> str:
        return "%"

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        match = re.match("%", value)
        if not match:
            raise InvalidValue(value)

        return {"operation": lambda number: number / 100.0}, value[1:]


class COMMA(Token):
    """
    Multiplier.

    When appearing after the digit characters the comma scales the digits by one thousand
    per comma.
    """

    regex = ",+"

    def format(self, value: Union[int, float], tokens: List[Token]) -> str:
        return ""

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        size = len(self.token)
        return {"operation": lambda number: number * 1000**size}, value


class E(Token):  # pylint: disable=invalid-name
    """
    Scientific format.

    Renders the number in scientific format, with the formatting to the left of the E
    used for the non-exponent portion and the formatting to the right of the E used for
    the exponent portion. E+ will show a + sign for positive exponents. E- will only
    show a sign for negative exponents. If lowercase is used, the output e is
    lowercased as well.
    """

    regex = r"(E|e)(-|\+)((0|#|\?)+)"

    def format(self, value: Union[int, float], tokens: List[Token]) -> str:
        exponent = 0

        if value >= 1:
            sign = "+" if "+" in self.token else ""
            while value > 10:
                value /= 10
                exponent += 1
        else:
            sign = "-"
            while value < 1:
                value *= 10
                exponent += 1

        match = re.match(self.regex, self.token)
        if not match:
            # should never happen
            raise Exception("You are likely to be eaten by a grue.")
        cased_e = match.group(1)

        # process the exponent according to the pattern
        pattern = match.group(3)
        formatted_exponent = format_number_pattern(exponent, pattern)

        return f"{cased_e}{sign}{formatted_exponent}"

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        match = re.match(r"(E|e)((\+|-)?\d+)", value)
        if not match:
            raise InvalidValue(value)

        exponent = int(match.group(2))
        size = len(match.group())

        return {"operation": lambda number: number * 10**exponent}, value[size:]


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

    regex = r"(?:((?:0|#|\?)+))/(?:((?:0|#|\?)+)|(\d+))"

    def format(self, value: Union[int, float], tokens: List[Token]) -> str:
        fractional_part = float(value - math.floor(value))
        numerator: Union[int, float]
        numerator, denominator = fractional_part.as_integer_ratio()

        # force denominator?
        match = re.match(self.regex, self.token)
        if not match:
            # should never happen
            raise Exception("You are likely to be eaten by a grue.")
        groups = match.groups()
        if groups[2] is not None:
            numerator *= int(groups[2]) / denominator
            formatted_denominator = groups[2]
        else:
            # the denominator needs to be formatted as a fraction, with spaces and zeros
            # right padded
            pattern = "." + groups[1]
            number = get_fraction(str(denominator))
            formatted_denominator = format_number_pattern(number, pattern).lstrip(".")

        if math.floor(numerator) == 0:
            return ""

        formatted_numerator = format_number_pattern(numerator, groups[0])

        return f"{formatted_numerator}/{formatted_denominator}"

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        match = re.match(r"\s*(\d+)/(\d+)", value)
        if not match:
            # fractions with numerator 0 are not shown
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

    def format(self, value: Union[int, float], tokens: List[Token]) -> str:
        return ""

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        return {}, value


class UNDERSCORE(Token):
    """
    Skips the next character and renders a space.

    This is used to line up number formats where the negative value is surrounded
    by parenthesis.
    """

    regex = "_."

    def format(self, value: Union[int, float], tokens: List[Token]) -> str:
        return " "

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        if value[:1] != " ":
            raise InvalidValue(value[:1])

        return {}, value[1:]


class AT(Token):
    """
    Inserts the raw text for the cell, if the cell has text input.

    Not compatible with any of the other special characters and won’t display for
    numeric values (which are displayed as general format).
    """

    regex = "@"

    def format(self, value: Union[int, float, str], tokens: List[Token]) -> str:
        return str(value)

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        i = tokens.index(self)

        if i == len(tokens) - 1:
            # last token, consume everything
            return {"operation": lambda number: value}, ""

        # LL(1) to see how much can be consumed
        sibling = tokens[i + 1]
        j = 0
        for j in range(len(value)):
            try:
                sibling.parse(value[j:], tokens)
                break
            except InvalidValue:
                continue
        else:
            raise InvalidValue(value)

        return {"operation": lambda number: value[:j]}, value[j:]


class COLOR(Token):
    """
    Causes any value that is rendered by this sub-format to appear with the given text color.

    Valid values for Color are Black, Blue, Cyan, Green, Magenta, Red, White or Yellow.
    Valid values for the "#" in Color# are 0 - 56 (this color palette shows a list of the
    colors that correspond to each number). Number format colors will override any
    user-entered colors on the cell, but will not override colors set by conditional
    formatting.
    """

    regex = r"\[(Black|Blue|Cyan|Green|Magenta|Red|White|Yellow|Color\d{1,2})\]"

    def format(self, value: Union[int, float], tokens: List[Token]) -> str:
        return ""

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        return {}, value


class CONDITION(Token):
    """
    Replaces the default comparison section with another conditional expression.

    Replaces the default positive, negative, or zero comparison section with another
    conditional expression. For example, [<100]”Low”;[>1000]”High”;000 will render the
    word “Low” for values below 100, “High” for values above 1000 and a three digit
    number (with leading 0s) for anything in between. Conditions can only be applied to
    the first two sub-formats and if a number matches more than one, it will use the
    first one it matches. If there is a third format, it will be used for "everything
    else", otherwise if a number doesn’t match either format, it will be rendered as all
    "#"s filling up the cell width. The fourth format is always used for text, if it
    exists.
    """

    regex = r"\[(>|>=|<|<=|=)\d*(\.\d*)?\]"

    def format(self, value: Union[int, float], tokens: List[Token]) -> str:
        return ""

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        return {}, value


def get_fraction(number: str) -> float:
    """
    Return the fraction associated with a fractional part.

        >>> get_fraction("9")
        0.9
        >>> get_fraction("123")
        0.123
        >>> get_fraction("1")
        0.1
        >>> get_fraction("001")
        0.001

    """
    if int(number) < 0:
        raise Exception("Number should be a positive integer")

    return cast(float, int(number) / (10 ** len(number)))


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
            # weird edge case
            if value.startswith("-"):
                try:
                    number = -parse_number_pattern(value[1:], format_)
                    break
                except InvalidValue:
                    pass
    else:
        try:
            return int(value)
        except ValueError:
            try:
                return float(value)
            except ValueError as ex:
                raise InvalidValue(
                    f"Unable to parse value {value} with pattern {pattern}",
                ) from ex

    # is negative?
    if i == 1 and (len(formats) > 2 or (len(formats) == 2 and "@" not in formats[1])):
        return -number

    return number


def fix_periods(tokens: Iterator[Token]) -> Iterator[Token]:
    """
    Convert periods into literals after the first one.
    """
    seen_period = False
    for token in tokens:
        if token.__class__.__name__ == "PERIOD":
            yield LITERAL(".") if seen_period else token
            seen_period = True
        else:
            yield token


def parse_number_format(value: str, format_: str) -> float:
    """
    Parse a value using a given format pattern.
    """
    classes = [
        FRACTION,  # should come first
        DIGITS,
        COMMA,
        PERIOD,
        PERCENT,
        E,
        STAR,
        UNDERSCORE,
        AT,
        COLOR,
        CONDITION,
        LITERAL,
    ]

    number = 0

    tokens = list(fix_periods(tokenize(format_, classes)))
    for token in tokens:
        consumed, value = token.parse(value, tokens)
        if "operation" in consumed:
            number = consumed["operation"](number)

    return number


def has_condition(pattern: str) -> bool:
    """
    Return true if the pattern has condition instructions.
    """
    return bool(re.match(r"\[(>|>=|<|<=|=)\d*(\.\d*)?\]", pattern))


def condition_matches(value: Union[int, float], format_: str) -> bool:
    """
    Return true if the value matches the condition in the pattern.
    """
    match = re.search(r"\[(>|>=|<|<=|=)(\d*(?:\.\d*)?)\]", format_)
    if not match:
        return True

    operators = {
        ">": operator.gt,
        ">=": operator.ge,
        "<": operator.lt,
        "<=": operator.le,
        "=": operator.eq,
    }
    comparison, threshold = match.groups()
    op = operators[comparison]  # pylint: disable=invalid-name
    return bool(op(float(value), float(threshold)))


def format_number_pattern(  # pylint: disable=too-many-branches
    value: Union[int, float, str],
    pattern: str,
) -> str:
    """
    Format a number to a given pattern.
    """
    formats = pattern.split(";")

    if not any(format_ for format_ in formats):
        raise Exception("Empty pattern!")

    if isinstance(value, str):
        for format_ in formats:
            if "@" in format_:
                break
        else:
            raise Exception("No text format found for string value")

    elif has_condition(pattern):
        for format_ in formats:
            if condition_matches(value, format_):
                break
        else:
            format_ = '"########"'

    else:
        # remove any text format
        formats = [format_ for format_ in formats if "@" not in format_]

        if value > 0:
            format_ = formats[0]
        elif value == 0:
            format_ = formats[2] if len(formats) == 3 else formats[0]
        else:
            if len(formats) >= 2:
                format_ = formats[1]
                value *= -1
            else:
                format_ = formats[0]

    classes = [
        FRACTION,  # should come first
        DIGITS,
        COMMA,
        PERIOD,
        PERCENT,
        E,
        STAR,
        UNDERSCORE,
        AT,
        COLOR,
        CONDITION,
        LITERAL,
    ]

    parts = []

    tokens = list(fix_periods(tokenize(format_, classes)))
    for token in tokens:
        parts.append(token.format(value, tokens))

    return "".join(parts)


def is_fractional(token: Token, tokens: List[Token]) -> bool:
    """
    Return true if the token is after the period.
    """
    seen_self = False
    for sibling in tokens:
        if sibling is token:
            seen_self = True
        elif isinstance(sibling, PERIOD) and not seen_self:
            return True
    return False
