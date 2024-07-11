"""
Token for parsing date and time.
"""

import re
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, Generic, Iterator, List, Tuple, Type, TypeVar

Valid = TypeVar("Valid", datetime, date, time, timedelta, str, int, float)


class Token(Generic[Valid]):
    """
    A token.
    """

    regex: str

    def __init__(self, token: str):
        self.token = token

    @classmethod
    def match(
        cls,
        pattern: str,
        history: List["Token"],  # pylint: disable=unused-argument
    ) -> bool:
        """
        Check if token handles the beginning of the pattern.
        """
        return bool(re.match(cls.regex, pattern))

    @classmethod
    def consume(
        cls,
        pattern: str,
        history: List["Token"],  # pylint: disable=unused-argument
    ) -> Tuple["Token", str]:
        """
        Consume the pattern, returning the token and the remaining pattern.
        """
        match = re.match(cls.regex, pattern)
        if not match:
            # pylint: disable=broad-exception-raised
            raise Exception("Token could not find match")
        token = match.group()
        return cls(token), pattern[len(token) :]

    def format(self, value: Valid, tokens: List["Token"]) -> str:
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

    def __eq__(self, other) -> bool:
        if other.__class__ != self.__class__:
            return NotImplemented

        return bool(self.token == other.token)


class LITERAL(Token):
    """
    A literal.
    """

    regex = r'(\\.)|(".*?")|(.)'

    def format(
        self,
        value: Valid,
        tokens: List[Token],
    ) -> str:
        if self.token.startswith("\\"):
            return self.token[1:]
        if self.token.startswith('"'):
            return self.token[1:-1]
        return self.token

    def parse(self, value: str, tokens: List[Token]) -> Tuple[Dict[str, Any], str]:
        if self.token.startswith("\\"):
            size = 1
            if not value[:size] == self.token[1:]:
                raise InvalidValue(value)
        elif self.token.startswith('"'):
            size = len(self.token) - 2
            if not value[:size] == self.token[1:-1]:
                raise InvalidValue(value)
        else:
            size = len(self.token)
            if not value[:size] == self.token:
                raise InvalidValue(value)
        return {}, value[size:]


def tokenize(pattern: str, classes: List[Type[Token]]) -> Iterator[Token]:
    """
    Tokenize a pattern.
    """
    tokens: List[Token] = []
    while pattern:
        for class_ in classes:  # pragma: no cover
            if class_.match(pattern, tokens):
                token, pattern = class_.consume(pattern, tokens)
                tokens.append(token)
                break
        else:
            raise InvalidPattern(f'Could not consume "{pattern}"')

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


class InvalidPattern(Exception):
    """
    Raised when the pattern can't be consumed.
    """


class InvalidValue(Exception):
    """
    Raised when the value can't be consumed.
    """
