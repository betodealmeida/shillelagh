"""
Token for parsing date and time.
"""
import re
from datetime import date
from datetime import datetime
from datetime import time
from datetime import timedelta
from typing import Any
from typing import Dict
from typing import Generic
from typing import List
from typing import Tuple
from typing import TypeVar

DateTime = TypeVar("DateTime", datetime, date, time, timedelta)


class Token(Generic[DateTime]):
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
        return bool(re.match(cls.regex, pattern))

    @classmethod
    def consume(cls, pattern: str) -> Tuple["Token", str]:
        """
        Consume the pattern, returning the token and the remaining pattern.
        """
        match = re.match(cls.regex, pattern)
        if not match:
            raise Exception("Token could not find match")
        token = match.group()
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

    def __eq__(self, other) -> bool:
        if other.__class__ != self.__class__:
            return NotImplemented

        return bool(self.token == other.token)
