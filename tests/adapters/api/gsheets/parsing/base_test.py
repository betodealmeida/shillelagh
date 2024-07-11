"""
Test the base parser/tokenizer.
"""

# pylint: disable=protected-access
from datetime import datetime

import pytest

from shillelagh.adapters.api.gsheets.parsing.base import (
    LITERAL,
    InvalidPattern,
    InvalidValue,
    is_unescaped_literal,
    tokenize,
)
from shillelagh.adapters.api.gsheets.parsing.date import DD, MM, YYYY


def test_literal_token() -> None:
    """
    Test the literal token.
    """
    classes = [
        DD,
        MM,
        YYYY,
        LITERAL,
    ]

    assert LITERAL.match(r"\d", [])
    assert LITERAL.match('"dd/mm/yy"', [])
    # matches eveything
    assert LITERAL.match("d", [])

    token = LITERAL("@")
    tokens = list(tokenize("@", classes))
    assert token.format(datetime(2021, 11, 12, 13, 14, 15, 16), tokens) == "@"

    token = LITERAL('"dd/mm/yy"')
    tokens = list(tokenize('"dd/mm/yy"', classes))
    assert token.format(datetime(2021, 11, 12, 13, 14, 15, 16), tokens) == "dd/mm/yy"

    token = LITERAL('"invalid"')
    assert token.parse("invalid", tokens) == ({}, "")

    token = LITERAL(r"\d")
    assert token.parse("d", tokens) == ({}, "")

    token = LITERAL('"test"')
    with pytest.raises(InvalidValue) as excinfo:
        token.parse("invalid", tokens)
    assert str(excinfo.value) == "invalid"

    token = LITERAL("A")
    with pytest.raises(InvalidValue) as excinfo:
        token.parse("B", tokens)
    assert str(excinfo.value) == "B"

    token = LITERAL(r"\.")
    with pytest.raises(InvalidValue) as excinfo:
        token.parse("B", tokens)
    assert str(excinfo.value) == "B"


def test_tokenize() -> None:
    """
    Test the tokenize function.
    """
    classes = [
        DD,
        MM,
        YYYY,
        LITERAL,
    ]
    tokens = list(tokenize('dd/mm/yyyy -> ("dd/mm/yyyy")', classes))
    assert tokens == [
        DD("dd"),
        LITERAL("/"),
        MM("mm"),
        LITERAL("/"),
        YYYY("yyyy"),
        LITERAL(" -> ("),
        LITERAL('"dd/mm/yyyy"'),
        LITERAL(")"),
    ]

    with pytest.raises(InvalidPattern) as excinfo:
        list(tokenize("dd/mm/yyyy", []))
    assert str(excinfo.value) == 'Could not consume "dd/mm/yyyy"'


def test_is_unescaped_literal() -> None:
    """
    Test the is_unescaped_literal function.
    """
    assert is_unescaped_literal(LITERAL("a"))
    assert not is_unescaped_literal(LITERAL(r"\d"))
    assert not is_unescaped_literal(LITERAL('"hello"'))
    assert not is_unescaped_literal(YYYY("yyyy"))
