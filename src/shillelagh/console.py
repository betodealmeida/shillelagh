#!/usr/bin/env python
"""
A simple REPL for Shillelagh.

To run the REPL, since run ``shillelagh``. Pressing return will execute the
query immediately, and multi-line queries are currently not supported.

Connection arguments can be passed via a ``shillelagh.yaml`` file located in the users
application directory (see https://pypi.org/project/appdirs/), eg::

    gsheestapi:
      service_account_file: /path/to/credentials.json
      subject: user@example.com
      catalog:
        # allows writing ``SELECT * FROM my_sheet``
        my_sheet:  https://docs.google.com/spreadsheets/d/1/edit#gid=0
    weatherapi:
      api_key: XXX

"""
import logging
import os.path
import sys
import time
from pathlib import Path
from typing import Iterable, Iterator, Optional

import yaml
from appdirs import user_config_dir
from prompt_toolkit import PromptSession
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.history import FileHistory
from prompt_toolkit.lexers import PygmentsLexer
from prompt_toolkit.styles.pygments import style_from_pygments_cls
from pygments.lexers.sql import SqlLexer
from pygments.styles import get_style_by_name
from tabulate import tabulate

from shillelagh.backends.apsw.db import connect
from shillelagh.exceptions import Error

_logger = logging.getLogger(__name__)

sql_completer = WordCompleter(
    [
        "ABORT",
        "ACTION",
        "ADD",
        "AFTER",
        "ALL",
        "ALTER",
        "ANALYZE",
        "AND",
        "AS",
        "ASC",
        "ATTACH",
        "AUTOINCREMENT",
        "BEFORE",
        "BEGIN",
        "BETWEEN",
        "BY",
        "CASCADE",
        "CASE",
        "CAST",
        "CHECK",
        "COLLATE",
        "COLUMN",
        "COMMIT",
        "CONFLICT",
        "CONSTRAINT",
        "CREATE",
        "CROSS",
        "CURRENT_DATE",
        "CURRENT_TIME",
        "CURRENT_TIMESTAMP",
        "DATABASE",
        "DEFAULT",
        "DEFERRABLE",
        "DEFERRED",
        "DELETE",
        "DESC",
        "DETACH",
        "DISTINCT",
        "DROP",
        "EACH",
        "ELSE",
        "END",
        "ESCAPE",
        "EXCEPT",
        "EXCLUSIVE",
        "EXISTS",
        "EXPLAIN",
        "FAIL",
        "FOR",
        "FOREIGN",
        "FROM",
        "FULL",
        "GET_METADATA",
        "GLOB",
        "GROUP",
        "HAVING",
        "IF",
        "IGNORE",
        "IMMEDIATE",
        "IN",
        "INDEX",
        "INDEXED",
        "INITIALLY",
        "INNER",
        "INSERT",
        "INSTEAD",
        "INTERSECT",
        "INTO",
        "IS",
        "ISNULL",
        "JOIN",
        "KEY",
        "LEFT",
        "LIKE",
        "LIMIT",
        "MATCH",
        "NATURAL",
        "NO",
        "NOT",
        "NOTNULL",
        "NULL",
        "OF",
        "OFFSET",
        "ON",
        "OR",
        "ORDER",
        "OUTER",
        "PLAN",
        "PRAGMA",
        "PRIMARY",
        "QUERY",
        "RAISE",
        "RECURSIVE",
        "REFERENCES",
        "REGEXP",
        "REINDEX",
        "RELEASE",
        "RENAME",
        "REPLACE",
        "RESTRICT",
        "RIGHT",
        "ROLLBACK",
        "ROW",
        "SLEEP",
        "SAVEPOINT",
        "SELECT",
        "SET",
        "TABLE",
        "TEMP",
        "TEMPORARY",
        "THEN",
        "TO",
        "TRANSACTION",
        "TRIGGER",
        "UNION",
        "UNIQUE",
        "UPDATE",
        "USING",
        "VACUUM",
        "VALUES",
        "VERSION",
        "VIEW",
        "VIRTUAL",
        "WHEN",
        "WHERE",
        "WITH",
        "WITHOUT",
    ],
    ignore_case=True,
)

style = style_from_pygments_cls(get_style_by_name("friendly"))
quote_chars = ('"', "'", "`")


def emit_statements(lines: Iterable[str]) -> Iterator[str]:
    """
    Consume lines and emit complete statements.
    """
    quote_context: Optional[str] = None

    rest = ""
    for line in lines:
        start = 0
        for pos, char in enumerate(line):
            if quote_context is not None and char == quote_context:
                # leave context
                quote_context = None
            elif quote_context is None and char == ";":
                yield (rest + line[start:pos]).strip()
                rest = ""
                start = pos + 1
            else:
                for quote in quote_chars:
                    if quote_context is None and char == quote:
                        # enter context
                        quote_context = quote

        rest += line[start:] + "\n"


def repl(session: PromptSession) -> Iterator[str]:
    """
    Yield lines.
    """
    quote_context: Optional[str] = None

    start = True
    while True:
        if start:
            prompt = "🍀> "
        elif quote_context is None:
            prompt = "  . "
        else:
            prompt = f" {quote_context}. "

        try:
            line = session.prompt(prompt)
            yield line
        except KeyboardInterrupt:
            continue  # Control-C pressed. Clear and try again.
        except EOFError:
            break  # Control-D pressed.

        quote_context = update_quote_context(line, quote_context)
        start = quote_context is None and line.strip().endswith(";")


def update_quote_context(line: str, quote_context: Optional[str]) -> Optional[str]:
    """
    Update the quote context.

    Inside single quotes, inside double quotes, neither.
    """
    for char in line:
        if quote_context is not None and char == quote_context:
            # leave context
            quote_context = None
        else:
            for quote in quote_chars:
                if quote_context is None and char == quote:
                    # enter context
                    quote_context = quote

    return quote_context


def main():  # pylint: disable=too-many-locals
    """
    Run a REPL until the user presses Control-D.
    """
    # read args from config file
    config_dir = Path(user_config_dir("shillelagh"))
    if not config_dir.exists():
        config_dir.mkdir(parents=True)

    config_path = config_dir / "shillelagh.yaml"
    history_path = config_dir / "shillelagh.history"

    adapter_kwargs = {}
    if os.path.exists(config_path):
        try:
            with open(config_path, encoding="utf-8") as stream:
                adapter_kwargs = yaml.load(stream, Loader=yaml.SafeLoader)
        except (PermissionError, yaml.parser.ParserError, yaml.scanner.ScannerError):
            _logger.exception("Unable to load configuration file")

    connection = connect(":memory:", adapter_kwargs=adapter_kwargs)
    cursor = connection.cursor()

    # non-interactive
    if not sys.stdin.isatty():
        for query in emit_statements(sys.stdin.readlines()):
            cursor.execute(query)
            results = cursor.fetchall()
            headers = [t[0] for t in cursor.description or []]
            sys.stdout.write(tabulate(results, headers=headers))
            sys.stdout.write("\n")
        return

    session = PromptSession(
        lexer=PygmentsLexer(SqlLexer),
        completer=sql_completer,
        style=style,
        history=FileHistory(history_path),
    )

    for query in emit_statements(repl(session)):
        start = time.time()
        results = None
        try:
            cursor.execute(query)
            results = cursor.fetchall()
        except Error as ex:
            print(ex)
            continue

        headers = [t[0] for t in cursor.description or []]
        print(tabulate(results, headers=headers))
        duration = time.time() - start
        print(
            f"({len(results)} row{'s' if len(results) != 1 else ''} "
            f"in {duration:.2f}s)\n",
        )

    connection.close()
    print("GoodBye!")


if __name__ == "__main__":
    main()
