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
from pathlib import Path
from typing import List, Tuple

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

    session = PromptSession(
        lexer=PygmentsLexer(SqlLexer),
        completer=sql_completer,
        style=style,
        history=FileHistory(history_path),
    )

    lines: List[str] = []
    quote_context = " "
    while True:
        prompt = "sql> " if not lines else f"  {quote_context}. "
        try:
            line = session.prompt(prompt)
        except KeyboardInterrupt:
            lines = []
            quote_context = " "
            continue  # Control-C pressed. Clear and try again.
        except EOFError:
            break  # Control-D pressed.

        lines.append(line)
        query = "\n".join(lines)

        is_terminated, quote_context = get_query_termination(query)
        if is_terminated:
            results = None
            try:
                cursor.execute(query)
                results = cursor.fetchall()
            except Error as ex:
                print(ex)
                continue
            finally:
                lines = []
                quote_context = " "

            headers = [t[0] for t in cursor.description or []]
            print(tabulate(results, headers=headers))

    connection.close()
    print("GoodBye!")


def get_query_termination(query: str) -> Tuple[bool, str]:
    """
    Check if a query is ended or if a new line should be created.

    This function looks for a semicolon at the end, making sure no quotation mark must be
    closed.
    """
    quote_context = " "
    quote_chars = ('"', "'", "`")

    for query_char in query:
        if quote_context == query_char:
            quote_context = " "
        else:
            for quote in quote_chars:
                if quote_context == " " and quote == query_char:
                    quote_context = quote

    is_terminated = quote_context == " " and query.endswith(";")

    return is_terminated, quote_context


if __name__ == "__main__":
    main()
