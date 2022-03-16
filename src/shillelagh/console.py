#!/usr/bin/env python
"""
A simple REPL for Shillelagh.

To run the REPL, since run ``shillelagh``. Pressing return will execute the
query immediately, and multi-line queries are currently not supported.

Connection arguments can be passed via a ``~/.config/shillelagh/shillelagh.yaml`` file, eg::

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

import yaml
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


def main():
    """
    Run a REPL until the user presses Control-D.
    """
    # read args from config file
    config = os.path.expanduser("~/.config/shillelagh/shillelagh.yaml")
    adapter_kwargs = {}
    if os.path.exists(config):
        try:
            with open(config, encoding="utf-8") as stream:
                adapter_kwargs = yaml.load(stream, Loader=yaml.SafeLoader)
        except (PermissionError, yaml.parser.ParserError, yaml.scanner.ScannerError):
            _logger.exception("Unable to load configuration file")

    connection = connect(":memory:", adapter_kwargs=adapter_kwargs)
    cursor = connection.cursor()

    history_dir = os.path.expanduser(
        os.sep.join(("~", ".config", "shillelagh"))
    )
    if not os.path.exists(history_dir):
        os.makedirs(history_dir)

    history_path = os.sep.join((history_dir, "shillelagh.history"))

    session = PromptSession(
        lexer=PygmentsLexer(SqlLexer),
        completer=sql_completer,
        style=style,
        history=FileHistory(history_path),
    )

    query = ""
    quote_context = None
    while True:
        try:
            sql = session.prompt(
                "sql> " if len(query) == 0 else
                f"  {quote_context if quote_context is not None else ' '}. "
            ).strip()
        except KeyboardInterrupt:
            query = ""
            quote_context = None
            continue  # Control-C pressed. Clear and try again.
        except EOFError:
            break  # Control-D pressed.

        if sql:
            query += f"\n{sql}"
            is_terminated, quote_context = query_termination(query)
            if is_terminated:
                results = None
                try:
                    cursor.execute(query)
                    results = cursor.fetchall()
                except Error as ex:
                    print(ex)
                    continue
                finally:
                    query = ""

                headers = [t[0] for t in cursor.description or []]
                print(tabulate(results, headers=headers))

    connection.close()
    print("GoodBye!")


def query_termination(query: str) -> (bool, str):
    """
    Check if a query is ended or if a new line should be created
    by looking for a semicolon at the end and making
    sure no quotation mark must be closed

    Returns a tuple :
        -True if the query is terminated
        -A quotation character that appears to need to be closed
    """
    quote_context = None
    quote_chars = ("\"", "\'", "`")

    for query_char in query:
        # same quotation mark as the current one : this is a closing mark
        if quote_context == query_char:
            quote_context = None
        else:
            for quote in quote_chars:
                if quote_context is None and quote == query_char:
                    quote_context = quote
    return quote_context is None and query.endswith(";"), quote_context


if __name__ == "__main__":
    main()
