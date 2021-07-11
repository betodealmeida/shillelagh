#!/usr/bin/env python
"""
A simple REPL for Shillelagh.

To run the REPL, since run ``shillelagh``. Pressing return will execute the
query immediately, and multi-line queries are currently not supported.

Connection arguments can be passed via a ``~/.shillelagh.yaml`` file, eg::

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
            with open(config) as stream:
                adapter_kwargs = yaml.load(stream, Loader=yaml.FullLoader)
        except (PermissionError, yaml.parser.ParserError, yaml.scanner.ScannerError):
            _logger.exception("Unable to load configuration file")

    connection = connect(":memory:", adapter_kwargs=adapter_kwargs)
    cursor = connection.cursor()

    session = PromptSession(
        lexer=PygmentsLexer(SqlLexer),
        completer=sql_completer,
        style=style,
        history=FileHistory(
            os.path.expanduser("~/.config/shillelagh/shillelagh.history"),
        ),
    )

    while True:
        try:
            sql = session.prompt("sql> ")
        except KeyboardInterrupt:
            continue  # Control-C pressed. Try again.
        except EOFError:
            break  # Control-D pressed.

        if sql.strip():
            try:
                cursor.execute(sql)
                results = cursor.fetchall()
            except Error as ex:
                print(ex)
                continue

            headers = [t[0] for t in cursor.description or []]
            print(tabulate(results, headers=headers))

    connection.close()
    print("GoodBye!")


if __name__ == "__main__":
    main()
