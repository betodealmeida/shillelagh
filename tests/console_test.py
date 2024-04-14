# pylint: disable=invalid-name, unused-argument
"""
Tests for shillelagh.console.
"""

from io import StringIO
from pathlib import Path

import yaml
from appdirs import user_config_dir
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from shillelagh import console


def test_main(mocker: MockerFixture) -> None:
    """
    Test ``main``.
    """
    mocker.patch("sys.stdin.isatty", return_value=True)
    stdout = mocker.patch("sys.stdout", new_callable=StringIO)
    PromptSession = mocker.patch("shillelagh.console.PromptSession")

    PromptSession.return_value.prompt.side_effect = ["SELECT 1;", "", EOFError()]
    console.main()
    result = stdout.getvalue()
    assert (
        result
        == """  1
---
  1
(1 row in 0.00s)

GoodBye!
"""
    )


def test_exception(mocker: MockerFixture) -> None:
    """
    Test that exceptions are captured and printed.
    """
    mocker.patch("sys.stdin.isatty", return_value=True)
    stdout = mocker.patch("sys.stdout", new_callable=StringIO)
    PromptSession = mocker.patch("shillelagh.console.PromptSession")

    PromptSession.return_value.prompt.side_effect = ["SSELECT 1;", EOFError()]
    console.main()
    result = stdout.getvalue()
    assert (
        result
        == """SQLError: near "SSELECT": syntax error
GoodBye!
"""
    )


def test_ctrl_c(mocker: MockerFixture) -> None:
    """
    Test that ``CTRL-C`` does not exit the REPL.
    """
    mocker.patch("sys.stdin.isatty", return_value=True)
    stdout = mocker.patch("sys.stdout", new_callable=StringIO)
    PromptSession = mocker.patch("shillelagh.console.PromptSession")

    PromptSession.return_value.prompt.side_effect = [
        KeyboardInterrupt(),
        "SELECT 1;",
        EOFError(),
    ]
    console.main()
    result = stdout.getvalue()
    assert (
        result
        == """  1
---
  1
(1 row in 0.00s)

GoodBye!
"""
    )


def test_configuration(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test loading the configuration file.
    """
    mocker.patch("sys.stdin.isatty", return_value=True)
    config_dir = Path(user_config_dir("shillelagh"))
    config_path = config_dir / "shillelagh.yaml"
    fs.create_file(config_path, contents=yaml.dump({"foo": {"bar": "baz"}}))

    connect = mocker.patch("shillelagh.console.connect")
    mocker.patch("sys.stdout", new_callable=StringIO)
    PromptSession = mocker.patch("shillelagh.console.PromptSession")

    PromptSession.return_value.prompt.side_effect = [EOFError()]
    console.main()

    connect.assert_called_with(":memory:", adapter_kwargs={"foo": {"bar": "baz"}})


def test_no_configuration(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test no configuration file found.
    """
    mocker.patch("sys.stdin.isatty", return_value=True)
    config_dir = Path(user_config_dir("shillelagh"))
    fs.create_dir(config_dir)

    connect = mocker.patch("shillelagh.console.connect")
    mocker.patch("sys.stdout", new_callable=StringIO)
    PromptSession = mocker.patch("shillelagh.console.PromptSession")

    PromptSession.return_value.prompt.side_effect = [EOFError()]
    console.main()

    connect.assert_called_with(":memory:", adapter_kwargs={})


def test_configuration_invalid(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test that an exception is raised if the configuration is invalid.
    """
    mocker.patch("sys.stdin.isatty", return_value=True)
    config_dir = Path(user_config_dir("shillelagh"))
    config_path = config_dir / "shillelagh.yaml"
    fs.create_file(config_path, contents="foo: *")

    _logger = mocker.patch("shillelagh.console._logger")
    mocker.patch("sys.stdout", new_callable=StringIO)
    PromptSession = mocker.patch("shillelagh.console.PromptSession")

    PromptSession.return_value.prompt.side_effect = [EOFError()]
    console.main()

    _logger.exception.assert_called_with("Unable to load configuration file")


def test_multiline(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test a simple multiline query
    """
    mocker.patch("sys.stdin.isatty", return_value=True)
    stdout = mocker.patch("sys.stdout", new_callable=StringIO)
    PromptSession = mocker.patch("shillelagh.console.PromptSession")

    PromptSession.return_value.prompt.side_effect = ["SELECT ", "1;", "", EOFError()]
    console.main()
    result = stdout.getvalue()
    assert (
        result
        == """  1
---
  1
(1 row in 0.00s)

GoodBye!
"""
    )


def test_multiline_quoted_semicolon(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test a multiline query that contains quoted semicolons.
    """
    mocker.patch("sys.stdin.isatty", return_value=True)
    stdout = mocker.patch("sys.stdout", new_callable=StringIO)
    PromptSession = mocker.patch("shillelagh.console.PromptSession")

    PromptSession.return_value.prompt.side_effect = ["SELECT ';'=", "';';", EOFError()]
    console.main()
    result = stdout.getvalue()

    assert (
        result
        == """  ';'=
   ';'
------
     1
(1 row in 0.00s)

GoodBye!
"""
    )


def test_multiline_quoted_semicolon_on_line_end(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test a multiline query that contains quoted semicolons on the line end.
    """
    mocker.patch("sys.stdin.isatty", return_value=True)
    stdout = mocker.patch("sys.stdout", new_callable=StringIO)
    PromptSession = mocker.patch("shillelagh.console.PromptSession")

    PromptSession.return_value.prompt.side_effect = ["SELECT ';'=';", "';", EOFError()]
    console.main()
    result = stdout.getvalue()

    assert (
        result
        == """  ';'=';
       '
--------
       0
(1 row in 0.00s)

GoodBye!
"""
    )


def test_multiline_triple_quoted_semicolon_on_line_end(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test a multiline query that contains quoted semicolons on the line end.
    """
    mocker.patch("sys.stdin.isatty", return_value=True)
    stdout = mocker.patch("sys.stdout", new_callable=StringIO)
    PromptSession = mocker.patch("shillelagh.console.PromptSession")

    PromptSession.return_value.prompt.side_effect = [
        "SELECT ''';'''=''';",
        "''';",
        EOFError(),
    ]
    console.main()
    result = stdout.getvalue()

    assert (
        result
        == """  ''';'''=''';
           '''
--------------
             0
(1 row in 0.00s)

GoodBye!
"""
    )


def test_emit_statements() -> None:
    """
    Test the ``emit_statements`` function.
    """
    script = """
SELECT
  1;
SELECT
  2
; SELECT 3; SELECT 4; SELECT
5
;
    """
    assert list(console.emit_statements(script.split("\n"))) == [
        "SELECT\n  1",
        "SELECT\n  2",
        "SELECT 3",
        "SELECT 4",
        "SELECT\n5",
    ]


def test_repl(mocker: MockerFixture) -> None:
    """
    Test the REPL.
    """
    session = mocker.MagicMock()
    session.prompt.side_effect = [
        "SELECT",
        "1",
        ";",
        EOFError(),
    ]

    lines = list(console.repl(session))
    assert lines == ["SELECT", "1", ";"]
    session.prompt.assert_has_calls(
        [
            mocker.call("🍀> "),
            mocker.call("  . "),
            mocker.call("  . "),
            mocker.call("🍀> "),
        ],
    )


def test_non_interactive(mocker: MockerFixture) -> None:
    """
    Test running ``shillelagh`` non-interactively.

        $ shillelagh < query.sql

    """
    stdin = mocker.patch("sys.stdin")
    stdin.isatty.return_value = False
    stdout = mocker.patch("sys.stdout", new_callable=StringIO)

    stdin.readlines.return_value = ["SELECT", "1", ";"]
    console.main()
    result = stdout.getvalue()
    assert (
        result
        == """  1
---
  1
"""
    )
