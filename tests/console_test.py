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
    stdout = mocker.patch("sys.stdout", new_callable=StringIO)
    PromptSession = mocker.patch("shillelagh.console.PromptSession")

    PromptSession.return_value.prompt.side_effect = ["SELECT 1;", "", EOFError()]
    console.main()
    result = stdout.getvalue()
    assert result == "  1\n---\n  1\nGoodBye!\n"


def test_exception(mocker: MockerFixture) -> None:
    """
    Test that exceptions are captured and printed.
    """
    stdout = mocker.patch("sys.stdout", new_callable=StringIO)
    PromptSession = mocker.patch("shillelagh.console.PromptSession")

    PromptSession.return_value.prompt.side_effect = ["SSELECT 1;", EOFError()]
    console.main()
    result = stdout.getvalue()
    assert result == 'SQLError: near "SSELECT": syntax error\nGoodBye!\n'


def test_ctrl_c(mocker: MockerFixture) -> None:
    """
    Test that ``CTRL-C`` does not exit the REPL.
    """
    stdout = mocker.patch("sys.stdout", new_callable=StringIO)
    PromptSession = mocker.patch("shillelagh.console.PromptSession")

    PromptSession.return_value.prompt.side_effect = [
        KeyboardInterrupt(),
        "SELECT 1;",
        EOFError(),
    ]
    console.main()
    result = stdout.getvalue()
    assert result == "  1\n---\n  1\nGoodBye!\n"


def test_configuration(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test loading the configuration file.
    """
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
    stdout = mocker.patch("sys.stdout", new_callable=StringIO)
    PromptSession = mocker.patch("shillelagh.console.PromptSession")

    PromptSession.return_value.prompt.side_effect = ["SELECT ", "1;", "", EOFError()]
    console.main()
    result = stdout.getvalue()
    assert result == "  1\n---\n  1\nGoodBye!\n"


def test_multiline_quoted_semicolon(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test a multiline query that contains quoted semicolons.
    """
    stdout = mocker.patch("sys.stdout", new_callable=StringIO)
    PromptSession = mocker.patch("shillelagh.console.PromptSession")

    PromptSession.return_value.prompt.side_effect = ["SELECT ';'=", "';';", EOFError()]
    console.main()
    result = stdout.getvalue()

    assert result == "  ';'=\n   ';'\n------\n     1\nGoodBye!\n"


def test_multiline_quoted_semicolon_on_line_end(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test a multiline query that contains quoted semicolons on the line end.
    """
    stdout = mocker.patch("sys.stdout", new_callable=StringIO)
    PromptSession = mocker.patch("shillelagh.console.PromptSession")

    PromptSession.return_value.prompt.side_effect = ["SELECT ';'=';", "';", EOFError()]
    console.main()
    result = stdout.getvalue()

    assert result == "  ';'=';\n       '\n--------\n       0\nGoodBye!\n"


def test_multiline_triple_quoted_semicolon_on_line_end(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test a multiline query that contains quoted semicolons on the line end.
    """
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
        == "  ''';'''=''';\n           '''\n--------------\n             0\nGoodBye!\n"
    )
