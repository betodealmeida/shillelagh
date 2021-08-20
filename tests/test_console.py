# pylint: disable=invalid-name
"""
Tests for shillelagh.console.
"""
import os.path
from io import StringIO

import yaml

from shillelagh import console


def test_main(mocker):
    """
    Test ``main``.
    """
    stdout = mocker.patch("sys.stdout", new_callable=StringIO)
    PromptSession = mocker.patch("shillelagh.console.PromptSession")

    PromptSession.return_value.prompt.side_effect = ["SELECT 1", "", EOFError()]
    console.main()
    result = stdout.getvalue()
    assert result == "  1\n---\n  1\nGoodBye!\n"


def test_exception(mocker):
    """
    Test that exceptions are captured and printed.
    """
    stdout = mocker.patch("sys.stdout", new_callable=StringIO)
    PromptSession = mocker.patch("shillelagh.console.PromptSession")

    PromptSession.return_value.prompt.side_effect = ["SSELECT 1", EOFError()]
    console.main()
    result = stdout.getvalue()
    assert result == 'SQLError: near "SSELECT": syntax error\nGoodBye!\n'


def test_ctrl_c(mocker):
    """
    Test that ``CTRL-C`` exists the REPL.
    """
    stdout = mocker.patch("sys.stdout", new_callable=StringIO)
    PromptSession = mocker.patch("shillelagh.console.PromptSession")

    PromptSession.return_value.prompt.side_effect = [
        KeyboardInterrupt(),
        "SELECT 1",
        EOFError(),
    ]
    console.main()
    result = stdout.getvalue()
    assert result == "  1\n---\n  1\nGoodBye!\n"


def test_configuration(mocker, fs):
    """
    Test loading the configuration file.
    """
    config = os.path.expanduser("~/.config/shillelagh/shillelagh.yaml")
    fs.create_file(config)
    with open(config, "w") as fp:
        yaml.dump({"foo": {"bar": "baz"}}, fp)

    connect = mocker.patch("shillelagh.console.connect")
    mocker.patch("sys.stdout", new_callable=StringIO)
    PromptSession = mocker.patch("shillelagh.console.PromptSession")

    PromptSession.return_value.prompt.side_effect = [EOFError()]
    console.main()

    connect.assert_called_with(":memory:", adapter_kwargs={"foo": {"bar": "baz"}})


def test_no_configuration(mocker, fs):
    """
    Test no configuration file found.
    """
    config_dir = os.path.expanduser("~/.config/shillelagh/")
    fs.create_dir(config_dir)

    connect = mocker.patch("shillelagh.console.connect")
    mocker.patch("sys.stdout", new_callable=StringIO)
    PromptSession = mocker.patch("shillelagh.console.PromptSession")

    PromptSession.return_value.prompt.side_effect = [EOFError()]
    console.main()

    connect.assert_called_with(":memory:", adapter_kwargs={})


def test_configuration_invalid(mocker, fs):
    """
    Test that an exception is raised if the configuration is invalid.
    """
    config = os.path.expanduser("~/.config/shillelagh/shillelagh.yaml")
    fs.create_file(config)
    with open(config, "w") as fp:
        fp.write("foo: *")

    _logger = mocker.patch("shillelagh.console._logger")
    mocker.patch("sys.stdout", new_callable=StringIO)
    PromptSession = mocker.patch("shillelagh.console.PromptSession")

    PromptSession.return_value.prompt.side_effect = [EOFError()]
    console.main()

    _logger.exception.assert_called_with("Unable to load configuration file")
