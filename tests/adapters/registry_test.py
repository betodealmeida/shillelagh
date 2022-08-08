"""
Tests for the adapter registry.
"""

import pytest

from shillelagh.adapters.file.csvfile import CSVFile
from shillelagh.adapters.registry import AdapterLoader
from shillelagh.exceptions import InterfaceError

from ..fakes import FakeAdapter


def test_registry(registry: AdapterLoader) -> None:
    """
    Basic tests for the adapter registry.
    """
    registry.clear()
    assert registry.loaders == {}

    registry.add("dummy", FakeAdapter)
    adapter = registry.load("dummy")
    assert adapter == FakeAdapter


def test_load_error(registry: AdapterLoader) -> None:
    """
    Test errors when loading an adapter.
    """
    registry.clear()

    def load_error() -> None:
        raise ImportError("Error!")

    registry.loaders["dummy"].append(load_error)
    with pytest.raises(InterfaceError) as excinfo:
        registry.load("dummy")
    assert str(excinfo.value) == "Unable to load adapter dummy"

    registry.add("dummy", FakeAdapter)
    assert len(registry.loaders["dummy"]) == 2
    adapter = registry.load("dummy")
    assert adapter == FakeAdapter


def test_register(registry: AdapterLoader) -> None:
    """
    Test that we can register new adapters.
    """
    registry.clear()

    registry.register("csvfile", "shillelagh.adapters.file.csvfile", "CSVFile")
    adapter = registry.load("csvfile")
    assert adapter == CSVFile

    registry.register("invalid", "shillelagh.adapters.file.csvfile", "WhatFile")
    with pytest.raises(InterfaceError) as excinfo:
        registry.load("invalid")
    assert str(excinfo.value) == "Unable to load adapter invalid"
