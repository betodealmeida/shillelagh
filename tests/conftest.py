"""
Fixtures for Shillelagh.
"""

import json
import logging
import os
from typing import Any, Dict, Iterator

import pytest
from pytest_mock import MockerFixture

from shillelagh.adapters.registry import AdapterLoader

_logger = logging.getLogger(__name__)


@pytest.fixture
def adapter_kwargs() -> Dict[str, Any]:
    """
    Load adapter configuration.

    This fixture looks for the environment variable `SHILLELAGH_ADAPTER_KWARGS`.
    The configuration should be encoded as JSON.
    """
    kwargs = {}
    if "SHILLELAGH_ADAPTER_KWARGS" in os.environ:
        try:
            kwargs = json.loads(os.environ["SHILLELAGH_ADAPTER_KWARGS"])
        except Exception as ex:  # pylint: disable=broad-except
            _logger.warning('Unable to load "SHILLELAGH_ADAPTER_KWARGS": %s', ex)

    return kwargs


@pytest.fixture
def registry(mocker: MockerFixture) -> Iterator[AdapterLoader]:
    """
    Create a custom adapter registry.
    """
    custom_registry = AdapterLoader()
    mocker.patch("shillelagh.adapters.registry.registry", new=custom_registry)
    mocker.patch("shillelagh.backends.apsw.db.registry", new=custom_registry)
    yield custom_registry
