"""
Fixtures for Shillelagh.
"""
import json
import logging
import os

import pytest

_logger = logging.getLogger(__name__)


@pytest.fixture
def adapter_kwargs():
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
