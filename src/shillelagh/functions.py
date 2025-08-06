"""
Custom functions available to the SQL backend.
"""

import importlib
import json
import subprocess
import sys
import time
from datetime import datetime, timedelta
from typing import Any, Optional

from packaging.version import InvalidVersion, Version

import shillelagh
from shillelagh.adapters.base import Adapter
from shillelagh.fields import FastISODateTime
from shillelagh.lib import find_adapter

if sys.version_info < (3, 10):
    from importlib_metadata import distribution
else:
    from importlib.metadata import distribution

__all__ = ["upgrade", "sleep", "get_metadata", "version", "date_trunc"]


def upgrade(target_version: str) -> str:
    """
    Upgrade the library to a given version.

    This function attempts to upgrade shillelagh using available package managers.
    It tries multiple package managers in order: uv, pip, pipx.
    """
    try:
        # Validate version format first
        Version(target_version)
    except InvalidVersion:
        return f"Invalid version: {target_version}"

    package_spec = f"shillelagh=={target_version}"

    # List of package managers to try, in order of preference
    package_managers = [
        # uv is fast and modern
        (["uv", "pip", "install", package_spec], "uv"),
        # Standard pip
        ([sys.executable, "-m", "pip", "install", package_spec], "pip"),
        # pipx for isolated environments
        (
            [
                "pipx",
                "upgrade",
                "--pip-args",
                f"shillelagh=={target_version}",
                "shillelagh",
            ],
            "pipx",
        ),
    ]

    errors = []
    for command, manager_name in package_managers:
        try:
            # Try to run the package manager command
            subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=True,
                timeout=300,  # 5 minute timeout
            )
            # If successful, reload the module
            importlib.reload(shillelagh)
            return f"Upgrade to {target_version} successful using {manager_name}."
        except subprocess.CalledProcessError as e:
            errors.append(f"{manager_name}: {e.stderr or e.stdout or 'Command failed'}")
        except FileNotFoundError:
            errors.append(f"{manager_name}: Not found")
        except subprocess.TimeoutExpired:
            errors.append(f"{manager_name}: Timeout after 5 minutes")
        except Exception as e:  # pylint: disable=broad-except
            errors.append(f"{manager_name}: {str(e)}")

    # If all package managers failed, return error message
    error_details = "; ".join(errors)
    return f"Upgrade failed. Tried: {error_details}"


def sleep(seconds: int) -> None:
    """
    Sleep for ``n`` seconds.

    This is useful for troubleshooting timeouts::

        sql> SELECT sleep(60);

    """
    time.sleep(seconds)


def get_metadata(
    adapter_kwargs: dict[str, dict[str, Any]],
    adapters: list[type[Adapter]],
    uri: str,
) -> str:
    """
    Return metadata about a given table.

    Returns the name of the adapter that supports the table, as well
    as any extra metadata provided by the adapter::

        sql> SELECT GET_METADATA("https://docs.google.com/spreadsheets/d/1/edit#gid=0");
        GET_METADATA("https://docs.google.com/spreadsheets/d/1/edit#gid=0")
        -------------------------------------------------------------------
        {
            "extra": {
                "Spreadsheet title": "A title",
                "Sheet title": "Another title"
            },
            "adapter": "GSheetsAPI"
        }

    """
    adapter, args, kwargs = find_adapter(uri, adapter_kwargs, adapters)
    instance = adapter(*args, **kwargs)

    return json.dumps(
        {
            "extra": instance.get_metadata(),
            "adapter": adapter.__name__,
        },
    )


def version() -> str:
    """
    Return the current version of Shillelagh.

    As an example::

        sql> SELECT VERSION();
        VERSION()
        -----------
        0.7.4

    """
    return str(distribution("shillelagh").version)


def date_trunc(  # pylint: disable=too-many-return-statements
    value: Optional[str],
    unit: str,
) -> Optional[str]:
    """
    Truncate a datetime to a given unit.
    """
    field = FastISODateTime()
    timestamp = field.parse(value)
    if timestamp is None:
        return None

    unit = unit.lower()
    if unit == "year":
        truncated = datetime(year=timestamp.year, month=1, day=1)
    elif unit == "quarter":
        month = ((timestamp.month - 1) // 3) * 3 + 1
        truncated = datetime(year=timestamp.year, month=month, day=1)
    elif unit == "month":
        truncated = datetime(year=timestamp.year, month=timestamp.month, day=1)
    elif unit == "week":
        # assumes the week starts on Monday
        start_of_week = timestamp - timedelta(days=timestamp.weekday())
        truncated = datetime(
            year=start_of_week.year,
            month=start_of_week.month,
            day=start_of_week.day,
        )
    elif unit == "day":
        truncated = datetime(
            year=timestamp.year,
            month=timestamp.month,
            day=timestamp.day,
        )
    elif unit == "hour":
        truncated = datetime(
            year=timestamp.year,
            month=timestamp.month,
            day=timestamp.day,
            hour=timestamp.hour,
        )
    elif unit == "minute":
        truncated = datetime(
            year=timestamp.year,
            month=timestamp.month,
            day=timestamp.day,
            hour=timestamp.hour,
            minute=timestamp.minute,
        )
    elif unit == "second":
        truncated = datetime(
            year=timestamp.year,
            month=timestamp.month,
            day=timestamp.day,
            hour=timestamp.hour,
            minute=timestamp.minute,
            second=timestamp.second,
        )
    else:
        raise ValueError(f"Unsupported truncation unit: {unit}")

    return field.format(truncated)
