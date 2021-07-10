"""
Tests for shilellagh.adapters.api.gsheets.lib.
"""
import itertools

import dateutil.tz
import pytest

from shillelagh.adapters.api.gsheets.fields import GSheetsBoolean
from shillelagh.adapters.api.gsheets.fields import GSheetsDate
from shillelagh.adapters.api.gsheets.fields import GSheetsDateTime
from shillelagh.adapters.api.gsheets.fields import GSheetsNumber
from shillelagh.adapters.api.gsheets.fields import GSheetsString
from shillelagh.adapters.api.gsheets.fields import GSheetsTime
from shillelagh.adapters.api.gsheets.lib import format_error_message
from shillelagh.adapters.api.gsheets.lib import gen_letters
from shillelagh.adapters.api.gsheets.lib import get_credentials
from shillelagh.adapters.api.gsheets.lib import get_field
from shillelagh.adapters.api.gsheets.lib import get_index_from_letters
from shillelagh.adapters.api.gsheets.lib import get_sync_mode
from shillelagh.adapters.api.gsheets.lib import get_url
from shillelagh.adapters.api.gsheets.lib import get_value_from_cell
from shillelagh.adapters.api.gsheets.lib import get_values_from_row
from shillelagh.adapters.api.gsheets.types import SyncMode
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Order
from shillelagh.filters import Equal
from shillelagh.filters import Range


def test_get_field():
    """
    Test get_field.
    """
    assert get_field({"type": "string"}, None) == GSheetsString(
        [Equal],
        Order.ANY,
        True,
    )
    assert get_field({"type": "number"}, None) == GSheetsNumber(
        [Range],
        Order.ANY,
        True,
    )
    assert get_field({"type": "boolean"}, None) == GSheetsBoolean(
        [Equal],
        Order.ANY,
        True,
    )
    assert get_field({"type": "date"}, None) == GSheetsDate([Range], Order.ANY, True)
    assert get_field({"type": "datetime"}, None) == GSheetsDateTime(
        [Range],
        Order.ANY,
        True,
    )
    timezone = dateutil.tz.gettz("America/Los_Angeles")
    assert get_field(
        {"type": "datetime", "pattern": "M/d/yyyy H:mm:ss"},
        timezone,
    ) == GSheetsDateTime(
        [Range],
        Order.ANY,
        True,
        "M/d/yyyy H:mm:ss",
        timezone,
    )
    assert get_field({"type": "timeofday"}, None) == GSheetsTime(
        [Range],
        Order.ANY,
        True,
    )
    assert get_field({"type": "invalid"}, None) == GSheetsString(
        [Equal],
        Order.ANY,
        True,
    )


def test_format_error_message():
    """
    Test format_error_message.
    """
    assert format_error_message([]) == ""
    response = {
        "version": "0.6",
        "reqId": "0",
        "status": "error",
        "errors": [
            {
                "reason": "invalid_query",
                "message": "INVALID_QUERY",
                "detailed_message": "Invalid query: NO_COLUMN: C",
            },
        ],
    }
    assert format_error_message(response["errors"]) == "Invalid query: NO_COLUMN: C"


def test_get_url():
    """
    Test get_url.
    """
    assert (
        get_url(
            "https://docs.google.com/spreadsheets/d/1/edit#gid=0",
        )
        == "https://docs.google.com/spreadsheets/d/1/gviz/tq?gid=0"
    )
    assert (
        get_url(
            "https://docs.google.com/spreadsheets/d/1/edit#gid=0",
            headers=2,
            gid=3,
            sheet="some-sheet",
        )
        == "https://docs.google.com/spreadsheets/d/1/gviz/tq?headers=2&sheet=some-sheet"
    )
    assert (
        get_url(
            "https://docs.google.com/spreadsheets/d/1/edit?headers=2&gid=1",
        )
        == "https://docs.google.com/spreadsheets/d/1/gviz/tq?headers=2&gid=1"
    )
    assert (
        get_url(
            "https://docs.google.com/spreadsheets/d/1/edit?headers=2&sheet=some-sheet",
        )
        == "https://docs.google.com/spreadsheets/d/1/gviz/tq?headers=2&sheet=some-sheet"
    )


def test_get_sync_mode():
    """
    Test get_sync_mode.
    """
    assert get_sync_mode("https://docs.google.com/spreadsheets/d/1/edit#gid=42") is None
    assert (
        get_sync_mode(
            "https://docs.google.com/spreadsheets/d/1/edit?sync_mode=BATCH#gid=42",
        )
        == SyncMode.BATCH
    )
    assert (
        get_sync_mode(
            "https://docs.google.com/spreadsheets/d/1/edit?sync_mode=batch#gid=42",
        )
        == SyncMode.BATCH
    )
    assert (
        get_sync_mode(
            "https://docs.google.com/spreadsheets/d/1/edit?sync_mode=1#gid=42",
        )
        == SyncMode.BIDIRECTIONAL
    )
    with pytest.raises(ProgrammingError) as excinfo:
        get_sync_mode(
            "https://docs.google.com/spreadsheets/d/1/edit?sync_mode=0#gid=42",
        )
    assert str(excinfo.value) == "Invalid sync mode: 0"
    with pytest.raises(ProgrammingError) as excinfo:
        get_sync_mode(
            "https://docs.google.com/spreadsheets/d/1/edit?sync_mode=INVALID#gid=42",
        )
    assert str(excinfo.value) == "Invalid sync mode: INVALID"


def test_gen_letters():
    """
    Test gen_letters.
    """
    letters = list(itertools.islice(gen_letters(), 60))
    assert letters == [
        "A",
        "B",
        "C",
        "D",
        "E",
        "F",
        "G",
        "H",
        "I",
        "J",
        "K",
        "L",
        "M",
        "N",
        "O",
        "P",
        "Q",
        "R",
        "S",
        "T",
        "U",
        "V",
        "W",
        "X",
        "Y",
        "Z",
        "AA",
        "AB",
        "AC",
        "AD",
        "AE",
        "AF",
        "AG",
        "AH",
        "AI",
        "AJ",
        "AK",
        "AL",
        "AM",
        "AN",
        "AO",
        "AP",
        "AQ",
        "AR",
        "AS",
        "AT",
        "AU",
        "AV",
        "AW",
        "AX",
        "AY",
        "AZ",
        "AAA",
        "AAB",
        "AAC",
        "AAD",
        "AAE",
        "AAF",
        "AAG",
        "AAH",
    ]


def test_get_index_from_letters():
    """
    Test get_index_from_letters.
    """
    assert get_index_from_letters("A") == 0
    assert get_index_from_letters("Z") == 25
    assert get_index_from_letters("AA") == 26
    assert get_index_from_letters("AB") == 27
    assert get_index_from_letters("AAA") == 702


def test_get_values_from_row():
    """
    Test get_values_from_row.
    """
    column_map = {"country": "A", "cnt": "C"}
    row = {"country": "BR", "cnt": 10}
    assert get_values_from_row(row, column_map) == ["BR", "", 10]


def test_get_credentials(mocker):
    """
    Test get_credentials.
    """
    service_account = mocker.patch(
        "shillelagh.adapters.api.gsheets.lib.google.oauth2.service_account.Credentials",
    )
    credentials = mocker.patch(
        "shillelagh.adapters.api.gsheets.lib.google.oauth2.credentials.Credentials",
    )

    # no credentials
    get_credentials(None, None, None, None)
    credentials.assert_not_called()
    service_account.assert_not_called()

    # access_token
    get_credentials("token", None, None, None)
    credentials.assert_called_with("token")
    credentials.reset_mock()
    service_account.assert_not_called()

    # service_account_file
    get_credentials(None, "credentials.json", None, None)
    credentials.assert_not_called()
    service_account.from_service_account_file.assert_called_with(
        "credentials.json",
        scopes=[
            "https://www.googleapis.com/auth/drive.readonly",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://spreadsheets.google.com/feeds",
        ],
        subject=None,
    )
    service_account.reset_mock()

    # service_account_info
    get_credentials(None, None, {"secret": "XXX"}, "user@example.com")
    credentials.assert_not_called()
    service_account.from_service_account_info.assert_called_with(
        {"secret": "XXX"},
        scopes=[
            "https://www.googleapis.com/auth/drive.readonly",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://spreadsheets.google.com/feeds",
        ],
        subject="user@example.com",
    )


def test_get_value_from_cell():
    """
    Test get_value_from_cell.
    """
    assert (
        get_value_from_cell({"v": "Date(2018,8,1,0,0,0)", "f": "9/1/2018 0:00:00"})
        == "9/1/2018 0:00:00"
    )
    assert get_value_from_cell({"v": "test"}) == "test"
    assert get_value_from_cell({"v": 1.0, "f": "1"}) == "1"
    assert get_value_from_cell({"v": True, "f": "TRUE"}) == "TRUE"
    assert get_value_from_cell(None) == ""
    assert get_value_from_cell({"v": None}) == ""
