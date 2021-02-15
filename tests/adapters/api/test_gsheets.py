from shillelagh.adapters.api.gsheets import GSheetsAPI
from shillelagh.adapters.base import Adapter
from shillelagh.db import connect


class MockEntryPoint:
    def __init__(self, name: str, adapter: Adapter):
        self.name = name
        self.adapter = adapter

    def load(self) -> Adapter:
        return self.adapter


def test_credentials(mocker):
    entry_points = [MockEntryPoint("gsheets", GSheetsAPI)]
    mocker.patch("shillelagh.db.iter_entry_points", return_value=entry_points)

    connection = connect(
        ":memory:",
        ["gsheets"],
        adapter_args={
            "_gsheets": (
                {"secret": "XXX"},
                "user@example.com",
            ),
        },
    )
    cursor = connection.cursor()

    sql = (
        "SELECT * FROM "
        '"https://docs.google.com/spreadsheets/d/1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/edit#gid=0"'
    )
    data = list(cursor.execute(sql))
    assert data == [
        ("BR", 1),
        ("BR", 3),
        ("IN", 5),
        ("ZA", 6),
        ("CR", 10),
    ]
