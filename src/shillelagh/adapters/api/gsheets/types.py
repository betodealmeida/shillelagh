"""Types for the GSheets adapter."""
from enum import Enum


class SyncMode(Enum):
    """
    Different synchronization modes for GSheets.

    There are 3 different synchronization modes in which the adapter can
    operate. Each one has different tradeoffs.

    The `BIDIRECTIONAL` mode is useful for pushing small changes, and working
    interactively with the spreadsheets. All changes are pushed immediately
    to the sheet, and the sheet is fully downloaded before every `UPDATE`
    or `DELETE`.

    The `BATCH` mode is useful for one-off large changes, as the name
    suggests. Changes are push at once when the adapter closes, and the sheet
    is only downloaded once, before the first `UPDATE` or `DELETE`.

    Finally, `UNIDIRECTIONAL` is a compromise between the other two modes.
    Changes are pushed immediately, since they're usually small. The sheet
    is downloaded in full only once, before the first `UPDATE` or `DELETE`.
    """

    # all changes are pushed immediately, and the spreadsheet is downloaded
    # before every UPDATE/DELETE
    BIDIRECTIONAL = 1

    # all changes are pushed imediately, but the spreadsheet is
    # downloaded only once before the first UPDATE/DELETE
    UNIDIRECTIONAL = 2

    # all changes are pushed at once when the connection closes, and the
    # spreadsheet is downloaded before the first UPDATE/DELETE
    BATCH = 3
