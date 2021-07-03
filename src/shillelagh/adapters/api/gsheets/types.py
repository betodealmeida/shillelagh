from enum import Enum


class SyncMode(Enum):
    # all changes are pushed immediately, and the spreadsheet is downloaded
    # before every UPDATE/DELETE
    BIDIRECTIONAL = 1

    # all changes are pushed imediately, but the spreadsheet is
    # downloaded only once before the first UPDATE/DELETE
    UNIDIRECTIONAL = 2

    # all changes are pushed at once when the connection closes, and the
    # spreadsheet is downloaded before the first UPDATE/DELETE
    BATCH = 3
