import datetime
from typing import Any
from typing import List
from typing import Optional
from typing import Type

from shillelagh.fields import Field
from shillelagh.fields import StringBoolean
from shillelagh.filters import Filter
from shillelagh.types import Order


FORMAT = "%m/%d/%Y %H:%M:%S"
QUOTE = "%Y-%m-%d %H:%M:%S"


class GSheetsDateTime(Field[str, datetime.datetime]):
    type = "TIMESTAMP"
    db_api_type = "DATETIME"

    def __init__(
        self,
        filters: Optional[List[Type[Filter]]] = None,
        order: Order = Order.NONE,
        exact: bool = False,
        timezone: Optional[datetime.tzinfo] = None,
    ):
        super().__init__(filters, order, exact)
        self.timezone = timezone

    def __eq__(self, other: Any) -> bool:
        if other.__class__ != self.__class__:
            return NotImplemented

        return bool(
            self.filters == other.filters
            and self.order == other.order
            and self.exact == other.exact
            and self.timezone == other.timezone,
        )

    def parse(self, value: Optional[str]) -> Optional[datetime.datetime]:
        if value is None:
            return None

        args = [int(number) for number in value[len("Date(") : -1].split(",")]
        args[1] += 1  # month is zero indexed in the response
        return datetime.datetime(*args, tzinfo=self.timezone)  # type: ignore

    def format(self, value: Optional[datetime.datetime]) -> Optional[str]:
        if value is None:
            return None
        # Google Sheets does not support timezones in datetime values, so we
        # convert all timestamps to the sheet timezone
        if self.timezone:
            value = value.astimezone(self.timezone)
        return value.strftime(FORMAT) if value else None

    def quote(self, value: Optional[str]) -> str:
        if value is None:
            return "NULL"
        value = datetime.datetime.strptime(value, FORMAT).strftime(QUOTE)
        return f"datetime '{value}'"


class GSheetsDate(Field[str, datetime.date]):
    type = "DATE"
    db_api_type = "DATETIME"

    def parse(self, value: Optional[str]) -> Optional[datetime.date]:
        """Parse a string like 'Date(2018,0,1)'."""
        if value is None:
            return None

        args = [int(number) for number in value[len("Date(") : -1].split(",")]
        args[1] += 1  # month is zero indexed in the response (WTF, Google!?)
        return datetime.date(*args)

    def format(self, value: Optional[datetime.date]) -> Optional[str]:
        if value is None:
            return None
        return value.isoformat()

    def quote(self, value: Optional[str]) -> str:
        if value is None:
            return "NULL"
        return f"date '{value}'"


class GSheetsTime(Field[List[int], datetime.time]):
    type = "TIME"
    db_api_type = "DATETIME"

    def parse(self, values: Optional[List[int]]) -> Optional[datetime.time]:
        """Parse time of day as returned from the API."""
        if values is None:
            return None

        return datetime.time(*values)  # type: ignore

    def format(self, value: Optional[datetime.time]) -> Optional[str]:  # type: ignore
        if value is None:
            return value
        return value.isoformat()

    def quote(self, value: Optional[str]) -> str:  # type: ignore
        if value is None:
            return "NULL"
        return f"timeofday '{value}'"


class GSheetsBoolean(StringBoolean):
    def quote(self, value: Optional[str]) -> str:
        if value is None:
            return "NULL"
        return value.lower()
