import inspect
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import timedelta
from enum import Enum
from typing import Any
from typing import Dict
from typing import List
from typing import Iterator
from typing import Optional
from typing import Tuple
from typing import Type

import apsw
import dateutil.parser
import requests
import requests_cache

requests_cache.install_cache(
    cache_name="weatherapi_cache", backend="sqlite", expire_after=180
)


operator_map = {
    apsw.SQLITE_INDEX_CONSTRAINT_EQ: "eq",
    apsw.SQLITE_INDEX_CONSTRAINT_GE: "ge",
    apsw.SQLITE_INDEX_CONSTRAINT_GT: "gt",
    apsw.SQLITE_INDEX_CONSTRAINT_LE: "le",
    apsw.SQLITE_INDEX_CONSTRAINT_LT: "lt",
}

SUPPORTED_OPERATIONS = set(operator_map.keys())


class Filter:
    pass


@dataclass
class Range(Filter):
    start: Optional[Any]
    end: Optional[Any]
    include_start: bool
    include_end: bool


class Order(Enum):
    ASCENDING = "ascending"
    DESCENDING = "descending"
    NONE = "none"


class Type:
    def __init__(
        self, indexes: List[Filter], order: Order = Order.NONE, exact: bool = False
    ):
        self.indexes = indexes
        self.order = order
        self.exact = exact


class DateTime(Type):
    type = "TIMESTAMP"

    @staticmethod
    def parse(value):
        return dateutil.parser.parse(value)


class Float(Type):
    type = "REAL"

    @staticmethod
    def parse(value):
        return float(value)


class VirtualTable:
    pass


class StaticVirtualTable(VirtualTable):
    @classmethod
    def Create(
        cls,
        connection: apsw.Connection,
        modulename: str,
        dbname: str,
        tablename: str,
        *args: str,
    ) -> Tuple[str, VirtualTable]:
        table = cls(*args)
        table.columns = get_columns(cls)
        create_table = get_create_table(tablename, table.columns)
        return create_table, table

    Connect = Create

    def BestIndex(self, constraints, orderbys):
        column_types = [column[1] for column in self.columns]

        index_number = 42
        indexes = []

        filter_index = 0
        constraints_used = []
        for column_index, operation in constraints:
            column_type = column_types[column_index]
            # TODO: different support ops depending on filter type
            if column_type.indexes and operation in SUPPORTED_OPERATIONS:
                constraints_used.append((filter_index, column_type.exact))
                filter_index += 1
                indexes.append((column_index, operator_map[operation]))
            else:
                constraints_used.append(None)

        index_name = json.dumps(indexes)

        orderby_consumed = True
        for column_index, descending in orderbys:
            column = column_types[column_index]
            if (
                column.order == Order.NONE
                or (column.order == Order.DESCENDING) == descending
            ):
                orderby_consumed = False
                break

        estimated_cost = 1000

        return (
            constraints_used,
            index_number,
            index_name,
            orderby_consumed,
            estimated_cost,
        )

    def Open(self):
        return Cursor(self)

    def Disconnect(self):
        pass

    Destroy = Disconnect


def compact_bounds(all_bounds):
    bounds = {}
    for name, constraints in all_bounds.items():
        start = end = None
        include_start = include_end = False
        exact_matches = set()
        for operator, value in constraints:
            if operator == "gt" and (start is None or start <= value):
                start = value
                include_start = False
            elif operator == "ge" and (start is None or start < value):
                start = value
                include_start = True
            elif operator == "lt" and (end is None or end >= value):
                end = value
                include_end = False
            elif operator == "le" and (end is None or end > value):
                end = value
                include_end = True
            elif operator == "eq":
                exact_matches.add(value)

        if len(exact_matches) > 1:
            bounds[name] = False
            continue

        if len(exact_matches) == 1:
            exact_match = exact_matches[0]
            continue

        bounds[name] = Range(start, end, include_start, include_end)

    return bounds


class Cursor:
    def __init__(self, table):
        self.table = table

    def Filter(self, indexnumber, indexname, constraintargs):
        indexes = json.loads(indexname)

        all_bounds = defaultdict(set)
        for (column_index, operator), constraint in zip(indexes, constraintargs):
            column_name, column_type = self.table.columns[column_index]
            value = column_type.parse(constraint)
            all_bounds[column_name].add((operator, value))

        bounds = compact_bounds(all_bounds)

        column_names = [column[0] for column in self.table.columns]
        self.data = (
            tuple(row[name] for name in ["rowid"] + column_names)
            for row in self.table.get_data(bounds)
        )
        self.Next()

    def Eof(self):
        return self.eof

    def Rowid(self):
        return self.current_row[0]

    def Column(self, col):
        return self.current_row[1 + col]

    def Next(self):
        try:
            self.current_row = next(self.data)
            self.eof = False
        except StopIteration:
            self.eof = True

    def Close(self):
        pass


def get_columns(cls) -> List[Tuple[str, Type]]:
    return inspect.getmembers(cls, lambda attribute: isinstance(attribute, Type))


def get_create_table(tablename: str, columns) -> str:
    formatted_columns = ", ".join(f"{k} {v.type}" for (k, v) in columns)
    return f"CREATE TABLE {tablename} ({formatted_columns})"


#############
# User code #
#############


class WeatherAPI(StaticVirtualTable):

    ts = DateTime(indexes=[Range], order=Order.ASCENDING, exact=False)
    temperature = Float(indexes=[])

    def __init__(self, location: str, api_key: str):
        self.location = location
        self.api_key = api_key

    def get_data(self, bounds) -> Iterator[Dict[str, Any]]:
        ts_range = bounds["ts"]
        today = date.today()
        start = ts_range.start.date() if ts_range.start else today - timedelta(days=7)
        end = ts_range.end.date() if ts_range.end else today

        while start <= end:
            url = (
                f"https://api.weatherapi.com/v1/history.json?key={self.api_key}"
                f"&q={self.location}&dt={start}"
            )
            response = requests.get(url)
            if response.ok:
                payload = response.json()
                hourly_data = payload["forecast"]["forecastday"][0]["hour"]
                for record in hourly_data:
                    dt = dateutil.parser.parse(record["time"])
                    yield {
                        "rowid": int(dt.timestamp()),
                        "ts": dt.isoformat(),
                        "temperature": record["temp_c"],
                    }

            start += timedelta(days=1)


if __name__ == "__main__":
    connection = apsw.Connection("dbfile")
    cursor = connection.cursor()
    connection.createmodule("weatherapi", WeatherAPI)

    cursor.execute(
        "CREATE VIRTUAL TABLE bodega_bay USING weatherapi(94923, f426b51ea9aa4e4ab68190907202309)"
    )
    cursor.execute(
        "CREATE VIRTUAL TABLE san_mateo USING weatherapi(94401, f426b51ea9aa4e4ab68190907202309)"
    )

    sql = "SELECT * FROM bodega_bay WHERE ts >= '2020-09-24T02:00:00'"
    for row in cursor.execute(sql):
        print(row)

    sql = """
SELECT * FROM bodega_bay
JOIN san_mateo
ON bodega_bay.ts = san_mateo.ts
WHERE bodega_bay.ts > '2020-09-20T12:00:00'
AND san_mateo.ts > '2020-09-20T12:00:00'
AND san_mateo.temperature < bodega_bay.temperature
    """
    for row in cursor.execute(sql):
        print(row)
