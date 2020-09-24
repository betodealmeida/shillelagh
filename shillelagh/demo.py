import inspect
import json
from collections import defaultdict
from datetime import date
from datetime import datetime
from datetime import timedelta
from enum import Enum
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


class Order(Enum):
    ASCENDING = "ascending"
    DESCENDING = "descending"
    NONE = "none"


class Type:
    def __init__(self, indexed: bool, order: Order = Order.NONE, exact: bool = False):
        self.indexed = indexed
        self.order = order
        self.exact = exact


class DateTime(Type):
    type = "TIMESTAMP"


class Float(Type):
    type = "REAL"


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
        # we only care about column types
        columns = [column[1] for column in self.columns]

        index_number = 42
        indexes = []

        filter_index = 0
        constraints_used = []
        for column_index, operation in constraints:
            column = columns[column_index]
            if column.indexed and operation in SUPPORTED_OPERATIONS:
                constraints_used.append((filter_index, column.exact))
                filter_index += 1
                indexes.append((column_index, operator_map[operation]))
            else:
                constraints_used.append(None)

        index_name = json.dumps(indexes)

        orderby_consumed = True
        for column_index, descending in orderbys:
            column = columns[column_index]
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
    print(all_bounds)
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

        bounds[name] = start, include_start, end, include_end

    print(bounds)
    return bounds


class Cursor:
    def __init__(self, table):
        self.table = table

    def Filter(self, indexnumber, indexname, constraintargs):
        indexes = json.loads(indexname)

        all_bounds = defaultdict(set)
        for (column_index, operator), constraint in zip(indexes, constraintargs):
            column_name = self.table.columns[column_index][0]
            all_bounds[column_name].add((operator, constraint))

        bounds = compact_bounds(all_bounds)

        self.data = list(self.table.get_data(bounds))
        self.pos = 0

    def Eof(self):
        return self.pos >= len(self.data)

    def Rowid(self):
        return self.data[self.pos][0]

    def Column(self, col):
        return self.data[self.pos][1 + col]

    def Next(self):
        self.pos += 1

    def Close(self):
        pass


def get_columns(cls) -> str:
    return [
        t
        for t in inspect.getmembers(cls, lambda attribute: isinstance(attribute, Type))
    ]


def get_create_table(tablename: str, columns) -> str:
    formatted_columns = ", ".join(f"{k} {v.type}" for (k, v) in columns)
    return f"CREATE TABLE {tablename} ({formatted_columns})"


#############
# User code #
#############


class WeatherAPI(StaticVirtualTable):

    ts = DateTime(indexed=True, order=Order.ASCENDING, exact=False)
    temperature = Float(indexed=False)

    def __init__(self, location: str, api_key: str):
        self.location = location
        self.api_key = api_key

    def get_data(self, bounds) -> Iterator[Tuple[datetime, float]]:
        start, include_start, end, include_end = bounds.get(
            "ts", (None, True, None, True)
        )

        today = date.today()
        start = (
            dateutil.parser.parse(start).date() if start else today - timedelta(days=7)
        )
        end = dateutil.parser.parse(end).date() if end else today

        while start <= end:
            url = f"https://api.weatherapi.com/v1/history.json?key={self.api_key}&q={self.location}&dt={start}"
            response = requests.get(url)
            if response.ok:
                payload = response.json()
                hourly_data = payload["forecast"]["forecastday"][0]["hour"]
                for i, data in enumerate(hourly_data):
                    yield i, dateutil.parser.parse(data["time"]).isoformat(), data[
                        "temp_c"
                    ]

            start += timedelta(days=1)


if __name__ == "__main__":
    connection = apsw.Connection("dbfile")
    cursor = connection.cursor()
    connection.createmodule("weatherapi", WeatherAPI)

    try:
        cursor.execute(
            "create virtual table bodega_bay using weatherapi(94923, f426b51ea9aa4e4ab68190907202309)"
        )
    except apsw.SQLError:
        pass

    for row in cursor.execute(
        "SELECT * FROM bodega_bay WHERE ts >= '2020-09-23T02:00:00'"
    ):
        print(row)
