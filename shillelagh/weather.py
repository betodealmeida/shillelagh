import json
from datetime import date
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any
from typing import List

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
    apsw.SQLITE_INDEX_CONSTRAINT_MATCH: "match",
}


def get_data(query: str, api_key: str, start: date, end: date) -> List[Any]:
    results = []

    while start <= end:
        url = f"https://api.weatherapi.com/v1/history.json?key={api_key}&q={query}&dt={start}"
        # print(f"Fetching {url}")
        response = requests.get(url)
        if response.ok:
            payload = response.json()
            results.extend(
                [
                    (i, hour["time"], hour["temp_c"])
                    for i, hour in enumerate(
                        payload["forecast"]["forecastday"][0]["hour"]
                    )
                ]
            )
        start += timedelta(days=1)

    return results


class WeatherAPI:
    def Create(self, db, modulename, dbname, tablename, query, api_key):
        create_table = "CREATE TABLE weatherapi (ts TIMESTAMP, temperature REAL)"
        table = Table(query, api_key)
        return create_table, table

    Connect = Create


class Table:
    def __init__(self, query: str, api_key: str):
        self.query = query
        self.api_key = api_key

    def BestIndex(self, constraints, orderbys):
        # we only filter on time
        index_number = 0
        index_names = []

        constraints_used = []
        for column_index, operation in constraints:
            if column_index == 0 and operation != apsw.SQLITE_INDEX_CONSTRAINT_MATCH:
                constraints_used.append((index_number, False))
                index_number += 1
                index_names.append(operator_map[operation])
            else:
                constraints_used.append(None)  # no index
        index_name = json.dumps(index_names)

        orderby_consumed = all(
            column_index == 0 and not descending
            for column_index, descending in orderbys
        )

        # XXX
        estimated_cost = 1000

        return (
            constraints_used,
            index_number,
            index_name,
            orderby_consumed,
            estimated_cost,
        )

    def Open(self):
        # XXX
        return Cursor(self.query, self.api_key)

    def Disconnect(self):
        pass

    Destroy = Disconnect


class Cursor:
    def __init__(self, query, api_key):
        self.query = query
        self.api_key = api_key

    def Filter(self, indexnumber, indexname, constraintargs):
        operations = json.loads(indexname)
        comparisons = zip(operations, constraintargs)

        # find start date; free API is limited to 7 days
        start = date.today() - timedelta(days=7)
        end = date.today()
        exact_matches = set()
        for operation, constraint in zip(operations, constraintargs):
            dt = dateutil.parser.parse(constraint).date()
            if operation in {"gt", "ge"} and start < dt:
                start = dt
            elif operation in {"lt", "le"} and end > dt:
                end = dt
            elif operation == "eq":
                exact_matches.add(dt)

        if len(exact_matches) > 1:
            self.data = []
        elif len(exact_matches) == 1:
            if not start <= exact_matches[0] <= end:
                self.data = []
            else:
                self.data = get_data(self.query, self.api_key, exact_match, exact_match)
        else:
            self.data = get_data(self.query, self.api_key, start, end)

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


connection = apsw.Connection("dbfile")
cursor = connection.cursor()
connection.createmodule("weatherapi", WeatherAPI())

try:
    cursor.execute(
        "create virtual table bodega_bay using weatherapi(94923, f426b51ea9aa4e4ab68190907202309)"
    )
except apsw.SQLError:
    pass
try:
    cursor.execute(
        "create virtual table channel_mission_bay using weatherapi(94158, f426b51ea9aa4e4ab68190907202309)"
    )
except apsw.SQLError:
    pass

for row in cursor.execute("SELECT * FROM bodega_bay WHERE ts >= '2020-09-23 02:00'"):
    print(row)

sql = """
SELECT * FROM bodega_bay
JOIN channel_mission_bay
ON bodega_bay.ts = channel_mission_bay.ts
WHERE bodega_bay.ts > '2020-09-20 12:00'
AND channel_mission_bay.ts > '2020-09-20 12:00'
AND channel_mission_bay.temperature < bodega_bay.temperature
"""
for row in cursor.execute(sql):
    print(row)
