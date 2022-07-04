from shillelagh.adapters.api.gsheets.lib import get_field
from shillelagh.adapters.base import Adapter
from typing import Tuple, Any, Dict, List, Iterator, Optional
from shillelagh.filters import Filter
from shillelagh.fields import Field, String
from shillelagh.typing import RequestedOrder, Row
from apiclient.discovery import build
from sqlparse import parse
from sqlparse.sql import Identifier, IdentifierList, Where, Comment

from oauth2client.service_account import ServiceAccountCredentials

# all apis: https://developers.google.com/analytics
# this implements adapter for core reporting api v4
# later port it to upcoming GA4 property as this will be deprecated by google

"""
TODO: checkout apsw doc for pagination with virtual cursor so we query GA with correct offset

TODO: fill all items in ALL_DIMESIONS and ALL_METRICS in below lists

TODO: in google_analytics dialect find a way to correctly decode http encoded content saved by superset

TODO: currently date ranges for query need to be setup when db is added, modify shillelagh to give adapter a hook to modify query which will remove those ranges from the query but use it when querying GA only.
        this is required because date ranges on GA are like "7daysAgo", "today" which are not valid in sqlite

TODO: currently all dimesions and metrics are treated as string, use appropriate data type
"""

# source: https://ga-dev-tools.web.app/dimensions-metrics-explorer/
ALL_DIMENSIONS = [
    #User
    "ga:userType",
    "ga:sessionCount",
    "ga:daysSinceLastSession",
    "ga:userDefinedValue",
    "ga:userBucket",

    #Session
    "ga:sessionDurationBucket",

    #Traffic sources
    "ga:referralPath",
    "ga:fullReferrer",
    "ga:campaign",
    "ga:source",
    "ga:medium",
    "ga:sourceMedium",
    "ga:keyword",
    "ga:adContent",
    "ga:adContent",
    "ga:hasSocialSourceReferral",
    "ga:campaignCode",
]
ALL_METRICS = [
    #User
    "ga:users",
    "ga:newUsers",
    "ga:percentNewSessions",
    "ga:1dayUsers",
    "ga:7dayUsers",
    "ga:14dayUsers",
    "ga:28dayUsers",
    "ga:30dayUsers",
    "ga:sessionsPerUser",

    #Session
    "ga:sessions",
    "ga:bounces",
    "ga:bounceRate",
    "ga:sessionDuration",
    "ga:avgSessionDuration",
    "ga:uniqueDimensionCombinations",
    "ga:hits",

    #Traffic Sources
    "ga:organicSearches"
]

SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]

class GoogleAnalytics(Adapter):
    columns: Dict[str, Field] = {}

    def __init__(
        self,
        uri,
        service_account_info: str = None,
        view_id=None,
        operation: str = None,
        parameters=Optional[Tuple[Any, ...]],
        date_range = None,
        # service_account_info = None
    ):
        super().__init__()
        self.view_id = view_id
        self.operation = operation
        self.parameters = parameters
        self.date_range = date_range

        credentials = ServiceAccountCredentials.from_json_keyfile_dict(
            service_account_info, SCOPES
        )
        self.analytics_service = build(
            "analyticsreporting", "v4", credentials=credentials
        )

        self.parsed_operation = parse(operation)
        parsed_columns = self.get_query_columns()
        self._set_columns(parsed_columns)

    def supports(uri: str, fast: bool, **kwargs):
        # check docstring on Adapter class
        # it shows how to pass additional args to while connecting
        # there we can pass creds file and view id

        if uri.startswith("google_analytics"):
            return True

    @staticmethod
    def parse_uri(uri: str) -> Tuple[Any, ...]:
        return (uri,)

    @staticmethod
    def need_operation():
        return True

    def get_query_columns(self):
        stmt = self.parsed_operation[0]
        columns = []
        column_identifiers = []

        # get column_identifieres
        in_select = False
        for token in stmt.tokens:
            if isinstance(token, Comment):
                continue
            if str(token).lower() == "select":
                in_select = True
            elif in_select and token.ttype is None:
                if isinstance(token, Identifier):
                    column_identifiers.append(token)
                elif isinstance(token, IdentifierList):
                    for identifier in token.get_identifiers():
                        column_identifiers.append(identifier)
                break

        # get column names
        for column_identifier in column_identifiers:
            columns.append(column_identifier.get_name().replace('`', ''))

        return columns

    # def get_date_range(self):
    #     parsed_operation = self.parsed_operation[0]
    #     for token in parsed_operation:
    #         if isinstance(token, Where) and token.value.startswith("where dateRange in"):
    #             range_string = token.value.replace("where dateRange in", "").strip().replace("(", "").replace(")", "").split(",")
    #             if (len(range_string) == 2):
    #                 return range_string[0].strip(), range_string[1].strip()
    #     return None

    def get_data(
        self, bound: Dict[str, Filter], order: List[Tuple[str, RequestedOrder]]
    ) -> Iterator[Row]:
        metrics = []
        dimensions = []

        for col in self.columns:
            if col in ALL_DIMENSIONS:
                dimensions.append({"name": col})
            elif col in ALL_METRICS:
                metrics.append({"expression": col})

        # date_range = self.get_date_range()
        
        data = (
            self.analytics_service.reports()
            .batchGet(
                body={
                    "reportRequests": [
                        {
                            "viewId": self.view_id,
                            "dateRanges": self.date_range,
                            "metrics": metrics,
                            "dimensions": dimensions,
                        }
                    ]
                }
            )
            .execute()
        )
        if len(data["reports"]) == 0:
            return []

        reports = data["reports"][0]
        dimension_columns = reports["columnHeader"]["dimensions"]
        metric_columns = [
            m["name"]
            for m in reports["columnHeader"]["metricHeader"]["metricHeaderEntries"]
        ]

        query_columns = dimension_columns + metric_columns
        columns = []
        for qc in query_columns:
            if qc in self.columns:
                columns.append(qc)

        for row in reports["data"]["rows"]:
            row_values = row["dimensions"] + row["metrics"][0]["values"]
            row_data = {}

            for index, c in enumerate(columns):
                row_data[c] = row_values[index]

            yield row_data

    def _set_columns(self, parsed_columns):
        for col in parsed_columns:
            self.columns[col] = String()

    def get_columns(self) -> Dict[str, Field]:
        return self.columns
