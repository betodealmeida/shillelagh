from shillelagh.adapters.base import Adapter
from typing import Tuple, Any, Dict, List, Iterator, Optional
from shillelagh.filters import Filter, Range
from shillelagh.fields import Field, Order, String, Integer, Float, Date, Time, DateTime
from shillelagh.typing import RequestedOrder, Row
from apiclient.discovery import build
from sqlparse import parse
from sqlparse.sql import Identifier, IdentifierList, Where, Comment

from oauth2client.service_account import ServiceAccountCredentials
from shillelagh.adapters.utils import get_credentials
from shillelagh.adapters.api.google_analytics.constants import ALL_DIMENSIONS, ALL_METRICS, SCOPES

# all apis: https://developers.google.com/analytics
# this implements adapter for core reporting api v4
# later port it to upcoming GA4 property as this will be deprecated by google

"""
TODO: fill all items in ALL_DIMESIONS and ALL_METRICS in below lists

TODO: Get date range from the query
"""

class GoogleAnalytics(Adapter):
    columns: Dict[str, Field] = {}

    def __init__(
        self,
        uri,
        access_token: Optional[str] = None,
        service_account_file: Optional[str] = None,
        service_account_info: Optional[Dict[str, Any]] = None,
        subject: Optional[str] = None,
        app_default_credentials: bool = False,
        view_id=None,
        operation: str = None,
        parameters=Optional[Tuple[Any, ...]],
        date_range = None,
    ):
        super().__init__()
        self.view_id = view_id
        self.operation = operation
        self.parameters = parameters
        self.date_range = date_range

        credentials = get_credentials(
            access_token,
            service_account_file,
            service_account_info,
            subject, 
            app_default_credentials,
            SCOPES
            )

        self.analytics_service = build(
            "analyticsreporting", "v4", credentials=credentials
        )

        self.parsed_operation = parse(operation)
        parsed_columns = self.get_query_columns()
        self._set_columns(parsed_columns)

    def supports(uri: str, fast: bool, **kwargs):
        return uri.startswith("google_analytics")
        
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

    def get_data(
        self, bound: Dict[str, Filter], order: List[Tuple[str, RequestedOrder]]
    ) -> Iterator[Row]:
        metrics = []
        dimensions = []

        print("bounds ", bound, "orders" , order)


        for col in self.columns:
            if list(filter(lambda d: col == d[0], ALL_DIMENSIONS)):
                dimensions.append({"name": f"ga:{col}"})
            elif list(filter(lambda d: col == d[0], ALL_METRICS)):
                metrics.append({"expression": f"ga:{col}"})

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
            if qc.split(":")[1] in self.columns:
                columns.append(qc.split(":")[1])

        for row in reports["data"]["rows"]:
            row_values = row["dimensions"] + row["metrics"][0]["values"]
            row_data = {}

            for index, c in enumerate(columns):
                row_data[c] = row_values[index]

            yield row_data

    def _set_columns(self, parsed_columns):
        for col in parsed_columns:
            if list(filter(lambda d: col == d[0], ALL_DIMENSIONS + ALL_METRICS)):
                data_type = list(filter(lambda d: col == d[0], ALL_DIMENSIONS + ALL_METRICS))[0][1]
                self.columns[col] = data_type()
        self.columns["date"] = Date(filters=[Range], order=Order.NONE, exact=True)

    def get_columns(self) -> Dict[str, Field]:
        return self.columns
