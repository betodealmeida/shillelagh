"""
An adapter to Google Analytics Core Reporting V4 Api.
all apis: https://developers.google.com/analytics
later port it to upcoming GA4 property as this will be deprecated by google
"""
import datetime
from typing import Any, Dict, Iterator, List, Optional, Tuple

from apiclient.discovery import build
from sqlparse import parse
from sqlparse.sql import Comment, Identifier, IdentifierList

from shillelagh.adapters.api.google_analytics.constants import (
    ALL_DIMENSIONS,
    ALL_METRICS,
    SCOPES,
)
from shillelagh.adapters.base import Adapter
from shillelagh.adapters.utils import get_credentials
from shillelagh.fields import Field, GoogleAnalyticsDate, Order
from shillelagh.filters import Range
from shillelagh.typing import RequestedOrder, Row


class GoogleAnalyticsAPI(Adapter):

    """
    An adapter to Google Analytics
    """

    need_operation = True
    columns: Dict[str, Field] = {}

    def __init__(  # pylint: disable=too-many-arguments
        self,
        uri=None,  # pylint: disable=unused-argument
        access_token: Optional[str] = None,
        service_account_file: Optional[str] = None,
        service_account_info: Optional[Dict[str, Any]] = None,
        subject: Optional[str] = None,
        app_default_credentials: bool = False,
        view_id: str = None,
        default_start_date: Optional[str] = None,
        default_end_date: Optional[str] = None,
        operation: str = None,
        parameters: Dict[str, Any] = None,  # pylint: disable=unused-argument
    ):
        super().__init__()
        self.view_id = view_id
        self.default_start_date = default_start_date
        self.default_end_date = default_end_date

        self.default_end = (
            datetime.date.fromisoformat(self.default_end_date)
            if self.default_end_date
            else datetime.date.today()
        )
        self.default_start = (
            datetime.date.fromisoformat(self.default_start_date)
            if self.default_start_date
            else self.default_end - datetime.timedelta(days=7)
        )

        credentials = get_credentials(
            access_token,
            service_account_file,
            service_account_info,
            subject,
            app_default_credentials,
            SCOPES,
        )

        self.analytics_service = build(
            "analyticsreporting",
            "v4",
            credentials=credentials,
        )

        self.parsed_operation = parse(operation)
        parsed_columns = self._get_query_columns()
        self._set_columns(parsed_columns)

    @staticmethod
    def supports(uri: str, fast: bool = True, **kwargs: Any) -> Optional[bool]:
        return uri.startswith("google_analytics")

    @staticmethod
    def parse_uri(uri: str) -> Tuple[str, ...]:
        return (uri,)

    def _get_query_columns(self) -> List[str]:
        """
        Get the list of columns used in the query
        """
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
            columns.append(column_identifier.get_name().replace("`", ""))

        return columns

    def get_data(  # type: ignore[override]  # pylint: disable=inconsistent-return-statements,disable=too-many-locals
        self,
        bounds: Dict[str, Range],
        order: List[Tuple[str, RequestedOrder]],
        **kwargs: Any,
    ) -> Iterator[Row]:
        metrics = []
        dimensions = []

        # for column in ALL_DIMENSIONS[:9]:
        #     dimensions.append({"name": f"ga:{column[0]}"})
        # for column in ALL_METRICS[:10]:
        #     metrics.append({"expression": f"ga:{column[0]}"})

        date_bounds = bounds.get("date", Range())

        date_range = [
            {
                "startDate": str(date_bounds.start or self.default_start),
                "endDate": str(date_bounds.end or self.default_end),
            },
        ]

        for col in self.columns:
            if list(
                filter(
                    lambda d: col == d[0],  # pylint: disable=cell-var-from-loop
                    ALL_DIMENSIONS,
                ),
            ):
                dimensions.append({"name": f"ga:{col}"})
            elif list(
                filter(
                    lambda d: col == d[0],  # pylint: disable=cell-var-from-loop
                    ALL_METRICS,
                ),
            ):
                metrics.append({"expression": f"ga:{col}"})

        data = (
            self.analytics_service.reports()
            .batchGet(
                body={
                    "reportRequests": [
                        {
                            "viewId": self.view_id,
                            "dateRanges": date_range,
                            "metrics": metrics,
                            "dimensions": dimensions,
                        },
                    ],
                },
            )
            .execute()
        )

        if len(data["reports"]) == 0:
            return iter([])

        reports = data["reports"][0]
        dimension_columns = reports["columnHeader"]["dimensions"]
        metric_columns = [
            m["name"]
            for m in reports["columnHeader"]["metricHeader"]["metricHeaderEntries"]
        ]

        columns = [qc.split(":")[1] for qc in dimension_columns + metric_columns]
        for rowid, row in enumerate(reports["data"]["rows"]):
            row_values = row["dimensions"] + row["metrics"][0]["values"]
            row_data = {"rowid": rowid}
            for index, col in enumerate(columns):
                row_data[col] = row_values[index]

            yield row_data

    def _set_columns(self, parsed_columns):
        for col in parsed_columns:
            if list(
                filter(
                    lambda d: col == d[0],  # pylint: disable=cell-var-from-loop
                    ALL_DIMENSIONS + ALL_METRICS,
                ),
            ):
                self.columns[col] = list(
                    filter(
                        lambda d: col == d[0],  # pylint: disable=cell-var-from-loop
                        ALL_DIMENSIONS + ALL_METRICS,
                    ),
                )[0][1]
        self.columns["date"] = GoogleAnalyticsDate(
            filters=[Range],
            order=Order.NONE,
            exact=True,
        )

    def get_columns(self) -> Dict[str, Field]:
        return self.columns
        # {
        #     column[0] : column[1] for column in ALL_DIMENSIONS + ALL_METRICS
        #    }
