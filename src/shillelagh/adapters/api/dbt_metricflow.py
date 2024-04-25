"""
An adapter for querying dbt Metric Flow metrics.

This is meant to be used with Apache Superset and has only been tested with the SQL that
it produces, eg:

    SELECT
        order_id__ordered_at__month AS order_id__ordered_at,
        order_id__is_food_order AS order_id__is_food_order,
        orders as orders
    FROM "https://semantic-layer.cloud.getdbt.com/"
    WHERE
        order_id__ordered_at >= '2017-01-01T00:00:00.000000'
        AND order_id__ordered_at < '2018-01-01T00:00:00.000000'
        AND order_id__is_food_order = true
    GROUP BY
        order_id__ordered_at__month,
        order_id__is_food_order
    ORDER BY order_id__ordered_at DESC
    LIMIT 10000

"""

import base64
import datetime
import inspect
import logging
import re
import time
from collections import defaultdict
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple, TypedDict, cast
from urllib.parse import urlparse

import pandas as pd
import pyarrow as pa
import sqlglot
from python_graphql_client import GraphqlClient
from sqlglot import expressions as exp
from sqlglot.optimizer.qualify_columns import qualify_columns
from sqlglot.optimizer.scope import traverse_scope

from shillelagh.adapters.base import Adapter
from shillelagh.backends.apsw.db import Cursor
from shillelagh.exceptions import ImpossibleFilterError, InternalError, ProgrammingError
from shillelagh.fields import (
    Boolean,
    Date,
    Decimal,
    Field,
    Filter,
    Integer,
    Order,
    String,
    Time,
    Unknown,
)
from shillelagh.filters import Equal, Impossible, IsNotNull, IsNull, NotEqual, Range
from shillelagh.typing import RequestedOrder, Row

_logger = logging.getLogger(__name__)

CUSTOM_URL_PATTERN = re.compile(
    r"""
        (?P<id>
            [a-zA-Z0-9]
            (?:
                [a-zA-Z0-9-]{0,61}
                [a-zA-Z0-9]
            )?
        )
        \.
        (?P<region>
            [a-zA-Z0-9]
            (?:
                [a-zA-Z0-9-]{0,61}
                [a-zA-Z0-9]
            )?
        )
        \.dbt.com
        $
    """,
    re.VERBOSE,
)

LIST_METRICS = """
    query GetMetrics($environmentId: BigInt!) {
        metrics(environmentId: $environmentId) {
            name
            description
            type
            dimensions {
                name
                description
                queryableGranularities
                type
                expr
            }
        }
    }
"""

CREATE_QUERY = """
    mutation GetData(
        $environmentId: BigInt!,
        $metrics: [MetricInput!]!,
        $where: [WhereInput!]!,
        $groupBy: [GroupByInput!]!,
        $orderBy: [OrderByInput!]!,
        $limit: Int,
    ) {
        createQuery(
            environmentId: $environmentId,
            metrics: $metrics,
            where: $where,
            groupBy: $groupBy,
            orderBy: $orderBy,
            limit: $limit,
        ) {
            queryId
        }
    }
"""

POLL_RESULTS = """
    query PollResults(
        $environmentId: BigInt!,
        $queryId: String!,
    ) {
        query(
            environmentId: $environmentId,
            queryId: $queryId,
        ) {
            sql
            status
            error
            arrowResult
        }
    }
"""

METRICS_FOR_DIMENSIONS = """
    query GetDimensions(
        $environmentId: BigInt!,
        $dimensions: [GroupByInput!]!,
    ) {
        metricsForDimensions(
            environmentId: $environmentId,
            dimensions: $dimensions,
        ) {
            name
        }
    }
"""

DIMENSIONS_FOR_METRICS = """
    query GetMetrics(
        $environmentId: BigInt!,
        $metrics: [MetricInput!]!,
    ) {
        dimensions(
            environmentId: $environmentId,
            metrics: $metrics,
        ) {
            name
        }
    }
"""


def find_cursor() -> Optional[Cursor]:
    """
    Inspects the stack up to find the cursor that called the adapter.

    This is ugly. It makes me want to quit tech and become a gardener.
    """
    for frame in inspect.stack():
        for value in frame.frame.f_locals.values():
            if isinstance(value, Cursor):
                return value

    return None


def extract_columns_from_sql(table: str, columns: Set[str], sql: str) -> Set[str]:
    """
    Parse the SQL and extract requested columns.
    """
    schema = {table: {column: "unused" for column in columns}}
    ast = sqlglot.parse_one(sql)
    ast = qualify_columns(ast, schema=schema)
    return {
        column.name
        for scope in traverse_scope(ast)
        for column in scope.columns
        if isinstance(scope.sources.get(column.table), exp.Table)
        and column.table == table
    }


class Timestamp(Field[pd.Timestamp, datetime.datetime]):
    """
    Pandas Timestamp.
    """

    type = "TIMESTAMP"
    db_api_type = "DATETIME"

    def parse(self, value: Optional[pd.Timestamp]) -> Optional[datetime.datetime]:
        return value.to_pydatetime() if value is not None else None

    def format(self, value: Optional[datetime.datetime]) -> Optional[pd.Timestamp]:
        return pd.Timestamp(value) if value is not None else None

    def quote(self, value: Optional[pd.Timestamp]) -> str:
        if value is None:
            return "NULL"
        return f"'{value.to_pydatetime().isoformat()}'"


class WhereInput(TypedDict):
    """
    Where parameter in the GraphQL API.
    """

    sql: str


class GroupByInput(TypedDict, total=False):
    """
    Group by parameter in the GraphQL API.
    """

    name: str
    grain: Optional[str]  # DAY, WEEK, MONTH, QUARTER, YEAR
    datePart: Optional[str]  # YEAR, QUARTER, MONTH, DAY, DOW, DOY


class MetricInput(TypedDict):
    """
    Metric input parameter in the GraphQL API.
    """

    name: str


class OrderByInput(TypedDict, total=False):
    """
    Order by parameter in the GraphQL API.
    """

    metric: MetricInput
    groupBy: GroupByInput
    descending: bool


def stream_to_dataframe(byte_string: str) -> pd.DataFrame:
    """
    Convert an Arrow stream to a Pandas DataFrame.
    """
    with pa.ipc.open_stream(base64.b64decode(byte_string)) as reader:
        return pa.Table.from_batches(reader, reader.schema).to_pandas()


class DbtMetricFlowAPI(Adapter):
    """
    An adapter for querying dbt Metric Flow metrics.
    """

    safe = True
    supports_limit = True
    supports_offset = False

    @staticmethod
    def supports(uri: str, fast: bool = True, **kwargs: Any) -> Optional[bool]:
        """
        Is this a semantic layer URL?

        The adapter supports both the old single URL or custom user URLs.
        """
        parsed = urlparse(uri)
        return uri == "https://semantic-layer.cloud.getdbt.com/" or (
            parsed.scheme == "https" and bool(CUSTOM_URL_PATTERN.match(parsed.netloc))
        )

    @staticmethod
    def parse_uri(uri: str) -> Tuple[str]:
        return (uri,)

    def __init__(self, table: str, service_token: str, environment_id: int):
        super().__init__()

        self.table = table
        self.environment_id = environment_id

        endpoint = self._get_endpoint(table)
        self.client = GraphqlClient(
            endpoint=endpoint,
            headers={"Authorization": f"Bearer {service_token}"},
        )

        self._set_columns()

    @staticmethod
    def _get_endpoint(url: str) -> str:
        """
        Return the GraphQL endpoint.
        """
        parsed = urlparse(url)
        if match := CUSTOM_URL_PATTERN.match(parsed.netloc):
            return f"https://{match['id']}.semantic-layer.{match['region']}.dbt.com/api/graphql"

        return "https://semantic-layer.cloud.getdbt.com/api/graphql"

    def _set_columns(self) -> None:
        payload = self.client.execute(
            query=LIST_METRICS,
            variables={"environmentId": self.environment_id},
        )

        self.columns: Dict[str, Field] = {}
        self.metrics: Dict[str, str] = {}
        self.dimensions: Dict[str, str] = {}
        self.grains: Dict[str, Tuple[str, str]] = {}

        built_dimensions: Set[str] = set()
        for metric in payload["data"]["metrics"]:
            self.columns[metric["name"]] = Decimal()
            self.metrics[metric["name"]] = metric["description"]

            for dimension in metric["dimensions"]:
                name = dimension["name"]
                if name in built_dimensions:
                    continue

                column = self._build_column_from_dimension(name)
                built_dimensions.add(name)

                # for time dimensions we create a dimension for each grain
                if dimension["type"] == "TIME":
                    for grain in dimension["queryableGranularities"]:
                        alias = f"{name}__{grain.lower()}"
                        self.columns[alias] = column
                        self.dimensions[alias] = dimension["description"]
                        self.grains[alias] = (name, grain.lower())
                else:
                    self.columns[name] = column
                    self.dimensions[name] = dimension["description"]

        self.columns = dict(sorted(self.columns.items()))

    def _run_query(self, **variables: Any) -> str:
        """
        Run a query, wait for it, and return Arrow payload.
        """
        payload = self.client.execute(query=CREATE_QUERY, variables=variables)
        if errors := payload.get("errors"):
            raise InternalError("\n\n".join(error["message"] for error in errors))

        query_id = payload["data"]["createQuery"]["queryId"]

        while True:
            payload = self.client.execute(
                query=POLL_RESULTS,
                variables={
                    "environmentId": self.environment_id,
                    "queryId": query_id,
                },
            )
            if error := payload["data"]["query"].get("error"):
                _logger.debug(payload["data"]["query"]["sql"])
                raise ProgrammingError(error)

            if payload["data"]["query"]["status"] == "SUCCESSFUL":
                break

            time.sleep(1)

        return cast(str, payload["data"]["query"]["arrowResult"])

    def _get_metrics_for_dimensions(self, dimensions: Set[str]) -> Set[str]:
        """
        Get metrics for a set of dimensions.
        """
        payload = self.client.execute(
            query=METRICS_FOR_DIMENSIONS,
            variables={
                "environmentId": self.environment_id,
                "dimensions": [{"name": dimension} for dimension in dimensions],
            },
        )

        return {metric["name"] for metric in payload["data"]["metricsForDimensions"]}

    def _get_dimensions_for_metrics(self, metrics: Set[str]) -> Set[str]:
        """
        Get dimensions for a set of metrics.
        """
        payload = self.client.execute(
            query=DIMENSIONS_FOR_METRICS,
            variables={
                "environmentId": self.environment_id,
                "metrics": [{"name": metric} for metric in metrics],
            },
        )

        reverse_grain: Dict[str, Set[str]] = defaultdict(set)
        for alias, (
            name,
            grain,  # pylint: disable=unused-variable
        ) in self.grains.items():
            reverse_grain[name].add(alias)

        dimensions: Set[str] = set()
        for dimension in payload["data"]["dimensions"]:
            if dimension["name"] in self.dimensions:
                dimensions.add(dimension["name"])
            elif dimension["name"] in reverse_grain:
                dimensions.update(reverse_grain[dimension["name"]])

        return dimensions

    def _build_column_from_dimension(  # pylint: disable=too-many-return-statements
        self,
        name: str,
    ) -> Field:
        """
        Build a field from a dbt dimension.

        Unfortunately the API does not provide the type of the dimension, so we
        need to fetch each one and read the type from the Arrow response.
        """
        try:
            byte_string = self._run_query(
                environmentId=self.environment_id,
                metrics=[],
                where=[],
                groupBy=[{"name": name}],
                orderBy=[],
                limit=0,
            )
        except ProgrammingError:
            return Unknown(
                filters=[Equal, NotEqual, IsNull, IsNotNull],
                order=Order.ANY,
                exact=True,
            )

        with pa.ipc.open_stream(base64.b64decode(byte_string)) as reader:
            field = reader.schema[0]

        if pa.types.is_boolean(field.type):
            return Boolean(
                filters=[Equal, NotEqual, IsNull, IsNotNull],
                order=Order.ANY,
                exact=True,
            )
        if pa.types.is_integer(field.type):
            return Integer(
                filters=[Equal, NotEqual, Range, IsNull, IsNotNull],
                order=Order.ANY,
                exact=True,
            )
        if pa.types.is_floating(field.type):
            return Decimal(
                filters=[Equal, NotEqual, Range, IsNull, IsNotNull],
                order=Order.ANY,
                exact=True,
            )
        if pa.types.is_string(field.type):
            return String(
                filters=[Equal, NotEqual, Range, IsNull, IsNotNull],
                order=Order.ANY,
                exact=True,
            )
        if pa.types.is_timestamp(field.type):
            return Timestamp(
                filters=[Equal, NotEqual, Range, IsNull, IsNotNull],
                order=Order.ANY,
                exact=True,
            )
        if pa.types.is_date(field.type):
            return Date(
                filters=[Equal, NotEqual, Range, IsNull, IsNotNull],
                order=Order.ANY,
                exact=True,
            )
        if pa.types.is_time(field.type):
            return Time(
                filters=[Equal, NotEqual, Range, IsNull, IsNotNull],
                order=Order.ANY,
                exact=True,
            )
        if pa.types.is_decimal(field.type):
            return Decimal(
                filters=[Equal, NotEqual, Range, IsNull, IsNotNull],
                order=Order.ANY,
                exact=True,
            )

        return Unknown(
            filters=[Equal, NotEqual, IsNull, IsNotNull],
            order=Order.ANY,
            exact=True,
        )

    def _build_where(
        self,
        columns: Dict[str, Field],
        bounds: Dict[str, Filter],
    ) -> List[WhereInput]:
        """
        Build a ``WhereInput`` list from the bounds to filter in GraphQL.
        """
        where: List[WhereInput] = []
        for column_name, filter_ in bounds.items():
            if isinstance(filter_, Impossible):
                raise ImpossibleFilterError()

            if column_name in self.grains:
                base_column_name, grain = self.grains[column_name]
                ref = f"{{{{ TimeDimension('{base_column_name}', '{grain}') }}}}"
            else:
                ref = f"{{{{ Dimension('{column_name}') }}}}"

            field = columns[column_name]
            if isinstance(filter_, Equal):
                sql = f"{ref} = {field.quote(filter_.value)}"
            elif isinstance(filter_, NotEqual):
                sql = f"{ref} != {field.quote(filter_.value)}"
            elif isinstance(filter_, IsNull):
                sql = f"{ref} IS NULL"
            elif isinstance(filter_, IsNotNull):
                sql = f"{ref} IS NOT NULL"
            elif isinstance(filter_, Range):
                conditions = []
                if filter_.start is not None:
                    operator_ = ">=" if filter_.include_start else ">"
                    conditions.append(f"{ref} {operator_} {field.quote(filter_.start)}")
                if filter_.end is not None:
                    operator_ = "<=" if filter_.include_end else "<"
                    conditions.append(f"{ref} {operator_} {field.quote(filter_.end)}")
                sql = " AND ".join(conditions)
            else:
                raise ValueError(f"Invalid filter: {filter_}")

            where.append({"sql": sql})

        return where

    def _build_groupbys(
        self,
        requested_columns: Set[str],
    ) -> List[GroupByInput]:
        """
        Build group bys based on the requested columns and the SQL query.
        """
        return [
            {"name": column}
            for column in requested_columns
            if column in self.dimensions
        ]

    def _build_orderbys(
        self,
        order: List[Tuple[str, RequestedOrder]],
        groupbys: List[GroupByInput],
    ) -> List[OrderByInput]:
        """
        Build order bys based on the requested columns and the SQL query.
        """
        groupbys_map = {groupby["name"]: groupby for groupby in groupbys}

        orderbys = []
        for column, requested_order in order:
            orderby: OrderByInput = {
                "descending": requested_order == Order.DESCENDING,
            }

            if column in self.metrics:
                orderby["metric"] = {"name": column}
            elif column in self.dimensions:
                orderby["groupBy"] = groupbys_map[column]
            else:
                raise ProgrammingError(f"Invalid order by column: {column}")

            orderbys.append(orderby)

        return orderbys

    def get_columns(self) -> Dict[str, Field]:
        return self.columns

    def get_data(
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
        limit: Optional[int] = None,
        **kwargs: Any,
    ) -> Iterator[Row]:
        cursor = find_cursor()
        if cursor is None or cursor.operation is None:
            raise InternalError("Unable to get reference to cursor")

        # When the virtual table has more than 63 columns apsw will return any requested
        # columns in the 1-63 range, plus ALL columns from 64-, since the mask is a 64-bit
        # value. This doesn't work for dbt, since only related columns should be requested
        # together. To solve this we need to parse the SQL and extract the columns
        # ourselves.
        requested_columns = extract_columns_from_sql(
            self.table,
            set(self.columns),
            cursor.operation,
        )

        groupbys = self._build_groupbys(requested_columns)
        orderbys = self._build_orderbys(order, groupbys)

        byte_string = self._run_query(
            environmentId=self.environment_id,
            metrics=[
                {"name": column}
                for column in requested_columns
                if column in self.metrics
            ],
            where=self._build_where(self.columns, bounds),
            groupBy=groupbys,
            orderBy=orderbys,
            limit=limit,
        )

        # pylint: disable=invalid-name
        df = stream_to_dataframe(byte_string)
        df.rename_axis("rowid", inplace=True)

        yield from df.reset_index().to_dict(orient="records")
