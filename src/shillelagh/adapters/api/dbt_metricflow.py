"""
An adapter for querying dbt Metric Flow metrics.

This is meant to be used with Apache Superset and has only been tested with the SQL that
it produces, eg:

    SELECT
        DATE_TRUNC(order_id__ordered_at, 'month') AS order_id__ordered_at
      , order_id__is_food_order AS order_id__is_food_order
      , orders as orders
    FROM "https://semantic-layer.cloud.getdbt.com/"
    GROUP BY
        order_id__ordered_at
      , order_id__is_food_order
    ORDER BY
        order_id__ordered_at DESC
    LIMIT 10000

Some notes worth mentioning:

1. The dbt semantic layer doesn't return the type of dimensions, but we need to know the
types in order to filter the data, since the filter is passed as a SQL string. To solve
this we need to run a query for each dimension, with a ``LIMIT 0``, to get the type of
the column from the Arrow schema of the response. This is extremely slow, so it needs a
disk cache.

2. Requesting metrics with a time dimension with a given grain is complicated. To make it
work with SQL I had to add a custom function ``DATE_TRUNC`` to Shillelagh, and the adapter
will parse the SQL using sqlglot to infer the time grain of each temporal column. Reading
the SQL at the adapter level is hacky — I had to write code that goes up the stack looking
for a cursor, where the current SQL is stored.

TODO:

    - Cache for dimension requests and ``_get_grains``

"""

import base64
import datetime
import inspect
import logging
import re
import time
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple, TypedDict, cast
from urllib.parse import urlparse

import pandas as pd
import pyarrow as pa
import sqlglot
from python_graphql_client import GraphqlClient

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


def extract_grains(sql: str) -> Dict[str, str]:
    """
    Extract the time grain from the SQL query.

    TODO: improve this so it works with queries that have other sources; it should only
    check for columns that are being selected directly from the semantic layer.
    """
    expression = sqlglot.parse_one(sql, dialect="sqlite")

    return {
        exp.unit.this: exp.this.this.upper()
        for exp in expression.find_all(sqlglot.expressions.DateTrunc)
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
    supports_requested_columns = True

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
        parsed = urlparse(uri)

        if match := CUSTOM_URL_PATTERN.match(parsed.netloc):
            endpoint = (
                f"https://{match['id']}.semantic-layer.{match['region']}.dbt.com/"
            )
        else:
            endpoint = "https://semantic-layer.cloud.getdbt.com/api/graphql"

        return (endpoint,)

    def __init__(self, endpoint: str, service_token: str, environment_id: int):
        super().__init__()

        self.client = GraphqlClient(
            endpoint=endpoint,
            headers={"Authorization": f"Bearer {service_token}"},
        )
        self.environment_id = environment_id
        self._set_columns()

    def _get_grains(self) -> Dict[str, str]:
        """
        Extract grains from current query.
        """
        cursor = find_cursor()
        return extract_grains(cursor.operation) if cursor and cursor.operation else {}

    def _set_columns(self) -> None:
        payload = self.client.execute(
            query=LIST_METRICS,
            variables={"environmentId": self.environment_id},
        )

        self.columns: Dict[str, Field] = {}
        self.metrics: Set[str] = set()
        self.dimensions: Dict[str, Set[str]] = {}
        self.aliases: Dict[str, str] = {}

        for metric in payload["data"]["metrics"]:
            self.columns[metric["name"]] = Decimal()
            self.metrics.add(metric["name"])

            for dimension in metric["dimensions"]:
                name = dimension["name"]
                if name not in self.columns:
                    self.columns[name] = self._build_column_from_dimension(name)
                    self.dimensions[name] = set(dimension["queryableGranularities"])
                    for grain in dimension["queryableGranularities"]:
                        self.aliases[f"{name}__{grain.lower()}"] = name

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

    def _build_column_from_dimension(  # pylint: disable=too-many-return-statements
        self,
        name: str,
    ) -> Field:
        """
        Build a field from a dbt dimension.

        Unfortunately the API does not provide the type of the dimension, so we
        need to fetch each one and read the type from the Arrow response.

        TODO: heavily cache this.
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
        grains = self._get_grains()

        where: List[WhereInput] = []
        for column_name, filter_ in bounds.items():
            if isinstance(filter_, Impossible):
                raise ImpossibleFilterError()

            grain = grains.get(column_name.upper())
            grain_method = f".grain('{grain.lower()}')" if grain else ""
            ref = f"{{{{ Dimension('{column_name}'){grain_method} }}}}"

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
        grains = self._get_grains()

        groupbys = []
        for column in requested_columns:
            if column in self.dimensions:
                groupby: GroupByInput = {"name": column}

                # Check if column needs a grain; for some reason SQLglot returns the
                # column name in uppercase.
                ref = column.upper()
                if ref in grains:
                    grain = grains[ref]
                    if grain not in self.dimensions[column]:
                        raise ProgrammingError(
                            f"Time grain {grain} not supported for {column}",
                        )
                    groupby["grain"] = grain

                groupbys.append(groupby)

        return groupbys

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
        requested_columns: Optional[Set[str]] = None,
        **kwargs: Any,
    ) -> Iterator[Row]:
        if requested_columns is None:
            raise ProgrammingError(
                "You are using an older version of apsw, please ugprade",
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
        df.rename(columns=self.aliases, inplace=True)
        df.rename_axis("rowid", inplace=True)

        yield from df.reset_index().to_dict(orient="records")
