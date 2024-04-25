"""
Tests for the DbtMetricFlowAPI adapter.
"""

# pylint: disable=line-too-long, invalid-name, unused-argument, redefined-outer-name, protected-access, too-many-lines

import base64
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict
from unittest.mock import MagicMock, call

import pandas as pd
import pyarrow as pa
import pytest
from pytest_mock import MockerFixture

from shillelagh.adapters.api.dbt_metricflow import (
    DbtMetricFlowAPI,
    Timestamp,
    find_cursor,
)
from shillelagh.backends.apsw.db import Cursor, connect
from shillelagh.exceptions import ImpossibleFilterError, InternalError, ProgrammingError
from shillelagh.fields import Boolean, Date
from shillelagh.fields import Decimal as DecimalField
from shillelagh.fields import Field, Integer, Order, String, Time, Unknown
from shillelagh.filters import (
    Equal,
    Filter,
    Impossible,
    IsNotNull,
    IsNull,
    NotEqual,
    Range,
)

INIT_PAYLOAD = [
    {
        "data": {
            "metrics": [
                {
                    "name": "orders",
                    "description": "Count of orders.",
                    "type": "SIMPLE",
                    "dimensions": [
                        {
                            "name": "order_id__is_food_order",
                            "description": (
                                "A boolean indicating if this order included any food "
                                "items."
                            ),
                            "queryableGranularities": [],
                            "type": "CATEGORICAL",
                        },
                    ],
                },
            ],
        },
    },
    {"data": {"createQuery": {"queryId": "2fWo7hWqRbyMJpBbBNTwD6WX2jb"}}},
    {
        "data": {
            "query": {
                "sql": """SELECT
  is_food_order AS order_id__is_food_order
FROM `dbt-tutorial-347100`.`dbt_beto`.`orders` orders_src_10000
GROUP BY
  order_id__is_food_order""",
                "status": "SUCCESSFUL",
                "error": None,
                "arrowResult": "/////4AAAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAMAAAACAAIAAAABAAIAAAABAAAAAEAAAAUAAAAEAAUAAgABgAHAAwAAAAQABAAAAAAAAEGEAAAACwAAAAEAAAAAAAAABcAAABvcmRlcl9pZF9faXNfZm9vZF9vcmRlcgAEAAQABAAAAP////+IAAAAFAAAAAAAAAAMABYABgAFAAgADAAMAAAAAAMEABgAAAAQAAAAAAAAAAAACgAYAAwABAAIAAoAAAA8AAAAEAAAAAMAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAEAAAAAAAAACAAAAAAAAAABAAAAAAAAAAAAAAABAAAAAwAAAAAAAAABAAAAAAAAAAYAAAAAAAAAAgAAAAAAAAD/////AAAAAA==",
            },
        },
    },
]


@pytest.fixture
def client(mocker: MockerFixture) -> MagicMock:
    """
    A client that mocks a successful query.
    """
    GraphqlClient: MagicMock = mocker.patch(
        "shillelagh.adapters.api.dbt_metricflow.GraphqlClient",
    )
    GraphqlClient().execute.side_effect = [
        *INIT_PAYLOAD,
        {"data": {"createQuery": {"queryId": "2fThegMTT9iBqmUHQE0gXY7dzEJ"}}},
        {
            "data": {
                "query": {
                    "sql": """SELECT
  order_id__is_food_order
  , SUM(order_count) AS orders
FROM (
  SELECT
    is_food_order AS order_id__is_food_order
    , 1 AS order_count
  FROM `dbt-tutorial-347100`.`dbt_beto`.`orders` orders_src_10000
) subq_2
GROUP BY
  order_id__is_food_order
LIMIT 10""",
                    "status": "COMPILED",
                    "error": None,
                    "arrowResult": None,
                },
            },
        },
        {
            "data": {
                "query": {
                    "sql": """SELECT
  order_id__is_food_order
  , SUM(order_count) AS orders
FROM (
  SELECT
    is_food_order AS order_id__is_food_order
    , 1 AS order_count
  FROM `dbt-tutorial-347100`.`dbt_beto`.`orders` orders_src_10000
) subq_2
GROUP BY
  order_id__is_food_order
LIMIT 10""",
                    "status": "COMPILED",
                    "error": None,
                    "arrowResult": "/////7gAAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAMAAAACAAIAAAABAAIAAAABAAAAAIAAABMAAAABAAAAMz///8AAAEHEAAAACAAAAAEAAAAAAAAAAYAAABvcmRlcnMAAAAABgAIAAQABgAAABMAAAAQABQACAAGAAcADAAAABAAEAAAAAAAAQYQAAAALAAAAAQAAAAAAAAAFwAAAG9yZGVyX2lkX19pc19mb29kX29yZGVyAAQABAAEAAAA/////7gAAAAUAAAAAAAAAAwAFgAGAAUACAAMAAwAAAAAAwQAGAAAAEAAAAAAAAAAAAAKABgADAAEAAgACgAAAFwAAAAQAAAAAwAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAQAAAAAAAAAIAAAAAAAAAAEAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAMAAAAAAAAAAAAAAAAgAAAAMAAAAAAAAAAQAAAAAAAAADAAAAAAAAAAAAAAAAAAAABgAAAAAAAAACAAAAAAAAAF8EAAAAAAAAAAAAAAAAAABVRwAAAAAAAAAAAAAAAAAAUJ0AAAAAAAAAAAAAAAAAAP////8AAAAA",
                },
            },
        },
        {
            "data": {
                "query": {
                    "sql": """SELECT
  order_id__is_food_order
  , SUM(order_count) AS orders
FROM (
  SELECT
    is_food_order AS order_id__is_food_order
    , 1 AS order_count
  FROM `dbt-tutorial-347100`.`dbt_beto`.`orders` orders_src_10000
) subq_2
GROUP BY
  order_id__is_food_order
LIMIT 10""",
                    "status": "SUCCESSFUL",
                    "error": None,
                    "arrowResult": "/////7gAAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAMAAAACAAIAAAABAAIAAAABAAAAAIAAABMAAAABAAAAMz///8AAAEHEAAAACAAAAAEAAAAAAAAAAYAAABvcmRlcnMAAAAABgAIAAQABgAAABMAAAAQABQACAAGAAcADAAAABAAEAAAAAAAAQYQAAAALAAAAAQAAAAAAAAAFwAAAG9yZGVyX2lkX19pc19mb29kX29yZGVyAAQABAAEAAAA/////7gAAAAUAAAAAAAAAAwAFgAGAAUACAAMAAwAAAAAAwQAGAAAAEAAAAAAAAAAAAAKABgADAAEAAgACgAAAFwAAAAQAAAAAwAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAQAAAAAAAAAIAAAAAAAAAAEAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAMAAAAAAAAAAAAAAAAgAAAAMAAAAAAAAAAQAAAAAAAAADAAAAAAAAAAAAAAAAAAAABgAAAAAAAAACAAAAAAAAAF8EAAAAAAAAAAAAAAAAAABVRwAAAAAAAAAAAAAAAAAAUJ0AAAAAAAAAAAAAAAAAAP////8AAAAA",
                },
            },
        },
        {
            "data": {
                "query": {
                    "sql": """SELECT
  order_id__is_food_order
  , SUM(order_count) AS orders
FROM (
  SELECT
    is_food_order AS order_id__is_food_order
    , 1 AS order_count
  FROM `dbt-tutorial-347100`.`dbt_beto`.`orders` orders_src_10000
) subq_2
GROUP BY
  order_id__is_food_order
LIMIT 10""",
                    "status": "SUCCESSFUL",
                    "error": None,
                    "arrowResult": "/////7gAAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAMAAAACAAIAAAABAAIAAAABAAAAAIAAABMAAAABAAAAMz///8AAAEHEAAAACAAAAAEAAAAAAAAAAYAAABvcmRlcnMAAAAABgAIAAQABgAAABMAAAAQABQACAAGAAcADAAAABAAEAAAAAAAAQYQAAAALAAAAAQAAAAAAAAAFwAAAG9yZGVyX2lkX19pc19mb29kX29yZGVyAAQABAAEAAAA/////7gAAAAUAAAAAAAAAAwAFgAGAAUACAAMAAwAAAAAAwQAGAAAAEAAAAAAAAAAAAAKABgADAAEAAgACgAAAFwAAAAQAAAAAwAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAQAAAAAAAAAIAAAAAAAAAAEAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAMAAAAAAAAAAAAAAAAgAAAAMAAAAAAAAAAQAAAAAAAAADAAAAAAAAAAAAAAAAAAAABgAAAAAAAAACAAAAAAAAAF8EAAAAAAAAAAAAAAAAAABVRwAAAAAAAAAAAAAAAAAAUJ0AAAAAAAAAAAAAAAAAAP////8AAAAA",
                },
            },
        },
    ]
    return GraphqlClient


@pytest.fixture
def client_with_error(mocker: MockerFixture) -> None:
    """
    A client that mocks a query with an error.
    """
    GraphqlClient = mocker.patch("shillelagh.adapters.api.dbt_metricflow.GraphqlClient")
    GraphqlClient().execute.side_effect = [
        *INIT_PAYLOAD,
        {"data": {"createQuery": {"queryId": "2fThegMTT9iBqmUHQE0gXY7dzEJ"}}},
        {
            "data": {
                "query": {
                    "sql": """SELECT
  customers_src_10000.customer_name AS customer__customer_name
  , COUNT(DISTINCT orders_src_10000.customer_id) AS customers_with_orders
FROM `dbt-tutorial-347100`.`dbt_beto`.`orders` orders_src_10000
LEFT OUTER JOIN
  `dbt-tutorial-347100`.`dbt_beto`.`customers` customers_src_10000
ON
  orders_src_10000.customer_id = customers_src_10000.customer_id
GROUP BY
  customer__customer_name
LIMIT 10""",
                    "status": "FAILED",
                    "error": """INVALID_ARGUMENT: [FlightSQL] [Simba][BigQueryJDBCDriver](100032) Error executing query job. Message: 400 Bad Request
POST https://bigquery.googleapis.com/bigquery/v2/projects/dbt-tutorial-347100/jobs
{
  "code": 400,
  "errors": [
    {
      "domain": "global",
      "location": "q",
      "locationType": "parameter",
      "message": "No matching signature for operator = for argument types: STRING, INT64. Supported signature: ANY = ANY at [8:3]",
      "reason": "invalidQuery"
    }
  ],
  "message": "No matching signature for operator = for argument types: STRING, INT64. Supported signature: ANY = ANY at [8:3]",
  "status": "INVALID_ARGUMENT"
} (InvalidArgument; Prepare)""",
                    "arrowResult": None,
                },
            },
        },
    ]


@pytest.fixture
def client_with_bad_request(mocker: MockerFixture) -> None:
    """
    A client that does an incorrect request when creating a query.
    """
    GraphqlClient = mocker.patch("shillelagh.adapters.api.dbt_metricflow.GraphqlClient")
    GraphqlClient().execute.side_effect = [
        {
            "data": {
                "metrics": [
                    {
                        "name": "orders",
                        "description": "Count of orders.",
                        "type": "SIMPLE",
                        "dimensions": [
                            {
                                "name": "order_id__is_food_order",
                                "description": (
                                    "A boolean indicating if this order included any "
                                    "food items."
                                ),
                                "queryableGranularities": [],
                                "type": "CATEGORICAL",
                            },
                        ],
                    },
                ],
            },
        },
        {
            "data": None,
            "errors": [
                {
                    "message": "Variable '$environmentId' of required type 'BigInt!' was not provided.",
                    "locations": [{"line": 3, "column": 9}],
                },
            ],
        },
    ]


def test_dbtmetricflowapi(mocker: MockerFixture, client: MockerFixture) -> None:
    """
    Run SQL against the adapter.
    """
    mocker.patch("time.sleep")

    connection = connect(
        ":memory:",
        adapter_kwargs={
            "dbtmetricflowapi": {
                "service_token": "dbtc_XXX",
                "environment_id": 123456,
            },
        },
    )
    cursor = connection.cursor()

    sql = """
        SELECT orders, order_id__is_food_order
        FROM "https://semantic-layer.cloud.getdbt.com/"
        LIMIT 10
    """
    cursor.execute(sql)
    assert cursor.fetchall() == [
        (Decimal("1119"), None),
        (Decimal("18261"), 1),
        (Decimal("40272"), 0),
    ]

    client().execute.assert_has_calls(
        [
            call(
                query="""
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
""",
                variables={"environmentId": 123456},
            ),
            call(
                query="""
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
""",
                variables={
                    "environmentId": 123456,
                    "metrics": [],
                    "where": [],
                    "groupBy": [{"name": "order_id__is_food_order"}],
                    "orderBy": [],
                    "limit": 0,
                },
            ),
            call(
                query="""
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
""",
                variables={
                    "environmentId": 123456,
                    "queryId": "2fWo7hWqRbyMJpBbBNTwD6WX2jb",
                },
            ),
            call(
                query="""
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
""",
                variables={
                    "environmentId": 123456,
                    "metrics": [{"name": "orders"}],
                    "where": [],
                    "groupBy": [{"name": "order_id__is_food_order"}],
                    "orderBy": [],
                    "limit": 10,
                },
            ),
            call(
                query="""
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
""",
                variables={
                    "environmentId": 123456,
                    "queryId": "2fThegMTT9iBqmUHQE0gXY7dzEJ",
                },
            ),
            call(
                query="""
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
""",
                variables={
                    "environmentId": 123456,
                    "queryId": "2fThegMTT9iBqmUHQE0gXY7dzEJ",
                },
            ),
            call(
                query="""
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
""",
                variables={
                    "environmentId": 123456,
                    "queryId": "2fThegMTT9iBqmUHQE0gXY7dzEJ",
                },
            ),
        ],
    )


def test_dbtmetricflowapi_error(
    mocker: MockerFixture,
    client_with_error: MockerFixture,
) -> None:
    """
    Test error capturing.
    """
    mocker.patch("time.sleep")

    connection = connect(
        ":memory:",
        adapter_kwargs={
            "dbtmetricflowapi": {
                "service_token": "dbtc_XXX",
                "environment_id": 123456,
            },
        },
    )
    cursor = connection.cursor()

    sql = """
        SELECT order_id__is_food_order
        FROM "https://semantic-layer.cloud.getdbt.com/"
        LIMIT 10
    """
    with pytest.raises(ProgrammingError):
        cursor.execute(sql)


def test_get_data_requested_columns(
    mocker: MockerFixture,
    client: MockerFixture,
) -> None:
    """
    Test ``get_data`` with newer versions of apsw with ``requested_columns``.
    """
    cursor = mocker.MagicMock()
    cursor.operation = "SELECT orders, order_id__is_food_order FROM t"
    mocker.patch(
        "shillelagh.adapters.api.dbt_metricflow.find_cursor",
        return_value=cursor,
    )
    mocker.patch("time.sleep")

    adapter = DbtMetricFlowAPI(
        "https://semantic-layer.cloud.getdbt.com/api/graphql",
        "dbtc_XXX",
        123456,
    )
    assert list(adapter.get_data(bounds={}, order=[])) == [
        {"rowid": 0, "order_id__is_food_order": None, "orders": Decimal("1119")},
        {"rowid": 1, "order_id__is_food_order": True, "orders": Decimal("18261")},
        {"rowid": 2, "order_id__is_food_order": False, "orders": Decimal("40272")},
    ]


def test_get_data_no_cursor(
    mocker: MockerFixture,
    client: MockerFixture,
) -> None:
    """
    Test that ``get_data`` fails when we can't fnd the cursor.
    """
    mocker.patch(
        "shillelagh.adapters.api.dbt_metricflow.find_cursor",
        return_value=None,
    )

    adapter = DbtMetricFlowAPI(
        "https://semantic-layer.cloud.getdbt.com/api/graphql",
        "dbtc_XXX",
        123456,
    )
    with pytest.raises(InternalError) as excinfo:
        list(adapter.get_data(bounds={}, order=[]))
    assert str(excinfo.value) == "Unable to get reference to cursor"


def test_parse_uri() -> None:
    """
    Test the ``parse_uri`` method.
    """
    assert DbtMetricFlowAPI.parse_uri("https://semantic-layer.cloud.getdbt.com/") == (
        "https://semantic-layer.cloud.getdbt.com/",
    )
    assert DbtMetricFlowAPI.parse_uri("https://ab123.us1.dbt.com/") == (
        "https://ab123.us1.dbt.com/",
    )


def test_get_endpoint() -> None:
    """
    Test the ``_get_endpoint`` method.
    """
    assert (
        DbtMetricFlowAPI._get_endpoint("https://semantic-layer.cloud.getdbt.com/")
        == "https://semantic-layer.cloud.getdbt.com/api/graphql"
    )
    assert DbtMetricFlowAPI._get_endpoint("https://ab123.us1.dbt.com/") == (
        "https://ab123.semantic-layer.us1.dbt.com/api/graphql"
    )


def test_build_where(
    mocker: MockerFixture,
    client: MockerFixture,
) -> None:
    """
    Test the ``_build_where`` method.
    """
    adapter = DbtMetricFlowAPI(
        "https://semantic-layer.cloud.getdbt.com/api/graphql",
        "dbtc_XXX",
        123456,
    )

    columns: Dict[str, Field] = {
        "is_food_order": Boolean(),
        "order_id": Integer(),
        "order_id__ordered_at__month": Date(),
    }
    adapter.grains["order_id__ordered_at__month"] = ("order_id__ordered_at", "month")

    assert adapter._build_where(columns, {}) == []

    with pytest.raises(ValueError):
        adapter._build_where(columns, {"is_food_order": Filter()})

    with pytest.raises(ImpossibleFilterError):
        adapter._build_where(columns, {"is_food_order": Impossible()})

    assert adapter._build_where(columns, {"is_food_order": Equal(True)}) == [
        {"sql": "{{ Dimension('is_food_order') }} = TRUE"},
    ]
    assert adapter._build_where(columns, {"is_food_order": NotEqual(True)}) == [
        {"sql": "{{ Dimension('is_food_order') }} != TRUE"},
    ]
    assert adapter._build_where(columns, {"is_food_order": IsNull()}) == [
        {"sql": "{{ Dimension('is_food_order') }} IS NULL"},
    ]
    assert adapter._build_where(columns, {"is_food_order": IsNotNull()}) == [
        {"sql": "{{ Dimension('is_food_order') }} IS NOT NULL"},
    ]
    assert adapter._build_where(columns, {"order_id": Range(10, 20)}) == [
        {
            "sql": "{{ Dimension('order_id') }} > 10 AND {{ Dimension('order_id') }} < 20",
        },
    ]
    assert adapter._build_where(
        columns,
        {"order_id": Range(10, 20, include_start=True, include_end=True)},
    ) == [
        {
            "sql": "{{ Dimension('order_id') }} >= 10 AND {{ Dimension('order_id') }} <= 20",
        },
    ]
    assert adapter._build_where(columns, {"order_id": Range(10, None)}) == [
        {
            "sql": "{{ Dimension('order_id') }} > 10",
        },
    ]
    assert adapter._build_where(columns, {"order_id": Range(None, 20)}) == [
        {
            "sql": "{{ Dimension('order_id') }} < 20",
        },
    ]
    assert adapter._build_where(
        columns,
        {"order_id__ordered_at__month": Range(None, datetime(2023, 1, 1, 12, 0))},
    ) == [
        {
            "sql": (
                "{{ TimeDimension('order_id__ordered_at', 'month') }}"
                " < '2023-01-01T12:00:00'"
            ),
        },
    ]


def test_timestamp() -> None:
    """
    Test ``Timestamp``.
    """
    assert Timestamp().parse(pd.Timestamp("2023-01-01 12:00")) == datetime(
        2023,
        1,
        1,
        12,
        0,
    )
    assert Timestamp().parse(None) is None
    assert Timestamp().format(datetime(2023, 1, 1, 12, 0)) == pd.Timestamp(
        "2023-01-01 12:00",
    )
    assert Timestamp().format(None) is None
    assert (
        Timestamp().quote(pd.Timestamp("2023-01-01 12:00")) == "'2023-01-01T12:00:00'"
    )
    assert Timestamp().quote(None) == "NULL"


def test_build_column_from_dimension(mocker: MockerFixture) -> None:
    """
    Test the ``_build_column_from_dimension`` method.
    """
    mocker.patch.object(DbtMetricFlowAPI, "_set_columns", return_value={})
    adapter = DbtMetricFlowAPI(
        "https://semantic-layer.cloud.getdbt.com/api/graphql",
        "dbtc_XXX",
        123456,
    )

    mocker.patch.object(
        adapter,
        "_run_query",
        side_effect=ProgrammingError("Something happened"),
    )
    assert adapter._build_column_from_dimension("dim") == Unknown(
        filters=[Equal, NotEqual, IsNull, IsNotNull],
        order=Order.ANY,
        exact=True,
    )

    mocker.patch.object(
        adapter,
        "_run_query",
        return_value=base64.b64encode(b"hello"),
    )
    ipc = mocker.patch("pyarrow.ipc")

    with ipc.open_stream() as reader:
        reader.schema = pa.schema([("dim", pa.bool_())])
        assert adapter._build_column_from_dimension("dim") == Boolean(
            filters=[Equal, NotEqual, IsNull, IsNotNull],
            order=Order.ANY,
            exact=True,
        )

        reader.schema = pa.schema([("dim", pa.int32())])
        assert adapter._build_column_from_dimension("dim") == Integer(
            filters=[Equal, NotEqual, Range, IsNull, IsNotNull],
            order=Order.ANY,
            exact=True,
        )

        reader.schema = pa.schema([("dim", pa.float32())])
        assert adapter._build_column_from_dimension("dim") == DecimalField(
            filters=[Equal, NotEqual, Range, IsNull, IsNotNull],
            order=Order.ANY,
            exact=True,
        )

        reader.schema = pa.schema([("dim", pa.string())])
        assert adapter._build_column_from_dimension("dim") == String(
            filters=[Equal, NotEqual, Range, IsNull, IsNotNull],
            order=Order.ANY,
            exact=True,
        )

        reader.schema = pa.schema([("dim", pa.timestamp("ms"))])
        assert adapter._build_column_from_dimension("dim") == Timestamp(
            filters=[Equal, NotEqual, Range, IsNull, IsNotNull],
            order=Order.ANY,
            exact=True,
        )

        reader.schema = pa.schema([("dim", pa.date32())])
        assert adapter._build_column_from_dimension("dim") == Date(
            filters=[Equal, NotEqual, Range, IsNull, IsNotNull],
            order=Order.ANY,
            exact=True,
        )

        reader.schema = pa.schema([("dim", pa.time32("ms"))])
        assert adapter._build_column_from_dimension("dim") == Time(
            filters=[Equal, NotEqual, Range, IsNull, IsNotNull],
            order=Order.ANY,
            exact=True,
        )

        reader.schema = pa.schema([("dim", pa.decimal128(10))])
        assert adapter._build_column_from_dimension("dim") == DecimalField(
            filters=[Equal, NotEqual, Range, IsNull, IsNotNull],
            order=Order.ANY,
            exact=True,
        )

        reader.schema = pa.schema([("dim", pa.binary())])
        assert adapter._build_column_from_dimension("dim") == Unknown(
            filters=[Equal, NotEqual, IsNull, IsNotNull],
            order=Order.ANY,
            exact=True,
        )


def test_run_query_error(client_with_bad_request: MockerFixture) -> None:
    """
    Test errors happening in ``run_query``.
    """
    with pytest.raises(InternalError) as excinfo:
        DbtMetricFlowAPI(
            "https://semantic-layer.cloud.getdbt.com/api/graphql",
            "dbtc_XXX",
            123456,
        )
    assert (
        str(excinfo.value)
        == "Variable '$environmentId' of required type 'BigInt!' was not provided."
    )


def test_time_dimension_aliases(mocker: MockerFixture) -> None:
    """
    Test that time dimensions are created for each valid grain.
    """
    GraphqlClient = mocker.patch("shillelagh.adapters.api.dbt_metricflow.GraphqlClient")
    GraphqlClient().execute.return_value = {
        "data": {
            "metrics": [
                {
                    "description": "Sum of the product revenue for each "
                    "order item. Excludes tax.",
                    "dimensions": [
                        {
                            "expr": None,
                            "name": "metric_time",
                            "description": "The metric time",
                            "queryableGranularities": [
                                "DAY",
                                "WEEK",
                                "MONTH",
                                "QUARTER",
                                "YEAR",
                            ],
                            "type": "TIME",
                        },
                    ],
                    "name": "revenue",
                    "type": "SIMPLE",
                },
                {
                    "description": "Sum of cost for each order item.",
                    "dimensions": [
                        {
                            "expr": None,
                            "name": "metric_time",
                            "description": "The metric time",
                            "queryableGranularities": [
                                "DAY",
                                "WEEK",
                                "MONTH",
                                "QUARTER",
                                "YEAR",
                            ],
                            "type": "TIME",
                        },
                    ],
                    "name": "order_cost",
                    "type": "SIMPLE",
                },
            ],
        },
    }

    mocker.patch.object(
        DbtMetricFlowAPI,
        "_build_column_from_dimension",
        return_value=Unknown(),
    )

    adapter = DbtMetricFlowAPI(
        "https://semantic-layer.cloud.getdbt.com/api/graphql",
        "dbtc_XXX",
        123456,
    )

    assert adapter.dimensions == {
        "metric_time__day": "The metric time",
        "metric_time__week": "The metric time",
        "metric_time__month": "The metric time",
        "metric_time__year": "The metric time",
        "metric_time__quarter": "The metric time",
    }
    assert adapter.grains == {
        "metric_time__day": ("metric_time", "day"),
        "metric_time__week": ("metric_time", "week"),
        "metric_time__month": ("metric_time", "month"),
        "metric_time__quarter": ("metric_time", "quarter"),
        "metric_time__year": ("metric_time", "year"),
    }


def test_build_groupbys(
    mocker: MockerFixture,
    client: MockerFixture,
) -> None:
    """
    Test the ``_build_groupbys`` method.
    """
    mocker.patch("time.sleep")

    adapter = DbtMetricFlowAPI(
        "https://semantic-layer.cloud.getdbt.com/api/graphql",
        "dbtc_XXX",
        123456,
    )

    assert adapter.dimensions == {
        "order_id__is_food_order": "A boolean indicating if this order included any food items.",
    }
    assert adapter._build_groupbys({"order_id__is_food_order"}) == [
        {"name": "order_id__is_food_order"},
    ]


def test_build_orderbys(
    mocker: MockerFixture,
    client: MockerFixture,
) -> None:
    """
    Test the ``_build_orderbys`` method.
    """
    mocker.patch("time.sleep")

    adapter = DbtMetricFlowAPI(
        "https://semantic-layer.cloud.getdbt.com/api/graphql",
        "dbtc_XXX",
        123456,
    )

    assert adapter.metrics == {"orders": "Count of orders."}
    assert adapter.dimensions == {
        "order_id__is_food_order": "A boolean indicating if this order included any food items.",
    }
    assert adapter._build_orderbys(
        [
            ("order_id__is_food_order", Order.ASCENDING),
            ("orders", Order.DESCENDING),
        ],
        [{"name": "order_id__is_food_order"}],
    ) == [
        {"descending": False, "groupBy": {"name": "order_id__is_food_order"}},
        {"descending": True, "metric": {"name": "orders"}},
    ]


def test_build_orderbys_error(
    mocker: MockerFixture,
    client: MockerFixture,
) -> None:
    """
    Test the ``_build_orderbys`` method when it encounters an error.
    """
    mocker.patch("time.sleep")

    adapter = DbtMetricFlowAPI(
        "https://semantic-layer.cloud.getdbt.com/api/graphql",
        "dbtc_XXX",
        123456,
    )

    with pytest.raises(ProgrammingError) as excinfo:
        adapter._build_orderbys(
            [
                ("order_id__is_food_order", Order.ASCENDING),
                ("orders", Order.DESCENDING),
                ("invalid", Order.DESCENDING),
            ],
            [{"name": "order_id__is_food_order"}],
        )
    assert str(excinfo.value) == "Invalid order by column: invalid"


@pytest.mark.slow_integration_test
def test_integration(adapter_kwargs: Dict[str, Any]) -> None:
    """
    Full integration test running a query.
    """
    connection = connect(":memory:", adapter_kwargs=adapter_kwargs)
    cursor = connection.cursor()

    sql = """
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
    cursor.execute(sql)
    data = cursor.fetchall()
    assert data == [
        (datetime(2017, 8, 1, 0, 0, tzinfo=timezone.utc), True, Decimal("2347")),
        (datetime(2017, 7, 1, 0, 0, tzinfo=timezone.utc), True, Decimal("2057")),
        (datetime(2017, 6, 1, 0, 0, tzinfo=timezone.utc), True, Decimal("2378")),
        (datetime(2017, 5, 1, 0, 0, tzinfo=timezone.utc), True, Decimal("2497")),
        (datetime(2017, 4, 1, 0, 0, tzinfo=timezone.utc), True, Decimal("2101")),
        (datetime(2017, 3, 1, 0, 0, tzinfo=timezone.utc), True, Decimal("1894")),
        (datetime(2017, 2, 1, 0, 0, tzinfo=timezone.utc), True, Decimal("1127")),
        (datetime(2017, 1, 1, 0, 0, tzinfo=timezone.utc), True, Decimal("1185")),
    ]


def test_find_cursor(mocker: MockerFixture) -> None:
    """
    Test the ``find_cursor`` helper.
    """
    assert find_cursor() is None

    cursor = Cursor(mocker.MagicMock(), [], {})
    assert find_cursor() == cursor

    def nested() -> None:
        assert find_cursor() == cursor

    nested()


def test_get_metrics_for_dimensions(mocker: MockerFixture) -> None:
    """
    Test the ``_get_metrics_for_dimensions`` helper.
    """
    GraphqlClient: MagicMock = mocker.patch(
        "shillelagh.adapters.api.dbt_metricflow.GraphqlClient",
    )
    GraphqlClient().execute.side_effect = [
        *INIT_PAYLOAD,
        {
            "data": {
                "metricsForDimensions": [
                    {"name": "revenue"},
                    {"name": "order_cost"},
                    {"name": "median_revenue"},
                    {"name": "food_revenue"},
                    {"name": "food_revenue_pct"},
                    {"name": "revenue_growth_mom"},
                    {"name": "order_gross_profit"},
                    {"name": "cumulative_revenue"},
                    {"name": "order_total"},
                    {"name": "large_order"},
                    {"name": "orders"},
                    {"name": "food_orders"},
                    {"name": "customers_with_orders"},
                    {"name": "new_customer"},
                ],
            },
        },
    ]

    adapter = DbtMetricFlowAPI(
        "https://semantic-layer.cloud.getdbt.com/api/graphql",
        "dbtc_XXX",
        123456,
    )
    assert adapter._get_metrics_for_dimensions({"order_id__is_food_order"}) == {
        "food_revenue_pct",
        "median_revenue",
        "food_orders",
        "food_revenue",
        "order_gross_profit",
        "cumulative_revenue",
        "large_order",
        "new_customer",
        "order_total",
        "order_cost",
        "revenue_growth_mom",
        "customers_with_orders",
        "revenue",
        "orders",
    }


def test_get_dimensions_for_metrics(mocker: MockerFixture) -> None:
    """
    Test the ``_get_dimensions_for_metrics`` helper.
    """
    GraphqlClient: MagicMock = mocker.patch(
        "shillelagh.adapters.api.dbt_metricflow.GraphqlClient",
    )
    GraphqlClient().execute.side_effect = [
        *INIT_PAYLOAD,
        {
            "data": {
                "dimensions": [
                    {"name": "customer__customer_name"},
                    {"name": "customer__customer_type"},
                    {"name": "customer__first_ordered_at"},
                    {"name": "customer__last_ordered_at"},
                    {"name": "location__location_name"},
                    {"name": "location__opened_at"},
                    {"name": "metric_time"},
                    {"name": "order_id__is_drink_order"},
                    {"name": "order_id__is_food_order"},
                    {"name": "order_id__order_total_dim"},
                    {"name": "order_id__ordered_at"},
                ],
            },
        },
    ]

    adapter = DbtMetricFlowAPI(
        "https://semantic-layer.cloud.getdbt.com/api/graphql",
        "dbtc_XXX",
        123456,
    )

    adapter.dimensions = {
        "order_id__is_food_order": "A boolean indicating if this order included any food items.",
        "metric_time__day": "The metric time",
        "metric_time__week": "The metric time",
        "metric_time__month": "The metric time",
        "metric_time__year": "The metric time",
        "metric_time__quarter": "The metric time",
    }
    adapter.grains = {
        "metric_time__day": ("metric_time", "day"),
        "metric_time__week": ("metric_time", "week"),
        "metric_time__month": ("metric_time", "month"),
        "metric_time__quarter": ("metric_time", "quarter"),
        "metric_time__year": ("metric_time", "year"),
    }

    assert adapter._get_dimensions_for_metrics({"orders"}) == {
        "order_id__is_food_order",
        "metric_time__day",
        "metric_time__week",
        "metric_time__month",
        "metric_time__year",
        "metric_time__quarter",
    }
