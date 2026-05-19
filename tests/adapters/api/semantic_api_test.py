"""
Tests for the Semantic API adapter.
"""

# pylint: disable=protected-access, redefined-outer-name

from typing import Any, Optional

import pytest
from requests_mock.mocker import Mocker

from shillelagh.adapters.api.semantic_api import SemanticAPI, _field_for
from shillelagh.backends.apsw.db import connect
from shillelagh.exceptions import ImpossibleFilterError, InternalError, ProgrammingError
from shillelagh.fields import (
    Boolean,
    Float,
    Integer,
    ISODate,
    ISODateTime,
    Order,
    String,
    Unknown,
)
from shillelagh.filters import (
    Equal,
    Impossible,
    IsNotNull,
    IsNull,
    NotEqual,
    Operator,
    Range,
)

BASE_URL = "http://localhost:8000/views/sales"
URI = "semantic-api+http://localhost:8000/views/sales"

VIEW_PAYLOAD: dict[str, Any] = {
    "name": "sales",
    "uid": "pandas.sales",
    "features": ["GROUP_LIMIT"],
    "dimensions": [
        {
            "id": "sales.region",
            "name": "region",
            "type": "string",
            "definition": "region",
            "description": "The region dimension.",
            "grain": None,
        },
        {
            "id": "sales.sale_date",
            "name": "sale_date",
            "type": "date32[day]",
            "definition": "sale_date",
            "description": "The sale date dimension.",
            "grain": None,
        },
    ],
    "metrics": [
        {
            "id": "sales.total_revenue",
            "name": "total_revenue",
            "type": "double",
            "definition": "SUM(revenue)",
            "description": "Total revenue.",
            "aggregation": None,
        },
    ],
}


def _stub_view(requests_mock: Mocker, payload: Optional[dict[str, Any]] = None) -> None:
    requests_mock.post(BASE_URL, json=payload or VIEW_PAYLOAD)


def test_supports() -> None:
    """
    Recognise URIs that point at a semantic view.
    """
    assert SemanticAPI.supports("semantic-api+http://h:8000/views/sales") is True
    assert SemanticAPI.supports("semantic-api+https://h/views/sales") is True
    assert SemanticAPI.supports("semantic-api+http://h/prefix/views/sales") is True
    assert SemanticAPI.supports("http://h:8000/views/sales") is False
    assert SemanticAPI.supports("semantic-api+ftp://h/views/sales") is False
    assert SemanticAPI.supports("semantic-api+http://h/sales") is False
    assert SemanticAPI.supports("semantic-api+http://h/views/") is False


def test_parse_uri() -> None:
    """
    Split the URI into a base URL and a view name.
    """
    assert SemanticAPI.parse_uri(URI) == (BASE_URL, "sales")
    assert SemanticAPI.parse_uri(
        "semantic-api+https://h/api/v1/views/orders",
    ) == ("https://h/api/v1/views/orders", "orders")


def test_parse_uri_invalid_scheme() -> None:
    """
    Reject URIs without the ``semantic-api+`` scheme prefix.
    """
    with pytest.raises(ProgrammingError, match="Invalid Semantic API URI"):
        SemanticAPI.parse_uri("http://h/views/sales")


def test_parse_uri_missing_views_segment() -> None:
    """
    Reject URIs that lack the ``/views/`` segment.
    """
    with pytest.raises(ProgrammingError, match="Invalid Semantic API URI"):
        SemanticAPI.parse_uri("semantic-api+http://h/sales")


def test_parse_uri_empty_view_name() -> None:
    """
    Reject URIs that end with ``/views/`` but no view name.
    """
    with pytest.raises(ProgrammingError, match="Missing view name"):
        SemanticAPI.parse_uri("semantic-api+http://h/views/")


@pytest.mark.parametrize(
    ("arrow_type", "field_cls"),
    [
        ("string", String),
        ("utf8", String),
        ("int64", Integer),
        ("uint32", Integer),
        ("double", Float),
        ("float32", Float),
        ("bool", Boolean),
        ("date32[day]", ISODate),
        ("date64", ISODate),
        ("timestamp[us]", ISODateTime),
        ("decimal128(10, 2)", Unknown),
    ],
)
def test_field_for(arrow_type: str, field_cls: type) -> None:
    """
    Map each Arrow type onto the right Shillelagh field class.
    """
    assert isinstance(_field_for(arrow_type), field_cls)


def test_field_for_exact() -> None:
    """
    The ``exact`` flag is forwarded to the underlying field.
    """
    assert _field_for("string", exact=True).exact is True
    assert _field_for("double", exact=False).exact is False


def test_init_loads_metadata(requests_mock: Mocker) -> None:
    """
    Construction fetches the view definition and builds the column map.
    """
    _stub_view(requests_mock)
    adapter = SemanticAPI(BASE_URL + "/", "sales")

    assert adapter.view_url == BASE_URL
    assert adapter.additional_configuration == {}
    assert adapter.dimension_ids == {
        "region": "sales.region",
        "sale_date": "sales.sale_date",
    }
    assert adapter.metric_ids == {"total_revenue": "sales.total_revenue"}
    assert list(adapter.get_columns()) == ["region", "sale_date", "total_revenue"]
    assert isinstance(adapter.get_columns()["region"], String)
    assert adapter.get_columns()["total_revenue"].exact is False
    assert adapter.get_columns()["region"].exact is True


def test_init_with_additional_configuration(requests_mock: Mocker) -> None:
    """
    ``additional_configuration`` is forwarded on every request.
    """
    matcher = requests_mock.post(BASE_URL, json=VIEW_PAYLOAD)
    SemanticAPI(BASE_URL, "sales", additional_configuration={"workspace": "acme"})
    assert matcher.last_request.json() == {
        "additional_configuration": {"workspace": "acme"},
    }


def test_post_404_with_detail(requests_mock: Mocker) -> None:
    """
    A 404 with a ``detail`` field surfaces as ``ProgrammingError``.
    """
    requests_mock.post(
        BASE_URL,
        status_code=404,
        json={"detail": "Semantic view 'sales' does not exist."},
    )
    with pytest.raises(ProgrammingError, match="does not exist"):
        SemanticAPI(BASE_URL, "sales")


def test_post_404_without_detail(requests_mock: Mocker) -> None:
    """
    A 404 without a ``detail`` field still raises ``ProgrammingError``.
    """
    requests_mock.post(BASE_URL, status_code=404, json={})
    with pytest.raises(ProgrammingError):
        SemanticAPI(BASE_URL, "sales")


def test_post_other_error(requests_mock: Mocker) -> None:
    """
    Non-404 errors bubble up via ``raise_for_status``.
    """
    requests_mock.post(BASE_URL, status_code=500, json={"detail": "boom"})
    with pytest.raises(Exception, match="500"):
        SemanticAPI(BASE_URL, "sales")


def _adapter(requests_mock: Mocker) -> SemanticAPI:
    _stub_view(requests_mock)
    return SemanticAPI(BASE_URL, "sales")


def test_get_data_no_columns(requests_mock: Mocker) -> None:
    """
    An empty ``requested_columns`` set short-circuits without an API call.
    """
    adapter = _adapter(requests_mock)
    assert list(adapter.get_data({}, [], requested_columns=set())) == []


def test_get_data(requests_mock: Mocker) -> None:
    """
    The query body carries metrics, dimensions, and pagination.
    """
    adapter = _adapter(requests_mock)
    rows = [{"region": "North", "total_revenue": 100.0}]
    query_matcher = requests_mock.post(
        f"{BASE_URL}/query",
        json={"requests": [], "results": {"schema": [], "rows": rows}},
    )

    result = list(
        adapter.get_data(
            bounds={},
            order=[],
            limit=10,
            offset=5,
            requested_columns={"region", "total_revenue"},
        ),
    )
    assert result == rows

    body = query_matcher.last_request.json()
    assert body["additional_configuration"] == {}
    assert sorted(body["query"]["metrics"]) == ["sales.total_revenue"]
    assert sorted(body["query"]["dimensions"]) == ["sales.region"]
    assert body["query"]["limit"] == 10
    assert body["query"]["offset"] == 5


def test_get_data_defaults_to_all_columns(requests_mock: Mocker) -> None:
    """
    When ``requested_columns`` is omitted, every column is requested.
    """
    adapter = _adapter(requests_mock)
    query_matcher = requests_mock.post(
        f"{BASE_URL}/query",
        json={"requests": [], "results": {"schema": [], "rows": []}},
    )

    list(adapter.get_data({}, []))

    body = query_matcher.last_request.json()
    assert sorted(body["query"]["dimensions"]) == ["sales.region", "sales.sale_date"]
    assert sorted(body["query"]["metrics"]) == ["sales.total_revenue"]
    assert body["query"]["limit"] is None
    assert body["query"]["offset"] is None


def test_build_filters(requests_mock: Mocker) -> None:
    """
    Equal and Range bounds translate to WHERE/HAVING filter dicts.
    """
    adapter = _adapter(requests_mock)
    bounds = {
        "region": Equal("North"),
        "sale_date": Range(
            start="2024-01-01",
            end="2024-12-31",
            include_start=True,
            include_end=False,
        ),
        "total_revenue": Range(
            start=100,
            end=None,
            include_start=False,
            include_end=False,
        ),
    }

    filters = adapter._build_filters(bounds)

    assert {
        "type": "WHERE",
        "column": "sales.region",
        "operator": "=",
        "value": "North",
    } in filters
    assert {
        "type": "WHERE",
        "column": "sales.sale_date",
        "operator": ">=",
        "value": "2024-01-01",
    } in filters
    assert {
        "type": "WHERE",
        "column": "sales.sale_date",
        "operator": "<",
        "value": "2024-12-31",
    } in filters
    assert {
        "type": "HAVING",
        "column": "sales.total_revenue",
        "operator": ">",
        "value": 100,
    } in filters


def test_build_filters_all_operators(requests_mock: Mocker) -> None:
    """
    NotEqual, IsNull, and IsNotNull each have a dedicated operator string.
    """
    adapter = _adapter(requests_mock)
    filters = adapter._build_filters(
        {
            "region": NotEqual("North"),
            "sale_date": IsNull(),
        },
    )
    assert {
        "type": "WHERE",
        "column": "sales.region",
        "operator": "!=",
        "value": "North",
    } in filters
    assert {
        "type": "WHERE",
        "column": "sales.sale_date",
        "operator": "IS NULL",
        "value": None,
    } in filters

    filters = adapter._build_filters({"region": IsNotNull()})
    assert filters == [
        {
            "type": "WHERE",
            "column": "sales.region",
            "operator": "IS NOT NULL",
            "value": None,
        },
    ]


def test_build_filters_inclusive_end(requests_mock: Mocker) -> None:
    """
    An inclusive Range upper bound uses ``<=``.
    """
    adapter = _adapter(requests_mock)
    filters = adapter._build_filters(
        {"sale_date": Range(start=None, end="2024-12-31", include_end=True)},
    )
    assert filters == [
        {
            "type": "WHERE",
            "column": "sales.sale_date",
            "operator": "<=",
            "value": "2024-12-31",
        },
    ]


def test_build_filters_impossible(requests_mock: Mocker) -> None:
    """
    An ``Impossible`` bound shortcuts to ``ImpossibleFilterError``.
    """
    adapter = _adapter(requests_mock)
    with pytest.raises(ImpossibleFilterError):
        adapter._build_filters({"region": Impossible()})


def test_build_filters_unsupported(requests_mock: Mocker) -> None:
    """
    Unknown filter types raise an internal error.
    """
    adapter = _adapter(requests_mock)

    class WeirdFilter:  # pylint: disable=too-few-public-methods
        """
        A filter type the adapter does not know how to translate.
        """

        operators = {Operator.EQ}

    with pytest.raises(InternalError, match="Unsupported filter"):
        adapter._build_filters({"region": WeirdFilter()})  # type: ignore[dict-item]


def test_build_order(requests_mock: Mocker) -> None:
    """
    ``order`` tuples become ``{by, direction}`` entries.
    """
    adapter = _adapter(requests_mock)
    assert adapter._build_order(
        [
            ("region", Order.ASCENDING),
            ("total_revenue", Order.DESCENDING),
        ],
    ) == [
        {"by": "sales.region", "direction": "ASC"},
        {"by": "sales.total_revenue", "direction": "DESC"},
    ]


def test_build_order_unknown_column(requests_mock: Mocker) -> None:
    """
    Ordering by an unknown column raises ``ProgrammingError``.
    """
    adapter = _adapter(requests_mock)
    with pytest.raises(ProgrammingError, match="unknown column"):
        adapter._build_order([("ghost", Order.ASCENDING)])


def test_end_to_end_via_sql(requests_mock: Mocker) -> None:
    """
    A real SQL query flows through SQLite and out to the API.
    """
    _stub_view(requests_mock)
    query_matcher = requests_mock.post(
        f"{BASE_URL}/query",
        json={
            "requests": [],
            "results": {
                "schema": [],
                "rows": [
                    {"region": "North", "total_revenue": 100.0},
                    {"region": "East", "total_revenue": 50.0},
                ],
            },
        },
    )

    cursor = connect(":memory:").cursor()
    rows = list(
        cursor.execute(
            f"""
            SELECT region, total_revenue
            FROM "{URI}"
            WHERE region = 'North'
            GROUP BY region
            ORDER BY total_revenue DESC
            LIMIT 5
            """,
        ),
    )

    assert rows == [("North", 100.0), ("East", 50.0)]
    body = query_matcher.last_request.json()
    assert {
        "type": "WHERE",
        "column": "sales.region",
        "operator": "=",
        "value": "North",
    } in body["query"]["filters"]
    assert sorted(body["query"]["dimensions"]) == ["sales.region"]
    assert sorted(body["query"]["metrics"]) == ["sales.total_revenue"]
