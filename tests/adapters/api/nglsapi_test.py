"""Tests the NGLSApi adapter"""

import datetime
import json
import types
from unittest.mock import patch

import pytest

from shillelagh.adapters.api.nglsapi import NglsAPI
from shillelagh.exceptions import InternalError
from shillelagh.fields import DateTime, Float, Integer, String


class Test:
    """Manages the tests for the NGLSApi adapter"""

    DATA = {
        "columns": [
            {
                "column_name": "agency",
                "field": {"class": "String"},
                "name": "Agency",
                "predicate": {"default": {}, "params": {}},
                "type": "TEXT",
            },
            {
                "column_name": "call_count",
                "field": {"class": "Integer"},
                "name": "Call count",
                "predicate": {"default": {}, "params": {}},
                "type": "INTEGER",
            },
            {
                "column_name": "queue_time",
                "field": {"class": "Float"},
                "name": "Queue time",
                "predicate": {"default": {}, "params": {}},
                "type": "FLOAT",
            },
            {
                "column_name": "date_time",
                "field": {
                    "class": "DateTime",
                    "exact": True,
                    "filters": ["Equal", "Range"],
                    "order": "Any",
                },
                "name": "Date/Time",
                "predicate": {
                    "default": {"for": "86400m"},
                    "params": {"from": "start", "to": "end"},
                },
                "type": "TIMESTAMP",
            },
            {
                "column_name": "pidflo_xml",
                "field": {"class": "String"},
                "name": "PIDF-LO",
                "predicate": {"default": {}, "params": {}},
                "type": "TEXT",
            },
            {
                "column_name": "available_elapsed_time",
                "field": {"class": "String"},
                "name": "Available",
                "predicate": {"default": {}, "params": {}},
                "type": "TEXT",
            },
            {
                "column_name": "call_type",
                "field": {
                    "class": "String",
                    "exact": True,
                    "filters": ["Equal", "Like", "NotEqual"],
                    "order": "Any",
                },
                "name": "Call type",
                "predicate": {"default": {}, "params": {"terms": "localCallType"}},
                "type": "TEXT",
            },
        ],
        "report": {"id": "b5c4c659-6b6a-41b1-a45a-c0da7c6c4834", "category": "Gemma"},
        "table_name": "table",
    }

    OUTPUT_DATE_STRING = "04.06.2023"
    OUTPUT_XML_STRING = "<result>Test</result>"
    EXPECTED_XML_STRING = (
        "<pre>"
        + '&lt;?xml version="1.0" ?&gt;\n&lt;result&gt;Test&lt;/result&gt;\n'
        + "</pre>"
    )

    class RequestResponse:  # pylint: disable=too-few-public-methods
        """Mocks requests"""

        def __init__(self, data, status_code=200):
            self.data = data
            self.status_code = status_code

        def json(self):
            """Returns data that is already a dictionary"""
            return self.data

    def test_get_columns(self):
        """Tests get_columns"""
        with patch(
            "requests.get",
            return_value=self.RequestResponse(self.DATA),
        ) as mock_request:
            ngls_api = NglsAPI(table="table", url="some.reporting.url")
            columns = ngls_api.get_columns()
            assert isinstance(columns.get("agency"), type(String()))
            assert isinstance(columns.get("call_count"), type(Integer()))
            assert isinstance(columns.get("queue_time"), type(Float()))
            assert isinstance(columns.get("date_time"), type(DateTime()))
            assert isinstance(columns.get("pidflo_xml"), type(String()))
            assert isinstance(columns.get("available_elapsed_time"), type(String()))
            assert isinstance(columns.get("call_type"), type(String()))

            mock_request.assert_called_once()

    def test_get_data_no_bounds(self):
        """Tests get_data with no bounds defined"""
        bounds = {}
        order = []

        get_output = {
            "data": [
                [
                    "Agency1",  # agency
                    1,  # call_count
                    1.0,  # queue_time
                    self.OUTPUT_DATE_STRING,  # date_time
                    self.OUTPUT_XML_STRING,  # pidflo_xml
                    "1",  # available_elapsed_time
                    "911",  # call_type
                ],
            ],
        }

        expected_row = [
            {
                "agency": "Agency1",
                "call_count": 1,
                "queue_time": 1.0,
                "date_time": datetime.datetime(2023, 4, 6, 0, 0),
                "pidflo_xml": self.EXPECTED_XML_STRING,
                "available_elapsed_time": "0:00:01",
                "call_type": "911",
            },
        ]

        with patch(
            "requests.get",
            return_value=self.RequestResponse(get_output),
        ) as mock_request:
            ngls_api = NglsAPI(table="table", url="some.reporting.url")
            results = ngls_api.get_data(bounds, order)
            assert list(results) == expected_row
            mock_request.assert_called_once()

    def test_get_data_no_results(self):
        """Tests get_data that returns no results"""
        bounds = {}
        order = []

        with patch(
            "requests.get",
            return_value=self.RequestResponse(None),
        ) as mock_request:
            ngls_api = NglsAPI(table="table", url="some.reporting.url")
            results = ngls_api.get_data(bounds, order)
            with pytest.raises(InternalError) as error:
                list(results)
            assert str(error.value) == (
                "Error while getting data from ngls-reporting service"
            )
            mock_request.assert_called_once()

    def test_get_formatted_xml_string(self):
        """Tests get_formatted_xml_string"""
        ngls_api = NglsAPI(table="table", url="some.reporting.url")
        result = ngls_api.get_formatted_xml_string("<result>Test</result>")
        assert result == self.EXPECTED_XML_STRING

    def test_get_formatted_xml_string_empty(self):
        """Tests get_formatted_xml_string with no input"""
        ngls_api = NglsAPI(table="table", url="some.reporting.url")
        result = ngls_api.get_formatted_xml_string("")
        assert result == ""

    def test_set_params_date_time(self):
        """Tests set_params with a date time set"""
        start_end_predicate = types.SimpleNamespace(
            start=datetime.datetime(2023, 4, 5),
            end=datetime.datetime(2023, 4, 6),
        )
        bounds = {"date_time": start_end_predicate}

        expected_params = {
            "format": "json",
            "from": "2023-04-05T00:00:00",
            "to": "2023-04-06T00:00:00",
        }

        ngls_api = NglsAPI(table="table", url="some.reporting.url")
        result = ngls_api.set_params(bounds)
        assert result == expected_params

    def test_set_params_terms(self):
        """Tests set_params that uses the busiest hour table."""
        bounds = {"call_type": types.SimpleNamespace(value='["a","b","c"]')}

        expected_params = {
            "format": "json",
            "for": "86400m",
        }

        expected_terms = {"localCallType": ["a", "b", "c"]}

        ngls_api = NglsAPI(table="table", url="some.reporting.url")
        result = ngls_api.set_params(bounds)
        terms_value = result.pop("terms")
        assert result == expected_params
        assert {(frozenset(item)) for item in json.loads(terms_value)} == {
            (frozenset(item)) for item in expected_terms
        }
