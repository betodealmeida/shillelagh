"""Tests the NGLSApi dialect"""

from unittest.mock import patch

from sqlalchemy.engine.url import URL
from sqlalchemy.pool.base import _ConnectionFairy

from shillelagh.backends.apsw.dialects.ngls import NglsDialect, NglsReports


class Test:
    """Manages the tests for the NGLS dialect"""

    REPORT_NAME = "some_table_name"
    REPORT_ID = "b5c4c659-6b6a-41b1-a45a-c0da7c6c4834"

    DATA = {
        "columns": [
            {
                "column_name": "agency",
                "field": {"class": "String"},
                "name": "Agency",
                "predicate": {"default": {}, "params": {}},
                "type": "TEXT",
            },
        ],
        "report": {"id": REPORT_ID, "category": "Gemma"},
        "table_name": REPORT_NAME,
    }

    OUTPUT_DATE_STRING = "04.06.2023"
    SQLALCHEMY_URL = URL.create(
        "drivername",
        host="some.reporting.url",
        port="443",
        database="reporting",
    )

    class RequestResponse:  # pylint: disable=too-few-public-methods
        """Mocks requests"""

        def __init__(self, data, status_code=200, text=""):
            self.data = data
            self.status_code = status_code
            self.text = text

        def json(self):
            """Returns data that is already a dictionary"""
            return self.data

    def test_nglsreports_get_table_names(self):
        """Tests get_table_names"""
        with patch(
            "requests.get",
            return_value=self.RequestResponse(self.DATA),
        ) as mock_request:
            ngls_reports = NglsReports(url=self.SQLALCHEMY_URL)
            ngls_reports.get_table_names()
            assert self.REPORT_NAME in ngls_reports.table_names
            assert ngls_reports.table_ids[self.REPORT_NAME] == self.REPORT_ID
            mock_request.assert_called_once()

    def test_nglsreports_get_table_names_exception(self):
        """Tests get_table_names where get request throws an exception"""
        with patch(
            "requests.get",
        ) as mock_request:
            mock_request.side_effect = Exception("Error")
            ngls_reports = NglsReports(url=self.SQLALCHEMY_URL)
            ngls_reports.get_table_names()
            assert ngls_reports.table_names == []
            assert ngls_reports.table_ids == {}
            mock_request.assert_called_once()

    def test_nglsreports_get_table_names_status(self):
        """Tests get_table_names where get request response is not 200"""
        with patch(
            "requests.get",
            return_value=self.RequestResponse(
                self.DATA,
                status_code=404,
                text="Not Found",
            ),
        ) as mock_request:
            ngls_reports = NglsReports(url=self.SQLALCHEMY_URL)
            ngls_reports.get_table_names()
            assert ngls_reports.table_names == []
            assert ngls_reports.table_ids == {}
            mock_request.assert_called_once()

    def test_nglsreports_url(self):
        """Tests url function"""
        with patch("requests.get", return_value=self.RequestResponse(self.DATA)):
            ngls_reports = NglsReports(url=self.SQLALCHEMY_URL)
            ngls_reports.get_table_names()
            url = ngls_reports.url(self.REPORT_NAME)
            assert (
                url
                == "https://some.reporting.url:443/reporting/v1/reports/b5c4c659-6b6a-41b1-a45a-c0da7c6c4834"  # pylint: disable=line-too-long
            )

    def test_nglsreports_url_no_table(self):
        """Tests url function, not specifying a table name"""
        ngls_reports = NglsReports(url=self.SQLALCHEMY_URL)
        url = ngls_reports.url()
        assert url == "https://some.reporting.url:443/reporting/v1/reports"

    def test_nglsreports_get_status(self):
        """Tests get function where request response is not 200"""
        with patch(
            "requests.get",
            return_value=self.RequestResponse(self.DATA),
        ):
            ngls_reports = NglsReports(url=self.SQLALCHEMY_URL)
            ngls_reports.get_table_names()

        with patch(
            "requests.get",
            return_value=self.RequestResponse(
                self.DATA,
                status_code=404,
                text="Not Found",
            ),
        ) as mock_request:
            result = ngls_reports.get(self.REPORT_NAME, [])
            assert result is None
            mock_request.assert_called_once()

    def test_nglsreport_get_columns_dict_no_table(self):
        """Tests get_columns_dict with a missing table name"""
        ngls_reports = NglsReports(url=self.SQLALCHEMY_URL)
        result = ngls_reports.get_columns_dict("nonexistent_table")
        assert result == {}

    def test_nglsdialect_do_ping(self):
        """Tests do_ping"""
        with patch(
            "requests.get",
            return_value=self.RequestResponse(self.DATA),
        ) as mock_request:
            ngls_dialect = NglsDialect()
            ngls_dialect.nglsreports = NglsReports(url=self.SQLALCHEMY_URL)
            result = ngls_dialect.do_ping(_ConnectionFairy)
            assert result
            mock_request.assert_called_once()

    def test_nglsdialect_do_ping_status(self):
        """Tests do_ping where request response is not 200"""
        with patch(
            "requests.get",
            return_value=self.RequestResponse(
                self.DATA,
                status_code=404,
                text="Not Found",
            ),
        ) as mock_request:
            ngls_dialect = NglsDialect()
            ngls_dialect.nglsreports = NglsReports(url=self.SQLALCHEMY_URL)
            result = ngls_dialect.do_ping(_ConnectionFairy)
            assert not result
            mock_request.assert_called_once()

    def test_nglsdialect_do_ping_not_initialized(self):
        """Tests do_ping where nglsreports is not initialized"""
        with patch(
            "requests.get",
            return_value=self.RequestResponse(self.DATA),
        ) as mock_request:
            ngls_dialect = NglsDialect()
            result = ngls_dialect.do_ping(_ConnectionFairy)
            assert not result
            mock_request.assert_not_called()

    def test_nglsdialect_get_table_names(self):
        """Tests get_table_names"""
        with patch("requests.get", return_value=self.RequestResponse(self.DATA)):
            ngls_reports = NglsReports(url=self.SQLALCHEMY_URL)
            ngls_reports.get_table_names()

        ngls_dialect = NglsDialect()
        ngls_dialect.nglsreports = ngls_reports
        result = ngls_dialect.get_table_names(_ConnectionFairy)
        assert self.REPORT_NAME in result

    def test_nglsdialect_has_table(self):
        """Tests has_table"""
        with patch("requests.get", return_value=self.RequestResponse(self.DATA)):
            ngls_reports = NglsReports(url=self.SQLALCHEMY_URL)
            ngls_reports.get_table_names()

        ngls_dialect = NglsDialect()
        ngls_dialect.nglsreports = ngls_reports
        result = ngls_dialect.has_table(_ConnectionFairy, self.REPORT_NAME)
        assert result
