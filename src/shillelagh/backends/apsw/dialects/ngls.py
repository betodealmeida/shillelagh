# pylint: disable=too-many-instance-attributes, logging-fstring-interpolation, broad-except
# NGLS exclusive
"""A dialect that only connects to GSheets.

This dialect was implemented to replace the ``gsheetsdb`` library.
"""
import os
import logging
from typing import Any, Dict, List, Optional, Tuple
import json
import time
import requests
import pandas as pd
from shillelagh.backends.apsw.dialects.base import APSWDialect
from shillelagh.fields import DateTime, Field, Float, Integer, String
from shillelagh.filters import Equal, Like, NotEqual, Range
from shillelagh.typing import Order
from sqlalchemy.engine.url import URL
from sqlalchemy.pool.base import _ConnectionFairy

# Default timeout for reporting API requests (in seconds)
TIMEOUT = 30
# Default time for an NglsReports instance before fetching again from NGLS
TTL = 300

_logger = logging.getLogger(__name__)


class NglsReports:

    instance = None
    timestamp = None

    def __init__(self, url: URL):
        self.url_str = str(url)
        self.host = url.host
        self.port = url.port
        self.database = url.database
        self.api_key = os.getenv('NGLS_API_KEY')
        self.verify = os.getenv('CA_CERT_FILE', '/app/certs/ca.crt')
        self.table_names = []
        self.columns = {}
        self.columns_dicts = {}
        self.table_ids = {}

    def _text_to_string_field(self, tablename: str, col_name: str) -> String:
        if (col_name in ('interval', 'abandoned_tag') or
            (tablename in ('agent_ready_not_ready') and col_name in ('agency')) or
                (tablename in ('busiest_hour') and col_name in ('call_type'))):
            return String(filters=[Equal, Like, NotEqual], order=Order.ANY, exact=True)
        return String()

    def get_table_names(self):
        try:
            headers = {'X-NGLS-API-Key': self.api_key}
            url = f'https://{self.host}:{self.port}/{self.database}/v1/reports?all_reports=true&superset=true'
            response = requests.get(url, headers=headers, verify=self.verify, timeout=TIMEOUT)
        except Exception as err:
            _logger.error(f"Caught error from GET {url}: {err}")
            return
        if response.status_code != 200:
            _logger.info(f"{url} - got response {response.status_code}, {response.text}")
            return
        reports_df = pd.json_normalize(response.json())
        # filter out the Gemma reports
        reports_df = reports_df[reports_df['report.category'] == 'Gemma']
        self.table_names = list(reports_df['table_name'].values)
        table_ids = dict(reports_df[['table_name', 'report.id']].set_index('table_name').T.to_dict('records')[0])
        self.table_ids = table_ids
        columns = dict(reports_df[['table_name', 'columns']].set_index('table_name').T.to_dict('records')[0])
        self.columns = columns
        reports_df['columns_dict'] = None
        columns_dicts = dict(reports_df[['table_name', 'columns_dict']].set_index('table_name').T.to_dict('records')[0])
        self.columns_dicts = columns_dicts
        _logger.info(f"table_names={self.table_names}")

    def has_table_name(self, tablename):
        return tablename in self.table_names

    def url(self, table: str = None):
        if not table:
            return f'https://{self.host}:{self.port}/{self.database}/v1/reports'
        return f'https://{self.host}:{self.port}/{self.database}/v1/reports/{self.table_ids[table]}'

    def get(self, tablename, params):
        headers = {'X-NGLS-API-Key': self.api_key}
        url = self.url(tablename)
        _logger.info(f'Get report from NGLS: GET {url} {json.dumps(params)}')
        response = requests.get(url, params=params, headers=headers, verify=self.verify, timeout=TIMEOUT)
        if response.status_code != 200:
            _logger.warning(f'ngls response code: {response.status_code}')
            return None
        return response.json()

    def get_columns(self, tablename):
        return self.columns[tablename] if self.has_table_name(tablename) else []

    def get_columns_dict(self, tablename) -> Dict[str, Field]:
        if not self.has_table_name(tablename):
            return {}
        if not self.columns_dicts[tablename]:
            columns_dict = {}
            for column in self.columns[tablename]:
                col_name = column['column_name']
                col_type = column['type']
                if col_type == 'TEXT':
                    columns_dict[col_name] = self._text_to_string_field(tablename, col_name)
                elif col_type == 'INTEGER':
                    columns_dict[col_name] = Integer()
                elif col_type == 'FLOAT':
                    columns_dict[col_name] = Float()
                elif col_type == 'TIMESTAMP':
                    columns_dict[col_name] = DateTime(filters=[Equal, Range], order=Order.ANY, exact=True)
                else:
                    _logger.error(f"get_columns - unhandled type: {column['type']}")
            _logger.info(f"Created columns dictionary for {tablename}")
            self.columns_dicts[tablename] = columns_dict
        return self.columns_dicts[tablename]

    def get_url_str(self) -> str:
        return self.url_str

    @staticmethod
    def get_instance(url: URL):
        timestamp = int(time.time())
        if not NglsReports.instance or NglsReports.timestamp < timestamp:
            _logger.info("Instantiating NglsReports instance")
            NglsReports.instance = NglsReports(url)
            NglsReports.instance.get_table_names()
            NglsReports.timestamp = timestamp + TTL
        return NglsReports.instance


class NglsDialect(APSWDialect):
    """This dialect loads the "nglsapi" adapter. To use it:

        >>> from sqlalchemy.engine import create_engine
        >>> engine = create_engine("ngls://host/api")
    """
    # This is supported in ``SQLiteDialect``, and equally supported here. See
    # https://docs.sqlalchemy.org/en/14/core/connections.html#caching-for-third-party-dialects
    # for more context.
    supports_statement_cache = True

    # scheme of the ngls URI (ngls://)
    name = "ngls"

    nglsreports = None

    def create_connect_args(self, url: URL) -> Tuple[Tuple[()], Dict[str, Any]]:
        # ngls connection string
        # The API Key is read from an environment variable NGLS_API_KEY.
        # The location of the CA certificate used by the reporting service is provided either via
        # environment variable or (for pods) is set to /app/certs/ca.crt.
        # debug
        _logger.debug(f'create_connect_args({url})')
        self.nglsreports = NglsReports.get_instance(url)
        return (), {
            "path": ":memory:",
            "adapters": ["nglsapi"],
            "adapter_kwargs": {
                "nglsapi": {
                    "url": self.nglsreports.get_url_str()
                },
            },
            "safe": True,
            "isolation_level": self.isolation_level,
        }

    def do_ping(
        self, dbapi_connection: _ConnectionFairy
    ) -> bool:
        """We will verify NGLS API status by getting a list of all the available reports.
        Return ngls API status.
        """
        _logger.debug(f"do_ping({dbapi_connection}): self.nglsreports={self.nglsreports}")
        # TODO: support HEAD requests on /v1/reports.
        headers = {'X-NGLS-API-Key': self.nglsreports.api_key}
        url = f'https://{self.nglsreports.host}:{self.nglsreports.port}/{self.nglsreports.database}/v1/reports'
        response = requests.get(url, headers=headers, verify=self.nglsreports.verify, timeout=TIMEOUT)
        if response.status_code == 200:
            return True
        _logger.info(f'Ping returned response code: {response.status_code}')
        return response.status_code == 200

    def get_table_names(
        self, connection: _ConnectionFairy, schema: str = None, **kwargs: Any
    ) -> List[str]:
        """Return a list of table names."""
        _logger.debug(f'get_table_names({connection}, {schema}, {kwargs})')
        return self.nglsreports.table_names

    def has_table(  # pylint: disable=unused-argument
        self,
        connection: _ConnectionFairy,
        table_name: str,
        schema: Optional[str] = None,
        info_cache: Optional[Dict[Any, Any]] = None,
        **kwargs: Any
    ) -> bool:
        """Return true if a given table exists.
        In order to determine if a table exists the method will build the full key
        and do a ``HEAD`` request on the resource.
        """
        _logger.debug(f'has_table({connection}, {table_name}, {schema})')
        return self.nglsreports.has_table_name(table_name)