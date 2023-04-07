# pylint: disable=too-many-instance-attributes, logging-fstring-interpolation, broad-except
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

    def __init__(self, url):
        self.host = url.host
        self.port = url.port
        self.database = url.database
        self.api_key = os.getenv('NGLS_API_KEY')
        self.verify = os.getenv('CA_CERT_FILE', '/app/certs/ca.crt')
        self.table_names = []
        self.columns = []
        self.table_ids = []

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

    def to_dict(self, obfuscate=False):
        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "api_key": "*****" if obfuscate else self.api_key,
            "verify": self.verify,
            "table_names": self.table_names,
            "table_ids": self.table_ids,
            "columns": self.columns
        }

    @staticmethod
    def get_instance(url: URL):
        timestamp = int(time.time())
        if not NglsReports.instance or NglsReports.timestamp < timestamp:
            _logger.info("Instantiating NglsReports instance")
            NglsReports.instance = NglsReports(url)
            NglsReports.instance.get_table_names()
            NglsReports.timestamp = timestamp + TTL
        return NglsReports.instance

    @staticmethod
    def from_dict(obj: dict):
        ngls_reports = NglsReports.get_instance(
            URL("ngls", host=obj["host"], port=obj["port"], database=obj["database"]))
        ngls_reports.api_key = obj["api_key"]
        ngls_reports.verify = obj["verify"]
        ngls_reports.table_names = obj["table_names"]
        ngls_reports.table_ids = obj["table_ids"]
        ngls_reports.columns = obj["columns"]
        return ngls_reports


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
                    "nglsreports": self.nglsreports.to_dict()
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
