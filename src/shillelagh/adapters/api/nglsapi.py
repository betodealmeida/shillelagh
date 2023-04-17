# pylint: disable=logging-fstring-interpolation
# NGLS exclusive
"""An adapter to NglsAPI."""

import logging
from typing import Any, Dict, Iterator, List, Optional, Tuple
from datetime import timedelta
import json
import xml.dom.minidom
import dateutil.parser
import dateutil.tz
import pytz
from shillelagh.adapters.base import Adapter
from shillelagh.backends.apsw.dialects.ngls import NglsReports
from shillelagh.exceptions import InternalError
from shillelagh.fields import Field
from shillelagh.filters import Filter
from shillelagh.lib import SimpleCostModel
from shillelagh.typing import RequestedOrder, Row
from sqlalchemy.engine.url import URL


_logger = logging.getLogger(__name__)
AVERAGE_NUMBER_OF_ROWS = 1000


class NglsAPI(Adapter):
    """An adapter for NglsAPI.
    An adapter for NglsAPI.
    It gets initialized with an NglsReports instance and the name of a table within that instance.
    """
    safe = True
    # Since the adapter doesn't return exact data (see the time columns below)
    # implementing limit/offset is not worth the trouble.
    supports_requested_columns = True
    supports_in_statements = True
    supports_limit = False
    supports_offset = False

    @staticmethod
    def supports(uri: str, fast: bool = True, **kwargs: Any) -> Optional[bool]:
        """Check if the table with name ${uri} is supported"""
        _logger.debug(f'supports({uri}, {fast}, {kwargs}')
        nglsreports = NglsReports.get_instance(URL(kwargs.get('url')))
        if not nglsreports:
            _logger.error('nglsreports does not exist')
            return True
        return nglsreports.has_table_name(uri)

    @staticmethod
    def parse_uri(uri: str) -> Tuple[str, str]:
        return (uri,)

    def __init__(
        self, table: str, url: str, **kwargs: Any  # pylint: disable=unused-argument
    ):
        super().__init__()
        self.table = table
        self.nglsreports = NglsReports.get_instance(URL(url))

    def get_columns(self) -> Dict[str, Field]:
        return self.nglsreports.get_columns_dict(self.table)

    get_cost = SimpleCostModel(AVERAGE_NUMBER_OF_ROWS)

    def get_data(
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
        **kwargs: Any,
    ) -> Iterator[Row]:
        # https://shillelagh.readthedocs.io/en/latest/development.html#creating-a-custom-sqlalchemy-dialect
        _logger.info(f"get_data for {self.table}: ({bounds}, {order}, {kwargs})")
        if self.table == "intervals":
            data = [["hour"], ["day"], ["month"]]
        elif self.table == "abandoned_tags":
            data = [["included"], ["excluded"], ["only"]]
        elif self.table == "call_types":
            data = [["911"], ["10-digit"], ["admin"], ["consultation"]]
        else:
            params = self.set_params(bounds)
            result = self.nglsreports.get(self.table, params)
            if not result:
                raise InternalError("Error while getting data from ngls-reporting service")
            data = result.get('data', [])

        _logger.info(f'Got {len(data)} rows for {self.table}')
        for record in data:
            # replace with -> row = {column: record[column] for column in self.get_columns()}
            # if columns are ordered
            row = {}
            for i, column in enumerate(self.nglsreports.get_columns(self.table)):
                if column['type'] == 'TIMESTAMP':
                    row[column['column_name']] = dateutil.parser.parse(record[i])
                else:
                    if column['column_name'].endswith('_xml'):
                        row[column['column_name']] = self.get_formatted_xml_string(record[i])
                    elif column['column_name'].endswith('_elapsed_time'):
                        row[column['column_name']] = str(timedelta(seconds=int(record[i])))
                    else:
                        row[column['column_name']] = record[i]
            yield row

    def get_formatted_xml_string(self, xml_string: str) -> str:
        if not xml_string:
            return ''
        formatted_xml_string = xml.dom.minidom.parseString(xml_string).toprettyxml()
        # Superset removes all content within XML tags < >, so replace them
        formatted_xml_string = formatted_xml_string.replace('<', '&lt;').replace('>', '&gt;')
        return '<pre>' + formatted_xml_string + '</pre>'

    def set_params(self, bounds) -> dict:
        params = {'format': 'json'}

        date_time_predicate = bounds.get('date_time')
        if date_time_predicate:
            params['from'] = pytz.utc.localize(date_time_predicate.start).strftime('%Y-%m-%dT%H:%M:%SZ')
            params['to'] = pytz.utc.localize(date_time_predicate.end).strftime('%Y-%m-%dT%H:%M:%SZ')
        else:
            # Get latest 60 days of data.
            params['for'] = '86400m'

        if self.table not in ['agent_activity', 'text_to_911', 'location_discrepancy', 'location_queries_and_results']:
            interval_predicate = bounds.get('interval')
            params['interval'] = interval_predicate.value if interval_predicate else 'hour'

        if self.table in ['psap_ring_time', 'psap_queue_time', 'busiest_hour', 'call_summary']:
            abandoned_predicate = bounds.get('abandoned_tag')
            params['abandoned'] = abandoned_predicate.value if abandoned_predicate else 'included'

        if self.table in ['busiest_hour', 'call_duration']:
            call_type_predicate = bounds.get('call_type')
            if call_type_predicate:
                params['terms'] = json.dumps({"localCallType": list(self.deserialize_set(call_type_predicate.value))})
        return params
