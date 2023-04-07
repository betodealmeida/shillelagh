# pylint: disable=logging-fstring-interpolation
"""An adapter to NglsAPI."""

import logging
from typing import Any, Dict, Iterator, List, Optional, Tuple
from datetime import timedelta
import xml.dom.minidom
import dateutil.parser
import dateutil.tz
import pytz
from shillelagh.adapters.base import Adapter
from shillelagh.backends.apsw.dialects.ngls import NglsReports
from shillelagh.fields import DateTime, Field, Float, Integer, String
from shillelagh.filters import Filter, Operator, Equal, Like, NotEqual, Range
from shillelagh.typing import RequestedOrder, Row, Order
from shillelagh.exceptions import InternalError


_logger = logging.getLogger(__name__)
INITIAL_COST = 0
FETCHING_COST = 1000


class NglsAPI(Adapter):
    """An adapter for NglsAPI.
    An adapter for NglsAPI.
    It gets initialized with an NglsReports instance and the name of a table within that instance.
    """
    safe = True
    # Since the adapter doesn't return exact data (see the time columns below)
    # implementing limit/offset is not worth the trouble.
    supports_limit = False
    supports_offset = False

    @staticmethod
    def supports(uri: str, fast: bool = True, **kwargs: Any) -> Optional[bool]:
        """Check if the table with name ${uri} is supported"""
        _logger.debug(f'supports({uri}, {fast}, {kwargs}')
        nglsreports = NglsReports.from_dict(kwargs.get('nglsreports'))
        if not nglsreports:
            _logger.error('nglsreports does not exist')
            return True
        return nglsreports.has_table_name(uri)

    @staticmethod
    def parse_uri(uri: str) -> Tuple[str, str]:
        return (uri,)

    def __init__(
        self, table: str, nglsreports: object, **kwargs: Any
    ):
        super().__init__()
        _logger.debug(f'__init__({table}, {nglsreports}, {kwargs})')
        self.table = table
        self.nglsreports = NglsReports.from_dict(nglsreports)

    def get_columns(self) -> Dict[str, Field]:
        _logger.debug("get_columns()")
        columns = self.nglsreports.get_columns(self.table)
        column_dict = {}
        for column in columns:
            col_name = column['column_name']
            col_type = column['type']
            if col_type == 'TEXT':
                if (col_name in ('interval', 'abandoned_tag')) or (self.table in ('agent_ready_not_ready')
                                                                   and col_name in ('agency')):
                    column_dict[col_name] = String(filters=[Equal, Like, NotEqual], order=Order.ANY, exact=True)
                else:
                    column_dict[col_name] = String()
            elif col_type == 'INTEGER':
                column_dict[col_name] = Integer()
            elif col_type == 'FLOAT':
                column_dict[col_name] = Float()
            elif col_type == 'TIMESTAMP':
                column_dict[col_name] = DateTime(filters=[Equal, Range], order=Order.ANY, exact=True)
            else:
                _logger.error(f"get_columns - unhandled type: {column['type']}")
        return column_dict

    def get_cost(
        self,
        filtered_columns: List[Tuple[str, Operator]],
        order: List[Tuple[str, RequestedOrder]],
    ) -> float:
        _logger.debug(f"get_cost({filtered_columns}, {order})")
        cost = INITIAL_COST
        # if the operator is ``Operator.EQ`` we only need to fetch 1 day of data;
        # otherwise we potentially need to fetch "window" days of data

        for _, operator in filtered_columns:  # pylint: disable=unused-variable
            weight = 1  # if operator == Operator.EQ else self.window
            cost += FETCHING_COST * weight
        return cost

    def get_data(
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
        **kwargs: Any,
    ) -> Iterator[Row]:
        # TODO: MIGRATE to use get_rows() instead.
        # https://shillelagh.readthedocs.io/en/latest/development.html#creating-a-custom-sqlalchemy-dialect
        _logger.info(f"get_data for {self.table}: ({bounds}, {order}, {kwargs})")
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

        if self.table in ['agent_ready_not_ready']:
            agency_predicate = bounds.get('agency')
            params['psap'] = agency_predicate.value if agency_predicate else ''

        result = self.nglsreports.get(self.table, params)
        if not result:
            raise InternalError("Error while getting data from ngls-reporting service")

        data = result.get('data', [])
        _logger.info(f'Got {len(data)} rows for {self.table}')
        for record in result.get('data'):
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
