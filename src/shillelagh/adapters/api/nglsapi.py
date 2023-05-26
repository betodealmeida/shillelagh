# pylint: disable=logging-fstring-interpolation
# NGLS exclusive
"""An adapter to NglsAPI."""

import json
import logging
import xml.dom.minidom
from datetime import timedelta
from typing import Any, Dict, Iterator, List, Optional, Tuple

import dateutil.parser
import dateutil.tz
import pytz
from sqlalchemy.engine.url import URL

from shillelagh.adapters.base import Adapter
from shillelagh.backends.apsw.dialects.ngls import NglsReports
from shillelagh.exceptions import InternalError
from shillelagh.fields import Field
from shillelagh.filters import Filter
from shillelagh.lib import SimpleCostModel
from shillelagh.typing import RequestedOrder, Row

_logger = logging.getLogger(__name__)
AVERAGE_NUMBER_OF_ROWS = 1000
ISO_FORMAT_TIMESTAMP = "%Y-%m-%dT%H:%M:%SZ"


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
        _logger.debug(f"supports({uri}, {fast}, {kwargs}")
        # Quick way to prevent unrelated unit tests from using nglsapi
        # since they use the URI argument.
        if kwargs.get("url") is None:
            return False
        nglsreports = NglsReports.get_instance(URL(kwargs.get("url")))
        if not nglsreports:
            _logger.error("nglsreports does not exist")
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
        self.get_static_data_table = {
            "intervals": [["hour"], ["day"], ["month"]],
            "abandoned_tags": [["included"], ["excluded"], ["only"]],
            "call_types": [["911"], ["10-digit"], ["admin"], ["consultation"]],
            "seq_nrs": [[str(x).zfill(4)] for x in range(1, 1001)],
        }

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
        if self.table in self.get_static_data_table:
            data = self.get_static_data_table[self.table]
        elif self.table == "agencies":
            result = self.nglsreports.get_agencies()
            if result is None:
                raise InternalError(
                    "Error while getting data from ngls-reporting service",
                )
            data = [[x.get("id")] for x in result]
        else:
            params = self.set_params(bounds)
            result = self.nglsreports.get(self.table, params)
            if not result:
                raise InternalError(
                    "Error while getting data from ngls-reporting service",
                )
            data = result.get("data", [])

        _logger.info(f"Got {len(data)} rows for {self.table}")
        for record in data:
            # replace with -> row = {column: record[column] for column in self.get_columns()}
            # if columns are ordered
            row = {}
            for i, column in enumerate(self.nglsreports.get_columns(self.table)):
                if column["type"] == "TIMESTAMP":
                    row[column["column_name"]] = dateutil.parser.parse(record[i])
                else:
                    if column["column_name"].endswith("_xml"):
                        row[column["column_name"]] = self.get_formatted_xml_string(
                            record[i],
                        )
                    elif column["column_name"].endswith("_elapsed_time"):
                        row[column["column_name"]] = str(
                            timedelta(seconds=int(record[i])),
                        )
                    else:
                        row[column["column_name"]] = record[i]
            yield row

    def get_formatted_xml_string(self, xml_string: str) -> str:
        """Parse and restore brackets for XML strings"""
        if not xml_string:
            return ""
        formatted_xml_string = xml.dom.minidom.parseString(xml_string).toprettyxml()
        # Superset removes all content within XML tags < >, so replace them
        formatted_xml_string = formatted_xml_string.replace("<", "&lt;").replace(
            ">",
            "&gt;",
        )
        return "<pre>" + formatted_xml_string + "</pre>"

    def set_params(self, bounds) -> dict:
        """Set the parameters for a reporting service query"""
        out_params = {"format": "json"}
        for (col_name, col_type, params, default) in [
            (
                x.get("column_name"),
                x.get("type"),
                x.get("predicate").get("params", {}),
                x.get("predicate").get("default", {}),
            )
            for x in self.nglsreports.get_columns(self.table)
            if x.get("predicate")
        ]:
            predicate = bounds.get(col_name)
            if predicate and params:
                for param, key in params.items():
                    if col_type == "TIMESTAMP":
                        out_params[param] = pytz.utc.localize(
                            eval(f"predicate.{key}"),  # pylint: disable=eval-used
                        ).strftime(ISO_FORMAT_TIMESTAMP)
                    elif param == "terms":
                        if isinstance(predicate.value, str):
                            out_params[param] = json.dumps(
                                {f"{key}": [predicate.value]},
                            )
                        else:
                            out_params[param] = json.dumps(
                                {f"{key}": predicate.value},
                            )
                    else:
                        out_params[param] = predicate.value
            elif not predicate and default:
                for param, value in default.items():
                    out_params[param] = value
        _logger.info(f"set_params({bounds}) result: {out_params}")
        return out_params
