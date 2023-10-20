"""
An adapter for fetching XML data.
"""

import logging
import xml.etree.ElementTree as ET
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

from defusedxml import ElementTree as DET

from shillelagh.adapters.api.generic_json import GenericJSONAPI
from shillelagh.exceptions import ProgrammingError
from shillelagh.filters import Filter
from shillelagh.lib import flatten
from shillelagh.typing import RequestedOrder, Row

_logger = logging.getLogger(__name__)


def element_to_dict(element: ET.Element) -> Any:
    """
    Convert XML element to a dictionary, recursively.

    This uses a super simple algorithm that focuses on text and ignores attributes.
    """
    if element.text and element.text.strip():
        return element.text.strip()

    result: Dict[str, Any] = {}
    for child in element:
        child_data = element_to_dict(child)
        if child.tag in result:
            # Convert to a list if multiple elements with the same tag exist
            if not isinstance(result[child.tag], list):
                result[child.tag] = [result[child.tag], child_data]
            else:
                result[child.tag].append(child_data)
        else:
            result.update({child.tag: child_data})

    return result


class GenericXMLAPI(GenericJSONAPI):

    """
    An adapter for fetching XML data.
    """

    safe = True

    supports_limit = False
    supports_offset = False
    supports_requested_columns = True

    content_type = "xml"  # works with text/xml and application/xml
    default_path = "*"
    cache_name = "generic_xml_cache"

    def get_data(  # pylint: disable=unused-argument, too-many-arguments
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        requested_columns: Optional[Set[str]] = None,
        **kwargs: Any,
    ) -> Iterator[Row]:
        response = self._session.get(self.uri)
        payload = response.content.decode("utf-8")
        if not response.ok:
            raise ProgrammingError(f"Error: {payload}")

        root = DET.fromstring(payload)
        result = root.findall(self.path)
        for i, element in enumerate(result):
            row = element_to_dict(element)
            row = {
                k: v
                for k, v in row.items()
                if requested_columns is None or k in requested_columns
            }
            row["rowid"] = i
            _logger.debug(row)
            yield flatten(row)
