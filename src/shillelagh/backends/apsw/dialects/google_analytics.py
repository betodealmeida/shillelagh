from datetime import date
from shillelagh.backends.apsw.dialects.base import APSWDialect
from sqlalchemy.engine.url import URL
from typing import Tuple, Dict, Any
from urllib.parse import unquote, unquote_plus
import json

class APSWGoogleAnalyticsDialect(APSWDialect):
    name = "google_analytics"

    def __init__(self, service_account_info=None, view_id=None, date_range=None,**kwargs):
        super().__init__(**kwargs)

    def create_connect_args(self, url: URL) -> Tuple[Tuple[()], Dict[str, Any]]:
        query_params = url.host.split("?")[1]
        date_range, service_account_info, view_id = query_params.split("&")
        
        date_range_ = unquote(date_range.split("=")[1]).split(",")
        date_range = [{"startDate": date_range_[0], "endDate" : date_range_[1]}]

        # TODO: find a better way to read http encoded content saved by superset
        # this sometimes replaces "+" signs in private key too, making it invalid
        service_account_info = json.loads(unquote_plus(unquote(service_account_info.split("=")[1])))

        view_id = view_id.split("=")[1]

        return (), {
            "adapters" : ["googleanalytics"],
            "path": ":memory:",
            "adapter_kwargs" : {"googleanalytics": {
                "service_account_info": service_account_info,
                "view_id": view_id,
                "date_range" : date_range
            }}
        }