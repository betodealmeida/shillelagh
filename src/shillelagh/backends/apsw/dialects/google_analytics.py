from datetime import date
from shillelagh.backends.apsw.dialects.base import APSWDialect
from sqlalchemy.engine.url import URL
from typing import Tuple, Dict, Any

class APSWGoogleAnalyticsDialect(APSWDialect):
    name = "google_analytics"

    def __init__(self, service_account_info=None, **kwargs):
        super().__init__(**kwargs)
        self.service_account_info = service_account_info

    def create_connect_args(self, url: URL) -> Tuple[Tuple[()], Dict[str, Any]]:
        params = str(url).split("/")
        view_id = params[2]
        date_range = params[3].split(",")
        date_range = [{'startDate': date_range[0], 'endDate': date_range[1]}],

        return (), {
            "adapters" : ["googleanalyticsapi"],
            "path": ":memory:",
            "adapter_kwargs" : {"googleanalyticsapi": {
                "service_account_info": self.service_account_info,
                "view_id": view_id,
                "date_range" : date_range
            }}
        }