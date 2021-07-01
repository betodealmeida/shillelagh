from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from shillelagh.backends.apsw.dialects.base import APSWDialect
from sqlalchemy.engine.url import URL


class APSWSafeDialect(APSWDialect):
    def __init__(
        self,
        adapters: Optional[List[str]] = None,
        adapter_kwargs: Optional[Dict[str, Dict[str, Any]]] = None,
        *args: Any,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)
        self._adapters = adapters
        self._adapter_kwargs = adapter_kwargs or {}
        self._safe = True

    def create_connect_args(
        self,
        url: URL,
    ) -> Tuple[Tuple[()], Dict[str, Any]]:
        return (), {
            "path": ":memory:",
            "adapters": self._adapters,
            "adapter_kwargs": self._adapter_kwargs,
            "safe": True,
            "isolation_level": self.isolation_level,
        }
