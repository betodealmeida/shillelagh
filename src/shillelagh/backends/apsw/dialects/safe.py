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
        adapter_args: Optional[Dict[str, Any]] = None,
        *args: Any,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)
        self._adapters = adapters
        self._adapter_args = adapter_args
        self._safe = True

    def create_connect_args(
        self,
        url: URL,
    ) -> Tuple[
        Tuple[str, Optional[List[str]], Optional[Dict[str, Any]], bool, Optional[str]],
        Dict[str, Any],
    ]:
        return (
            (
                ":memory:",
                self._adapters,
                self._adapter_args,
                True,
                self.isolation_level,
            ),
            {},
        )
