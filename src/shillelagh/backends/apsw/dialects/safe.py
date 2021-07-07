# pylint: disable=abstract-method
"""
A "safe" Shillelagh dialect.

When this dialect is used only adapters marked as safe and explicitly
listed are loaded.
"""
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from sqlalchemy.engine.url import URL

from shillelagh.backends.apsw.dialects.base import APSWDialect


class APSWSafeDialect(APSWDialect):

    """
    A "safe" Shillelagh dialect.

    This dialect can be used with the `shillelagh+safe://` URI:

        >>> from sqlalchemy.engine import create_engine
        >>> engine = create_engine("shillelagh+safe://", adapters=["socrata"])

    The dialect only loads the adapters explicitly listed ("socrata", in the
    example above), and only if they're marked as safe.

    """

    def __init__(
        self,
        adapters: Optional[List[str]] = None,
        adapter_kwargs: Optional[Dict[str, Dict[str, Any]]] = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
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
