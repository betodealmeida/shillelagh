# pylint: disable=abstract-method
"""
A "safe" Shillelagh dialect.

When this dialect is used only adapters marked as safe and explicitly
listed are loaded.
"""

from typing import Any, Dict, List, Optional, Tuple

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

    # This is supported in ``SQLiteDialect``, and equally supported here. See
    # https://docs.sqlalchemy.org/en/14/core/connections.html#caching-for-third-party-dialects
    # for more context.
    supports_statement_cache = True

    def __init__(
        self,
        adapters: Optional[List[str]] = None,
        adapter_kwargs: Optional[Dict[str, Dict[str, Any]]] = None,
        **kwargs: Any,
    ):
        super().__init__(adapters, adapter_kwargs, safe=True, **kwargs)

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
