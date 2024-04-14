"""
Tests for shillelagh.backends.apsw.dialects.safe.
"""

from sqlalchemy.engine.url import make_url

from shillelagh.backends.apsw.dialects.safe import APSWSafeDialect


def test_safe_dialect() -> None:
    """
    Test that ``shillelagh+safe://`` forces safe mode.
    """
    dialect = APSWSafeDialect()
    assert dialect.create_connect_args(make_url("shillelagh+safe://")) == (
        (),
        {
            "path": ":memory:",
            "adapters": None,
            "adapter_kwargs": {},
            "safe": True,
            "isolation_level": None,
        },
    )
