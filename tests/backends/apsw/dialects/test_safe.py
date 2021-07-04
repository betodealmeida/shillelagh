from sqlalchemy.engine.url import make_url

from shillelagh.backends.apsw.dialects.safe import APSWSafeDialect


def test_safe_dialect(fs):
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
