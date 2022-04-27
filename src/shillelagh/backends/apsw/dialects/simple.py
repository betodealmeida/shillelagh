"""
A simple database.

This offers a SQLAlchemy dialect ``simple://`` that can't be configured. The location of
the file is determined externally, and its size is limited. This is to be used in cases
where you don't trust the user and don't want to offer unlimited access to the
filesystem.
"""
# pylint: disable=abstract-method, c-extension-no-member, invalid-name

import os
import tempfile
from typing import Any, Dict, List, Optional, Tuple, Union

import apsw
from sqlalchemy.engine.url import URL

from shillelagh.backends.apsw.dialects.base import APSWDialect


class SimpleVFSWithQuota(apsw.VFS):  # pylint: disable=too-few-public-methods
    """
    A VFS with a quota.
    """

    def __init__(self, quota: int, vfsname: str = "simple", basevfs: str = ""):
        super().__init__(vfsname, basevfs)

        self.quota = quota
        self.vfsname = vfsname
        self.basevfs = basevfs

    def xOpen(
        self,
        name: Optional[Union[str, apsw.URIFilename]],
        flags: List[int],
    ) -> apsw.VFSFile:
        """
        Return a new file object based on name.
        """
        return SimpleVFSFile(self.quota, self.basevfs, name, flags)


class SimpleVFSFile(apsw.VFSFile):  # pylint: disable=too-few-public-methods
    """
    A simple VFSFile that enforces a quota.
    """

    def __init__(
        self,
        quota: int,
        vfs: str,
        filename: Union[str, apsw.URIFilename],
        flags: List[int],
    ):
        super().__init__(vfs, filename, flags)

        self.quota = quota

    def xWrite(self, data, offset):
        """
        Write the data starting at absolute offset.
        """
        if self.xFileSize() > self.quota * 1e6:
            raise apsw.FullError("Reached quota!")
        return super().xWrite(data, offset)


class APSWSimpleDialect(APSWDialect):

    """
    A simple Shillelagh dialect.
    """

    # This is supported in ``SQLiteDialect``, and equally supported here. See
    # https://docs.sqlalchemy.org/en/14/core/connections.html#caching-for-third-party-dialects
    # for more context.
    supports_statement_cache = True

    def __init__(self, **kwargs: Any):
        super().__init__(safe=True, **kwargs)

        quota = int(os.environ.get("SIMPLE_DB_QUOTA_MB", "1000"))
        self.vfs = SimpleVFSWithQuota(quota)

    def create_connect_args(
        self,
        url: URL,
    ) -> Tuple[Tuple[()], Dict[str, Any]]:
        path = os.environ.get(
            "SIMPLE_DB_PATH",
            os.path.join(tempfile.mkdtemp(), "simple.db"),
        )

        return (), {
            "path": path,
            "adapters": None,
            "adapter_kwargs": {},
            "safe": True,
            "isolation_level": self.isolation_level,
            "apsw_connection_kwargs": {"vfs": self.vfs.vfsname},
        }
