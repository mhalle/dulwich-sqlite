"""SQLite-backed repository for Dulwich."""

import sqlite3
import sys
import time
from collections.abc import Generator
from io import BytesIO

from dulwich import reflog
from dulwich.errors import NoIndexPresent, NotGitRepository
from dulwich.repo import BaseRepo

from ._schema import (
    SCHEMA_VERSION,
    apply_pragmas,
    init_db,
    migrate_v3_to_v4,
    migrate_v4_to_v5,
)
from .object_store import SqliteObjectStore
from .refs import SqliteRefsContainer


class SqliteRepo(BaseRepo):
    """Git repository backed by a SQLite database.

    Always bare: no working tree or index.
    """

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self.path = db_path
        self._conn = sqlite3.connect(db_path)
        try:
            apply_pragmas(self._conn)
            self._verify_schema()
        except NotGitRepository:
            self._conn.close()
            raise
        except sqlite3.DatabaseError:
            self._conn.close()
            raise NotGitRepository(
                f"Not a dulwich-sqlite repository: {self._db_path}"
            )
        object_store = SqliteObjectStore(self._conn)
        refs_container = SqliteRefsContainer(self._conn, logger=self._write_reflog)
        super().__init__(object_store, refs_container)
        self.bare = True
        self._load_config()

    def _verify_schema(self) -> None:
        """Check that the database has been initialized with our schema.

        Automatically migrates v3 and v4 databases to the current version.
        Raises NotGitRepository for missing/invalid schemas or unsupported versions.
        """
        try:
            row = self._conn.execute(
                "SELECT value FROM metadata WHERE key = 'schema_version'"
            ).fetchone()
        except sqlite3.OperationalError:
            raise NotGitRepository(
                f"Not a dulwich-sqlite repository: {self._db_path}"
            )
        if row is None:
            raise NotGitRepository(
                f"Not a dulwich-sqlite repository: {self._db_path}"
            )
        version = row[0]
        if version == "3":
            migrate_v3_to_v4(self._conn)
            version = "4"
        if version == "4":
            migrate_v4_to_v5(self._conn)
            version = "5"
        if version != SCHEMA_VERSION:
            raise NotGitRepository(
                f"Unsupported schema version {version} "
                f"(expected {SCHEMA_VERSION}): {self._db_path}"
            )

    def _write_reflog(
        self,
        ref: bytes,
        old_sha: bytes,
        new_sha: bytes,
        committer: bytes | None,
        timestamp: int | None,
        timezone: int | None,
        message: bytes,
    ) -> None:
        if committer is None:
            committer = b"dulwich-sqlite <dulwich-sqlite@localhost>"
        if timestamp is None:
            timestamp = int(time.time())
        if timezone is None:
            timezone = 0
        self._conn.execute(
            "INSERT INTO reflog (ref_name, old_sha, new_sha, committer, timestamp, timezone, message) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (ref, old_sha, new_sha, committer, timestamp, timezone, message),
        )
        self._conn.commit()

    def read_reflog(self, ref: bytes) -> Generator[reflog.Entry, None, None]:
        rows = self._conn.execute(
            "SELECT old_sha, new_sha, committer, timestamp, timezone, message "
            "FROM reflog WHERE ref_name = ? ORDER BY id ASC",
            (ref,),
        ).fetchall()
        for row in rows:
            yield reflog.Entry(
                bytes(row[0]), bytes(row[1]), bytes(row[2]),
                row[3], row[4], bytes(row[5]),
            )

    def _load_config(self) -> None:
        from dulwich.config import ConfigFile

        config_data = self.get_named_file("config")
        if config_data is not None:
            self._config = ConfigFile.from_file(config_data)
        else:
            self._config = ConfigFile()

    @classmethod
    def init_bare(cls, db_path: str, *, compress: bool = False) -> "SqliteRepo":
        conn = sqlite3.connect(db_path)
        init_db(conn)
        if compress:
            conn.execute(
                "UPDATE metadata SET value = 'zlib' WHERE key = 'compression'"
            )
            conn.commit()
        conn.close()
        repo = cls(db_path)
        repo._init_files(bare=True)
        return repo

    def enable_compression(self, method: str = "zlib") -> None:
        if method not in ("zlib",):
            raise ValueError(f"Unsupported compression method: {method}")
        self._conn.execute(
            "UPDATE metadata SET value = ? WHERE key = 'compression'",
            (method,),
        )
        self._conn.commit()
        self.object_store._compression = method

    def disable_compression(self) -> None:
        self._conn.execute(
            "UPDATE metadata SET value = 'none' WHERE key = 'compression'"
        )
        self._conn.commit()
        self.object_store._compression = "none"

    def get_named_file(
        self,
        path: str | bytes,
        basedir: str | None = None,
    ) -> BytesIO | None:
        path_str = path.decode() if isinstance(path, bytes) else path
        row = self._conn.execute(
            "SELECT contents FROM named_files WHERE path = ?",
            (path_str,),
        ).fetchone()
        if row is None:
            return None
        return BytesIO(bytes(row[0]))

    def _put_named_file(self, path: str, contents: bytes) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO named_files (path, contents) VALUES (?, ?)",
            (path, contents),
        )
        self._conn.commit()

    def _del_named_file(self, path: str) -> None:
        self._conn.execute(
            "DELETE FROM named_files WHERE path = ?", (path,)
        )
        self._conn.commit()

    def _init_config(self, config: "ConfigFile") -> None:
        from dulwich.config import ConfigFile

        self._config = config

    def get_config(self) -> "ConfigFile":
        return self._config

    def get_description(self) -> bytes | None:
        f = self.get_named_file("description")
        if f is None:
            return None
        return f.read()

    def set_description(self, description: bytes) -> None:
        self._put_named_file("description", description)

    def open_index(self):
        raise NoIndexPresent

    def _determine_file_mode(self) -> bool:
        return sys.platform != "win32"

    def close(self) -> None:
        self.object_store.close()
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
