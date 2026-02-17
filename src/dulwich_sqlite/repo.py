"""SQLite-backed repository for Dulwich."""

import sqlite3
import sys
from io import BytesIO

from dulwich.errors import NoIndexPresent, NotGitRepository
from dulwich.repo import BaseRepo

from ._schema import apply_pragmas, init_db
from .object_store import SqliteObjectStore
from .refs import SqliteRefsContainer


class SqliteRepo(BaseRepo):
    """Git repository backed by a SQLite database.

    Always bare: no working tree or index.
    """

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._conn = sqlite3.connect(db_path)
        apply_pragmas(self._conn)
        self._verify_schema()
        object_store = SqliteObjectStore(self._conn)
        refs_container = SqliteRefsContainer(self._conn)
        super().__init__(object_store, refs_container)
        self.bare = True
        self._load_config()

    def _verify_schema(self) -> None:
        """Check that the database has been initialized with our schema."""
        try:
            row = self._conn.execute(
                "SELECT value FROM metadata WHERE key = 'schema_version'"
            ).fetchone()
        except sqlite3.OperationalError:
            self._conn.close()
            raise NotGitRepository(
                f"Not a dulwich-sqlite repository: {self._db_path}"
            )
        if row is None:
            self._conn.close()
            raise NotGitRepository(
                f"Not a dulwich-sqlite repository: {self._db_path}"
            )

    def _load_config(self) -> None:
        from dulwich.config import ConfigFile

        config_data = self.get_named_file("config")
        if config_data is not None:
            self._config = ConfigFile.from_file(config_data)
        else:
            self._config = ConfigFile()

    @classmethod
    def init_bare(cls, db_path: str) -> "SqliteRepo":
        conn = sqlite3.connect(db_path)
        init_db(conn)
        conn.close()
        repo = cls(db_path)
        repo._init_files(bare=True)
        return repo

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
