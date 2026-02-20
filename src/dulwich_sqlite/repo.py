"""SQLite-backed repository for Dulwich."""

import sqlite3
import sys
import time
from collections.abc import Generator
from io import BytesIO

from dulwich import porcelain, reflog
from dulwich.errors import NoIndexPresent, NotGitRepository
from dulwich.repo import BaseRepo

from ._schema import (
    SCHEMA_VERSION,
    apply_pragmas,
    init_db,
    migrate_v3_to_v4,
    migrate_v4_to_v5,
    migrate_v5_to_v6,
    migrate_v6_to_v7,
    migrate_v7_to_v8,
    migrate_v8_to_v9,
    migrate_v9_to_v10,
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
        if version == "5":
            migrate_v5_to_v6(self._conn)
            version = "6"
        if version == "6":
            migrate_v6_to_v7(self._conn)
            version = "7"
        if version == "7":
            migrate_v7_to_v8(self._conn)
            version = "8"
        if version == "8":
            migrate_v8_to_v9(self._conn)
            version = "9"
        if version == "9":
            migrate_v9_to_v10(self._conn)
            version = "10"
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
    def init_bare(cls, db_path: str, *, compress: bool | str = False) -> "SqliteRepo":
        conn = sqlite3.connect(db_path)
        init_db(conn)
        if compress:
            method = compress if isinstance(compress, str) else "zstd"
            conn.execute(
                "UPDATE metadata SET value = ? WHERE key = 'compression'",
                (method,),
            )
            conn.commit()
        conn.close()
        repo = cls(db_path)
        repo._init_files(bare=True)
        return repo

    def enable_compression(self, method: str = "zlib") -> None:
        if method not in ("zlib", "zstd"):
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

    def _save_config(self) -> None:
        """Persist the in-memory config to the database."""
        buf = BytesIO()
        self._config.write_to_file(buf)
        self._put_named_file("config", buf.getvalue())

    @classmethod
    def clone_from(
        cls,
        source: str,
        db_path: str,
        *,
        origin: str = "origin",
        compress: bool | str = False,
        depth: int | None = None,
        branch: str | bytes | None = None,
        errstream: "BinaryIO | None" = None,
    ) -> "SqliteRepo":
        """Clone a remote repository into a new SQLite database.

        Sets up remote tracking config just like ``git clone --bare``, then
        fetches objects and refs via :func:`dulwich.porcelain.fetch`.

        Args:
            source: URL or local path of the source repository.
            db_path: Path for the new SQLite database file.
            origin: Name for the remote (default ``"origin"``).
            compress: Enable compression. ``True`` uses zstd, or pass
                ``"zlib"``/``"zstd"`` explicitly.
            depth: Create a shallow clone with this many commits.
            branch: Branch to checkout as HEAD (default: remote HEAD).
            errstream: Optional stream for progress output.
        """
        repo = cls.init_bare(db_path, compress=compress)
        try:
            # Configure the remote — same as dulwich's client.clone()
            config = repo.get_config()
            origin_b = origin.encode()
            section = (b"remote", origin_b)
            config.set(section, b"url", source.encode())
            config.set(
                section, b"fetch",
                b"+refs/heads/*:refs/remotes/" + origin_b + b"/*",
            )
            repo._save_config()

            # Fetch objects; porcelain.fetch resolves "origin" from config,
            # calls _import_remote_refs to populate refs/remotes/origin/*.
            kwargs: dict = {"depth": depth}
            if errstream is not None:
                kwargs["errstream"] = errstream
            result = porcelain.fetch(repo, origin, **kwargs)

            # Determine the default branch from the remote.
            # Mirrors the logic in dulwich's client.clone().
            target_ref = None
            if branch is not None:
                if isinstance(branch, str):
                    branch = branch.encode()
                if not branch.startswith(b"refs/"):
                    branch = b"refs/heads/" + branch
                target_ref = branch
            elif result.symrefs and b"HEAD" in result.symrefs:
                symref_target = result.symrefs[b"HEAD"]
                if symref_target in result.refs:
                    target_ref = symref_target

            # Fall back: find a branch whose SHA matches remote HEAD
            if target_ref is None:
                head_sha = result.refs.get(b"HEAD")
                if head_sha is not None:
                    for ref_name, sha in result.refs.items():
                        if (
                            ref_name.startswith(b"refs/heads/")
                            and sha == head_sha
                        ):
                            target_ref = ref_name
                            break

            # Create a local branch tracking the remote and point HEAD at it
            if target_ref is not None and target_ref in result.refs:
                repo.refs[target_ref] = result.refs[target_ref]
                repo.refs.set_symbolic_ref(b"HEAD", target_ref)

            # Train zstd dictionary from the freshly fetched data
            if repo.object_store._compression == "zstd":
                repo.train_dictionary()
        except BaseException:
            repo.close()
            raise
        return repo

    def fetch(
        self,
        remote_location: str = "origin",
        *,
        depth: int | None = None,
        errstream: "BinaryIO | None" = None,
    ) -> "FetchPackResult":
        """Fetch objects and refs from a remote repository.

        Thin wrapper around :func:`dulwich.porcelain.fetch`.  When
        *remote_location* is a configured remote name (e.g. ``"origin"``),
        remote tracking refs under ``refs/remotes/<name>/`` are updated
        automatically by dulwich.

        Args:
            remote_location: Remote name or URL.  Defaults to ``"origin"``.
            depth: Fetch only this many commits of history.
            errstream: Optional stream for progress output.

        Returns:
            :class:`~dulwich.client.FetchPackResult` with remote refs
            and symrefs.
        """
        kwargs: dict = {"depth": depth}
        if errstream is not None:
            kwargs["errstream"] = errstream
        return porcelain.fetch(self, remote_location, **kwargs)

    def push(
        self,
        remote_location: str | None = None,
        refspecs: str | bytes | list[str | bytes] | None = None,
        *,
        errstream: "BinaryIO | None" = None,
    ) -> None:
        """Push refs and objects to a remote repository.

        Thin wrapper around :func:`dulwich.porcelain.push`.

        Args:
            remote_location: Remote name or URL.  Defaults to the
                tracking remote of the current branch (usually ``"origin"``).
            refspecs: Refspec(s) to push (e.g. ``"refs/heads/main"``).
                Defaults to the current active branch.
            errstream: Optional stream for progress output.
        """
        kwargs: dict = {}
        if refspecs is not None:
            kwargs["refspecs"] = refspecs
        if errstream is not None:
            kwargs["errstream"] = errstream
        porcelain.push(self, remote_location, **kwargs)

    def train_dictionary(self, dict_size: int = 32768) -> None:
        """Train type-specific zstd compression dictionaries.

        Trains separate dictionaries for commits, trees, and chunks, then
        re-compresses all existing zstd data with the appropriate dictionary.
        Stores dictionaries in named_files (``_zstd_dict_commit``,
        ``_zstd_dict_tree``, ``_zstd_dict_chunk``) and loads them into the
        object store for immediate use.

        Args:
            dict_size: Size of each trained dictionary in bytes (default 32 KB).
        """
        import zstandard

        from .object_store import _TYPE_TO_DICT_KEY

        # 1. Sample by type
        commit_samples: list[bytes] = []
        tree_samples: list[bytes] = []
        for row in self._conn.execute(
            "SELECT type_num, data, compression FROM objects "
            "WHERE data IS NOT NULL LIMIT 15000"
        ):
            raw = self.object_store._decompress(bytes(row[1]), row[2])
            if row[0] == 1:
                commit_samples.append(raw)
            elif row[0] == 2:
                tree_samples.append(raw)

        chunk_samples: list[bytes] = []
        for row in self._conn.execute(
            "SELECT data, compression FROM chunks LIMIT 10000"
        ):
            chunk_samples.append(self.object_store._decompress(bytes(row[0]), row[1]))

        # 2. Train type-specific dicts (min 10 samples each)
        new_dicts: dict[str, zstandard.ZstdCompressionDict] = {}
        for key, samples in [('commit', commit_samples), ('tree', tree_samples),
                              ('chunk', chunk_samples)]:
            if len(samples) >= 10:
                d = zstandard.train_dictionary(dict_size, samples)
                new_dicts[key] = d

        if not new_dicts:
            return  # not enough data

        # 3. Store new dicts
        named_file_keys = {'commit': '_zstd_dict_commit', 'tree': '_zstd_dict_tree',
                           'chunk': '_zstd_dict_chunk'}
        for key, d in new_dicts.items():
            self._put_named_file(named_file_keys[key], d.as_bytes())

        # 4. Load into object store (keep old dicts in by_id map for decompression during re-compress)
        for key, d in new_dicts.items():
            zdict = zstandard.ZstdCompressionDict(d.as_bytes())
            zdict.precompute_compress(level=3)
            self.object_store._zstd_dicts[key] = zdict
            self.object_store._zstd_dicts_by_id[zdict.dict_id()] = zdict

        # 5. Re-compress all zstd data with type-specific dicts
        with self._conn:
            # Inline objects
            for row in self._conn.execute(
                "SELECT sha, type_num, data, compression FROM objects "
                "WHERE data IS NOT NULL AND compression = 'zstd'"
            ).fetchall():
                sha, type_num, old_data, comp = row
                raw = self.object_store._decompress(bytes(old_data), comp)
                dict_key = _TYPE_TO_DICT_KEY.get(type_num)
                new_data = self.object_store._compress(raw, dict_key=dict_key)
                self._conn.execute("UPDATE objects SET data = ? WHERE sha = ?",
                                   (new_data, bytes(sha)))
            # Chunks
            for row in self._conn.execute(
                "SELECT rowid, data, compression FROM chunks WHERE compression = 'zstd'"
            ).fetchall():
                rowid, old_data, comp = row
                raw = self.object_store._decompress(bytes(old_data), comp)
                new_data = self.object_store._compress(raw, dict_key='chunk')
                self._conn.execute("UPDATE chunks SET data = ? WHERE rowid = ?",
                                   (new_data, rowid))

        # 6. Remove legacy single dict
        self._conn.execute("DELETE FROM named_files WHERE path = '_zstd_dict'")
        self._conn.commit()
        self.object_store._zstd_dicts.pop('legacy', None)

        # 7. Reclaim freed pages from re-compression
        self._conn.execute("VACUUM")

    def close(self) -> None:
        self.object_store.close()
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
