"""SQLite-backed object store for Dulwich."""

import sqlite3
from collections.abc import Callable, Iterable, Iterator
from typing import BinaryIO, cast

from dulwich.object_store import PackCapableObjectStore
from dulwich.objects import ObjectID, RawObjectID, ShaFile, sha_to_hex
from dulwich.pack import (
    Pack,
    PackData,
    PackInflater,
    PackStreamCopier,
    UnpackedObject,
    write_pack_data,
)

from ._chunking import chunk_blob

PACK_SPOOL_FILE_MAX_SIZE = 200 * 1024 * 1024
_BLOB_TYPE_NUM = 3


class SqliteObjectStore(PackCapableObjectStore):
    """Object store backed by a SQLite database."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        super().__init__()
        self._conn = conn
        self.pack_compression_level = -1

    def _to_hexsha(self, sha: ObjectID | RawObjectID) -> ObjectID:
        if len(sha) == self.object_format.hex_length:
            return cast(ObjectID, sha)
        elif len(sha) == self.object_format.oid_length:
            return sha_to_hex(cast(RawObjectID, sha))
        else:
            raise ValueError(f"Invalid sha {sha!r}")

    def contains_loose(self, sha: ObjectID | RawObjectID) -> bool:
        hexsha = self._to_hexsha(sha)
        row = self._conn.execute(
            "SELECT 1 FROM objects WHERE sha = ?",
            (hexsha.decode("ascii"),),
        ).fetchone()
        return row is not None

    def contains_packed(self, sha: ObjectID | RawObjectID) -> bool:
        return False

    def __iter__(self) -> Iterator[ObjectID]:
        rows = self._conn.execute("SELECT sha FROM objects").fetchall()
        for (sha_text,) in rows:
            yield sha_text.encode("ascii")

    @property
    def packs(self) -> list[Pack]:
        return []

    def get_object_size(self, sha: ObjectID | RawObjectID) -> int:
        hexsha = self._to_hexsha(sha)
        row = self._conn.execute(
            "SELECT size_bytes FROM objects WHERE sha = ?",
            (hexsha.decode("ascii"),),
        ).fetchone()
        if row is None:
            raise KeyError(hexsha)
        return row[0]

    def get_raw(self, name: RawObjectID | ObjectID) -> tuple[int, bytes]:
        hexsha = self._to_hexsha(name)
        row = self._conn.execute(
            "SELECT type_num, data FROM objects WHERE sha = ?",
            (hexsha.decode("ascii"),),
        ).fetchone()
        if row is None:
            raise KeyError(hexsha)
        type_num, data = row
        if data is not None:
            return type_num, bytes(data)
        # Reassemble from chunks
        sha_str = hexsha.decode("ascii")
        chunk_rows = self._conn.execute(
            "SELECT c.data FROM object_chunks oc "
            "JOIN chunks c ON oc.chunk_sha = c.chunk_sha "
            "WHERE oc.object_sha = ? ORDER BY oc.chunk_index",
            (sha_str,),
        ).fetchall()
        return type_num, b"".join(bytes(r[0]) for r in chunk_rows)

    def _insert_object(self, obj: ShaFile) -> None:
        """Insert a single object without committing the transaction."""
        sha_str = obj.id.decode("ascii")
        raw_data = obj.as_raw_string()

        chunks = None
        if obj.type_num == _BLOB_TYPE_NUM:
            chunks = chunk_blob(raw_data)

        if chunks is not None:
            # Chunked storage: clear any existing chunks for REPLACE semantics
            self._conn.execute(
                "DELETE FROM object_chunks WHERE object_sha = ?",
                (sha_str,),
            )
            self._conn.execute(
                "INSERT OR REPLACE INTO objects (sha, type_num, data, total_size) "
                "VALUES (?, ?, NULL, ?)",
                (sha_str, obj.type_num, len(raw_data)),
            )
            for idx, (chunk_sha, chunk_data) in enumerate(chunks):
                self._conn.execute(
                    "INSERT OR IGNORE INTO chunks (chunk_sha, data) VALUES (?, ?)",
                    (chunk_sha, chunk_data),
                )
                self._conn.execute(
                    "INSERT INTO object_chunks (object_sha, chunk_index, chunk_sha) "
                    "VALUES (?, ?, ?)",
                    (sha_str, idx, chunk_sha),
                )
        else:
            # Inline storage
            self._conn.execute(
                "INSERT OR REPLACE INTO objects (sha, type_num, data) VALUES (?, ?, ?)",
                (sha_str, obj.type_num, raw_data),
            )

    def add_object(self, obj: ShaFile) -> None:
        self._insert_object(obj)
        self._conn.commit()

    def add_objects(
        self,
        objects: Iterable[tuple[ShaFile, str | None]],
        progress: Callable[[str], None] | None = None,
    ) -> None:
        with self._conn:
            for obj, path in objects:
                self._insert_object(obj)

    def add_pack(self) -> tuple[BinaryIO, Callable[[], None], Callable[[], None]]:
        from tempfile import SpooledTemporaryFile

        f = SpooledTemporaryFile(max_size=PACK_SPOOL_FILE_MAX_SIZE, prefix="incoming-")

        def commit() -> None:
            size = f.tell()
            if size > 0:
                f.seek(0)
                p = PackData.from_file(f, self.object_format, size)
                with self._conn:
                    for obj in PackInflater.for_pack_data(p, self.get_raw):
                        self._insert_object(obj)
                p.close()
                f.close()
            else:
                f.close()

        def abort() -> None:
            f.close()

        return f, commit, abort  # type: ignore[return-value]

    def add_pack_data(
        self,
        count: int,
        unpacked_objects: Iterator[UnpackedObject],
        progress: Callable[[str], None] | None = None,
    ) -> None:
        if count == 0:
            return
        f, commit, abort = self.add_pack()
        try:
            write_pack_data(
                f.write,
                unpacked_objects,
                num_records=count,
                progress=progress,
                object_format=self.object_format,
            )
        except BaseException:
            abort()
            raise
        else:
            commit()

    def add_thin_pack(
        self,
        read_all: Callable[[int], bytes],
        read_some: Callable[[int], bytes] | None,
        progress: Callable[[str], None] | None = None,
    ) -> None:
        f, commit, abort = self.add_pack()
        try:
            copier = PackStreamCopier(
                self.object_format.hash_func,
                read_all,
                read_some,
                f,
            )
            copier.verify()
        except BaseException:
            abort()
            raise
        else:
            commit()
