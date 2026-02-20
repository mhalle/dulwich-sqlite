"""SQLite-backed object store for Dulwich."""

import sqlite3
import struct
import zlib
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
        row = conn.execute(
            "SELECT value FROM metadata WHERE key = 'compression'"
        ).fetchone()
        self._compression: str = row[0] if row is not None else "none"
        self._zstd_dict = None
        dict_row = conn.execute(
            "SELECT contents FROM named_files WHERE path = '_zstd_dict'"
        ).fetchone()
        if dict_row is not None:
            import zstandard

            self._zstd_dict = zstandard.ZstdCompressionDict(bytes(dict_row[0]))

    def _to_hexsha(self, sha: ObjectID | RawObjectID) -> ObjectID:
        if len(sha) == self.object_format.hex_length:
            return cast(ObjectID, sha)
        elif len(sha) == self.object_format.oid_length:
            return sha_to_hex(cast(RawObjectID, sha))
        else:
            raise ValueError(f"Invalid sha {sha!r}")

    def _compress(self, data: bytes) -> bytes:
        if self._compression == "none":
            return data
        if self._compression == "zlib":
            return zlib.compress(data)
        if self._compression == "zstd":
            import zstandard

            kwargs = {}
            if self._zstd_dict is not None:
                kwargs["dict_data"] = self._zstd_dict
            cctx = zstandard.ZstdCompressor(level=3, **kwargs)
            return cctx.compress(data)
        raise ValueError(f"Unknown compression method: {self._compression}")

    def _decompress(self, data: bytes, method: str) -> bytes:
        if method == "none":
            return data
        if method == "zlib":
            return zlib.decompress(data)
        if method == "zstd":
            import zstandard

            kwargs = {}
            if self._zstd_dict is not None:
                kwargs["dict_data"] = self._zstd_dict
            dctx = zstandard.ZstdDecompressor(**kwargs)
            return dctx.decompress(data)
        raise ValueError(f"Unknown compression method: {method}")

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
            "SELECT type_num, data, compression, chunk_refs FROM objects WHERE sha = ?",
            (hexsha.decode("ascii"),),
        ).fetchone()
        if row is None:
            raise KeyError(hexsha)
        type_num, data, compression, chunk_refs = row
        if data is not None:
            return type_num, self._decompress(bytes(data), compression)
        # Reassemble from chunks using packed rowids
        chunk_refs = bytes(chunk_refs)
        n = len(chunk_refs) // 8
        rowids = struct.unpack(f'<{n}Q', chunk_refs)
        placeholders = ','.join('?' * n)
        chunk_rows = self._conn.execute(
            f"SELECT rowid, data, compression FROM chunks WHERE rowid IN ({placeholders})",
            rowids,
        ).fetchall()
        by_rowid = {r[0]: (r[1], r[2]) for r in chunk_rows}
        parts = [self._decompress(bytes(by_rowid[rid][0]), by_rowid[rid][1]) for rid in rowids]
        return type_num, b"".join(parts)

    def _insert_object(self, obj: ShaFile) -> None:
        """Insert a single object without committing the transaction."""
        sha_str = obj.id.decode("ascii")
        raw_data = obj.as_raw_string()

        chunks = None
        if obj.type_num == _BLOB_TYPE_NUM:
            chunks = chunk_blob(raw_data)

        if chunks is not None:
            chunk_rowids = []
            for chunk_sha, chunk_data in chunks:
                stored_data = self._compress(chunk_data)
                self._conn.execute(
                    "INSERT OR IGNORE INTO chunks (chunk_sha, data, compression) "
                    "VALUES (?, ?, ?)",
                    (chunk_sha, stored_data, self._compression),
                )
                chunk_rowid = self._conn.execute(
                    "SELECT rowid FROM chunks WHERE chunk_sha = ?",
                    (chunk_sha,),
                ).fetchone()[0]
                chunk_rowids.append(chunk_rowid)
            packed = struct.pack(f'<{len(chunk_rowids)}Q', *chunk_rowids)
            self._conn.execute(
                "INSERT OR REPLACE INTO objects (sha, type_num, data, chunk_refs, total_size, compression) "
                "VALUES (?, ?, NULL, ?, ?, 'none')",
                (sha_str, obj.type_num, packed, len(raw_data)),
            )
        else:
            # Inline storage
            stored_data = self._compress(raw_data)
            self._conn.execute(
                "INSERT OR REPLACE INTO objects (sha, type_num, data, chunk_refs, total_size, compression) "
                "VALUES (?, ?, ?, NULL, ?, ?)",
                (sha_str, obj.type_num, stored_data, len(raw_data), self._compression),
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

    def search_content(
        self,
        query: str,
        *,
        limit: int | None = None,
    ) -> list[ObjectID]:
        """Search blob content for matching objects via substring match.

        Args:
            query: Substring to search for in blob content.
            limit: Maximum number of results to return.
        """
        results: set[str] = set()
        query_bytes = query.encode("utf-8", errors="surrogateescape")

        # 1. SQL LIKE on uncompressed inline blobs
        for row in self._conn.execute(
            "SELECT sha FROM objects "
            "WHERE data IS NOT NULL AND type_num = 3 AND compression = 'none' "
            "AND CAST(data AS TEXT) LIKE ?",
            (f"%{query}%",),
        ).fetchall():
            results.add(row[0])

        # 2. Python-side search on compressed inline blobs
        for row in self._conn.execute(
            "SELECT sha, data, compression FROM objects "
            "WHERE data IS NOT NULL AND type_num = 3 AND compression != 'none'"
        ).fetchall():
            if row[0] not in results:
                if query_bytes in self._decompress(bytes(row[1]), row[2]):
                    results.add(row[0])

        # 3. Find matching chunk rowids (uncompressed via SQL, compressed via Python)
        matching_chunk_rowids: set[int] = set()
        for row in self._conn.execute(
            "SELECT rowid FROM chunks "
            "WHERE compression = 'none' AND CAST(data AS TEXT) LIKE ?",
            (f"%{query}%",),
        ).fetchall():
            matching_chunk_rowids.add(row[0])

        for row in self._conn.execute(
            "SELECT rowid, data, compression FROM chunks WHERE compression != 'none'"
        ).fetchall():
            if query_bytes in self._decompress(bytes(row[1]), row[2]):
                matching_chunk_rowids.add(row[0])

        # 4. Scan chunked objects' chunk_refs blobs for matching rowids
        if matching_chunk_rowids:
            for row in self._conn.execute(
                "SELECT sha, chunk_refs FROM objects "
                "WHERE chunk_refs IS NOT NULL AND type_num = 3"
            ).fetchall():
                if row[0] not in results:
                    refs_blob = bytes(row[1])
                    n = len(refs_blob) // 8
                    rowids = set(struct.unpack(f'<{n}Q', refs_blob))
                    if rowids & matching_chunk_rowids:
                        results.add(row[0])

        out = list(results)
        if limit is not None:
            out = out[: int(limit)]
        return [r.encode("ascii") for r in out]
