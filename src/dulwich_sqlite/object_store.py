"""SQLite-backed object store for Dulwich."""

import sqlite3
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
_TYPE_TO_DICT_KEY = {1: 'commit', 2: 'tree'}


def _encode_unsigned_varint(value: int) -> bytes:
    """Encode unsigned int as LEB128 varint."""
    parts = []
    while value > 0x7F:
        parts.append((value & 0x7F) | 0x80)
        value >>= 7
    parts.append(value & 0x7F)
    return bytes(parts)


def _decode_unsigned_varint(data: bytes, offset: int) -> tuple[int, int]:
    """Decode LEB128 varint, return (value, new_offset)."""
    value = shift = 0
    while True:
        b = data[offset]
        value |= (b & 0x7F) << shift
        offset += 1
        if not (b & 0x80):
            break
        shift += 7
    return value, offset


def pack_chunk_refs(rowids: list[int]) -> bytes:
    """Pack ordered chunk rowids as delta-zigzag-varint blob."""
    if not rowids:
        return b""
    parts = [_encode_unsigned_varint(rowids[0])]
    prev = rowids[0]
    for rid in rowids[1:]:
        delta = rid - prev
        zigzag = (delta << 1) ^ (delta >> 63)
        parts.append(_encode_unsigned_varint(zigzag))
        prev = rid
    return b"".join(parts)


def unpack_chunk_refs(data: bytes) -> list[int]:
    """Unpack delta-zigzag-varint blob into ordered chunk rowids."""
    if not data:
        return []
    offset = 0
    first, offset = _decode_unsigned_varint(data, offset)
    rowids = [first]
    prev = first
    while offset < len(data):
        zigzag, offset = _decode_unsigned_varint(data, offset)
        delta = (zigzag >> 1) ^ -(zigzag & 1)
        prev += delta
        rowids.append(prev)
    return rowids


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
        self._zstd_dicts: dict[str, "zstandard.ZstdCompressionDict"] = {}
        self._zstd_dicts_by_id: dict[int, "zstandard.ZstdCompressionDict"] = {}
        for key, path in [('commit', '_zstd_dict_commit'), ('tree', '_zstd_dict_tree'),
                          ('chunk', '_zstd_dict_chunk'), ('legacy', '_zstd_dict')]:
            dict_row = conn.execute(
                "SELECT contents FROM named_files WHERE path = ?", (path,)
            ).fetchone()
            if dict_row is not None:
                import zstandard

                d = zstandard.ZstdCompressionDict(bytes(dict_row[0]))
                d.precompute_compress(level=3)
                self._zstd_dicts[key] = d
                self._zstd_dicts_by_id[d.dict_id()] = d

    def _to_hexsha(self, sha: ObjectID | RawObjectID) -> ObjectID:
        if len(sha) == self.object_format.hex_length:
            return cast(ObjectID, sha)
        elif len(sha) == self.object_format.oid_length:
            return sha_to_hex(cast(RawObjectID, sha))
        else:
            raise ValueError(f"Invalid sha {sha!r}")

    def _to_dbsha(self, sha: ObjectID | RawObjectID) -> bytes:
        """Convert Dulwich ObjectID/RawObjectID to 20-byte binary for DB lookup."""
        if len(sha) == self.object_format.oid_length:  # 20 bytes raw
            return bytes(sha)
        hexsha = self._to_hexsha(sha)
        return bytes.fromhex(hexsha.decode("ascii"))

    def _compress(self, data: bytes, dict_key: str | None = None) -> bytes:
        if self._compression == "none":
            return data
        if self._compression == "zlib":
            return zlib.compress(data)
        if self._compression == "zstd":
            import zstandard

            kwargs = {}
            zdict = self._zstd_dicts.get(dict_key) if dict_key else None
            if zdict is not None:
                kwargs["dict_data"] = zdict
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

            params = zstandard.get_frame_parameters(data)
            dict_data = self._zstd_dicts_by_id.get(params.dict_id)
            if dict_data is not None:
                dctx = zstandard.ZstdDecompressor(dict_data=dict_data)
            else:
                dctx = zstandard.ZstdDecompressor()
            return dctx.decompress(data)
        raise ValueError(f"Unknown compression method: {method}")

    def contains_loose(self, sha: ObjectID | RawObjectID) -> bool:
        dbsha = self._to_dbsha(sha)
        row = self._conn.execute(
            "SELECT 1 FROM objects WHERE sha = ?",
            (dbsha,),
        ).fetchone()
        return row is not None

    def contains_packed(self, sha: ObjectID | RawObjectID) -> bool:
        return False

    def __iter__(self) -> Iterator[ObjectID]:
        rows = self._conn.execute("SELECT sha FROM objects").fetchall()
        for (sha_bytes,) in rows:
            yield bytes(sha_bytes).hex().encode("ascii")

    @property
    def packs(self) -> list[Pack]:
        return []

    def get_object_size(self, sha: ObjectID | RawObjectID) -> int:
        dbsha = self._to_dbsha(sha)
        row = self._conn.execute(
            "SELECT size_bytes FROM objects WHERE sha = ?",
            (dbsha,),
        ).fetchone()
        if row is None:
            raise KeyError(self._to_hexsha(sha))
        return row[0]

    def get_raw(self, name: RawObjectID | ObjectID) -> tuple[int, bytes]:
        dbsha = self._to_dbsha(name)
        row = self._conn.execute(
            "SELECT type_num, data, compression, chunk_refs FROM objects WHERE sha = ?",
            (dbsha,),
        ).fetchone()
        if row is None:
            raise KeyError(self._to_hexsha(name))
        type_num, data, compression, chunk_refs = row
        if data is not None:
            return type_num, self._decompress(bytes(data), compression)
        # Reassemble from chunks using delta-varint packed rowids
        rowids = unpack_chunk_refs(bytes(chunk_refs))
        n = len(rowids)
        placeholders = ','.join('?' * n)
        chunk_rows = self._conn.execute(
            f"SELECT rowid, data, compression FROM chunks WHERE rowid IN ({placeholders})",
            rowids,
        ).fetchall()
        by_rowid = {r[0]: (r[1], r[2]) for r in chunk_rows}
        parts = [self._decompress(bytes(by_rowid[rid][0]), by_rowid[rid][1]) for rid in rowids]
        return type_num, b"".join(parts)

    def get_raw_range(
        self,
        name: RawObjectID | ObjectID,
        offset: int,
        length: int,
    ) -> tuple[int, bytes]:
        """Return a byte range of an object's raw data.

        For chunked objects, only the chunks overlapping the requested range
        are fetched and decompressed.  For inline objects the full data is
        decompressed and sliced (they are small by definition).

        Args:
            name: Object SHA (hex or binary).
            offset: Byte offset into the raw data.
            length: Number of bytes to read.

        Returns:
            ``(type_num, data_slice)`` — same shape as ``get_raw()``.
            Clamps to object bounds: returns available data if the range
            extends past the end, or empty bytes if offset is past the end.

        Raises:
            KeyError: If the object does not exist.
        """
        dbsha = self._to_dbsha(name)
        row = self._conn.execute(
            "SELECT type_num, data, compression, chunk_refs, total_size FROM objects WHERE sha = ?",
            (dbsha,),
        ).fetchone()
        if row is None:
            raise KeyError(self._to_hexsha(name))
        type_num, data, compression, chunk_refs, total_size = row

        # Inline object — decompress full data and slice
        if data is not None:
            raw = self._decompress(bytes(data), compression)
            return type_num, raw[offset : offset + length]

        # Chunked object — use raw_size to identify overlapping chunks
        rowids = unpack_chunk_refs(bytes(chunk_refs))
        n = len(rowids)
        if n == 0 or offset >= (total_size or 0):
            return type_num, b""

        # Fetch raw_size for each chunk
        placeholders = ",".join("?" * n)
        size_rows = self._conn.execute(
            f"SELECT rowid, raw_size FROM chunks WHERE rowid IN ({placeholders})",
            rowids,
        ).fetchall()
        size_by_rowid = {r[0]: r[1] for r in size_rows}

        # Build cumulative offset array
        cumulative = [0]
        for rid in rowids:
            cumulative.append(cumulative[-1] + size_by_rowid[rid])

        # Find overlapping chunks
        end = min(offset + length, cumulative[-1])
        if offset >= end:
            return type_num, b""

        first_chunk = 0
        for i in range(n):
            if cumulative[i + 1] > offset:
                first_chunk = i
                break

        last_chunk = first_chunk
        for i in range(first_chunk, n):
            last_chunk = i
            if cumulative[i + 1] >= end:
                break

        # Fetch and decompress only the overlapping chunks
        needed_rowids = rowids[first_chunk : last_chunk + 1]
        needed_placeholders = ",".join("?" * len(needed_rowids))
        chunk_rows = self._conn.execute(
            f"SELECT rowid, data, compression FROM chunks WHERE rowid IN ({needed_placeholders})",
            needed_rowids,
        ).fetchall()
        by_rowid = {r[0]: (r[1], r[2]) for r in chunk_rows}

        parts = []
        for rid in needed_rowids:
            parts.append(self._decompress(bytes(by_rowid[rid][0]), by_rowid[rid][1]))
        assembled = b"".join(parts)

        # Slice relative to first chunk's start
        slice_start = offset - cumulative[first_chunk]
        slice_end = slice_start + (end - offset)
        return type_num, assembled[slice_start:slice_end]

    def _insert_object(self, obj: ShaFile) -> None:
        """Insert a single object without committing the transaction."""
        sha_bin = bytes.fromhex(obj.id.decode("ascii"))
        raw_data = obj.as_raw_string()

        chunks = None
        if obj.type_num == _BLOB_TYPE_NUM:
            chunks = chunk_blob(raw_data)

        if chunks is not None:
            chunk_rowids = []
            for chunk_sha_bin, chunk_data in chunks:
                stored_data = self._compress(chunk_data, dict_key='chunk')
                self._conn.execute(
                    "INSERT OR IGNORE INTO chunks (chunk_sha, data, compression, raw_size) "
                    "VALUES (?, ?, ?, ?)",
                    (chunk_sha_bin, stored_data, self._compression, len(chunk_data)),
                )
                chunk_rowid = self._conn.execute(
                    "SELECT rowid FROM chunks WHERE chunk_sha = ?",
                    (chunk_sha_bin,),
                ).fetchone()[0]
                chunk_rowids.append(chunk_rowid)
            packed = pack_chunk_refs(chunk_rowids)
            self._conn.execute(
                "INSERT OR REPLACE INTO objects (sha, type_num, data, chunk_refs, total_size, compression) "
                "VALUES (?, ?, NULL, ?, ?, 'none')",
                (sha_bin, obj.type_num, packed, len(raw_data)),
            )
        else:
            # Inline storage
            stored_data = self._compress(raw_data, dict_key=_TYPE_TO_DICT_KEY.get(obj.type_num))
            self._conn.execute(
                "INSERT OR REPLACE INTO objects (sha, type_num, data, chunk_refs, total_size, compression) "
                "VALUES (?, ?, ?, NULL, ?, ?)",
                (sha_bin, obj.type_num, stored_data, len(raw_data), self._compression),
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

    @staticmethod
    def _escape_like(s: str) -> str:
        """Escape ``%``, ``_``, and ``\\`` for use in a LIKE pattern."""
        return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

    def search_content(
        self,
        query: str,
        *,
        limit: int | None = None,
    ) -> list[ObjectID]:
        """Search blob content for matching objects via literal substring match.

        Args:
            query: Substring to search for in blob content.
            limit: Maximum number of results to return.
        """
        results: set[bytes] = set()
        query_bytes = query.encode("utf-8", errors="surrogateescape")
        escaped = self._escape_like(query)

        # 1. SQL LIKE on uncompressed inline blobs (escape LIKE wildcards)
        for row in self._conn.execute(
            "SELECT sha FROM objects "
            "WHERE data IS NOT NULL AND type_num = 3 AND compression = 'none' "
            "AND CAST(data AS TEXT) LIKE ? ESCAPE '\\'",
            (f"%{escaped}%",),
        ).fetchall():
            results.add(bytes(row[0]))

        # 2. Python-side search on compressed inline blobs
        for row in self._conn.execute(
            "SELECT sha, data, compression FROM objects "
            "WHERE data IS NOT NULL AND type_num = 3 AND compression != 'none'"
        ).fetchall():
            sha_bin = bytes(row[0])
            if sha_bin not in results:
                if query_bytes in self._decompress(bytes(row[1]), row[2]):
                    results.add(sha_bin)

        # 3. Find candidate chunk rowids (uncompressed via SQL, compressed via Python)
        candidate_chunk_rowids: set[int] = set()
        for row in self._conn.execute(
            "SELECT rowid FROM chunks "
            "WHERE compression = 'none' AND CAST(data AS TEXT) LIKE ? ESCAPE '\\'",
            (f"%{escaped}%",),
        ).fetchall():
            candidate_chunk_rowids.add(row[0])

        for row in self._conn.execute(
            "SELECT rowid, data, compression FROM chunks WHERE compression != 'none'"
        ).fetchall():
            if query_bytes in self._decompress(bytes(row[1]), row[2]):
                candidate_chunk_rowids.add(row[0])

        # 4. Scan chunked objects: check single-chunk matches and boundary spans
        for row in self._conn.execute(
            "SELECT sha, chunk_refs FROM objects "
            "WHERE chunk_refs IS NOT NULL AND type_num = 3"
        ).fetchall():
            sha_bin = bytes(row[0])
            if sha_bin in results:
                continue
            rowids = unpack_chunk_refs(bytes(row[1]))
            # Fast path: any single chunk contains the query
            if set(rowids) & candidate_chunk_rowids:
                results.add(sha_bin)
                continue
            # Slow path: check chunk boundaries for spans
            if len(query_bytes) > 1 and len(rowids) > 1:
                overlap = len(query_bytes) - 1
                prev_tail = b""
                found = False
                for rid in rowids:
                    chunk_row = self._conn.execute(
                        "SELECT data, compression FROM chunks WHERE rowid = ?",
                        (rid,),
                    ).fetchone()
                    chunk_data = self._decompress(bytes(chunk_row[0]), chunk_row[1])
                    if prev_tail:
                        window = prev_tail + chunk_data[:overlap]
                        if query_bytes in window:
                            found = True
                            break
                    prev_tail = chunk_data[-overlap:] if len(chunk_data) >= overlap else chunk_data
                if found:
                    results.add(sha_bin)

        out = sorted(results)
        if limit is not None:
            out = out[: int(limit)]
        return [r.hex().encode("ascii") for r in out]
