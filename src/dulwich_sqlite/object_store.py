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

PACK_SPOOL_FILE_MAX_SIZE = 200 * 1024 * 1024


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

    def get_raw(self, name: RawObjectID | ObjectID) -> tuple[int, bytes]:
        hexsha = self._to_hexsha(name)
        row = self._conn.execute(
            "SELECT type_num, data FROM objects WHERE sha = ?",
            (hexsha.decode("ascii"),),
        ).fetchone()
        if row is None:
            raise KeyError(hexsha)
        return row[0], bytes(row[1])

    def _insert_object(self, obj: ShaFile) -> None:
        """Insert a single object without committing the transaction."""
        self._conn.execute(
            "INSERT OR REPLACE INTO objects (sha, type_num, data) VALUES (?, ?, ?)",
            (obj.id.decode("ascii"), obj.type_num, obj.as_raw_string()),
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
