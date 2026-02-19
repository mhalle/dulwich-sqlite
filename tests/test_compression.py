"""Tests for optional zlib compression of chunks."""

import hashlib
import sqlite3
import zlib

import pytest
from dulwich.objects import Blob

from dulwich_sqlite import SqliteRepo
from dulwich_sqlite._schema import init_db, migrate_v4_to_v5
from dulwich_sqlite.object_store import SqliteObjectStore


def _large_text(keyword: str = "hello", n: int = 500) -> bytes:
    return b"".join(f"{keyword} line {i} of the file\n".encode() for i in range(n))


@pytest.fixture
def compressed_store(tmp_path):
    db = str(tmp_path / "test.db")
    conn = sqlite3.connect(db)
    init_db(conn)
    conn.execute("UPDATE metadata SET value = 'zlib' WHERE key = 'compression'")
    conn.commit()
    s = SqliteObjectStore(conn)
    yield s
    s.close()
    conn.close()


@pytest.fixture
def repo(tmp_path):
    db = str(tmp_path / "comp.db")
    r = SqliteRepo.init_bare(db, compress=True)
    yield r
    r.close()


@pytest.fixture
def repo_no_compress(tmp_path):
    db = str(tmp_path / "nocomp.db")
    r = SqliteRepo.init_bare(db)
    yield r
    r.close()


class TestCompressedRoundtrip:
    def test_compressed_blob_roundtrip(self, compressed_store):
        data = _large_text("roundtrip")
        blob = Blob.from_string(data)
        compressed_store.add_object(blob)
        type_num, retrieved = compressed_store.get_raw(blob.id)
        assert type_num == blob.type_num
        assert retrieved == data

    def test_compressed_binary_roundtrip(self, compressed_store):
        import random

        rng = random.Random(42)
        data = bytes(rng.getrandbits(8) for _ in range(51200))
        blob = Blob.from_string(data)
        compressed_store.add_object(blob)
        type_num, retrieved = compressed_store.get_raw(blob.id)
        assert type_num == blob.type_num
        assert retrieved == data

    def test_small_blob_stays_inline(self, compressed_store):
        data = b"small content"
        blob = Blob.from_string(data)
        compressed_store.add_object(blob)
        row = compressed_store._conn.execute(
            "SELECT data FROM objects WHERE sha = ?",
            (blob.id.decode("ascii"),),
        ).fetchone()
        # Small blobs are stored inline, not compressed
        assert row[0] is not None
        assert bytes(row[0]) == data


class TestChunksInDB:
    def test_chunks_actually_compressed_in_db(self, compressed_store):
        data = _large_text("compressed_check")
        blob = Blob.from_string(data)
        compressed_store.add_object(blob)
        rows = compressed_store._conn.execute(
            "SELECT data, compression FROM chunks"
        ).fetchall()
        assert len(rows) > 0
        for stored_data, compression in rows:
            assert compression == "zlib"
            # Stored data should not equal raw chunk data (it's compressed)
            decompressed = zlib.decompress(bytes(stored_data))
            assert decompressed != bytes(stored_data)

    def test_chunk_sha_on_raw_data(self, compressed_store):
        data = _large_text("sha_check")
        blob = Blob.from_string(data)
        compressed_store.add_object(blob)
        rows = compressed_store._conn.execute(
            "SELECT chunk_sha, data, compression FROM chunks"
        ).fetchall()
        for chunk_sha, stored_data, compression in rows:
            raw = zlib.decompress(bytes(stored_data))
            expected_sha = hashlib.sha256(raw).hexdigest()
            assert chunk_sha == expected_sha


class TestDedup:
    def test_dedup_across_compression_toggle(self, tmp_path):
        db = str(tmp_path / "toggle.db")
        repo = SqliteRepo.init_bare(db)
        try:
            data = _large_text("dedup_test")
            blob = Blob.from_string(data)

            # Write without compression
            repo.object_store.add_object(blob)
            count1 = repo._conn.execute(
                "SELECT COUNT(*) FROM chunks"
            ).fetchone()[0]

            # Enable compression and re-add
            repo.enable_compression()
            repo.object_store.add_object(blob)
            count2 = repo._conn.execute(
                "SELECT COUNT(*) FROM chunks"
            ).fetchone()[0]

            # Chunk count should be unchanged (INSERT OR IGNORE)
            assert count1 == count2
        finally:
            repo.close()


class TestMixedRead:
    def test_mixed_compression_chunks_readable(self, tmp_path):
        db = str(tmp_path / "mixed.db")
        repo = SqliteRepo.init_bare(db)
        try:
            data1 = _large_text("uncompressed_data")
            blob1 = Blob.from_string(data1)
            repo.object_store.add_object(blob1)

            repo.enable_compression()

            data2 = _large_text("compressed_data")
            blob2 = Blob.from_string(data2)
            repo.object_store.add_object(blob2)

            # Both should be readable
            _, retrieved1 = repo.object_store.get_raw(blob1.id)
            _, retrieved2 = repo.object_store.get_raw(blob2.id)
            assert retrieved1 == data1
            assert retrieved2 == data2

            # Verify mixed compression in DB
            methods = set(
                r[0]
                for r in repo._conn.execute(
                    "SELECT DISTINCT compression FROM chunks"
                ).fetchall()
            )
            assert methods == {"none", "zlib"}
        finally:
            repo.close()


class TestToggle:
    def test_enable_disable_toggle(self, repo_no_compress):
        repo = repo_no_compress
        assert repo.object_store._compression == "none"

        repo.enable_compression()
        assert repo.object_store._compression == "zlib"

        data = _large_text("toggle")
        blob = Blob.from_string(data)
        repo.object_store.add_object(blob)

        row = repo._conn.execute(
            "SELECT compression FROM chunks LIMIT 1"
        ).fetchone()
        assert row[0] == "zlib"

        repo.disable_compression()
        assert repo.object_store._compression == "none"

    def test_enable_invalid_method_raises(self, repo_no_compress):
        with pytest.raises(ValueError, match="Unsupported"):
            repo_no_compress.enable_compression("lz4")


class TestInitBare:
    def test_init_bare_compress_true(self, tmp_path):
        db = str(tmp_path / "bare_comp.db")
        repo = SqliteRepo.init_bare(db, compress=True)
        try:
            assert repo.object_store._compression == "zlib"
            row = repo._conn.execute(
                "SELECT value FROM metadata WHERE key = 'compression'"
            ).fetchone()
            assert row[0] == "zlib"
        finally:
            repo.close()

    def test_init_bare_compress_false(self, tmp_path):
        db = str(tmp_path / "bare_nocomp.db")
        repo = SqliteRepo.init_bare(db)
        try:
            assert repo.object_store._compression == "none"
        finally:
            repo.close()


class TestSearchCompressed:
    def test_search_finds_compressed_chunks(self, repo):
        data = _large_text("searchable_keyword")
        blob = Blob.from_string(data)
        repo.object_store.add_object(blob)

        results = repo.object_store.search_content("searchable_keyword")
        assert blob.id in results

    def test_search_mixed(self, tmp_path):
        db = str(tmp_path / "search_mixed.db")
        repo = SqliteRepo.init_bare(db)
        try:
            # Uncompressed
            data1 = _large_text("findme_uncompressed")
            blob1 = Blob.from_string(data1)
            repo.object_store.add_object(blob1)

            # Enable compression
            repo.enable_compression()

            # Compressed
            data2 = _large_text("findme_compressed")
            blob2 = Blob.from_string(data2)
            repo.object_store.add_object(blob2)

            results = repo.object_store.search_content("findme_uncompressed")
            assert blob1.id in results

            results = repo.object_store.search_content("findme_compressed")
            assert blob2.id in results
        finally:
            repo.close()


class TestMigration:
    def test_v4_to_v5_migration(self, tmp_path):
        """Open a v4 DB and verify it migrates to v5 with existing data readable."""
        db = str(tmp_path / "v4.db")
        conn = sqlite3.connect(db)
        # Create a v4-style schema manually
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            """
            CREATE TABLE objects (
                sha TEXT PRIMARY KEY NOT NULL,
                type_num INTEGER NOT NULL,
                data BLOB,
                total_size INTEGER,
                type_name TEXT GENERATED ALWAYS AS (
                    CASE type_num
                        WHEN 1 THEN 'commit'
                        WHEN 2 THEN 'tree'
                        WHEN 3 THEN 'blob'
                        WHEN 4 THEN 'tag'
                    END
                ) VIRTUAL,
                size_bytes INTEGER GENERATED ALWAYS AS (
                    CASE WHEN data IS NOT NULL THEN length(data) ELSE total_size END
                ) VIRTUAL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE chunks (
                chunk_sha TEXT PRIMARY KEY NOT NULL,
                data BLOB NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE object_chunks (
                object_sha TEXT NOT NULL,
                chunk_index INTEGER NOT NULL,
                chunk_sha TEXT NOT NULL,
                PRIMARY KEY (object_sha, chunk_index)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_object_chunks_chunk ON object_chunks (chunk_sha)"
        )
        conn.execute(
            """
            CREATE TABLE refs (
                name BLOB PRIMARY KEY NOT NULL,
                value BLOB NOT NULL,
                name_hex TEXT GENERATED ALWAYS AS (hex(name)) VIRTUAL,
                value_hex TEXT GENERATED ALWAYS AS (hex(value)) VIRTUAL,
                name_text TEXT GENERATED ALWAYS AS (cast(name AS TEXT)) VIRTUAL,
                value_text TEXT GENERATED ALWAYS AS (cast(value AS TEXT)) VIRTUAL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE peeled_refs (
                name BLOB PRIMARY KEY NOT NULL,
                value BLOB NOT NULL,
                name_hex TEXT GENERATED ALWAYS AS (hex(name)) VIRTUAL,
                value_hex TEXT GENERATED ALWAYS AS (hex(value)) VIRTUAL,
                name_text TEXT GENERATED ALWAYS AS (cast(name AS TEXT)) VIRTUAL,
                value_text TEXT GENERATED ALWAYS AS (cast(value AS TEXT)) VIRTUAL
            )
            """
        )
        conn.execute(
            "CREATE TABLE named_files (path TEXT PRIMARY KEY NOT NULL, contents BLOB NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE metadata (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL)"
        )
        conn.execute(
            """
            CREATE TABLE reflog (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ref_name BLOB NOT NULL,
                old_sha BLOB NOT NULL,
                new_sha BLOB NOT NULL,
                committer BLOB NOT NULL,
                timestamp INTEGER NOT NULL,
                timezone INTEGER NOT NULL,
                message BLOB NOT NULL,
                ref_name_text TEXT GENERATED ALWAYS AS (cast(ref_name AS TEXT)) VIRTUAL,
                message_text TEXT GENERATED ALWAYS AS (cast(message AS TEXT)) VIRTUAL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_reflog_ref ON reflog (ref_name, id)"
        )
        conn.execute(
            "INSERT INTO metadata (key, value) VALUES ('schema_version', '4')"
        )
        # Insert a test inline object
        conn.execute(
            "INSERT INTO objects (sha, type_num, data) VALUES (?, ?, ?)",
            ("abcd" * 10, 3, b"test data"),
        )
        conn.commit()
        conn.close()

        # Open with SqliteRepo — should trigger v4→v5→v6 migration
        repo = SqliteRepo(db)
        try:
            row = repo._conn.execute(
                "SELECT value FROM metadata WHERE key = 'schema_version'"
            ).fetchone()
            assert row[0] == "6"

            # compression metadata inserted
            row = repo._conn.execute(
                "SELECT value FROM metadata WHERE key = 'compression'"
            ).fetchone()
            assert row[0] == "none"

            # Existing data still accessible
            row = repo._conn.execute(
                "SELECT data FROM objects WHERE sha = ?",
                ("abcd" * 10,),
            ).fetchone()
            assert bytes(row[0]) == b"test data"

            # chunks table has compression column
            repo._conn.execute(
                "SELECT compression FROM chunks LIMIT 1"
            )
        finally:
            repo.close()
