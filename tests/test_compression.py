"""Tests for optional zlib compression of chunks."""

import hashlib
import sqlite3
import zlib

import pytest
from dulwich.objects import Blob

from dulwich_sqlite import SqliteRepo
from dulwich_sqlite._schema import init_db, migrate_v4_to_v5, migrate_v6_to_v7, migrate_v7_to_v8, migrate_v8_to_v9, migrate_v9_to_v10
from dulwich_sqlite.object_store import SqliteObjectStore, unpack_chunk_refs


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
    r = SqliteRepo.init_bare(db, compress="zlib")
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
        sha_bin = bytes.fromhex(blob.id.decode("ascii"))
        row = compressed_store._conn.execute(
            "SELECT data, compression FROM objects WHERE sha = ?",
            (sha_bin,),
        ).fetchone()
        # Small blobs are stored inline (data is not NULL)
        assert row[0] is not None
        # With compression enabled, inline data is compressed
        assert row[1] == "zlib"
        # Verify via get_raw roundtrip
        type_num, retrieved = compressed_store.get_raw(blob.id)
        assert retrieved == data


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
            expected_sha = hashlib.sha256(raw).digest()
            assert bytes(chunk_sha) == expected_sha


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
    def test_init_bare_compress_true_uses_zstd(self, tmp_path):
        db = str(tmp_path / "bare_comp.db")
        repo = SqliteRepo.init_bare(db, compress=True)
        try:
            assert repo.object_store._compression == "zstd"
            row = repo._conn.execute(
                "SELECT value FROM metadata WHERE key = 'compression'"
            ).fetchone()
            assert row[0] == "zstd"
        finally:
            repo.close()

    def test_init_bare_compress_zlib(self, tmp_path):
        db = str(tmp_path / "bare_zlib.db")
        repo = SqliteRepo.init_bare(db, compress="zlib")
        try:
            assert repo.object_store._compression == "zlib"
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

        # Open with SqliteRepo — should trigger v4→v5→...→v10 migration
        repo = SqliteRepo(db)
        try:
            row = repo._conn.execute(
                "SELECT value FROM metadata WHERE key = 'schema_version'"
            ).fetchone()
            assert row[0] == "10"

            # compression metadata inserted
            row = repo._conn.execute(
                "SELECT value FROM metadata WHERE key = 'compression'"
            ).fetchone()
            assert row[0] == "none"

            # Existing data still accessible via binary SHA
            sha_bin = bytes.fromhex("abcd" * 10)
            row = repo._conn.execute(
                "SELECT data FROM objects WHERE sha = ?",
                (sha_bin,),
            ).fetchone()
            assert bytes(row[0]) == b"test data"

            # chunks table has compression column
            repo._conn.execute(
                "SELECT compression FROM chunks LIMIT 1"
            )
        finally:
            repo.close()


class TestZstdCompression:
    def test_zstd_blob_roundtrip(self, tmp_path):
        db = str(tmp_path / "zstd.db")
        repo = SqliteRepo.init_bare(db, compress="zstd")
        try:
            data = _large_text("zstd_roundtrip")
            blob = Blob.from_string(data)
            repo.object_store.add_object(blob)
            type_num, retrieved = repo.object_store.get_raw(blob.id)
            assert type_num == blob.type_num
            assert retrieved == data
            # Verify chunks are stored with zstd compression
            row = repo._conn.execute(
                "SELECT compression FROM chunks LIMIT 1"
            ).fetchone()
            assert row[0] == "zstd"
        finally:
            repo.close()

    def test_zstd_with_dictionary(self, tmp_path):
        db = str(tmp_path / "zstd_dict.db")
        repo = SqliteRepo.init_bare(db, compress="zstd")
        try:
            # Store several blobs to have enough samples for chunks + inline objects
            for i in range(20):
                blob = Blob.from_string(_large_text(f"sample_{i}"))
                repo.object_store.add_object(blob)

            # Add commits and trees for type-specific dict training
            from dulwich.objects import Commit, Tree
            import time as _time

            trees_and_commits = []
            for i in range(15):
                blob = Blob.from_string(f"file content {i}".encode())
                repo.object_store.add_object(blob)
                tree = Tree()
                tree.add(f"file_{i}.txt".encode(), 0o100644, blob.id)
                repo.object_store.add_object(tree)
                commit = Commit()
                commit.tree = tree.id
                commit.author = commit.committer = b"Test <test@example.com>"
                commit.author_time = commit.commit_time = int(_time.time()) + i
                commit.author_timezone = commit.commit_timezone = 0
                commit.encoding = b"UTF-8"
                commit.message = f"commit {i}".encode()
                repo.object_store.add_object(commit)
                trees_and_commits.append((tree, commit))

            # Train dictionary
            repo.train_dictionary()
            assert len(repo.object_store._zstd_dicts) > 0

            # Verify type-specific dict files exist
            for path in ['_zstd_dict_commit', '_zstd_dict_tree', '_zstd_dict_chunk']:
                row = repo._conn.execute(
                    "SELECT contents FROM named_files WHERE path = ?", (path,)
                ).fetchone()
                assert row is not None, f"Expected {path} to exist"

            # Verify legacy dict was removed
            row = repo._conn.execute(
                "SELECT contents FROM named_files WHERE path = '_zstd_dict'"
            ).fetchone()
            assert row is None

            # Store a new blob with dictionary and verify roundtrip
            data = _large_text("after_dict_training")
            blob = Blob.from_string(data)
            repo.object_store.add_object(blob)
            _, retrieved = repo.object_store.get_raw(blob.id)
            assert retrieved == data

            # Verify existing data is still readable after re-compression
            for tree, commit in trees_and_commits:
                _, tree_data = repo.object_store.get_raw(tree.id)
                assert tree_data == tree.as_raw_string()
                _, commit_data = repo.object_store.get_raw(commit.id)
                assert commit_data == commit.as_raw_string()
        finally:
            repo.close()

    def test_zstd_search(self, tmp_path):
        db = str(tmp_path / "zstd_search.db")
        repo = SqliteRepo.init_bare(db, compress="zstd")
        try:
            data = _large_text("zstd_searchable_term")
            blob = Blob.from_string(data)
            repo.object_store.add_object(blob)

            results = repo.object_store.search_content("zstd_searchable_term")
            assert blob.id in results
        finally:
            repo.close()

    def test_mixed_zlib_zstd_readable(self, tmp_path):
        db = str(tmp_path / "mixed_zstd.db")
        repo = SqliteRepo.init_bare(db)
        try:
            # Store uncompressed
            data1 = _large_text("uncompressed_data")
            blob1 = Blob.from_string(data1)
            repo.object_store.add_object(blob1)

            # Switch to zlib
            repo.enable_compression("zlib")
            data2 = _large_text("zlib_compressed")
            blob2 = Blob.from_string(data2)
            repo.object_store.add_object(blob2)

            # Switch to zstd
            repo.enable_compression("zstd")
            data3 = _large_text("zstd_compressed")
            blob3 = Blob.from_string(data3)
            repo.object_store.add_object(blob3)

            # All should be readable
            _, r1 = repo.object_store.get_raw(blob1.id)
            _, r2 = repo.object_store.get_raw(blob2.id)
            _, r3 = repo.object_store.get_raw(blob3.id)
            assert r1 == data1
            assert r2 == data2
            assert r3 == data3

            # Verify mixed compression in DB
            methods = set(
                r[0]
                for r in repo._conn.execute(
                    "SELECT DISTINCT compression FROM chunks"
                ).fetchall()
            )
            assert methods == {"none", "zlib", "zstd"}
        finally:
            repo.close()

    def test_enable_zstd(self, tmp_path):
        db = str(tmp_path / "enable_zstd.db")
        repo = SqliteRepo.init_bare(db)
        try:
            repo.enable_compression("zstd")
            assert repo.object_store._compression == "zstd"

            data = _large_text("zstd_enabled")
            blob = Blob.from_string(data)
            repo.object_store.add_object(blob)

            row = repo._conn.execute(
                "SELECT compression FROM chunks LIMIT 1"
            ).fetchone()
            assert row[0] == "zstd"
        finally:
            repo.close()

    def test_type_specific_dicts_compress_with_right_dict(self, tmp_path):
        """Verify each type uses its own dictionary (check frame dict_id)."""
        import zstandard

        db = str(tmp_path / "typedict.db")
        repo = SqliteRepo.init_bare(db, compress="zstd")
        try:
            from dulwich.objects import Commit, Tree
            import time as _time

            # Add enough data for training
            for i in range(20):
                blob = Blob.from_string(_large_text(f"typedict_{i}"))
                repo.object_store.add_object(blob)
                small_blob = Blob.from_string(f"content {i}".encode())
                repo.object_store.add_object(small_blob)
                tree = Tree()
                tree.add(f"f_{i}.txt".encode(), 0o100644, small_blob.id)
                repo.object_store.add_object(tree)
                commit = Commit()
                commit.tree = tree.id
                commit.author = commit.committer = b"A <a@b.c>"
                commit.author_time = commit.commit_time = int(_time.time()) + i
                commit.author_timezone = commit.commit_timezone = 0
                commit.encoding = b"UTF-8"
                commit.message = f"msg {i}".encode()
                repo.object_store.add_object(commit)

            repo.train_dictionary()

            # Verify commit data uses commit dict
            commit_dict_id = repo.object_store._zstd_dicts['commit'].dict_id()
            for row in repo._conn.execute(
                "SELECT data FROM objects WHERE type_num = 1 AND compression = 'zstd'"
            ).fetchall():
                params = zstandard.get_frame_parameters(bytes(row[0]))
                assert params.dict_id == commit_dict_id

            # Verify tree data uses tree dict
            tree_dict_id = repo.object_store._zstd_dicts['tree'].dict_id()
            for row in repo._conn.execute(
                "SELECT data FROM objects WHERE type_num = 2 AND compression = 'zstd'"
            ).fetchall():
                params = zstandard.get_frame_parameters(bytes(row[0]))
                assert params.dict_id == tree_dict_id

            # Verify chunks use chunk dict
            chunk_dict_id = repo.object_store._zstd_dicts['chunk'].dict_id()
            for row in repo._conn.execute(
                "SELECT data FROM chunks WHERE compression = 'zstd'"
            ).fetchall():
                params = zstandard.get_frame_parameters(bytes(row[0]))
                assert params.dict_id == chunk_dict_id

            # Verify blobs (type_num=3 inline) use no dict (dict_id=0)
            for row in repo._conn.execute(
                "SELECT data FROM objects WHERE type_num = 3 AND data IS NOT NULL AND compression = 'zstd'"
            ).fetchall():
                params = zstandard.get_frame_parameters(bytes(row[0]))
                assert params.dict_id == 0
        finally:
            repo.close()

    def test_legacy_dict_backward_compat(self, tmp_path):
        """Data compressed with old single dict is still readable after type-specific training."""
        import zstandard

        db = str(tmp_path / "legacy_compat.db")
        repo = SqliteRepo.init_bare(db, compress="zstd")
        try:
            from dulwich.objects import Commit, Tree
            import time as _time

            # Add data and manually create a legacy single dict
            samples = []
            for i in range(20):
                blob = Blob.from_string(_large_text(f"legacy_{i}"))
                repo.object_store.add_object(blob)
                small_blob = Blob.from_string(f"file {i}".encode())
                repo.object_store.add_object(small_blob)
                tree = Tree()
                tree.add(f"f_{i}.txt".encode(), 0o100644, small_blob.id)
                repo.object_store.add_object(tree)
                commit = Commit()
                commit.tree = tree.id
                commit.author = commit.committer = b"A <a@b.c>"
                commit.author_time = commit.commit_time = int(_time.time()) + i
                commit.author_timezone = commit.commit_timezone = 0
                commit.encoding = b"UTF-8"
                commit.message = f"commit {i}".encode()
                repo.object_store.add_object(commit)

            # Save all raw data for later verification
            all_objects = {}
            for row in repo._conn.execute(
                "SELECT sha, type_num, data, compression FROM objects WHERE data IS NOT NULL"
            ).fetchall():
                raw = repo.object_store._decompress(bytes(row[2]), row[3])
                all_objects[bytes(row[0])] = (row[1], raw)

            # Now train type-specific dicts — this re-compresses everything
            repo.train_dictionary()

            # Verify all data still readable
            for sha_bin, (type_num, expected_raw) in all_objects.items():
                hexsha = sha_bin.hex().encode("ascii")
                got_type, got_raw = repo.object_store.get_raw(hexsha)
                assert got_type == type_num
                assert got_raw == expected_raw
        finally:
            repo.close()

    def test_train_dictionary_skips_sparse_types(self, tmp_path):
        """Only dict for types with >= 10 samples is created."""
        db = str(tmp_path / "sparse.db")
        repo = SqliteRepo.init_bare(db, compress="zstd")
        try:
            # Only add chunks (blobs), no commits/trees
            for i in range(20):
                blob = Blob.from_string(_large_text(f"sparse_{i}"))
                repo.object_store.add_object(blob)

            repo.train_dictionary()

            # Only chunk dict should exist
            assert 'chunk' in repo.object_store._zstd_dicts
            assert 'commit' not in repo.object_store._zstd_dicts
            assert 'tree' not in repo.object_store._zstd_dicts

            # Verify named_files
            row = repo._conn.execute(
                "SELECT 1 FROM named_files WHERE path = '_zstd_dict_chunk'"
            ).fetchone()
            assert row is not None
            row = repo._conn.execute(
                "SELECT 1 FROM named_files WHERE path = '_zstd_dict_commit'"
            ).fetchone()
            assert row is None
            row = repo._conn.execute(
                "SELECT 1 FROM named_files WHERE path = '_zstd_dict_tree'"
            ).fetchone()
            assert row is None
        finally:
            repo.close()

    def test_recompression_reduces_size(self, tmp_path):
        """Re-compression with type-specific dicts should not increase size."""
        db = str(tmp_path / "recomp.db")
        repo = SqliteRepo.init_bare(db, compress="zstd")
        try:
            from dulwich.objects import Commit, Tree
            import time as _time

            for i in range(20):
                blob = Blob.from_string(_large_text(f"recomp_{i}"))
                repo.object_store.add_object(blob)
                small_blob = Blob.from_string(f"content {i}".encode())
                repo.object_store.add_object(small_blob)
                tree = Tree()
                tree.add(f"f_{i}.txt".encode(), 0o100644, small_blob.id)
                repo.object_store.add_object(tree)
                commit = Commit()
                commit.tree = tree.id
                commit.author = commit.committer = b"A <a@b.c>"
                commit.author_time = commit.commit_time = int(_time.time()) + i
                commit.author_timezone = commit.commit_timezone = 0
                commit.encoding = b"UTF-8"
                commit.message = f"msg {i}".encode()
                repo.object_store.add_object(commit)

            # Measure size before training
            size_before = repo._conn.execute(
                "SELECT SUM(LENGTH(data)) FROM objects WHERE data IS NOT NULL AND compression = 'zstd'"
            ).fetchone()[0] or 0
            chunk_size_before = repo._conn.execute(
                "SELECT SUM(LENGTH(data)) FROM chunks WHERE compression = 'zstd'"
            ).fetchone()[0] or 0
            total_before = size_before + chunk_size_before

            repo.train_dictionary()

            # Measure size after training
            size_after = repo._conn.execute(
                "SELECT SUM(LENGTH(data)) FROM objects WHERE data IS NOT NULL AND compression = 'zstd'"
            ).fetchone()[0] or 0
            chunk_size_after = repo._conn.execute(
                "SELECT SUM(LENGTH(data)) FROM chunks WHERE compression = 'zstd'"
            ).fetchone()[0] or 0
            total_after = size_after + chunk_size_after

            # Type-specific dicts should not make things worse
            assert total_after <= total_before
        finally:
            repo.close()


class TestChunkRefs:
    def test_chunk_refs_packed_correctly(self, tmp_path):
        db = str(tmp_path / "chunkrefs.db")
        repo = SqliteRepo.init_bare(db)
        try:
            data = _large_text("chunk_refs_test")
            blob = Blob.from_string(data)
            repo.object_store.add_object(blob)

            # Verify object_chunks table does not exist
            tables = [
                r[0] for r in repo._conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            ]
            assert "object_chunks" not in tables

            # Verify chunk_refs is a delta-varint packed blob
            sha_bin = bytes.fromhex(blob.id.decode("ascii"))
            row = repo._conn.execute(
                "SELECT chunk_refs FROM objects WHERE sha = ?",
                (sha_bin,),
            ).fetchone()
            assert row[0] is not None
            refs_blob = bytes(row[0])
            rowids = unpack_chunk_refs(refs_blob)
            assert len(rowids) > 0
            # All rowids should be positive integers
            for rid in rowids:
                assert isinstance(rid, int)
                assert rid > 0
        finally:
            repo.close()

    def test_v6_to_v7_migration(self, tmp_path):
        """Create a v6 DB with text object_chunks columns, open, verify migration."""
        db = str(tmp_path / "v6.db")
        conn = sqlite3.connect(db)
        conn.execute("PRAGMA journal_mode=WAL")
        # Create v6-style schema
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
                ) VIRTUAL,
                is_chunked INTEGER GENERATED ALWAYS AS (data IS NULL) VIRTUAL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE chunks (
                chunk_sha TEXT PRIMARY KEY NOT NULL,
                data BLOB NOT NULL,
                compression TEXT NOT NULL DEFAULT 'none',
                stored_size INTEGER GENERATED ALWAYS AS (length(data)) VIRTUAL
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
            "CREATE INDEX idx_object_chunks_chunk ON object_chunks (chunk_sha)"
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
                old_sha_text TEXT GENERATED ALWAYS AS (cast(old_sha AS TEXT)) VIRTUAL,
                new_sha_text TEXT GENERATED ALWAYS AS (cast(new_sha AS TEXT)) VIRTUAL,
                committer_text TEXT GENERATED ALWAYS AS (cast(committer AS TEXT)) VIRTUAL,
                message_text TEXT GENERATED ALWAYS AS (cast(message AS TEXT)) VIRTUAL,
                datetime_text TEXT GENERATED ALWAYS AS (datetime(timestamp, 'unixepoch')) VIRTUAL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_reflog_ref ON reflog (ref_name, id)"
        )
        conn.execute(
            "INSERT INTO metadata (key, value) VALUES ('schema_version', '6')"
        )
        conn.execute(
            "INSERT INTO metadata (key, value) VALUES ('compression', 'none')"
        )

        # Insert a chunked object manually
        obj_sha = "abcd" * 10
        chunk_sha = "ef01" * 16
        chunk_data = b"test chunk data that is stored"
        conn.execute(
            "INSERT INTO objects (sha, type_num, data, total_size) VALUES (?, 3, NULL, ?)",
            (obj_sha, len(chunk_data)),
        )
        conn.execute(
            "INSERT INTO chunks (chunk_sha, data, compression) VALUES (?, ?, 'none')",
            (chunk_sha, chunk_data),
        )
        conn.execute(
            "INSERT INTO object_chunks (object_sha, chunk_index, chunk_sha) VALUES (?, 0, ?)",
            (obj_sha, chunk_sha),
        )
        conn.commit()
        conn.close()

        # Open with SqliteRepo — should trigger v6→v7→v8→v9→v10 migration
        repo = SqliteRepo(db)
        try:
            row = repo._conn.execute(
                "SELECT value FROM metadata WHERE key = 'schema_version'"
            ).fetchone()
            assert row[0] == "10"

            # Verify object_chunks table is gone
            tables = [
                r[0] for r in repo._conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            ]
            assert "object_chunks" not in tables

            # Verify chunk_refs is populated for the chunked object (now binary SHA)
            obj_sha_bin = bytes.fromhex(obj_sha)
            row = repo._conn.execute(
                "SELECT chunk_refs FROM objects WHERE sha = ?",
                (obj_sha_bin,),
            ).fetchone()
            assert row[0] is not None
            rowids = unpack_chunk_refs(bytes(row[0]))
            assert len(rowids) == 1  # 1 chunk

            # Verify data roundtrip through the new schema
            type_num, data = repo.object_store.get_raw(obj_sha.encode("ascii"))
            assert type_num == 3
            assert data == chunk_data
        finally:
            repo.close()


class TestInlineCompression:
    def test_inline_object_compressed(self, tmp_path):
        """Verify commit/tree objects are compressed when compression is enabled."""
        from dulwich.objects import Commit, Tree
        import time

        db = str(tmp_path / "inline_comp.db")
        repo = SqliteRepo.init_bare(db, compress="zlib")
        try:
            blob = Blob.from_string(b"content")
            repo.object_store.add_object(blob)
            tree = Tree()
            tree.add(b"file.txt", 0o100644, blob.id)
            repo.object_store.add_object(tree)
            commit = Commit()
            commit.tree = tree.id
            commit.author = commit.committer = b"A <a@b.c>"
            commit.author_time = commit.commit_time = int(time.time())
            commit.author_timezone = commit.commit_timezone = 0
            commit.encoding = b"UTF-8"
            commit.message = b"test commit"
            repo.object_store.add_object(commit)

            # All inline objects should have compression='zlib'
            for obj_id in [blob.id, tree.id, commit.id]:
                sha_bin = bytes.fromhex(obj_id.decode("ascii"))
                row = repo._conn.execute(
                    "SELECT compression FROM objects WHERE sha = ?",
                    (sha_bin,),
                ).fetchone()
                assert row[0] == "zlib", f"Expected zlib for {obj_id}"

            # Verify roundtrip
            _, r = repo.object_store.get_raw(commit.id)
            assert r == commit.as_raw_string()
        finally:
            repo.close()

    def test_inline_compressed_search(self, tmp_path):
        """Verify search_content finds compressed inline blobs."""
        db = str(tmp_path / "inline_search.db")
        repo = SqliteRepo.init_bare(db, compress="zlib")
        try:
            data = b"unique_inline_keyword_here"
            blob = Blob.from_string(data)
            repo.object_store.add_object(blob)

            # Verify it's stored inline and compressed
            sha_bin = bytes.fromhex(blob.id.decode("ascii"))
            row = repo._conn.execute(
                "SELECT compression, data FROM objects WHERE sha = ?",
                (sha_bin,),
            ).fetchone()
            assert row[0] == "zlib"
            assert row[1] is not None  # inline

            results = repo.object_store.search_content("unique_inline_keyword_here")
            assert blob.id in results
        finally:
            repo.close()

    def test_v7_to_v8_migration(self, tmp_path):
        """Manually create a v7 DB, open, verify migration adds compression column."""
        db = str(tmp_path / "v7.db")
        conn = sqlite3.connect(db)
        conn.execute("PRAGMA journal_mode=WAL")
        # Create v7-style schema
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
                ) VIRTUAL,
                is_chunked INTEGER GENERATED ALWAYS AS (data IS NULL) VIRTUAL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE chunks (
                chunk_sha TEXT PRIMARY KEY NOT NULL,
                data BLOB NOT NULL,
                compression TEXT NOT NULL DEFAULT 'none',
                stored_size INTEGER GENERATED ALWAYS AS (length(data)) VIRTUAL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE object_chunks (
                object_id INTEGER NOT NULL,
                chunk_index INTEGER NOT NULL,
                chunk_id INTEGER NOT NULL,
                PRIMARY KEY (object_id, chunk_index)
            )
            """
        )
        conn.execute(
            "CREATE INDEX idx_object_chunks_chunk ON object_chunks (chunk_id)"
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
                old_sha_text TEXT GENERATED ALWAYS AS (cast(old_sha AS TEXT)) VIRTUAL,
                new_sha_text TEXT GENERATED ALWAYS AS (cast(new_sha AS TEXT)) VIRTUAL,
                committer_text TEXT GENERATED ALWAYS AS (cast(committer AS TEXT)) VIRTUAL,
                message_text TEXT GENERATED ALWAYS AS (cast(message AS TEXT)) VIRTUAL,
                datetime_text TEXT GENERATED ALWAYS AS (datetime(timestamp, 'unixepoch')) VIRTUAL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_reflog_ref ON reflog (ref_name, id)"
        )
        conn.execute(
            "INSERT INTO metadata (key, value) VALUES ('schema_version', '7')"
        )
        conn.execute(
            "INSERT INTO metadata (key, value) VALUES ('compression', 'none')"
        )
        # Insert test inline object (no total_size, as v7 didn't set it for inline)
        conn.execute(
            "INSERT INTO objects (sha, type_num, data) VALUES (?, ?, ?)",
            ("abcd" * 10, 3, b"test inline data"),
        )
        conn.commit()
        conn.close()

        # Open with SqliteRepo — should trigger v7→v8→v9→v10 migration
        repo = SqliteRepo(db)
        try:
            row = repo._conn.execute(
                "SELECT value FROM metadata WHERE key = 'schema_version'"
            ).fetchone()
            assert row[0] == "10"

            # compression column exists with default 'none'
            sha_bin = bytes.fromhex("abcd" * 10)
            row = repo._conn.execute(
                "SELECT compression FROM objects WHERE sha = ?",
                (sha_bin,),
            ).fetchone()
            assert row[0] == "none"

            # total_size was backfilled
            row = repo._conn.execute(
                "SELECT total_size, size_bytes FROM objects WHERE sha = ?",
                (sha_bin,),
            ).fetchone()
            assert row[0] == len(b"test inline data")
            assert row[1] == len(b"test inline data")

            # Data still accessible
            row = repo._conn.execute(
                "SELECT data FROM objects WHERE sha = ?",
                (sha_bin,),
            ).fetchone()
            assert bytes(row[0]) == b"test inline data"
        finally:
            repo.close()

    def test_v8_to_v9_migration(self, tmp_path):
        """Manually create a v8 DB with object_chunks, open, verify migration."""
        import struct

        db = str(tmp_path / "v8.db")
        conn = sqlite3.connect(db)
        conn.execute("PRAGMA journal_mode=WAL")
        # Create v8-style schema
        conn.execute(
            """
            CREATE TABLE objects (
                sha TEXT PRIMARY KEY NOT NULL,
                type_num INTEGER NOT NULL,
                data BLOB,
                total_size INTEGER,
                compression TEXT NOT NULL DEFAULT 'none',
                type_name TEXT GENERATED ALWAYS AS (
                    CASE type_num
                        WHEN 1 THEN 'commit'
                        WHEN 2 THEN 'tree'
                        WHEN 3 THEN 'blob'
                        WHEN 4 THEN 'tag'
                    END
                ) VIRTUAL,
                size_bytes INTEGER GENERATED ALWAYS AS (total_size) VIRTUAL,
                is_chunked INTEGER GENERATED ALWAYS AS (data IS NULL) VIRTUAL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE chunks (
                chunk_sha TEXT PRIMARY KEY NOT NULL,
                data BLOB NOT NULL,
                compression TEXT NOT NULL DEFAULT 'none',
                stored_size INTEGER GENERATED ALWAYS AS (length(data)) VIRTUAL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE object_chunks (
                object_id INTEGER NOT NULL,
                chunk_index INTEGER NOT NULL,
                chunk_id INTEGER NOT NULL,
                PRIMARY KEY (object_id, chunk_index)
            )
            """
        )
        conn.execute(
            "CREATE INDEX idx_object_chunks_chunk ON object_chunks (chunk_id)"
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
                old_sha_text TEXT GENERATED ALWAYS AS (cast(old_sha AS TEXT)) VIRTUAL,
                new_sha_text TEXT GENERATED ALWAYS AS (cast(new_sha AS TEXT)) VIRTUAL,
                committer_text TEXT GENERATED ALWAYS AS (cast(committer AS TEXT)) VIRTUAL,
                message_text TEXT GENERATED ALWAYS AS (cast(message AS TEXT)) VIRTUAL,
                datetime_text TEXT GENERATED ALWAYS AS (datetime(timestamp, 'unixepoch')) VIRTUAL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_reflog_ref ON reflog (ref_name, id)"
        )
        conn.execute(
            "INSERT INTO metadata (key, value) VALUES ('schema_version', '8')"
        )
        conn.execute(
            "INSERT INTO metadata (key, value) VALUES ('compression', 'none')"
        )

        # Insert a chunked object with 2 chunks
        obj_sha = "deadbeef" * 5
        chunk1_data = b"first chunk data here"
        chunk2_data = b"second chunk data here"
        conn.execute(
            "INSERT INTO objects (sha, type_num, data, total_size, compression) "
            "VALUES (?, 3, NULL, ?, 'none')",
            (obj_sha, len(chunk1_data) + len(chunk2_data)),
        )
        obj_rowid = conn.execute(
            "SELECT rowid FROM objects WHERE sha = ?", (obj_sha,)
        ).fetchone()[0]
        chunk1_sha = "aa" * 32
        chunk2_sha = "bb" * 32
        conn.execute(
            "INSERT INTO chunks (chunk_sha, data, compression) VALUES (?, ?, 'none')",
            (chunk1_sha, chunk1_data),
        )
        chunk1_rowid = conn.execute(
            "SELECT rowid FROM chunks WHERE chunk_sha = ?", (chunk1_sha,)
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO chunks (chunk_sha, data, compression) VALUES (?, ?, 'none')",
            (chunk2_sha, chunk2_data),
        )
        chunk2_rowid = conn.execute(
            "SELECT rowid FROM chunks WHERE chunk_sha = ?", (chunk2_sha,)
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO object_chunks (object_id, chunk_index, chunk_id) VALUES (?, 0, ?)",
            (obj_rowid, chunk1_rowid),
        )
        conn.execute(
            "INSERT INTO object_chunks (object_id, chunk_index, chunk_id) VALUES (?, 1, ?)",
            (obj_rowid, chunk2_rowid),
        )

        # Also insert an inline object
        inline_sha = "abcd" * 10
        conn.execute(
            "INSERT INTO objects (sha, type_num, data, total_size, compression) "
            "VALUES (?, 3, ?, ?, 'none')",
            (inline_sha, b"inline data", len(b"inline data")),
        )
        conn.commit()
        conn.close()

        # Open with SqliteRepo — should trigger v8→v9→v10 migration
        repo = SqliteRepo(db)
        try:
            row = repo._conn.execute(
                "SELECT value FROM metadata WHERE key = 'schema_version'"
            ).fetchone()
            assert row[0] == "10"

            # object_chunks table should be gone
            tables = [
                r[0] for r in repo._conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            ]
            assert "object_chunks" not in tables

            # chunk_refs should be populated for chunked object (now binary SHA + delta-varint)
            obj_sha_bin = bytes.fromhex(obj_sha)
            row = repo._conn.execute(
                "SELECT chunk_refs FROM objects WHERE sha = ?",
                (obj_sha_bin,),
            ).fetchone()
            assert row[0] is not None
            rowids = unpack_chunk_refs(bytes(row[0]))
            assert len(rowids) == 2
            assert rowids == [chunk1_rowid, chunk2_rowid]

            # Inline object should have NULL chunk_refs
            inline_sha_bin = bytes.fromhex(inline_sha)
            row = repo._conn.execute(
                "SELECT chunk_refs FROM objects WHERE sha = ?",
                (inline_sha_bin,),
            ).fetchone()
            assert row[0] is None

            # Chunked data should be readable
            type_num, data = repo.object_store.get_raw(obj_sha.encode("ascii"))
            assert type_num == 3
            assert data == chunk1_data + chunk2_data

            # Inline data should be readable
            type_num, data = repo.object_store.get_raw(inline_sha.encode("ascii"))
            assert type_num == 3
            assert data == b"inline data"
        finally:
            repo.close()

    def test_v9_to_v10_migration(self, tmp_path):
        """Create a v9 DB with TEXT SHAs + fixed-8-byte chunk_refs, open, verify binary SHAs + delta-varint."""
        import struct

        db = str(tmp_path / "v9.db")
        conn = sqlite3.connect(db)
        conn.execute("PRAGMA journal_mode=WAL")
        # Create v9-style schema (TEXT SHAs, no sha_hex/chunk_sha_hex generated columns)
        conn.execute(
            """
            CREATE TABLE objects (
                sha TEXT PRIMARY KEY NOT NULL,
                type_num INTEGER NOT NULL,
                data BLOB,
                chunk_refs BLOB,
                total_size INTEGER,
                compression TEXT NOT NULL DEFAULT 'none',
                type_name TEXT GENERATED ALWAYS AS (
                    CASE type_num
                        WHEN 1 THEN 'commit'
                        WHEN 2 THEN 'tree'
                        WHEN 3 THEN 'blob'
                        WHEN 4 THEN 'tag'
                    END
                ) VIRTUAL,
                size_bytes INTEGER GENERATED ALWAYS AS (total_size) VIRTUAL,
                is_chunked INTEGER GENERATED ALWAYS AS (data IS NULL) VIRTUAL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE chunks (
                chunk_sha TEXT PRIMARY KEY NOT NULL,
                data BLOB NOT NULL,
                compression TEXT NOT NULL DEFAULT 'none',
                stored_size INTEGER GENERATED ALWAYS AS (length(data)) VIRTUAL
            )
            """
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
                old_sha_text TEXT GENERATED ALWAYS AS (cast(old_sha AS TEXT)) VIRTUAL,
                new_sha_text TEXT GENERATED ALWAYS AS (cast(new_sha AS TEXT)) VIRTUAL,
                committer_text TEXT GENERATED ALWAYS AS (cast(committer AS TEXT)) VIRTUAL,
                message_text TEXT GENERATED ALWAYS AS (cast(message AS TEXT)) VIRTUAL,
                datetime_text TEXT GENERATED ALWAYS AS (datetime(timestamp, 'unixepoch')) VIRTUAL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_reflog_ref ON reflog (ref_name, id)"
        )
        conn.execute(
            "INSERT INTO metadata (key, value) VALUES ('schema_version', '9')"
        )
        conn.execute(
            "INSERT INTO metadata (key, value) VALUES ('compression', 'none')"
        )

        # Insert chunks with TEXT SHA-256 hex keys
        chunk1_sha_hex = "a" * 64
        chunk2_sha_hex = "b" * 64
        chunk1_data = b"first chunk data here"
        chunk2_data = b"second chunk data here"
        conn.execute(
            "INSERT INTO chunks (chunk_sha, data, compression) VALUES (?, ?, 'none')",
            (chunk1_sha_hex, chunk1_data),
        )
        chunk1_rowid = conn.execute(
            "SELECT rowid FROM chunks WHERE chunk_sha = ?", (chunk1_sha_hex,)
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO chunks (chunk_sha, data, compression) VALUES (?, ?, 'none')",
            (chunk2_sha_hex, chunk2_data),
        )
        chunk2_rowid = conn.execute(
            "SELECT rowid FROM chunks WHERE chunk_sha = ?", (chunk2_sha_hex,)
        ).fetchone()[0]

        # Insert chunked object with TEXT SHA and fixed 8-byte LE chunk_refs
        obj_sha_hex = "deadbeef" * 5
        packed_refs = struct.pack('<2Q', chunk1_rowid, chunk2_rowid)
        conn.execute(
            "INSERT INTO objects (sha, type_num, data, chunk_refs, total_size, compression) "
            "VALUES (?, 3, NULL, ?, ?, 'none')",
            (obj_sha_hex, packed_refs, len(chunk1_data) + len(chunk2_data)),
        )

        # Insert inline object with TEXT SHA
        inline_sha_hex = "abcd" * 10
        conn.execute(
            "INSERT INTO objects (sha, type_num, data, chunk_refs, total_size, compression) "
            "VALUES (?, 3, ?, NULL, ?, 'none')",
            (inline_sha_hex, b"inline data", len(b"inline data")),
        )
        conn.commit()
        conn.close()

        # Open with SqliteRepo — should trigger v9→v10 migration
        repo = SqliteRepo(db)
        try:
            row = repo._conn.execute(
                "SELECT value FROM metadata WHERE key = 'schema_version'"
            ).fetchone()
            assert row[0] == "10"

            # Verify SHAs are now binary BLOB
            obj_sha_bin = bytes.fromhex(obj_sha_hex)
            row = repo._conn.execute(
                "SELECT sha FROM objects WHERE sha = ?",
                (obj_sha_bin,),
            ).fetchone()
            assert row is not None
            assert isinstance(row[0], bytes)
            assert len(bytes(row[0])) == 20

            # Verify sha_hex generated column works
            row = repo._conn.execute(
                "SELECT sha_hex FROM objects WHERE sha = ?",
                (obj_sha_bin,),
            ).fetchone()
            assert row[0] == obj_sha_hex

            # Verify chunk_sha is now binary BLOB
            chunk1_sha_bin = bytes.fromhex(chunk1_sha_hex)
            row = repo._conn.execute(
                "SELECT chunk_sha FROM chunks WHERE chunk_sha = ?",
                (chunk1_sha_bin,),
            ).fetchone()
            assert row is not None
            assert len(bytes(row[0])) == 32

            # Verify chunk_sha_hex generated column
            row = repo._conn.execute(
                "SELECT chunk_sha_hex FROM chunks WHERE chunk_sha = ?",
                (chunk1_sha_bin,),
            ).fetchone()
            assert row[0] == chunk1_sha_hex

            # Verify chunk_refs are delta-varint encoded
            row = repo._conn.execute(
                "SELECT chunk_refs FROM objects WHERE sha = ?",
                (obj_sha_bin,),
            ).fetchone()
            rowids = unpack_chunk_refs(bytes(row[0]))
            assert rowids == [chunk1_rowid, chunk2_rowid]

            # Inline object has NULL chunk_refs
            inline_sha_bin = bytes.fromhex(inline_sha_hex)
            row = repo._conn.execute(
                "SELECT chunk_refs FROM objects WHERE sha = ?",
                (inline_sha_bin,),
            ).fetchone()
            assert row[0] is None

            # Data roundtrip works
            type_num, data = repo.object_store.get_raw(obj_sha_hex.encode("ascii"))
            assert type_num == 3
            assert data == chunk1_data + chunk2_data

            type_num, data = repo.object_store.get_raw(inline_sha_hex.encode("ascii"))
            assert type_num == 3
            assert data == b"inline data"
        finally:
            repo.close()
