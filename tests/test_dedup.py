"""Integration tests for chunk-based deduplication."""

import random
import sqlite3

import pytest
from dulwich.objects import Blob, Tree

from dulwich_sqlite import SqliteRepo
from dulwich_sqlite._schema import enable_fts, has_fts, init_db
from dulwich_sqlite.object_store import SqliteObjectStore


@pytest.fixture
def store(tmp_path):
    db = str(tmp_path / "test.db")
    conn = sqlite3.connect(db)
    init_db(conn)
    s = SqliteObjectStore(conn)
    yield s
    s.close()
    conn.close()


@pytest.fixture
def repo(tmp_path):
    db = str(tmp_path / "dedup.db")
    r = SqliteRepo.init_bare(db)
    yield r
    r.close()


class TestChunkedStorageRoundtrip:
    def test_large_text_blob_roundtrip(self, store):
        data = b"".join(f"line {i} of the file\n".encode() for i in range(500))
        blob = Blob.from_string(data)
        store.add_object(blob)
        type_num, retrieved = store.get_raw(blob.id)
        assert type_num == blob.type_num
        assert retrieved == data

    def test_large_binary_blob_roundtrip(self, store):
        import random
        rng = random.Random(42)
        data = bytes(rng.getrandbits(8) for _ in range(51200))
        blob = Blob.from_string(data)
        store.add_object(blob)
        type_num, retrieved = store.get_raw(blob.id)
        assert type_num == blob.type_num
        assert retrieved == data

    def test_small_blob_stays_inline(self, store):
        data = b"small content"
        blob = Blob.from_string(data)
        store.add_object(blob)
        # Verify data is inline (not NULL)
        row = store._conn.execute(
            "SELECT data FROM objects WHERE sha = ?",
            (blob.id.decode("ascii"),),
        ).fetchone()
        assert row[0] is not None

    def test_chunked_blob_has_null_data(self, store):
        data = b"".join(f"line {i} of the file\n".encode() for i in range(500))
        blob = Blob.from_string(data)
        store.add_object(blob)
        row = store._conn.execute(
            "SELECT data, total_size FROM objects WHERE sha = ?",
            (blob.id.decode("ascii"),),
        ).fetchone()
        assert row[0] is None
        assert row[1] == len(data)

    def test_non_blob_objects_stay_inline(self, store):
        # Trees are always stored inline regardless of size
        blob = Blob.from_string(b"content")
        store.add_object(blob)
        tree = Tree()
        tree.add(b"file.txt", 0o100644, blob.id)
        store.add_object(tree)
        row = store._conn.execute(
            "SELECT data FROM objects WHERE sha = ?",
            (tree.id.decode("ascii"),),
        ).fetchone()
        assert row[0] is not None

    def test_get_object_size_chunked(self, store):
        data = b"".join(f"line {i} of the file\n".encode() for i in range(500))
        blob = Blob.from_string(data)
        store.add_object(blob)
        assert store.get_object_size(blob.id) == len(data)

    def test_get_object_size_inline(self, store):
        data = b"small"
        blob = Blob.from_string(data)
        store.add_object(blob)
        assert store.get_object_size(blob.id) == len(data)

    def test_contains_works_for_chunked(self, store):
        data = b"".join(f"line {i} of the file\n".encode() for i in range(500))
        blob = Blob.from_string(data)
        store.add_object(blob)
        assert store.contains_loose(blob.id)


class TestDeduplication:
    def test_shared_chunks_stored_once(self, store):
        shared = b"".join(f"shared line {i}\n".encode() for i in range(300))
        unique1 = b"".join(f"unique1 line {i}\n".encode() for i in range(100))
        unique2 = b"".join(f"unique2 line {i}\n".encode() for i in range(100))
        blob1 = Blob.from_string(shared + unique1)
        blob2 = Blob.from_string(shared + unique2)
        store.add_object(blob1)
        store.add_object(blob2)

        # Both should roundtrip correctly
        _, data1 = store.get_raw(blob1.id)
        _, data2 = store.get_raw(blob2.id)
        assert data1 == shared + unique1
        assert data2 == shared + unique2

        # Count chunk references vs unique chunks
        ref_count = store._conn.execute(
            "SELECT COUNT(*) FROM object_chunks"
        ).fetchone()[0]
        unique_chunks = store._conn.execute(
            "SELECT COUNT(*) FROM chunks"
        ).fetchone()[0]
        # Should have some dedup: unique chunks < total references
        assert unique_chunks < ref_count

    def test_replace_semantics(self, store):
        """Adding the same object twice should work cleanly."""
        data = b"".join(f"line {i}\n".encode() for i in range(500))
        blob = Blob.from_string(data)
        store.add_object(blob)
        store.add_object(blob)  # re-add
        _, retrieved = store.get_raw(blob.id)
        assert retrieved == data


class TestMigration:
    def test_v3_database_migrated_on_open(self, tmp_path):
        """Opening a v3 database should auto-migrate to v4."""
        db = str(tmp_path / "v3.db")
        conn = sqlite3.connect(db)
        # Create a v3-style schema manually
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            """
            CREATE TABLE objects (
                sha TEXT PRIMARY KEY NOT NULL,
                type_num INTEGER NOT NULL,
                data BLOB NOT NULL,
                type_name TEXT GENERATED ALWAYS AS (
                    CASE type_num
                        WHEN 1 THEN 'commit'
                        WHEN 2 THEN 'tree'
                        WHEN 3 THEN 'blob'
                        WHEN 4 THEN 'tag'
                    END
                ) VIRTUAL,
                size_bytes INTEGER GENERATED ALWAYS AS (length(data)) VIRTUAL
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
                message_text TEXT GENERATED ALWAYS AS (cast(message AS TEXT)) VIRTUAL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_reflog_ref ON reflog (ref_name, id)"
        )
        conn.execute(
            "INSERT INTO metadata (key, value) VALUES ('schema_version', '3')"
        )
        # Insert a test object
        conn.execute(
            "INSERT INTO objects (sha, type_num, data) VALUES (?, ?, ?)",
            ("abcd" * 10, 3, b"test data"),
        )
        conn.commit()
        conn.close()

        # Open with SqliteRepo — should trigger migration
        repo = SqliteRepo(db)
        try:
            # Verify version is now 4
            row = repo._conn.execute(
                "SELECT value FROM metadata WHERE key = 'schema_version'"
            ).fetchone()
            assert row[0] == "4"

            # Verify old data is still accessible
            row = repo._conn.execute(
                "SELECT data FROM objects WHERE sha = ?",
                ("abcd" * 10,),
            ).fetchone()
            assert bytes(row[0]) == b"test data"

            # Verify new tables exist
            repo._conn.execute("SELECT COUNT(*) FROM chunks").fetchone()
            repo._conn.execute("SELECT COUNT(*) FROM object_chunks").fetchone()
        finally:
            repo.close()


def _large_text(keyword: str, n: int = 500) -> bytes:
    """Create text data large enough to be chunked, containing keyword."""
    return b"".join(f"{keyword} line {i} of the file\n".encode() for i in range(n))


class TestFTSSearch:
    def test_search_content_without_fts(self, store):
        """LIKE fallback finds inline + chunked objects."""
        small_blob = Blob.from_string(b"hello world inline")
        store.add_object(small_blob)
        large_blob = Blob.from_string(_large_text("hello"))
        store.add_object(large_blob)

        results = store.search_content("hello")
        assert small_blob.id in results
        assert large_blob.id in results

    def test_search_content_with_fts(self, store):
        """FTS path returns correct results."""
        small_blob = Blob.from_string(b"hello world inline")
        store.add_object(small_blob)
        large_blob = Blob.from_string(_large_text("hello"))
        store.add_object(large_blob)

        enable_fts(store._conn)

        results = store.search_content("hello")
        assert small_blob.id in results
        assert large_blob.id in results

    def test_search_fts5_syntax(self, store):
        """AND/OR/NOT/phrase/prefix queries work with FTS."""
        blob1 = Blob.from_string(_large_text("alpha beta"))
        blob2 = Blob.from_string(_large_text("alpha gamma"))
        store.add_object(blob1)
        store.add_object(blob2)

        enable_fts(store._conn)

        # AND
        results = store.search_content("alpha AND beta")
        assert blob1.id in results
        assert blob2.id not in results

        # OR
        results = store.search_content("beta OR gamma")
        assert blob1.id in results
        assert blob2.id in results

        # NOT
        results = store.search_content("alpha NOT beta")
        assert blob2.id in results
        assert blob1.id not in results

        # Prefix
        results = store.search_content("alph*")
        assert blob1.id in results
        assert blob2.id in results

    def test_search_ranked(self, store):
        """ranked=True returns results ordered by relevance."""
        blob = Blob.from_string(_large_text("hello"))
        store.add_object(blob)

        enable_fts(store._conn)

        results = store.search_content("hello", ranked=True)
        assert blob.id in results

    def test_search_limit(self, store):
        """limit caps results."""
        blobs = []
        for i in range(5):
            b = Blob.from_string(_large_text(f"searchterm{i} common"))
            store.add_object(b)
            blobs.append(b)

        enable_fts(store._conn)

        results = store.search_content("common", limit=3)
        assert len(results) <= 3

    def test_enable_fts_backfills_existing(self, store):
        """Enabling FTS on DB with data indexes existing chunks."""
        blob = Blob.from_string(_large_text("backfillword"))
        store.add_object(blob)

        assert not has_fts(store._conn)
        enable_fts(store._conn)
        assert has_fts(store._conn)

        results = store.search_content("backfillword")
        assert blob.id in results

    def test_disable_fts_falls_back(self, store):
        """After disable, search still works via LIKE."""
        from dulwich_sqlite._schema import disable_fts

        blob = Blob.from_string(_large_text("persistword"))
        store.add_object(blob)

        enable_fts(store._conn)
        results = store.search_content("persistword")
        assert blob.id in results

        disable_fts(store._conn)
        assert not has_fts(store._conn)

        results = store.search_content("persistword")
        assert blob.id in results

    def test_init_bare_with_fts(self, tmp_path):
        """FTS ready from creation."""
        db = str(tmp_path / "fts.db")
        repo = SqliteRepo.init_bare(db, fts=True)
        try:
            assert has_fts(repo._conn)
            blob = Blob.from_string(_large_text("initword"))
            repo.object_store.add_object(blob)
            results = repo.object_store.search_content("initword")
            assert blob.id in results
        finally:
            repo.close()

    def test_fts_excludes_binary(self, store):
        """Binary chunks containing null bytes are not indexed in FTS."""
        # Create binary data with null bytes every 64 bytes, ensuring every
        # chunk (min 2048 bytes) will contain null bytes.
        binary_data = (b"A" * 63 + b"\x00") * 800  # 51200 bytes
        blob = Blob.from_string(binary_data)
        store.add_object(blob)

        enable_fts(store._conn)

        # Verify no binary chunks were indexed
        count = store._conn.execute(
            "SELECT COUNT(*) FROM chunks_fts"
        ).fetchone()[0]
        assert count == 0

    def test_search_quote_disables_operators(self, store):
        """quote=True treats AND/OR/NOT as literal words."""
        # Both blobs contain "alpha", only blob1 also contains "NOT"
        blob1 = Blob.from_string(_large_text("alpha NOT"))
        blob2 = Blob.from_string(_large_text("alpha beta"))
        store.add_object(blob1)
        store.add_object(blob2)

        enable_fts(store._conn)

        # Without quote: "alpha NOT beta" is FTS syntax → exclude blob2's "beta"
        results = store.search_content("alpha NOT beta")
        assert blob1.id in results
        assert blob2.id not in results

        # With quote: "alpha NOT beta" becomes '"alpha" "NOT" "beta"' →
        # match chunks containing all three literal words
        results = store.search_content("alpha NOT beta", quote=True)
        # Neither blob has all three words "alpha", "NOT", and "beta" in one chunk
        assert blob2.id not in results
