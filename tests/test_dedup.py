"""Integration tests for chunk-based deduplication."""

import sqlite3

import pytest
from dulwich.objects import Blob, Tree

from dulwich_sqlite import SqliteRepo
from dulwich_sqlite._schema import init_db
from dulwich_sqlite.object_store import SqliteObjectStore, unpack_chunk_refs


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
        sha_bin = bytes.fromhex(blob.id.decode("ascii"))
        row = store._conn.execute(
            "SELECT data FROM objects WHERE sha = ?",
            (sha_bin,),
        ).fetchone()
        assert row[0] is not None

    def test_chunked_blob_has_null_data(self, store):
        data = b"".join(f"line {i} of the file\n".encode() for i in range(500))
        blob = Blob.from_string(data)
        store.add_object(blob)
        sha_bin = bytes.fromhex(blob.id.decode("ascii"))
        row = store._conn.execute(
            "SELECT data, total_size FROM objects WHERE sha = ?",
            (sha_bin,),
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
        sha_bin = bytes.fromhex(tree.id.decode("ascii"))
        row = store._conn.execute(
            "SELECT data FROM objects WHERE sha = ?",
            (sha_bin,),
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
        total_refs = 0
        for row in store._conn.execute(
            "SELECT chunk_refs FROM objects WHERE chunk_refs IS NOT NULL"
        ).fetchall():
            total_refs += len(unpack_chunk_refs(bytes(row[0])))
        unique_chunks = store._conn.execute(
            "SELECT COUNT(*) FROM chunks"
        ).fetchone()[0]
        # Should have some dedup: unique chunks < total references
        assert unique_chunks < total_refs

    def test_replace_semantics(self, store):
        """Adding the same object twice should work cleanly."""
        data = b"".join(f"line {i}\n".encode() for i in range(500))
        blob = Blob.from_string(data)
        store.add_object(blob)
        store.add_object(blob)  # re-add
        _, retrieved = store.get_raw(blob.id)
        assert retrieved == data


def _large_text(keyword: str, n: int = 500) -> bytes:
    """Create text data large enough to be chunked, containing keyword."""
    return b"".join(f"{keyword} line {i} of the file\n".encode() for i in range(n))


class TestSearchContent:
    def test_search_finds_inline_and_chunked(self, store):
        """LIKE search finds both inline and chunked objects."""
        small_blob = Blob.from_string(b"hello world inline")
        store.add_object(small_blob)
        large_blob = Blob.from_string(_large_text("hello"))
        store.add_object(large_blob)

        results = store.search_content("hello")
        assert small_blob.id in results
        assert large_blob.id in results

    def test_search_limit(self, store):
        """limit caps results."""
        blobs = []
        for i in range(5):
            b = Blob.from_string(_large_text(f"searchterm{i} common"))
            store.add_object(b)
            blobs.append(b)

        results = store.search_content("common", limit=3)
        assert len(results) <= 3

    def test_search_no_match(self, store):
        """Returns empty list when nothing matches."""
        blob = Blob.from_string(b"hello world")
        store.add_object(blob)

        results = store.search_content("nonexistent")
        assert results == []
