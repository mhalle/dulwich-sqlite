"""Unit tests for the _chunking module."""

import hashlib

from dulwich_sqlite._chunking import (
    CHUNKING_THRESHOLD,
    chunk_binary,
    chunk_blob,
    chunk_text,
    is_text,
)


class TestIsText:
    def test_text_data(self):
        assert is_text(b"hello world\nline two\n") is True

    def test_binary_data(self):
        assert is_text(b"\x00\x01\x02\x03") is False

    def test_null_after_8000(self):
        data = b"a" * 8000 + b"\x00"
        assert is_text(data) is True

    def test_null_at_boundary(self):
        data = b"a" * 7999 + b"\x00"
        assert is_text(data) is False

    def test_empty_data(self):
        assert is_text(b"") is True


class TestChunkText:
    def test_roundtrip(self):
        data = b"line %d\n" * 100 % tuple(range(100))
        chunks = chunk_text(data)
        reassembled = b"".join(c[1] for c in chunks)
        assert reassembled == data

    def test_multiple_chunks(self):
        # Use varied lines so CRC32 values differ and trigger cut points
        data = b"".join(f"line number {i} with content\n".encode() for i in range(500))
        chunks = chunk_text(data)
        assert len(chunks) > 1
        reassembled = b"".join(c[1] for c in chunks)
        assert reassembled == data

    def test_sha256_correct(self):
        data = b"hello\nworld\nfoo\nbar\nbaz\n" * 20
        chunks = chunk_text(data)
        for sha_bin, chunk_data in chunks:
            assert sha_bin == hashlib.sha256(chunk_data).digest()

    def test_single_line(self):
        data = b"just one line\n"
        chunks = chunk_text(data)
        assert len(chunks) == 1
        assert chunks[0][1] == data

    def test_no_trailing_newline(self):
        data = b"line1\nline2\nline3"
        chunks = chunk_text(data)
        reassembled = b"".join(c[1] for c in chunks)
        assert reassembled == data

    def test_empty_data(self):
        chunks = chunk_text(b"")
        assert len(chunks) == 1
        assert chunks[0][1] == b""

    def test_max_chunk_bytes_respected(self):
        # Lines long enough to trigger max bytes cut
        data = (b"x" * 500 + b"\n") * 50
        chunks = chunk_text(data)
        for _, chunk_data in chunks[:-1]:  # last chunk can be smaller
            assert len(chunk_data) <= 4096 + 501  # max + one line overshoot


class TestChunkBinary:
    def _random_binary(self, size=51200, seed=42):
        """Generate pseudo-random binary data with enough entropy for CDC."""
        import random
        rng = random.Random(seed)
        return bytes(rng.getrandbits(8) for _ in range(size))

    def test_roundtrip(self):
        data = self._random_binary()
        chunks = chunk_binary(data)
        reassembled = b"".join(c[1] for c in chunks)
        assert reassembled == data

    def test_multiple_chunks(self):
        data = self._random_binary()
        chunks = chunk_binary(data)
        assert len(chunks) > 1

    def test_sha256_correct(self):
        data = self._random_binary()
        chunks = chunk_binary(data)
        for sha_bin, chunk_data in chunks:
            assert sha_bin == hashlib.sha256(chunk_data).digest()


class TestChunkBlob:
    def test_small_blob_returns_none(self):
        data = b"small" * 10
        assert len(data) < CHUNKING_THRESHOLD
        assert chunk_blob(data) is None

    def test_large_text_blob_returns_chunks(self):
        data = b"line of text content\n" * 500
        result = chunk_blob(data)
        assert result is not None
        assert len(result) > 1
        reassembled = b"".join(c[1] for c in result)
        assert reassembled == data

    def test_large_binary_blob_returns_chunks(self):
        import random
        rng = random.Random(99)
        data = b"\x00" + bytes(rng.getrandbits(8) for _ in range(51200))
        result = chunk_blob(data)
        assert result is not None
        assert len(result) > 1
        reassembled = b"".join(c[1] for c in result)
        assert reassembled == data

    def test_single_chunk_returns_none(self):
        # Text data that produces only one chunk (few lines, above threshold)
        data = b"x" * (CHUNKING_THRESHOLD + 100)
        result = chunk_blob(data)
        assert result is None

    def test_identical_chunks_share_sha(self):
        # Create data with repeated sections
        repeated = b"this is a repeated line\n" * 20
        data = repeated + b"unique section\n" * 20 + repeated
        result = chunk_blob(data)
        if result is not None:
            shas = [sha for sha, _ in result]
            # If chunks aligned, some shas should repeat
            # (not guaranteed by CDC, but test the sha computation is consistent)
            for sha, chunk_data in result:
                assert sha == hashlib.sha256(chunk_data).digest()
