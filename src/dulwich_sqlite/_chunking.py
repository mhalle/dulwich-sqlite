"""Content-defined chunking for blob deduplication."""

import hashlib
import zlib

from fastcdc import fastcdc

CHUNKING_THRESHOLD = 4096
TEXT_CDC_MASK = 0x7  # cut when crc32(line) & MASK == 0 → ~8-line avg chunks
TEXT_MIN_LINES = 3
TEXT_MAX_CHUNK_BYTES = 4096
BINARY_AVG_SIZE = 8192
BINARY_MIN_SIZE = 2048
BINARY_MAX_SIZE = 65536


def is_text(data: bytes) -> bool:
    """Return True if data looks like text (no null bytes in first 8000 bytes)."""
    return b"\x00" not in data[:8000]


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def chunk_text(data: bytes) -> list[tuple[str, bytes]]:
    """Split text data into chunks at line boundaries using CRC32.

    Returns list of (sha256_hex, chunk_data) tuples.
    """
    lines = data.split(b"\n")
    # Re-attach newlines to each line (except possibly the last if no trailing newline)
    parts: list[bytes] = []
    for i, line in enumerate(lines):
        if i < len(lines) - 1:
            parts.append(line + b"\n")
        elif line:  # last element, only include if non-empty
            parts.append(line)

    if not parts:
        chunk_data = data
        return [(_sha256_hex(chunk_data), chunk_data)]

    chunks: list[tuple[str, bytes]] = []
    current_lines: list[bytes] = []
    current_bytes = 0
    line_count = 0

    for part in parts:
        current_lines.append(part)
        current_bytes += len(part)
        line_count += 1
        crc = zlib.crc32(part) & 0xFFFFFFFF

        should_cut = (
            line_count >= TEXT_MIN_LINES
            and (crc & TEXT_CDC_MASK) == 0
        ) or current_bytes >= TEXT_MAX_CHUNK_BYTES

        if should_cut:
            chunk_data = b"".join(current_lines)
            chunks.append((_sha256_hex(chunk_data), chunk_data))
            current_lines = []
            current_bytes = 0
            line_count = 0

    # Flush remaining lines
    if current_lines:
        chunk_data = b"".join(current_lines)
        chunks.append((_sha256_hex(chunk_data), chunk_data))

    return chunks


def chunk_binary(data: bytes) -> list[tuple[str, bytes]]:
    """Split binary data into chunks using FastCDC.

    Returns list of (sha256_hex, chunk_data) tuples.
    """
    chunks: list[tuple[str, bytes]] = []
    for chunk in fastcdc(
        data,
        min_size=BINARY_MIN_SIZE,
        avg_size=BINARY_AVG_SIZE,
        max_size=BINARY_MAX_SIZE,
    ):
        chunk_data = data[chunk.offset : chunk.offset + chunk.length]
        chunks.append((_sha256_hex(chunk_data), chunk_data))
    return chunks


def chunk_blob(data: bytes) -> list[tuple[str, bytes]] | None:
    """Chunk blob data for deduplication.

    Returns None if the blob should be stored inline (too small or only one chunk).
    Otherwise returns list of (sha256_hex, chunk_data) tuples.
    """
    if len(data) < CHUNKING_THRESHOLD:
        return None

    if is_text(data):
        chunks = chunk_text(data)
    else:
        chunks = chunk_binary(data)

    if len(chunks) <= 1:
        return None

    return chunks
