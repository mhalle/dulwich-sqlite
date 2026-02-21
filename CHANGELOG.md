# Changelog

All notable changes to dulwich-sqlite are documented in this file.

## [0.6.0] — 2026-02-20

First public release. Stores a full bare Git repository in a single SQLite file.

### Features

- **Core**: `SqliteRepo`, `SqliteObjectStore`, `SqliteRefsContainer` — full bare Git repository in a single SQLite file
- **Object model**: Blob, tree, commit, and tag storage with binary SHA-1 addressing and generated hex columns
- **Chunking**: Content-defined chunking for large blobs (text CDC with line boundaries, binary CDC with FastCDC)
- **Deduplication**: Cross-version chunk dedup via SHA-256 keying and `INSERT OR IGNORE`
- **Compression**: Optional zlib or zstd compression for chunks and inline objects. Type-specific zstd dictionaries (commit, tree, chunk) trained via `train_dictionary()`
- **Delta-varint chunk_refs**: Packed chunk rowid references on the objects table using delta-zigzag-varint encoding
- **Binary SHAs**: 20-byte `objects.sha` and 32-byte `chunks.chunk_sha` BLOB columns with generated hex virtual columns
- **Byte range access**: `get_raw_range(name, offset, length)` reads byte ranges from objects without reassembling the entire blob. Per-chunk `raw_size` column enables efficient offset calculation
- **Refs**: Branches, tags, HEAD, symbolic refs with compare-and-swap operations
- **Reflog**: Automatic ref change logging with generated text and datetime columns
- **Remote operations**: `clone_from`, `fetch`, and `push` methods
- **Content search**: `search_content()` substring search across inline blobs and chunks
- **Pack ingestion**: `add_pack`, `add_pack_data`, `add_thin_pack` for fetch/push interop
- **Context manager**: `with SqliteRepo(...) as repo:` for automatic cleanup
- **SQLite pragmas**: WAL mode, `synchronous=NORMAL`, `busy_timeout=5000`
