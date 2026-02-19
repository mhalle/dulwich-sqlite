# Changelog

All notable changes to dulwich-sqlite are documented in this file.

## [0.2.0] — 2026-02-19

### Added

- **Documentation**: Comprehensive docs split into `docs/` — getting started, API reference, database schema, SQL querying guide, internals, and Git comparison
- **Schema v6**: Convenience generated columns for easier SQL queries
  - `objects.is_chunked` — boolean flag for chunked vs inline objects
  - `chunks.stored_size` — on-disk size without needing `LENGTH()`
  - `reflog.old_sha_text`, `reflog.new_sha_text`, `reflog.committer_text` — text casts matching the existing `ref_name_text` pattern
  - `reflog.datetime_text` — pre-formatted ISO-8601 UTC datetime
- **Remote operations**: `clone_from`, `fetch`, and `push` methods on `SqliteRepo`
- **Compression**: Optional zlib compression for chunk data (schema v5)
  - `enable_compression()` / `disable_compression()` on `SqliteRepo`
  - `compress=True` parameter on `init_bare()` and `clone_from()`
  - Mixed mode: compressed and uncompressed chunks coexist in the same database
  - Chunk SHA computed on raw data so dedup works across compression modes
- **Ref atomicity**: Compare-and-swap uses single SQL statements; unconditional set/delete uses `BEGIN IMMEDIATE`
- **Content search**: `search_content()` substring search across inline blobs and chunks via SQL `LIKE`, with Python-side fallback for compressed chunks

### Changed

- README trimmed to a concise landing page with links to `docs/`
- Schema migrated from v5 to v6 (automatic on open)

## [0.1.0] — Initial release

### Added

- **Core**: `SqliteRepo`, `SqliteObjectStore`, `SqliteRefsContainer` storing a full bare Git repository in a single SQLite file
- **Object model**: Blob, tree, commit, and tag storage with SHA-1 addressing
- **Chunking**: Content-defined chunking for large blobs (text CDC with line boundaries, binary CDC with FastCDC)
- **Deduplication**: Cross-version chunk dedup via SHA-256 keying and `INSERT OR IGNORE`
- **Refs**: Branches, tags, HEAD, symbolic refs with compare-and-swap operations
- **Reflog**: Automatic ref change logging
- **Object size**: `get_object_size()` using generated `size_bytes` column
- **Pack ingestion**: `add_pack`, `add_pack_data`, `add_thin_pack` for fetch/push interop
- **Context manager**: `with SqliteRepo(...) as repo:` for automatic cleanup
- **SQLite pragmas**: WAL mode, `synchronous=NORMAL`, `busy_timeout=5000`
- **Schema migrations**: Automatic v3 → v4 → v5 migration chain on open
