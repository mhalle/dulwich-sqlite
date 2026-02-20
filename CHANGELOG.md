# Changelog

All notable changes to dulwich-sqlite are documented in this file.

## [0.4.0] — 2026-02-19

### Changed

- **Schema v9**: Inline chunk lists — replaced `object_chunks` join table with `chunk_refs BLOB` on the `objects` table
  - Chunk references are packed as little-endian 8-byte unsigned integers directly on each chunked object row
  - Eliminates the `object_chunks` table and its index entirely (~45% storage savings for chunk-heavy repos)
  - Automatic migration from v8 on open (packs existing `object_chunks` rows, drops the table)
  - `_insert_object()` simplified: no rowid lookup, no DELETE, just pack and INSERT OR REPLACE
  - `get_raw()` unpacks chunk_refs and fetches chunks by rowid IN clause
  - `search_content()` restructured to scan chunk_refs blobs for matching chunk rowids
  - Direct SQL queries for chunk-to-object mappings are no longer available; use the Python API

## [0.3.1] — 2026-02-19

### Changed

- **Schema v8**: Inline objects (commits, trees, tags, small blobs) are now compressed when compression is enabled
  - New `compression` column on `objects` table tracks per-object compression method
  - `total_size` is now always set for inline objects (raw uncompressed size)
  - `size_bytes` generated column uses `total_size` instead of `length(data)` to report correct size for compressed inline data
  - Automatic migration from v7 on open (recreates objects table to change generated column)
  - `search_content()` handles compressed inline blobs via Python-side decompression
  - `train_dictionary()` decompresses inline objects when sampling

## [0.3.0] — 2026-02-19

### Added

- **zstd compression**: New default compression method when `compress=True`. Faster and better ratios than zlib
  - `zstandard` added as a required dependency
  - Dictionary training via `train_dictionary()` for improved compression of small chunks
  - `clone_from()` automatically trains a zstd dictionary after fetching
  - `enable_compression("zstd")` to switch an existing repo to zstd
  - Mixed mode: `none`, `zlib`, and `zstd` chunks coexist in the same database

### Changed

- **Schema v7**: `object_chunks` table now uses integer rowid references (`object_id`, `chunk_id`) instead of text SHA columns (`object_sha`, `chunk_sha`). Reduces storage overhead significantly
  - Automatic migration from v6 on open
  - Direct SQL queries on `object_chunks` now need JOINs through `objects`/`chunks` tables to resolve SHAs
- `compress=True` on `init_bare()` and `clone_from()` now defaults to zstd (was zlib). Use `compress="zlib"` for the old behavior
- `compress` parameter type changed from `bool` to `bool | str`

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
