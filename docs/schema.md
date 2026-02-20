# Database Schema

dulwich-sqlite stores all repository data in a single SQLite database. This document describes every table, column, index, pragma, and metadata key.

## Schema Version

The current schema version is **11**. The version is stored in the `metadata` table under the key `schema_version`.

### Version History

| Version | Changes |
|---|---|
| v3 | Initial schema: `objects`, `refs`, `peeled_refs`, `named_files`, `metadata`, `reflog` |
| v4 | Added chunking: new `chunks` and `object_chunks` tables. `objects.data` made nullable, `total_size` column added. Recreated `objects` table with generated columns (`type_name`, `size_bytes`) |
| v5 | Added compression: `compression` column on `chunks` table. `compression` key added to `metadata` |
| v6 | Added convenience generated columns: `objects.is_chunked`, `chunks.stored_size`, `reflog.old_sha_text`, `reflog.new_sha_text`, `reflog.committer_text`, `reflog.datetime_text` |
| v7 | Integer keys in `object_chunks`: replaced `object_sha`/`chunk_sha` text columns with `object_id`/`chunk_id` integer rowid references. Added zstd compression support |
| v8 | Inline object compression: added `compression` column to `objects` table. Changed `size_bytes` to use `total_size` (always set). Inline objects (commits, trees, tags, small blobs) are now compressed when compression is enabled |
| v9 | Inline chunk lists: replaced `object_chunks` table with `chunk_refs BLOB` column on `objects`. Packed little-endian 8-byte rowids eliminate the join table entirely |
| v10 | Binary SHAs + delta-varint chunk_refs: `objects.sha` changed from TEXT(40) to BLOB(20), `chunks.chunk_sha` from TEXT(64) to BLOB(32). Added `sha_hex`/`chunk_sha_hex` generated columns. `chunk_refs` re-encoded from fixed 8-byte LE to delta-zigzag-varint (~81% smaller) |
| v11 | Byte range access: added `raw_size INTEGER` column to `chunks` table tracking decompressed chunk size. Enables efficient range reads without decompressing all chunks |

Migration from v3 through v10 happens automatically when opening a database with `SqliteRepo()`.

## Pragmas

These are applied on every connection:

```sql
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA busy_timeout=5000;
```

| Pragma | Value | Why |
|---|---|---|
| `journal_mode` | `WAL` | Write-ahead logging allows concurrent readers while writing. Prevents readers from blocking writers |
| `synchronous` | `NORMAL` | Balances durability with write performance. Data is safe against application crashes; only an OS crash during a WAL checkpoint could theoretically lose data |
| `busy_timeout` | `5000` | Wait up to 5 seconds when another connection holds the write lock, rather than failing immediately |

## Tables

### `objects`

Stores all Git objects (blobs, trees, commits, tags).

```sql
CREATE TABLE objects (
    sha BLOB PRIMARY KEY NOT NULL,
    type_num INTEGER NOT NULL,
    data BLOB,
    chunk_refs BLOB,
    total_size INTEGER,
    compression TEXT NOT NULL DEFAULT 'none',
    sha_hex TEXT GENERATED ALWAYS AS (lower(hex(sha))) VIRTUAL,
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
);
```

| Column | Type | Description |
|---|---|---|
| `sha` | BLOB PK | 20-byte binary SHA-1 of the Git object |
| `type_num` | INTEGER | Git object type: 1=commit, 2=tree, 3=blob, 4=tag |
| `data` | BLOB (nullable) | Object data, possibly compressed. NULL for chunked blobs (data is in the `chunks` table) |
| `chunk_refs` | BLOB (nullable) | Packed chunk rowids for chunked objects. NULL for inline objects. Delta-zigzag-varint encoded (see Internals) |
| `total_size` | INTEGER | Total raw (uncompressed) data size in bytes. Always set for both inline and chunked objects |
| `compression` | TEXT | Compression method for inline data: `'none'`, `'zlib'`, or `'zstd'`. Always `'none'` for chunked objects (their chunks have their own compression) |
| `sha_hex` | TEXT (generated) | Lowercase hex encoding of `sha` for human-readable queries |
| `type_name` | TEXT (generated) | Human-readable type name derived from `type_num` |
| `size_bytes` | INTEGER (generated) | Object size in bytes, derived from `total_size` |
| `is_chunked` | INTEGER (generated) | 1 if the object is chunked (data is NULL), 0 if inline |

**Notes:**
- Inline objects have `data` populated, `chunk_refs` as NULL, and `total_size` set to the raw data size
- Chunked objects have `data` as NULL, `chunk_refs` packed with ordered chunk rowids (delta-zigzag-varint), and `total_size` set to the total reassembled size
- Non-blob objects (commits, trees, tags) are always stored inline
- Only blobs >= 4096 bytes that produce multiple chunks are stored in chunked form
- When compression is enabled, inline data is compressed; decompress using the `compression` column value
- The `chunk_refs` blob is opaque binary — use the Python API (`unpack_chunk_refs()`) to decode the delta-varint rowids
- Use the `sha_hex` generated column for human-readable queries (e.g., `WHERE sha_hex LIKE 'a1b2c3%'`)

### `chunks`

Deduplicated content chunks keyed by SHA-256.

```sql
CREATE TABLE chunks (
    chunk_sha BLOB PRIMARY KEY NOT NULL,
    data BLOB NOT NULL,
    compression TEXT NOT NULL DEFAULT 'none',
    raw_size INTEGER,
    chunk_sha_hex TEXT GENERATED ALWAYS AS (lower(hex(chunk_sha))) VIRTUAL,
    stored_size INTEGER GENERATED ALWAYS AS (length(data)) VIRTUAL
);
```

| Column | Type | Description |
|---|---|---|
| `chunk_sha` | BLOB PK | 32-byte binary SHA-256 digest of the **raw** (uncompressed) chunk data |
| `data` | BLOB | Chunk data, possibly compressed |
| `compression` | TEXT | Compression method: `'none'`, `'zlib'`, or `'zstd'` |
| `raw_size` | INTEGER | Decompressed size of the chunk data in bytes. Used for byte range offset calculation |
| `chunk_sha_hex` | TEXT (generated) | Lowercase hex encoding of `chunk_sha` for human-readable queries |
| `stored_size` | INTEGER (generated) | On-disk size of the stored data in bytes (may differ from raw size if compressed) |

**Notes:**
- The SHA-256 key is always computed on raw data, regardless of whether the stored data is compressed. This ensures deduplication works across compression modes
- Use the `chunk_sha_hex` generated column for human-readable queries
- Chunks are inserted with `INSERT OR IGNORE`, so if two objects share the same chunk, only the first copy is stored
- A single database can have a mix of `'none'`, `'zlib'`, and `'zstd'` chunks

### `refs`

Git references (branches, tags, HEAD).

```sql
CREATE TABLE refs (
    name BLOB PRIMARY KEY NOT NULL,
    value BLOB NOT NULL,
    name_hex TEXT GENERATED ALWAYS AS (hex(name)) VIRTUAL,
    value_hex TEXT GENERATED ALWAYS AS (hex(value)) VIRTUAL,
    name_text TEXT GENERATED ALWAYS AS (cast(name AS TEXT)) VIRTUAL,
    value_text TEXT GENERATED ALWAYS AS (cast(value AS TEXT)) VIRTUAL
);
```

| Column | Type | Description |
|---|---|---|
| `name` | BLOB PK | Ref name as bytes (e.g., `refs/heads/main`, `HEAD`) |
| `value` | BLOB | Hex SHA or symbolic ref target (prefixed with `ref: `) |
| `name_hex` | TEXT (generated) | Hex encoding of name, for debugging |
| `value_hex` | TEXT (generated) | Hex encoding of value, for debugging |
| `name_text` | TEXT (generated) | UTF-8 text cast of name, for human-readable queries |
| `value_text` | TEXT (generated) | UTF-8 text cast of value, for human-readable queries |

**Notes:**
- Names and values are stored as raw bytes (BLOB) to match Dulwich's byte-string ref model
- Symbolic refs (like HEAD) store `ref: refs/heads/main` as the value
- The generated `_text` columns make it easy to query refs with plain SQL

### `peeled_refs`

Cached peeled (dereferenced) values for annotated tags.

```sql
CREATE TABLE peeled_refs (
    name BLOB PRIMARY KEY NOT NULL,
    value BLOB NOT NULL,
    name_hex TEXT GENERATED ALWAYS AS (hex(name)) VIRTUAL,
    value_hex TEXT GENERATED ALWAYS AS (hex(value)) VIRTUAL,
    name_text TEXT GENERATED ALWAYS AS (cast(name AS TEXT)) VIRTUAL,
    value_text TEXT GENERATED ALWAYS AS (cast(value AS TEXT)) VIRTUAL
);
```

Same structure as `refs`. Stores the ultimate object SHA that an annotated tag points to.

### `named_files`

Stores files that would normally live in `.git/` — config, description, info/exclude.

```sql
CREATE TABLE named_files (
    path TEXT PRIMARY KEY NOT NULL,
    contents BLOB NOT NULL
);
```

| Column | Type | Description |
|---|---|---|
| `path` | TEXT PK | File path relative to the repository root (e.g., `"config"`, `"description"`) |
| `contents` | BLOB | File contents |

### `metadata`

Key-value store for repository metadata.

```sql
CREATE TABLE metadata (
    key TEXT PRIMARY KEY NOT NULL,
    value TEXT NOT NULL
);
```

| Key | Values | Description |
|---|---|---|
| `schema_version` | `"3"` through `"10"` | Current schema version |
| `compression` | `"none"`, `"zlib"`, `"zstd"` | Current compression setting for new chunks |

### `reflog`

Records ref changes for auditing and recovery.

```sql
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
);

CREATE INDEX idx_reflog_ref ON reflog (ref_name, id);
```

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto-incrementing entry ID |
| `ref_name` | BLOB | Ref that changed |
| `old_sha` | BLOB | Previous value (or the symbolic ref string) |
| `new_sha` | BLOB | New value |
| `committer` | BLOB | Who made the change (defaults to `dulwich-sqlite <dulwich-sqlite@localhost>`) |
| `timestamp` | INTEGER | Unix timestamp |
| `timezone` | INTEGER | Timezone offset in seconds |
| `message` | BLOB | Log message |
| `ref_name_text` | TEXT (generated) | Human-readable ref name |
| `old_sha_text` | TEXT (generated) | Human-readable previous SHA |
| `new_sha_text` | TEXT (generated) | Human-readable new SHA |
| `committer_text` | TEXT (generated) | Human-readable committer identity |
| `message_text` | TEXT (generated) | Human-readable message |
| `datetime_text` | TEXT (generated) | ISO-8601 UTC datetime (e.g., `2025-01-15 12:30:00`) |

**Notes:**
- The index on `(ref_name, id)` makes per-ref history lookups fast
- Entries are ordered by `id` ascending (chronological)

## Migration Details

### v3 to v4

Added content-defined chunking:

1. `objects` table recreated: `data` column made nullable, `total_size` column added, generated columns `type_name` and `size_bytes` added
2. Existing object data migrated to the new table schema
3. New `chunks` table created
4. New `object_chunks` table created with composite primary key and index
5. Schema version updated to `"4"`

SQLite cannot `ALTER COLUMN` to drop `NOT NULL`, so the migration renames the old table, creates a new one, copies data, and drops the old table.

### v4 to v5

Added compression support:

1. `compression TEXT NOT NULL DEFAULT 'none'` column added to `chunks` via `ALTER TABLE`
2. `compression` metadata key inserted with value `"none"`
3. Schema version updated to `"5"`

All existing chunks get the default `'none'` compression value.

### v5 to v6

Added convenience generated columns via `ALTER TABLE ADD COLUMN`:

1. `objects.is_chunked` — `INTEGER GENERATED ALWAYS AS (data IS NULL) VIRTUAL`
2. `chunks.stored_size` — `INTEGER GENERATED ALWAYS AS (length(data)) VIRTUAL`
3. `reflog.old_sha_text` — `TEXT GENERATED ALWAYS AS (cast(old_sha AS TEXT)) VIRTUAL`
4. `reflog.new_sha_text` — `TEXT GENERATED ALWAYS AS (cast(new_sha AS TEXT)) VIRTUAL`
5. `reflog.committer_text` — `TEXT GENERATED ALWAYS AS (cast(committer AS TEXT)) VIRTUAL`
6. `reflog.datetime_text` — `TEXT GENERATED ALWAYS AS (datetime(timestamp, 'unixepoch')) VIRTUAL`
7. Schema version updated to `"6"`

All columns are VIRTUAL (computed on read, no storage overhead).

### v6 to v7

Replaced text SHA columns in `object_chunks` with integer rowid references (intermediate step, table removed in v9):

1. New `object_chunks_new` table created with `(object_id INTEGER, chunk_index INTEGER, chunk_id INTEGER)` schema
2. Data migrated via `INSERT INTO object_chunks_new SELECT o.rowid, oc.chunk_index, c.rowid FROM object_chunks oc JOIN objects o ON o.sha = oc.object_sha JOIN chunks c ON c.chunk_sha = oc.chunk_sha`
3. Old index and table dropped, new table renamed to `object_chunks`
4. New index created on `chunk_id`
5. Schema version updated to `"7"`

### v7 to v8

Added inline object compression:

1. `compression TEXT NOT NULL DEFAULT 'none'` column added to `objects` via `ALTER TABLE`
2. `total_size` backfilled for inline objects: `UPDATE objects SET total_size = length(data) WHERE data IS NOT NULL AND total_size IS NULL`
3. `objects` table recreated to change `size_bytes` generated column from `CASE WHEN data IS NOT NULL THEN length(data) ELSE total_size END` to simply `total_size`
4. `object_chunks` references updated to match new rowids
5. Schema version updated to `"8"`

The table recreation is needed because SQLite cannot alter generated column expressions in place. The `size_bytes` column now uses `total_size` because inline data may be compressed, so `length(data)` would return the compressed size rather than the original size.

### v8 to v9

Replaced the `object_chunks` join table with inline `chunk_refs` BLOB:

1. `chunk_refs BLOB` column added to `objects` via `ALTER TABLE`
2. All `object_chunks` rows fetched ordered by `(object_id, chunk_index)`, grouped by `object_id`, chunk rowids packed as little-endian 8-byte unsigned integers, and written to each object's `chunk_refs`
3. `idx_object_chunks_chunk` index dropped
4. `object_chunks` table dropped
5. Schema version updated to `"9"`

This eliminates the `object_chunks` table entirely (~45% of database size for large repos), replacing it with a compact binary blob on each chunked object row. The packed format uses `struct.pack('<NQ', ...)` — length / 8 = chunk count.

### v9 to v10

Binary SHAs and delta-varint chunk_refs:

1. `chunks` table rebuilt: `chunk_sha` column changed from TEXT(64) to BLOB(32). `chunk_sha_hex TEXT GENERATED ALWAYS AS (lower(hex(chunk_sha))) VIRTUAL` added for queryability. Rowids preserved explicitly to maintain chunk_refs correctness
2. `objects` table rebuilt: `sha` column changed from TEXT(40) to BLOB(20). `sha_hex TEXT GENERATED ALWAYS AS (lower(hex(sha))) VIRTUAL` added. `chunk_refs` re-encoded from fixed 8-byte little-endian integers to delta-zigzag-varint format
3. Schema version updated to `"10"`

Binary SHAs halve storage for both SHA columns and their indices. Delta-varint encoding reduces `chunk_refs` by ~81% — consecutive chunk rowids (delta=1) encode to a single byte instead of 8. Combined savings: ~15 MB for a typical large repository.

### v10 to v11

Added `raw_size` column for byte range access:

1. `raw_size INTEGER` column added to `chunks` via `ALTER TABLE`
2. Backfill for uncompressed chunks: `UPDATE chunks SET raw_size = length(data) WHERE compression = 'none'`
3. Backfill for compressed chunks: Python-side decompression to measure raw size (loads zstd dicts from `named_files` if needed)
4. Schema version updated to `"11"`

The `raw_size` column enables efficient byte range reads by computing cumulative byte offsets across chunks without decompressing. Storage overhead is minimal (~4 bytes per chunk row).
