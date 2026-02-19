# Database Schema

dulwich-sqlite stores all repository data in a single SQLite database. This document describes every table, column, index, pragma, and metadata key.

## Schema Version

The current schema version is **6**. The version is stored in the `metadata` table under the key `schema_version`.

### Version History

| Version | Changes |
|---|---|
| v3 | Initial schema: `objects`, `refs`, `peeled_refs`, `named_files`, `metadata`, `reflog` |
| v4 | Added chunking: new `chunks` and `object_chunks` tables. `objects.data` made nullable, `total_size` column added. Recreated `objects` table with generated columns (`type_name`, `size_bytes`) |
| v5 | Added compression: `compression` column on `chunks` table. `compression` key added to `metadata` |
| v6 | Added convenience generated columns: `objects.is_chunked`, `chunks.stored_size`, `reflog.old_sha_text`, `reflog.new_sha_text`, `reflog.committer_text`, `reflog.datetime_text` |

Migration from v3, v4, or v5 happens automatically when opening a database with `SqliteRepo()`.

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
);
```

| Column | Type | Description |
|---|---|---|
| `sha` | TEXT PK | Hex-encoded SHA-1 of the Git object |
| `type_num` | INTEGER | Git object type: 1=commit, 2=tree, 3=blob, 4=tag |
| `data` | BLOB (nullable) | Raw object data. NULL for chunked blobs (data is in the `chunks` table) |
| `total_size` | INTEGER (nullable) | Total data size for chunked blobs. NULL for inline objects |
| `type_name` | TEXT (generated) | Human-readable type name derived from `type_num` |
| `size_bytes` | INTEGER (generated) | Object size: `length(data)` for inline, `total_size` for chunked |
| `is_chunked` | INTEGER (generated) | 1 if the object is chunked (data is NULL), 0 if inline |

**Notes:**
- Inline objects have `data` populated and `total_size` as NULL
- Chunked objects have `data` as NULL and `total_size` set to the total reassembled size
- Non-blob objects (commits, trees, tags) are always stored inline
- Only blobs >= 4096 bytes that produce multiple chunks are stored in chunked form

### `chunks`

Deduplicated content chunks keyed by SHA-256.

```sql
CREATE TABLE chunks (
    chunk_sha TEXT PRIMARY KEY NOT NULL,
    data BLOB NOT NULL,
    compression TEXT NOT NULL DEFAULT 'none',
    stored_size INTEGER GENERATED ALWAYS AS (length(data)) VIRTUAL
);
```

| Column | Type | Description |
|---|---|---|
| `chunk_sha` | TEXT PK | SHA-256 hex digest of the **raw** (uncompressed) chunk data |
| `data` | BLOB | Chunk data, possibly compressed |
| `compression` | TEXT | Compression method: `'none'` or `'zlib'` |
| `stored_size` | INTEGER (generated) | On-disk size of the stored data in bytes (may differ from raw size if compressed) |

**Notes:**
- The SHA-256 key is always computed on raw data, regardless of whether the stored data is compressed. This ensures deduplication works across compression modes
- Chunks are inserted with `INSERT OR IGNORE`, so if two objects share the same chunk, only the first copy is stored
- A single database can have a mix of `'none'` and `'zlib'` chunks

### `object_chunks`

Maps objects to their ordered sequence of chunks.

```sql
CREATE TABLE object_chunks (
    object_sha TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    chunk_sha TEXT NOT NULL,
    PRIMARY KEY (object_sha, chunk_index)
);

CREATE INDEX idx_object_chunks_chunk ON object_chunks (chunk_sha);
```

| Column | Type | Description |
|---|---|---|
| `object_sha` | TEXT | Hex SHA-1 of the Git object (FK to `objects.sha`) |
| `chunk_index` | INTEGER | 0-based position of this chunk in the object's data |
| `chunk_sha` | TEXT | SHA-256 of the chunk (FK to `chunks.chunk_sha`) |

**Notes:**
- To reassemble a chunked object, query `object_chunks` ordered by `chunk_index` and concatenate the referenced chunk data
- The index on `chunk_sha` supports reverse lookups (finding all objects that share a chunk)

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
| `schema_version` | `"3"`, `"4"`, `"5"`, `"6"` | Current schema version |
| `compression` | `"none"`, `"zlib"` | Current compression setting for new chunks |

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
