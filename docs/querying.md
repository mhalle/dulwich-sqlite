# Querying the Database

A dulwich-sqlite repository is a standard SQLite file. You can open it directly with `sqlite3` (or any SQLite client) and run SQL queries against the Git data.

## Opening the Database

### Command Line

```bash
sqlite3 my-repo.db
```

### Python

```python
import sqlite3
conn = sqlite3.connect("my-repo.db")
```

## Object Queries

### List All Objects

```sql
SELECT sha, type_name, size_bytes FROM objects;
```

### Filter by Type

The `type_name` generated column maps numeric types to human-readable names:

```sql
SELECT sha, size_bytes FROM objects WHERE type_name = 'commit';
SELECT sha, size_bytes FROM objects WHERE type_name = 'tree';
SELECT sha, size_bytes FROM objects WHERE type_name = 'blob';
SELECT sha, size_bytes FROM objects WHERE type_name = 'tag';
```

The underlying `type_num` values are 1=commit, 2=tree, 3=blob, 4=tag if you prefer numeric filters.

### Count Objects by Type

```sql
SELECT type_name, COUNT(*) as count, SUM(size_bytes) as total_bytes
FROM objects
GROUP BY type_name;
```

### Find by SHA Prefix

```sql
SELECT sha, type_name, size_bytes
FROM objects
WHERE sha LIKE 'a1b2c3%';
```

### Inline vs Chunked Objects

```sql
-- Inline objects (data stored directly)
SELECT sha, type_name, size_bytes FROM objects WHERE NOT is_chunked;

-- Chunked objects (data stored in chunks table)
SELECT sha, type_name, size_bytes FROM objects WHERE is_chunked;
```

## Blob Content

### Reading Inline Blobs

For small blobs stored inline, the data is directly in the `objects` table. Note that inline data may be compressed — check the `compression` column:

```sql
-- Uncompressed inline blobs can be read directly
SELECT sha, CAST(data AS TEXT) FROM objects
WHERE type_name = 'blob' AND NOT is_chunked AND compression = 'none'
LIMIT 5;
```

For compressed inline blobs, use the Python API or decompress in your application based on the `compression` column value (`'zlib'` or `'zstd'`).

### Reassembling Chunked Blobs

Chunked blobs have `data IS NULL` and `chunk_refs` populated. The `chunk_refs` column is a packed binary blob of little-endian 8-byte unsigned integers, each being a rowid into the `chunks` table. Use the Python API to reassemble:

```python
import struct
from dulwich_sqlite import SqliteRepo

repo = SqliteRepo("my-repo.db")
type_num, data = repo.object_store.get_raw(b"abc123...")
repo.close()
```

For direct SQL access to individual chunks of a chunked object, you need to unpack the `chunk_refs` blob in Python first:

```python
import sqlite3, struct

conn = sqlite3.connect("my-repo.db")
row = conn.execute(
    "SELECT chunk_refs FROM objects WHERE sha = 'abc123...'"
).fetchone()
refs = bytes(row[0])
n = len(refs) // 8
rowids = struct.unpack(f'<{n}Q', refs)
# Now fetch chunks by rowid
for rid in rowids:
    chunk = conn.execute(
        "SELECT data, compression FROM chunks WHERE rowid = ?", (rid,)
    ).fetchone()
    # decompress if needed based on compression column
```

**Note:** The `chunk_refs` blob is opaque binary. SQL-only chunk reassembly is not practical — use the Python API for chunked objects.

## Chunk Queries

### Chunk Size Distribution

```sql
SELECT
    compression,
    COUNT(*) as count,
    AVG(stored_size) as avg_stored_size,
    MIN(stored_size) as min_size,
    MAX(stored_size) as max_size
FROM chunks
GROUP BY compression;
```

### Chunked Object Statistics

Count how many objects are chunked and total chunk references:

```python
import sqlite3, struct

conn = sqlite3.connect("my-repo.db")
total_refs = 0
chunked_count = 0
for row in conn.execute("SELECT chunk_refs FROM objects WHERE chunk_refs IS NOT NULL"):
    chunked_count += 1
    total_refs += len(bytes(row[0])) // 8

unique_chunks = conn.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
print(f"Chunked objects: {chunked_count}")
print(f"Total chunk references: {total_refs}")
print(f"Unique chunks: {unique_chunks}")
print(f"Duplicates avoided: {total_refs - unique_chunks}")
```

**Note:** Since schema v9, chunk references are packed into the `chunk_refs` BLOB column on the `objects` table. The `object_chunks` join table no longer exists. Deduplication statistics require Python-side unpacking.

## Text Search

### Search Inline Blobs

Search for a substring in uncompressed inline blobs:

```sql
SELECT sha
FROM objects
WHERE type_name = 'blob'
  AND NOT is_chunked
  AND compression = 'none'
  AND CAST(data AS TEXT) LIKE '%def main%';
```

**Note:** SQL `LIKE` only works on uncompressed inline blobs (`compression = 'none'`). Compressed inline blobs require Python-side decompression for searching.

### Search Using the Python API

The `search_content()` method handles all cases: uncompressed and compressed inline blobs, plus chunked objects. This is the recommended approach:

```python
from dulwich_sqlite import SqliteRepo

repo = SqliteRepo("my-repo.db")
matches = repo.object_store.search_content("search_term")
for sha in matches:
    print(sha)
repo.close()
```

`search_content()` searches in four phases:
1. SQL `LIKE` on uncompressed inline blobs (fast, done in SQLite)
2. Python-side search on compressed inline blobs (requires decompression)
3. SQL `LIKE` on uncompressed chunks + Python-side search on compressed chunks
4. Scan chunked objects' `chunk_refs` blobs for matching chunk rowids

**Note:** Since schema v9, chunk-to-object mappings are stored as packed binary in `chunk_refs`. Direct SQL queries for chunk content search across objects are no longer practical — use `search_content()` instead.

## Ref Queries

### List All Refs

```sql
SELECT name_text, value_text FROM refs;
```

Example output:

| name_text | value_text |
|---|---|
| HEAD | ref: refs/heads/main |
| refs/heads/main | a1b2c3d4e5f6... |
| refs/heads/develop | f6e5d4c3b2a1... |

### List Branches

```sql
SELECT name_text, value_text
FROM refs
WHERE name_text LIKE 'refs/heads/%';
```

### List Tags

```sql
SELECT name_text, value_text
FROM refs
WHERE name_text LIKE 'refs/tags/%';
```

### Follow HEAD

```sql
-- See what HEAD points to
SELECT value_text FROM refs WHERE name_text = 'HEAD';
-- Returns something like: ref: refs/heads/main

-- Resolve the symbolic ref
SELECT r2.value_text
FROM refs r1
JOIN refs r2 ON r2.name_text = REPLACE(r1.value_text, 'ref: ', '')
WHERE r1.name_text = 'HEAD';
```

### Peeled Refs

```sql
SELECT name_text, value_text FROM peeled_refs;
```

## Reflog Queries

### History for a Specific Ref

```sql
SELECT id, old_sha_text, new_sha_text, committer_text, datetime_text, message_text
FROM reflog
WHERE ref_name_text = 'refs/heads/main'
ORDER BY id ASC;
```

### Recent Reflog Entries

```sql
SELECT ref_name_text, datetime_text, message_text
FROM reflog
ORDER BY id DESC
LIMIT 20;
```

## Metadata Queries

### Schema Version

```sql
SELECT value FROM metadata WHERE key = 'schema_version';
```

### Compression Setting

```sql
SELECT value FROM metadata WHERE key = 'compression';
```

### All Metadata

```sql
SELECT * FROM metadata;
```

## Storage Analysis

### Database Size Summary

```sql
SELECT
    (SELECT COUNT(*) FROM objects) as total_objects,
    (SELECT COUNT(*) FROM objects WHERE NOT is_chunked) as inline_objects,
    (SELECT COUNT(*) FROM objects WHERE is_chunked) as chunked_objects,
    (SELECT COUNT(*) FROM chunks) as unique_chunks,
    (SELECT COUNT(*) FROM refs) as refs,
    (SELECT COUNT(*) FROM reflog) as reflog_entries;
```

### Object Size Distribution

```sql
SELECT
    type_name,
    COUNT(*) as count,
    SUM(size_bytes) as total_bytes,
    AVG(size_bytes) as avg_bytes,
    MAX(size_bytes) as max_bytes
FROM objects
GROUP BY type_name
ORDER BY total_bytes DESC;
```

### Largest Objects

```sql
SELECT sha, type_name, size_bytes
FROM objects
ORDER BY size_bytes DESC
LIMIT 10;
```

### Compression Ratio

For repositories with compression enabled:

```sql
SELECT
    compression,
    COUNT(*) as chunks,
    SUM(stored_size) as stored_bytes
FROM chunks
GROUP BY compression;
```

Compare stored size to original size for compressed chunks (requires Python):

```python
import sqlite3, zlib

conn = sqlite3.connect("my-repo.db")
stored = 0
original = 0
for row in conn.execute("SELECT data FROM chunks WHERE compression = 'zlib'"):
    data = bytes(row[0])
    stored += len(data)
    original += len(zlib.decompress(data))

print(f"Original: {original:,} bytes")
print(f"Stored:   {stored:,} bytes")
print(f"Ratio:    {stored/original:.1%}")
```

### Named Files

```sql
SELECT path, LENGTH(contents) as size FROM named_files;
```
