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

For small blobs stored inline, the data is directly in the `objects` table:

```sql
SELECT sha, CAST(data AS TEXT) FROM objects
WHERE type_name = 'blob' AND NOT is_chunked
LIMIT 5;
```

### Reassembling Chunked Blobs

Chunked blobs have `data IS NULL`. Reassemble by joining with the chunk tables through integer rowid references:

```sql
SELECT c.data, c.compression
FROM object_chunks oc
JOIN chunks c ON c.rowid = oc.chunk_id
WHERE oc.object_id = (SELECT rowid FROM objects WHERE sha = 'abc123...')
ORDER BY oc.chunk_index;
```

Each row is one chunk in order. Concatenate all chunk data to get the full blob content. If `compression` is `'zlib'` or `'zstd'`, decompress each chunk first.

For uncompressed chunks, you can reassemble directly in SQL:

```sql
SELECT GROUP_CONCAT(CAST(c.data AS TEXT), '') as full_content
FROM object_chunks oc
JOIN chunks c ON c.rowid = oc.chunk_id
WHERE oc.object_id = (SELECT rowid FROM objects WHERE sha = 'abc123...')
  AND c.compression = 'none'
ORDER BY oc.chunk_index;
```

**Note:** Since schema v7, `object_chunks` uses integer rowid references (`object_id`, `chunk_id`) instead of text SHA columns. To resolve SHAs, join through the `objects` and `chunks` tables.

## Chunk Queries

### Chunks for a Specific Object

```sql
SELECT oc.chunk_index, c.chunk_sha, c.stored_size, c.compression
FROM object_chunks oc
JOIN chunks c ON c.rowid = oc.chunk_id
WHERE oc.object_id = (SELECT rowid FROM objects WHERE sha = 'abc123...')
ORDER BY oc.chunk_index;
```

### Objects Sharing a Chunk

Find all objects that share a specific chunk (demonstrates deduplication):

```sql
SELECT o.sha
FROM object_chunks oc
JOIN objects o ON o.rowid = oc.object_id
WHERE oc.chunk_id = (SELECT rowid FROM chunks WHERE chunk_sha = 'def456...');
```

### Most Shared Chunks

```sql
SELECT c.chunk_sha, COUNT(*) as shared_by
FROM object_chunks oc
JOIN chunks c ON c.rowid = oc.chunk_id
GROUP BY oc.chunk_id
HAVING shared_by > 1
ORDER BY shared_by DESC
LIMIT 10;
```

### Deduplication Savings

Compare total chunk references to unique chunks:

```sql
SELECT
    COUNT(*) as total_chunk_references,
    COUNT(DISTINCT chunk_id) as unique_chunks,
    COUNT(*) - COUNT(DISTINCT chunk_id) as duplicates_avoided
FROM object_chunks;
```

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

## Text Search

### Search Inline Blobs

Search for a substring in inline (non-chunked) blobs:

```sql
SELECT sha
FROM objects
WHERE type_name = 'blob'
  AND NOT is_chunked
  AND CAST(data AS TEXT) LIKE '%def main%';
```

### Search Uncompressed Chunks

```sql
SELECT DISTINCT o.sha
FROM chunks c
JOIN object_chunks oc ON c.rowid = oc.chunk_id
JOIN objects o ON o.rowid = oc.object_id
WHERE c.compression = 'none'
  AND CAST(c.data AS TEXT) LIKE '%TODO%';
```

### Combined Search (Inline + Uncompressed Chunks)

This matches what `search_content()` does for the SQL portion:

```sql
SELECT DISTINCT o.sha FROM chunks c
JOIN object_chunks oc ON c.rowid = oc.chunk_id
JOIN objects o ON o.rowid = oc.object_id
WHERE c.compression = 'none' AND CAST(c.data AS TEXT) LIKE '%search_term%'
UNION
SELECT sha FROM objects
WHERE NOT is_chunked AND type_name = 'blob'
  AND CAST(data AS TEXT) LIKE '%search_term%';
```

### Compressed Chunks

Compressed chunks (`compression = 'zlib'` or `'zstd'`) cannot be searched with SQL `LIKE`. You need Python-side decompression:

```python
import sqlite3, zlib

conn = sqlite3.connect("my-repo.db")
query = b"search_term"

for row in conn.execute(
    "SELECT DISTINCT o.sha, c.data "
    "FROM chunks c "
    "JOIN object_chunks oc ON c.rowid = oc.chunk_id "
    "JOIN objects o ON o.rowid = oc.object_id "
    "WHERE c.compression = 'zlib'"
):
    if query in zlib.decompress(bytes(row[1])):
        print(f"Found in object {row[0]}")
```

Or simply use the Python API which handles both cases:

```python
from dulwich_sqlite import SqliteRepo

repo = SqliteRepo("my-repo.db")
matches = repo.object_store.search_content("search_term")
for sha in matches:
    print(sha)
repo.close()
```

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
    (SELECT COUNT(*) FROM object_chunks) as chunk_references,
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
