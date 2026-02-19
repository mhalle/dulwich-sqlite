# Internals

This document describes how dulwich-sqlite stores and retrieves Git objects internally, including the chunking algorithms, deduplication mechanism, compression, pack ingestion, and transaction model.

## Object Storage Flow

When `add_object(obj)` is called, the object follows one of two storage paths depending on its type and size:

```
add_object(obj)
  |
  ├── Is it a blob?
  |     |
  |     ├── No  → store inline (data column)
  |     └── Yes → chunk_blob(data)
  |                 |
  |                 ├── Returns None (< 4096 bytes or single chunk)
  |                 |     └── store inline (data column)
  |                 └── Returns chunks
  |                       └── store chunked (NULL data + chunks table)
  |
  └── commit transaction
```

### Inline Storage

For non-blob objects (commits, trees, tags) and small blobs:

```sql
INSERT OR REPLACE INTO objects (sha, type_num, data) VALUES (?, ?, ?)
```

The full raw object data goes in the `data` column.

### Chunked Storage

For blobs >= 4096 bytes that produce multiple chunks:

1. Clear any existing chunk mappings (for `REPLACE` semantics):
   ```sql
   DELETE FROM object_chunks WHERE object_sha = ?
   ```

2. Insert the object row with NULL data:
   ```sql
   INSERT OR REPLACE INTO objects (sha, type_num, data, total_size)
   VALUES (?, ?, NULL, ?)
   ```

3. For each chunk, insert into the chunk store (dedup via `INSERT OR IGNORE`):
   ```sql
   INSERT OR IGNORE INTO chunks (chunk_sha, data, compression) VALUES (?, ?, ?)
   ```

4. Record the chunk ordering:
   ```sql
   INSERT INTO object_chunks (object_sha, chunk_index, chunk_sha) VALUES (?, ?, ?)
   ```

## Chunking Algorithms

Only blobs are candidates for chunking. The chunking decision tree:

1. **Size check**: If the blob is smaller than 4096 bytes (`CHUNKING_THRESHOLD`), store inline
2. **Content type detection**: Check for null bytes in the first 8000 bytes
   - No null bytes → **text** → use text CDC
   - Has null bytes → **binary** → use binary CDC (FastCDC)
3. **Chunk count check**: If the algorithm produces only 1 chunk, store inline instead

### Text CDC (Content-Defined Chunking)

Text CDC splits data at line boundaries using CRC32 hashing. This produces chunks that align with logical content boundaries in source code, config files, and other line-oriented text.

**Algorithm:**

1. Split the data on `\n`, re-attaching the newline to each line
2. Accumulate lines into the current chunk
3. For each line, compute `CRC32(line) & 0xFFFFFFFF`
4. Cut the chunk when **both** conditions are met:
   - At least `TEXT_MIN_LINES` (3) lines accumulated
   - `crc32 & TEXT_CDC_MASK (0x7) == 0` (statistically ~1 in 8 lines)
5. Also force a cut when `current_bytes >= TEXT_MAX_CHUNK_BYTES` (4096)
6. Flush any remaining lines as the final chunk

**Parameters:**

| Constant | Value | Effect |
|---|---|---|
| `TEXT_CDC_MASK` | `0x7` | Average ~8 lines per chunk |
| `TEXT_MIN_LINES` | `3` | Minimum 3 lines before a cut point |
| `TEXT_MAX_CHUNK_BYTES` | `4096` | Maximum bytes per chunk |

**Why line boundaries?** When a few lines change between blob versions, only the chunks containing those lines differ. Unchanged regions produce identical chunks with the same SHA-256, which are deduplicated.

### Binary CDC (FastCDC)

Binary blobs use the [FastCDC](https://pypi.org/project/fastcdc/) library for content-defined chunking. FastCDC uses a rolling hash to find chunk boundaries in arbitrary binary data.

**Parameters:**

| Constant | Value |
|---|---|
| `BINARY_MIN_SIZE` | 2048 bytes |
| `BINARY_AVG_SIZE` | 8192 bytes |
| `BINARY_MAX_SIZE` | 65536 bytes |

## Deduplication

Chunks are keyed by the SHA-256 hash of their **raw** (uncompressed) content. When two blobs share identical regions, those regions produce chunks with the same SHA-256 hash.

The insertion uses `INSERT OR IGNORE`:

```sql
INSERT OR IGNORE INTO chunks (chunk_sha, data, compression) VALUES (?, ?, ?)
```

If a chunk with that SHA-256 already exists, the insert is silently skipped. This means:

- The first blob to introduce a chunk stores it
- Subsequent blobs reference the same chunk row
- No extra logic needed — SQLite's constraint handling does the dedup

The `object_chunks` table is a many-to-many mapping: multiple objects can reference the same chunk.

### SHA-256 on Raw Data

The chunk SHA is always computed on the raw data, even when compression is enabled. This is critical: it means the same content produces the same chunk SHA regardless of whether it was inserted with compression on or off. Deduplication works correctly even in mixed-mode databases.

## Compression

### How It Works

When compression is enabled (`compression = 'zlib'`), new chunks are compressed with `zlib.compress()` before storage:

```python
if self._compression == "zlib":
    stored_data = zlib.compress(chunk_data)
else:
    stored_data = chunk_data
```

The `compression` column in the `chunks` table records the method used for each chunk.

### On Read

When reassembling a chunked object, each chunk is decompressed according to its stored `compression` value:

```python
for data, compression in chunk_rows:
    parts.append(self._decompress(bytes(data), compression))
```

### Mixed Mode

A single database can contain both compressed and uncompressed chunks. This happens when:

- Compression is enabled after some objects were already stored
- Compression is toggled off and then on again
- A chunk was first stored uncompressed, then the same chunk is referenced by a new object stored with compression on — the existing uncompressed chunk is kept (INSERT OR IGNORE)

Mixed mode is fully supported. Each chunk records its own compression method.

### What's NOT Compressed

- Inline objects (non-blob objects and small blobs) — data stored directly in `objects.data` is never compressed
- Only chunk data in the `chunks` table is affected by the compression setting

## Pack Ingestion

When fetching from or pushing to a remote, Git uses the pack protocol. dulwich-sqlite handles incoming packs by unpacking them into individual objects.

### Flow

1. `add_pack()` returns a `(fileobj, commit_fn, abort_fn)` tuple
2. Pack data is written to a `SpooledTemporaryFile` (up to 200 MB in memory, then spills to disk)
3. On `commit_fn()`:
   - The pack is parsed with `PackData.from_file()`
   - Objects are extracted via `PackInflater.for_pack_data()`, which resolves delta chains
   - Each resolved object is inserted via `_insert_object()` — the same path as `add_object`, including chunking
   - All inserts happen in a single transaction (`with self._conn:`)
4. On `abort_fn()`: the temporary file is closed and discarded

### No Persistent Packs

Unlike standard Git, dulwich-sqlite never stores packfiles. All incoming pack data is immediately unpacked into individual objects. The `packs` property always returns `[]`.

This matches the behavior of Dulwich's `MemoryObjectStore`.

### Thin Packs

`add_thin_pack()` handles thin packs (which reference objects the receiver already has). It uses `PackStreamCopier` to verify and copy the pack data, then commits via the same `add_pack()` flow.

## Transaction Model

### Single-Object Operations

`add_object()` inserts the object and immediately commits:

```python
def add_object(self, obj):
    self._insert_object(obj)
    self._conn.commit()
```

### Batch Operations

`add_objects()` wraps all inserts in a single transaction:

```python
def add_objects(self, objects, progress=None):
    with self._conn:
        for obj, path in objects:
            self._insert_object(obj)
```

### Ref CAS (Compare-and-Swap)

Ref mutations use SQLite's locking for atomicity:

- **Unconditional set/delete**: Uses `BEGIN IMMEDIATE` to ensure the read-then-write is atomic
- **Compare-and-swap**: Uses a single `UPDATE ... WHERE name = ? AND value = ?` statement — the comparison and update are atomic within SQLite
- **Add-if-new**: Relies on the `PRIMARY KEY` constraint via `INSERT INTO refs` — SQLite rejects the insert atomically if the key exists

### Context Manager

Using `SqliteRepo` as a context manager ensures the connection is closed on exit, but does not wrap the entire session in a transaction. Each operation manages its own transactions.
