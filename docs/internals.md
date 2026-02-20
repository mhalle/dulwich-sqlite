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
INSERT OR REPLACE INTO objects (sha, type_num, data, total_size, compression)
VALUES (?, ?, ?, ?, ?)
```

The object data goes in the `data` column (compressed if compression is enabled). `total_size` is always set to the raw (uncompressed) data size. `compression` records the method used (`'none'`, `'zlib'`, or `'zstd'`).

### Chunked Storage

For blobs >= 4096 bytes that produce multiple chunks:

1. For each chunk, insert into the chunk store (dedup via `INSERT OR IGNORE`), then get its rowid:
   ```sql
   INSERT OR IGNORE INTO chunks (chunk_sha, data, compression, raw_size) VALUES (?, ?, ?, ?)
   SELECT rowid FROM chunks WHERE chunk_sha = ?
   ```
   Where `chunk_sha` is a 32-byte binary SHA-256 digest and `raw_size` is the decompressed chunk size in bytes.

2. Pack all chunk rowids into a delta-zigzag-varint blob:
   ```python
   packed = pack_chunk_refs(chunk_rowids)
   ```

3. Insert the object row with NULL data and the packed chunk_refs:
   ```sql
   INSERT OR REPLACE INTO objects (sha, type_num, data, chunk_refs, total_size, compression)
   VALUES (?, ?, NULL, ?, ?, 'none')
   ```
   Where `sha` is the 20-byte binary SHA-1 of the Git object.

### Delta-Varint Encoding

The `chunk_refs` blob uses delta-zigzag-varint encoding for compact storage of ordered chunk rowids:

1. **First value**: Encoded as an unsigned LEB128 varint (absolute rowid)
2. **Subsequent values**: Signed delta from previous value, zigzag-encoded as unsigned LEB128 varint
3. **Zigzag encoding**: `(delta << 1) ^ (delta >> 63)` — maps signed integers to unsigned, so delta=1 encodes as `0x02` (1 byte instead of 8)

Since ~56% of consecutive chunk rowids differ by exactly 1 (consecutive inserts), most deltas are 1 byte. This reduces `chunk_refs` storage by ~81% compared to fixed 8-byte LE integers.

Use `pack_chunk_refs()` / `unpack_chunk_refs()` from `dulwich_sqlite.object_store` to encode/decode.

### Binary SHA Storage

Object SHAs are stored as 20-byte binary BLOBs (SHA-1) instead of 40-character hex TEXT. Chunk SHAs are stored as 32-byte binary BLOBs (SHA-256) instead of 64-character hex TEXT. This halves storage for both data and indices.

Generated virtual columns `sha_hex` and `chunk_sha_hex` provide lowercase hex representations for human-readable SQL queries without storage overhead.

## Byte Range Access

The `get_raw_range(name, offset, length)` method reads a byte range from an object without reassembling the entire blob. This is efficient for large chunked objects where only a small portion is needed.

### How It Works

Each chunk stores its decompressed size in the `raw_size` column. This enables computing cumulative byte offsets without decompressing:

```
chunk_refs: [rowid_0, rowid_1, rowid_2, ...]
raw_sizes:  [size_0,  size_1,  size_2,  ...]
cumulative: [0, size_0, size_0+size_1, size_0+size_1+size_2, ...]

Request: offset=5000, length=100
  -> find first chunk where cumulative[i+1] > 5000
  -> find last chunk where cumulative[i] < 5100
  -> fetch + decompress only those chunks
  -> slice the assembled range relative to first chunk's start
```

For a typical p99 chunk of ~4 KB, a range read touching one chunk uses ~4 KB of memory instead of the full object size (up to 1.7 MB).

### Inline Objects

For inline objects (data stored directly in the `objects` table), the full data is decompressed and sliced. This is acceptable because inline objects are small by definition (< 4 KB or single-chunk blobs).

### Clamping

The method clamps to object bounds: if `offset + length` exceeds the total size, available data is returned. If `offset` is past the end, empty bytes are returned.

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
- Subsequent blobs reference the same chunk row via their `chunk_refs` blob
- No extra logic needed — SQLite's constraint handling does the dedup

Multiple objects can reference the same chunk rowid in their `chunk_refs` blobs.

### SHA-256 on Raw Data

The chunk SHA is always computed on the raw data, even when compression is enabled. This is critical: it means the same content produces the same chunk SHA regardless of whether it was inserted with compression on or off. Deduplication works correctly even in mixed-mode databases.

## Compression

### How It Works

When compression is enabled, new chunks are compressed before storage using the configured method:

```python
stored_data = self._compress(chunk_data)
```

The `_compress()` method dispatches based on the current compression setting:
- `"none"`: no compression
- `"zlib"`: standard zlib compression
- `"zstd"`: zstandard compression (level 3), optionally with a trained dictionary

The `compression` column in the `chunks` table records the method used for each chunk.

### zstd Compression

zstd (Zstandard) is the default compression method when `compress=True`. It offers better compression ratios and faster speed than zlib.

**Type-specific dictionary training**: Different Git object types have distinct internal structures, so `train_dictionary()` trains separate dictionaries for each type:

| Named file key | Dict name | Used for |
|---|---|---|
| `_zstd_dict_commit` | `commit` | Inline objects with type_num=1 |
| `_zstd_dict_tree` | `tree` | Inline objects with type_num=2 |
| `_zstd_dict_chunk` | `chunk` | All chunk data |
| `_zstd_dict` (legacy) | `legacy` | Backward compat: data compressed with old single dict |

Inline blobs (type_num=3) and tags (type_num=4) are compressed without a dictionary — they are too small or rare for a dictionary to help.

**Decompression (dict_id-based lookup)**: zstd frames contain a `dict_id` header field identifying which dictionary was used (0 = no dict). On decompression, the frame header is read to determine the correct dictionary automatically:

```python
params = zstandard.get_frame_parameters(data)
dict_data = self._zstd_dicts_by_id.get(params.dict_id)
```

This handles all cases — type-specific dicts, legacy single dict, and no-dict frames — without try/except.

**Re-compression**: When `train_dictionary()` trains new dictionaries, it re-compresses all existing zstd data with the appropriate type-specific dictionary and removes the legacy single dictionary if present. The dictionaries are loaded automatically when opening a repository. `clone_from()` trains dictionaries automatically after fetching when using zstd.

### On Read

When reassembling a chunked object, each chunk is decompressed according to its stored `compression` value:

```python
for data, compression in chunk_rows:
    parts.append(self._decompress(bytes(data), compression))
```

### Mixed Mode

A single database can contain chunks with different compression methods (`'none'`, `'zlib'`, `'zstd'`). This happens when:

- Compression is enabled after some objects were already stored
- Compression method is changed (e.g., from zlib to zstd)
- A chunk was first stored uncompressed, then the same chunk is referenced by a new object stored with compression on — the existing uncompressed chunk is kept (INSERT OR IGNORE)

Mixed mode is fully supported. Each chunk records its own compression method.

### Inline Object Compression

Since schema v8, inline objects (commits, trees, tags, and small blobs) are also compressed when compression is enabled. The `objects` table has its own `compression` column that records the method used for each inline object's data. On read, `get_raw()` decompresses inline data using this column.

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
