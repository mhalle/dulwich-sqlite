# Git Comparison

How dulwich-sqlite compares to a standard Git repository.

## What's Identical

These aspects work exactly the same as standard Git:

- **Object model**: Blobs, trees, commits, and tags with the same content and structure
- **SHA-1 addressing**: Objects are identified by the same SHA-1 hashes
- **Ref semantics**: Branches, tags, HEAD, and symbolic refs behave the same way
- **Reflog**: Ref mutations are logged with the same entry structure (old SHA, new SHA, committer, timestamp, message)
- **Pack protocol**: Fetch and push interoperate with any standard Git server or repository. dulwich-sqlite can clone from GitHub, push to a bare repo, etc.
- **Config format**: Repository config uses the same INI-like format via Dulwich's `ConfigFile`

## What's Different

### Storage Model

| Aspect | Standard Git | dulwich-sqlite |
|---|---|---|
| Storage format | Loose objects + packfiles on the filesystem | Single SQLite file |
| Filesystem layout | `.git/objects/`, `.git/refs/`, etc. | No filesystem layout |
| Packfiles | Delta-compressed packs with idx files | No packfiles — objects are stored individually |
| Loose objects | Individual files in `objects/xx/yyyy...` | Rows in the `objects` table |
| Object compression | zlib per loose object, delta chains in packs | Optional zstd or zlib per chunk and per inline object |

### Always Bare

dulwich-sqlite repositories are always bare. There is no working tree and no index. Calling `open_index()` raises `NoIndexPresent`.

### Chunking Layer

Standard Git does not have a chunking layer. dulwich-sqlite splits large blobs (>= 4 KB) into content-defined chunks for deduplication across blob versions. This is an additional layer that sits between the Git object model and the SQLite storage.

### No Delta Compression

Standard Git packfiles use delta compression: similar objects are stored as a base object plus a delta. This can be extremely space-efficient for repositories with many similar versions of the same file.

dulwich-sqlite stores full data for each chunk. Deduplication comes from content-defined chunking — if two blob versions share unchanged regions, those regions produce identical chunks that are stored once. This is a different trade-off: simpler implementation, but less compression than optimal delta chains.

### No Garbage Collection

In standard Git, `git gc` repacks loose objects, prunes unreachable objects, and optimizes storage. dulwich-sqlite has no GC mechanism. Specifically:

- Orphaned chunks (chunks no longer referenced by any object) can accumulate if objects are replaced
- There is no `repack` equivalent since there are no packfiles

## Strengths

### Single-File Portability

The entire repository is a single `.db` file. Copy it, email it, embed it in another application, back it up — just one file.

### Atomic Transactions

SQLite's WAL mode provides atomic transactions. Writing multiple objects via `add_objects()` is all-or-nothing. Ref updates use compare-and-swap semantics backed by SQLite's locking. No risk of a half-written packfile or a corrupt ref.

### Content Deduplication

The chunking layer provides cross-version deduplication for blob content. When a file changes a few lines, only the affected chunks are new — the rest are shared with previous versions. Standard Git achieves similar savings with delta compression in packfiles, but dulwich-sqlite's approach works without requiring a separate `gc` step.

### SQL Queryability

The database is directly queryable with SQL. You can:

- Search blob content with `LIKE`
- Aggregate object sizes and types
- Analyze storage efficiency (chunk sharing, compression ratios)
- List refs and reflog entries
- Join across tables for custom analysis

See [Querying the Database](querying.md) for examples.

### Embeddable

Any application that already uses SQLite can embed a full Git repository. No need to shell out to `git` or manage a filesystem layout. The entire API is Python with SQLite — no external processes.

### No GC Needed for Dedup

Chunk deduplication is handled at insert time via `INSERT OR IGNORE`. There's no separate garbage collection pass needed to realize dedup savings — they happen immediately as objects are stored.

## Limitations

### Always Bare

No working tree, no index, no checkout. dulwich-sqlite is designed for programmatic access to Git data, not as a replacement for `git` on the command line.

### No Delta Compression

Without delta chains, dulwich-sqlite databases may be larger than an optimally packed Git repository, especially for repositories with many versions of large files. The chunking dedup helps, but delta compression can be more efficient in many cases.

### Substring Search Only

Content search uses SQL `LIKE`, which is substring matching only. There is no regex support and no FTS5 full-text search index. For compressed chunks, search requires Python-side decompression, which is slower.

### Single Writer

SQLite's locking model allows only one writer at a time. Concurrent readers are fine (WAL mode), but concurrent writes will block (up to 5 seconds via `busy_timeout`) and then fail. This is fine for most use cases but not for high-concurrency write workloads.

### No Shallow Clone Persistence

The `depth` parameter on `clone_from` and `fetch` fetches limited history, but dulwich-sqlite does not track shallow boundaries. There is no equivalent to Git's `.git/shallow` file. A subsequent fetch without `depth` will fetch the full history.

### Orphaned Chunks

When a chunked object is replaced (via `INSERT OR REPLACE`), the old `chunk_refs` blob is overwritten, but the chunks themselves remain in the `chunks` table. Over time, chunks that are no longer referenced by any object's `chunk_refs` can accumulate. There is no built-in cleanup mechanism yet.

## Compression: Off vs zlib vs zstd

| Aspect | Off | zlib | zstd |
|---|---|---|---|
| Database size | Largest | ~40-60% smaller | ~55-65% smaller (with dictionary) |
| Compressed data | Nothing | Chunks and inline objects | Chunks and inline objects |
| SQL search | `LIKE` works on all data | Compressed data needs Python decompression | Compressed data needs Python decompression |
| Write speed | Fastest | Slower | Faster than zlib |
| Read speed | Direct reads | Decompression needed | Decompression needed (faster than zlib) |
| Dictionary support | N/A | No | Yes — trained dictionaries improve ratios further |
| Mixed mode | N/A | All methods coexist | All methods coexist |

**Recommendation**: Use `compress=True` (zstd) for most repositories. It provides the best compression ratios with fast performance. Both chunk data and inline objects (commits, trees, tags, small blobs) are compressed. Leave compression off only when fast SQL `LIKE` queries on all content are critical.
