# API Reference

## SqliteRepo

`SqliteRepo` is the main entry point. It extends Dulwich's `BaseRepo` and stores all repository data in a single SQLite database. It is always bare (no working tree or index).

### Constructor

```python
SqliteRepo(db_path: str)
```

Opens an existing dulwich-sqlite repository. Applies WAL pragmas, verifies the schema version, and auto-migrates v3/v4 databases to the current schema (v5).

**Raises:** `NotGitRepository` if the file is not a valid dulwich-sqlite database or has an unsupported schema version.

### Class Methods

#### `init_bare`

```python
@classmethod
SqliteRepo.init_bare(db_path: str, *, compress: bool = False) -> SqliteRepo
```

Creates a new bare repository in a SQLite file. Initializes the schema, sets up default metadata, and returns an open `SqliteRepo`.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `db_path` | `str` | required | Path for the new SQLite database file |
| `compress` | `bool` | `False` | Enable zlib compression for chunk data |

#### `clone_from`

```python
@classmethod
SqliteRepo.clone_from(
    source: str,
    db_path: str,
    *,
    origin: str = "origin",
    compress: bool = False,
    depth: int | None = None,
    branch: str | bytes | None = None,
    errstream: BinaryIO | None = None,
) -> SqliteRepo
```

Clones a remote repository into a new SQLite database. Sets up remote tracking config and fetches objects via `dulwich.porcelain.fetch`.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `source` | `str` | required | URL or local path of the source repository |
| `db_path` | `str` | required | Path for the new SQLite database file |
| `origin` | `str` | `"origin"` | Name for the remote |
| `compress` | `bool` | `False` | Enable zlib compression |
| `depth` | `int \| None` | `None` | Shallow clone depth (number of commits) |
| `branch` | `str \| bytes \| None` | `None` | Branch to set as HEAD (default: remote HEAD) |
| `errstream` | `BinaryIO \| None` | `None` | Stream for progress output |

### Instance Methods

#### `fetch`

```python
repo.fetch(
    remote_location: str = "origin",
    *,
    depth: int | None = None,
    errstream: BinaryIO | None = None,
) -> FetchPackResult
```

Fetches objects and refs from a remote. When `remote_location` is a configured remote name, remote tracking refs are updated automatically.

**Returns:** `dulwich.client.FetchPackResult` with remote refs and symrefs.

#### `push`

```python
repo.push(
    remote_location: str | None = None,
    refspecs: str | bytes | list[str | bytes] | None = None,
    *,
    errstream: BinaryIO | None = None,
) -> None
```

Pushes refs and objects to a remote repository via `dulwich.porcelain.push`.

#### `close`

```python
repo.close() -> None
```

Closes the object store and the underlying SQLite connection. Always call this when done (or use the context manager).

#### `get_config`

```python
repo.get_config() -> ConfigFile
```

Returns the in-memory `dulwich.config.ConfigFile`. Loaded from the `named_files` table on open.

#### `get_description` / `set_description`

```python
repo.get_description() -> bytes | None
repo.set_description(description: bytes) -> None
```

Read/write the repository description (stored in `named_files` as `"description"`).

#### `enable_compression` / `disable_compression`

```python
repo.enable_compression(method: str = "zlib") -> None
repo.disable_compression() -> None
```

Toggle compression for future chunk writes. Only `"zlib"` is currently supported. Existing chunks are not modified.

#### `read_reflog`

```python
repo.read_reflog(ref: bytes) -> Generator[reflog.Entry, None, None]
```

Yields `dulwich.reflog.Entry` objects for the given ref, in chronological order.

#### `open_index`

```python
repo.open_index()
```

Always raises `NoIndexPresent`. dulwich-sqlite repos are always bare.

### Context Manager

```python
with SqliteRepo.init_bare("repo.db") as repo:
    ...
# Automatically calls repo.close()
```

### Instance Attributes

| Attribute | Type | Description |
|---|---|---|
| `repo.object_store` | `SqliteObjectStore` | Object store for reading/writing Git objects |
| `repo.refs` | `SqliteRefsContainer` | Refs container for branches, tags, HEAD |
| `repo.path` | `str` | The database file path |
| `repo.bare` | `bool` | Always `True` |

---

## SqliteObjectStore

Extends Dulwich's `PackCapableObjectStore`. Stores Git objects in SQLite, with optional content-defined chunking for large blobs.

### Constructor

```python
SqliteObjectStore(conn: sqlite3.Connection)
```

Not typically called directly — access via `repo.object_store`.

### Object Storage

#### `add_object`

```python
store.add_object(obj: ShaFile) -> None
```

Stores a single Git object. Commits the transaction immediately.

For blobs >= 4096 bytes that produce multiple chunks, the blob is stored in chunked form (data column set to NULL, chunks in the `chunks` table). Otherwise the full data is stored inline in the `objects` table.

#### `add_objects`

```python
store.add_objects(
    objects: Iterable[tuple[ShaFile, str | None]],
    progress: Callable[[str], None] | None = None,
) -> None
```

Stores multiple objects atomically in a single transaction. Each element is a `(object, path)` tuple; the path is ignored but kept for API compatibility with Dulwich.

### Object Retrieval

#### `__contains__`

```python
sha in store -> bool
```

Returns `True` if the object exists. Uses `contains_loose` internally.

#### `__getitem__`

```python
store[sha] -> ShaFile
```

Returns the parsed Dulwich object. Inherited from `BaseObjectStore` — calls `get_raw` internally.

#### `get_raw`

```python
store.get_raw(name: RawObjectID | ObjectID) -> tuple[int, bytes]
```

Returns `(type_num, raw_data)`. For chunked objects, reassembles the data from chunks (decompressing any zlib-compressed chunks).

| `type_num` | Object Type |
|---|---|
| 1 | commit |
| 2 | tree |
| 3 | blob |
| 4 | tag |

**Raises:** `KeyError` if the object does not exist.

#### `get_object_size`

```python
store.get_object_size(sha: ObjectID | RawObjectID) -> int
```

Returns the object's data size in bytes. For inline objects this is `length(data)`. For chunked objects this is the stored `total_size`.

**Raises:** `KeyError` if the object does not exist.

#### `__iter__`

```python
iter(store) -> Iterator[ObjectID]
```

Yields the hex SHA of every stored object.

### Pack Ingestion

These methods handle incoming pack data from fetch/push operations. Pack data is never stored as-is — it's unpacked into individual objects via `PackInflater`.

#### `add_pack`

```python
store.add_pack() -> tuple[BinaryIO, Callable[[], None], Callable[[], None]]
```

Returns `(fileobj, commit_fn, abort_fn)`. Write pack data to `fileobj`, then call `commit_fn()` to unpack and insert all objects atomically, or `abort_fn()` to discard.

#### `add_pack_data`

```python
store.add_pack_data(
    count: int,
    unpacked_objects: Iterator[UnpackedObject],
    progress: Callable[[str], None] | None = None,
) -> None
```

Writes unpacked objects as pack data, then ingests the pack.

#### `add_thin_pack`

```python
store.add_thin_pack(
    read_all: Callable[[int], bytes],
    read_some: Callable[[int], bytes] | None,
    progress: Callable[[str], None] | None = None,
) -> None
```

Ingests a thin pack from a stream (used during fetch).

### Content Search

#### `search_content`

```python
store.search_content(query: str, *, limit: int | None = None) -> list[ObjectID]
```

Searches blob content for a substring match. Returns a list of matching object SHAs.

The search works in two passes:
1. SQL `LIKE` on uncompressed chunks and inline blobs (fast, done in SQLite)
2. Python-side search on compressed chunks (slower, requires decompression)

| Parameter | Type | Default | Description |
|---|---|---|---|
| `query` | `str` | required | Substring to search for |
| `limit` | `int \| None` | `None` | Maximum number of results |

### Properties

| Property | Type | Description |
|---|---|---|
| `store.packs` | `list[Pack]` | Always returns `[]` (no packfiles) |
| `store.pack_compression_level` | `int` | Set to `-1` (default zlib level) |

---

## SqliteRefsContainer

Extends Dulwich's `RefsContainer`. Stores Git refs (branches, tags, HEAD) in the `refs` table.

### Constructor

```python
SqliteRefsContainer(conn: sqlite3.Connection, logger: Callable | None = None)
```

Not typically called directly — access via `repo.refs`.

### Reading Refs

#### `__getitem__`

```python
refs[name] -> ObjectID
```

Returns the SHA the ref points to. For symbolic refs, follows the chain. Inherited from `RefsContainer`.

#### `allkeys`

```python
refs.allkeys() -> set[Ref]
```

Returns a set of all ref names (as bytes).

#### `read_loose_ref`

```python
refs.read_loose_ref(name: Ref) -> bytes | None
```

Returns the raw value of a ref, or `None` if it doesn't exist. Does not follow symbolic refs.

#### `get_packed_refs`

```python
refs.get_packed_refs() -> dict[Ref, ObjectID]
```

Always returns `{}`. There are no packed refs in dulwich-sqlite.

#### `get_peeled`

```python
refs.get_peeled(name: Ref) -> ObjectID | None
```

Returns the cached peeled value from the `peeled_refs` table, or `None`.

### Writing Refs

#### `__setitem__`

```python
refs[name] = sha
```

Sets a ref unconditionally. Inherited from `RefsContainer`, calls `set_if_equals` with `old_ref=None`.

#### `__delitem__`

```python
del refs[name]
```

Deletes a ref unconditionally. Inherited from `RefsContainer`, calls `remove_if_equals` with `old_ref=None`.

#### `set_if_equals`

```python
refs.set_if_equals(
    name: Ref,
    old_ref: ObjectID | None,
    new_ref: ObjectID,
    committer: bytes | None = None,
    timestamp: int | None = None,
    timezone: int | None = None,
    message: bytes | None = None,
) -> bool
```

Atomic compare-and-swap. Updates the ref only if its current value matches `old_ref`. Pass `old_ref=None` for an unconditional set (uses `BEGIN IMMEDIATE` for atomicity). Returns `True` on success, `False` if the CAS check failed.

Handles `ZERO_SHA` as a special case meaning "ref should not exist" — falls back to an atomic INSERT.

#### `add_if_new`

```python
refs.add_if_new(
    name: Ref,
    ref: ObjectID,
    committer: bytes | None = None,
    timestamp: int | None = None,
    timezone: int | None = None,
    message: bytes | None = None,
) -> bool
```

Creates a ref only if it doesn't already exist. Uses SQLite's PRIMARY KEY constraint for atomicity. Returns `True` on success, `False` if the ref already exists.

#### `remove_if_equals`

```python
refs.remove_if_equals(
    name: Ref,
    old_ref: ObjectID | None,
    committer: bytes | None = None,
    timestamp: int | None = None,
    timezone: int | None = None,
    message: bytes | None = None,
) -> bool
```

Deletes a ref, optionally checking the current value first. Pass `old_ref=None` for unconditional delete. Returns `True` on success.

#### `set_symbolic_ref`

```python
refs.set_symbolic_ref(
    name: Ref,
    other: Ref,
    committer: bytes | None = None,
    timestamp: int | None = None,
    timezone: int | None = None,
    message: bytes | None = None,
) -> None
```

Creates a symbolic ref. Stores the value as `b"ref: " + other` in the `refs` table. Uses `BEGIN IMMEDIATE` for atomicity.

### Reflog

All ref mutations (`set_if_equals`, `add_if_new`, `remove_if_equals`, `set_symbolic_ref`) automatically write reflog entries via the logger callback. The `SqliteRepo` constructor wires this up to `_write_reflog`, which inserts into the `reflog` table.
