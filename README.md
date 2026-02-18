# dulwich-sqlite

A SQLite storage backend for [Dulwich](https://www.dulwich.io/), the pure-Python Git implementation. Stores an entire bare Git repository — objects, refs, and config — in a single SQLite file.

## Why?

- **Portable** — a Git repo is a single `.db` file you can copy, email, or embed
- **No filesystem layout** — no `.git/objects/`, no loose files, no packfiles on disk
- **Embeddable** — use Git data structures inside any application that already uses SQLite
- **Transactional** — writes go through SQLite's WAL journal, giving you atomic commits for free
- **Deduplicated** — large blobs are content-chunked so shared regions across versions are stored once
- **Searchable** — opt-in FTS5 full-text search over blob content with dedup-efficient indexing

## Installation

```
pip install dulwich-sqlite
```

Requires Python 3.12+ and `dulwich >= 1.0.0`, `fastcdc`. SQLite is in the standard library.

## Quick start

### Create a repository

```python
from dulwich_sqlite import SqliteRepo
from dulwich.objects import Blob, Tree, Commit
import time

repo = SqliteRepo.init_bare("my-repo.db")

# Store a blob
blob = Blob.from_string(b"hello world")
repo.object_store.add_object(blob)

# Build a tree
tree = Tree()
tree.add(b"greeting.txt", 0o100644, blob.id)
repo.object_store.add_object(tree)

# Create a commit
commit = Commit()
commit.tree = tree.id
commit.author = commit.committer = b"Alice <alice@example.com>"
commit.author_time = commit.commit_time = int(time.time())
commit.author_timezone = commit.commit_timezone = 0
commit.encoding = b"UTF-8"
commit.message = b"Initial commit"
repo.object_store.add_object(commit)

# Point a branch at the commit
repo.refs[b"refs/heads/main"] = commit.id

repo.close()
```

### Reopen and read back

```python
repo = SqliteRepo("my-repo.db")

head = repo.refs[b"refs/heads/main"]
commit = repo.object_store[head]
print(commit.message)  # b"Initial commit"

repo.close()
```

### Use as a context manager

```python
with SqliteRepo.init_bare("/tmp/repo.db") as repo:
    blob = Blob.from_string(b"data")
    repo.object_store.add_object(blob)
```

### Fetch from another repository

```python
from dulwich.repo import MemoryRepo

source = MemoryRepo.init_bare([], {})
# ... add objects and refs to source ...

target = SqliteRepo.init_bare("fetched.db")
source.fetch(target)  # transfers all reachable objects via pack protocol
target.close()
```

## API

### `SqliteRepo`

| Method | Description |
|---|---|
| `SqliteRepo.init_bare(db_path, fts=False)` | Create a new bare repository in a SQLite file |
| `SqliteRepo(db_path)` | Open an existing repository |
| `repo.object_store` | `SqliteObjectStore` instance for reading/writing git objects |
| `repo.refs` | `SqliteRefsContainer` instance for branches, tags, HEAD |
| `repo.enable_fts()` | Enable FTS5 full-text search (backfills existing data) |
| `repo.disable_fts()` | Disable FTS5 and drop the index |
| `repo.get_config()` | Returns the repository `ConfigFile` |
| `repo.get_description()` | Returns the repository description as bytes |
| `repo.set_description(desc)` | Sets the repository description |
| `repo.close()` | Closes the SQLite connection |

### `SqliteObjectStore`

Extends `PackCapableObjectStore`. Supports the full Dulwich object store interface:

- `add_object(obj)` / `add_objects([(obj, path), ...])` — store git objects
- `obj in store` / `store[sha]` — check existence / retrieve objects
- `store.get_raw(sha)` — returns `(type_num, data)` tuple
- `iter(store)` — iterate over all stored object SHAs
- `add_pack()` / `add_pack_data()` / `add_thin_pack()` — ingest pack data (used by fetch/push)

Pack data is never stored as-is. Incoming packs are unpacked into individual objects via `PackInflater`, matching how `MemoryObjectStore` works.

#### Content search

```python
# Enable FTS at creation or on an existing repo
repo = SqliteRepo.init_bare("repo.db", fts=True)
# repo.enable_fts()  # on an existing repo

# Search blob content (full FTS5 syntax when FTS is enabled)
results = repo.object_store.search_content("def main")
results = repo.object_store.search_content("error OR exception", ranked=True, limit=10)

# Safe quoting for user-provided input (disables FTS operators)
results = repo.object_store.search_content(user_input, quote=True)

# Falls back to LIKE substring matching when FTS is not enabled
```

When FTS is enabled, the index lives on the deduplicated chunks table — shared chunks across blob versions are indexed once. A one-line edit to a large file only adds the changed chunk to the index, not the whole file.

FTS5 operators (AND/OR/NOT, phrases, NEAR) match within a single chunk (~4 KB). To search across an entire blob — e.g. find files containing both "import flask" and "def create_app" even if they're far apart — intersect per-term searches:

```python
store = repo.object_store

# Document-level AND: both terms anywhere in the same blob
flask_hits = set(store.search_content("flask"))
create_app_hits = set(store.search_content("create_app"))
both = flask_hits & create_app_hits

# Document-level OR
either = flask_hits | create_app_hits

# Document-level NOT: has "flask" but not "django"
django_hits = set(store.search_content("django"))
flask_only = flask_hits - django_hits
```

Each `search_content` call uses FTS5 MATCH for fast chunk lookup, then the set operations combine results at the blob level.

### `SqliteRefsContainer`

Extends `RefsContainer`. Supports:

- `refs[name] = sha` / `del refs[name]` — set/delete refs
- `refs.allkeys()` — all ref names
- `set_if_equals(name, old, new)` — compare-and-swap
- `add_if_new(name, ref)` — create only if absent
- `remove_if_equals(name, old)` — delete only if matching
- `set_symbolic_ref(name, target)` — create symbolic refs (e.g., HEAD)

## Schema

| Table | Purpose |
|---|---|
| `objects` | Git objects keyed by hex SHA. Large blobs store `data` as NULL with `total_size` set |
| `chunks` | Deduplicated content chunks keyed by SHA-256 |
| `object_chunks` | Maps objects to their ordered chunk sequence |
| `refs` | Git references (branches, tags, HEAD) as byte names |
| `peeled_refs` | Cached peeled ref values |
| `named_files` | Control directory files (config, description, info/exclude) |
| `metadata` | Schema version tracking |
| `reflog` | Ref change history |
| `chunks_fts` | *(opt-in)* FTS5 external-content index on `chunks` |

Large text blobs (>4 KB) are split into content-defined chunks using line-boundary CDC. Binary blobs use FastCDC. Chunks are deduplicated by SHA-256 — shared content across blob versions is stored once.

SQLite is configured with `journal_mode=WAL`, `synchronous=NORMAL`, and `busy_timeout=5000` for good concurrent read performance.

## Development

```bash
# Install with dev dependencies
uv pip install -e ".[dev]"

# Run tests
uv run pytest tests/ -v
```

The test suite includes:

- **Dulwich's `ObjectStoreTests` mixin** — the same test suite that validates `MemoryObjectStore` and `DiskObjectStore`
- **Chunk deduplication tests** — roundtrip, shared chunks, migration from v3
- **FTS search tests** — LIKE fallback, FTS5 syntax, ranking, quoting, backfill, binary exclusion
- **Ref CAS tests** — compare-and-swap, add-if-new, symbolic refs
- **Repo tests** — init, reopen, config persistence, named files
- **Integration tests** — full commit workflows, cross-repo fetch, branch operations

## License

See the project license file.
