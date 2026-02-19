# dulwich-sqlite

A SQLite storage backend for [Dulwich](https://www.dulwich.io/), the pure-Python Git implementation. Stores an entire bare Git repository — objects, refs, and config — in a single SQLite file.

## Why?

- **Portable** — a Git repo is a single `.db` file you can copy, email, or embed
- **No filesystem layout** — no `.git/objects/`, no loose files, no packfiles on disk
- **Embeddable** — use Git data structures inside any application that already uses SQLite
- **Transactional** — writes go through SQLite's WAL journal, giving you atomic commits for free
- **Deduplicated** — large blobs are content-chunked so shared regions across versions are stored once
- **Searchable** — substring search across blob content (inline and chunked)

## Installation

```
pip install dulwich-sqlite
```

Requires Python 3.12+ and `dulwich >= 1.0.0`, `fastcdc`. SQLite is in the standard library.

## Quick Start

```python
from dulwich_sqlite import SqliteRepo
from dulwich.objects import Blob, Tree, Commit
import time

# Create a repository
repo = SqliteRepo.init_bare("my-repo.db")

# Store a blob, tree, and commit
blob = Blob.from_string(b"hello world")
repo.object_store.add_object(blob)

tree = Tree()
tree.add(b"greeting.txt", 0o100644, blob.id)
repo.object_store.add_object(tree)

commit = Commit()
commit.tree = tree.id
commit.author = commit.committer = b"Alice <alice@example.com>"
commit.author_time = commit.commit_time = int(time.time())
commit.author_timezone = commit.commit_timezone = 0
commit.encoding = b"UTF-8"
commit.message = b"Initial commit"
repo.object_store.add_object(commit)

repo.refs[b"refs/heads/main"] = commit.id
repo.close()

# Reopen and read back
repo = SqliteRepo("my-repo.db")
head = repo.refs[b"refs/heads/main"]
print(repo.object_store[head].message)  # b"Initial commit"
repo.close()
```

## Documentation

| Document | Description |
|---|---|
| [Getting Started](docs/getting-started.md) | Installation, creating repos, storing and reading objects, cloning, fetch/push, compression |
| [API Reference](docs/api.md) | Full reference for `SqliteRepo`, `SqliteObjectStore`, and `SqliteRefsContainer` |
| [Database Schema](docs/schema.md) | All 8 tables, columns, generated columns, indexes, pragmas, migrations |
| [Querying](docs/querying.md) | SQL examples for exploring objects, chunks, refs, text search, and storage analysis |
| [Internals](docs/internals.md) | Chunking algorithms, deduplication, compression, pack ingestion, transaction model |
| [Git Comparison](docs/git-comparison.md) | What's identical to standard Git, what's different, strengths, and limitations |

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
- **Content search tests** — LIKE substring search across inline and chunked blobs
- **Ref CAS tests** — compare-and-swap, add-if-new, symbolic refs
- **Repo tests** — init, reopen, config persistence, named files
- **Integration tests** — full commit workflows, cross-repo fetch, branch operations

## License

See the project license file.
