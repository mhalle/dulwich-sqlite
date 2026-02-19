# Getting Started

## Installation

Install from PyPI:

```bash
pip install dulwich-sqlite
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv pip install dulwich-sqlite
```

### Dependencies

- Python 3.12+
- [dulwich](https://www.dulwich.io/) >= 1.0.0 (pure-Python Git implementation)
- [fastcdc](https://pypi.org/project/fastcdc/) >= 1.5.0 (content-defined chunking for binary blobs)
- SQLite 3 (included in the Python standard library)

## Creating a Repository

Use `SqliteRepo.init_bare()` to create a new bare Git repository in a SQLite file:

```python
from dulwich_sqlite import SqliteRepo

repo = SqliteRepo.init_bare("my-repo.db")
```

This creates a new SQLite database at the given path, initializes the schema, and returns an open `SqliteRepo` instance.

To enable zlib compression for stored chunks:

```python
repo = SqliteRepo.init_bare("my-repo.db", compress=True)
```

## Storing Objects

dulwich-sqlite uses the standard Dulwich object model. You create blob, tree, commit, and tag objects with Dulwich, then store them via the object store.

### Full Workflow: Blob, Tree, Commit

```python
from dulwich_sqlite import SqliteRepo
from dulwich.objects import Blob, Tree, Commit
import time

repo = SqliteRepo.init_bare("my-repo.db")

# 1. Create and store a blob
blob = Blob.from_string(b"hello world\n")
repo.object_store.add_object(blob)

# 2. Build a tree referencing the blob
tree = Tree()
tree.add(b"greeting.txt", 0o100644, blob.id)
repo.object_store.add_object(tree)

# 3. Create a commit pointing at the tree
commit = Commit()
commit.tree = tree.id
commit.author = commit.committer = b"Alice <alice@example.com>"
commit.author_time = commit.commit_time = int(time.time())
commit.author_timezone = commit.commit_timezone = 0
commit.encoding = b"UTF-8"
commit.message = b"Initial commit"
repo.object_store.add_object(commit)

# 4. Point a branch at the commit
repo.refs[b"refs/heads/main"] = commit.id

repo.close()
```

### Storing Multiple Objects Atomically

`add_objects` wraps all inserts in a single transaction:

```python
repo.object_store.add_objects([
    (blob, None),
    (tree, None),
    (commit, None),
])
```

## Reading Objects Back

Open an existing repository and read objects by SHA:

```python
repo = SqliteRepo("my-repo.db")

# Follow a branch ref to the commit
head_sha = repo.refs[b"refs/heads/main"]
commit = repo.object_store[head_sha]
print(commit.message)  # b"Initial commit"

# Walk from commit to tree to blob
tree = repo.object_store[commit.tree]
for item in tree.items():
    print(item.path, item.sha)
    blob = repo.object_store[item.sha]
    print(blob.data)

repo.close()
```

### Checking Existence

```python
sha = b"a1b2c3..."
if sha in repo.object_store:
    print("Object exists")
```

### Getting Raw Data

```python
type_num, raw_data = repo.object_store.get_raw(sha)
# type_num: 1=commit, 2=tree, 3=blob, 4=tag
```

### Getting Object Size

```python
size = repo.object_store.get_object_size(sha)
```

## Working with Refs

Refs (branches, tags, HEAD) are accessed through `repo.refs`:

```python
# Set a branch
repo.refs[b"refs/heads/main"] = commit.id

# Read a branch
sha = repo.refs[b"refs/heads/main"]

# Delete a branch
del repo.refs[b"refs/heads/main"]

# List all refs
for ref in repo.refs.allkeys():
    print(ref)
```

### Symbolic Refs

HEAD is typically a symbolic ref pointing to a branch:

```python
repo.refs.set_symbolic_ref(b"HEAD", b"refs/heads/main")
```

### Compare-and-Swap

For concurrent safety, use compare-and-swap operations:

```python
# Only update if the current value matches
success = repo.refs.set_if_equals(
    b"refs/heads/main",
    old_sha,  # expected current value
    new_sha,  # new value to set
)

# Only create if the ref doesn't exist yet
success = repo.refs.add_if_new(b"refs/heads/feature", commit_sha)

# Only delete if the current value matches
success = repo.refs.remove_if_equals(b"refs/heads/feature", expected_sha)
```

## Context Manager

`SqliteRepo` supports the context manager protocol, which ensures the database connection is closed properly:

```python
with SqliteRepo.init_bare("my-repo.db") as repo:
    blob = Blob.from_string(b"data")
    repo.object_store.add_object(blob)
# Connection automatically closed here
```

## Cloning from a Remote

Clone any Git repository (local or remote) into a SQLite database:

```python
repo = SqliteRepo.clone_from("https://github.com/user/project.git", "project.db")
```

With options:

```python
repo = SqliteRepo.clone_from(
    "https://github.com/user/project.git",
    "project.db",
    origin="origin",       # remote name (default: "origin")
    compress=True,         # enable zlib compression
    depth=10,              # shallow clone with 10 commits
    branch="develop",      # checkout this branch as HEAD
)
```

This sets up remote tracking config (equivalent to `git clone --bare`), fetches all reachable objects, and points HEAD at the default branch.

## Fetching and Pushing

### Fetch

Fetch new objects and update remote tracking refs:

```python
repo = SqliteRepo("project.db")

# Fetch from the configured "origin" remote
result = repo.fetch()

# Fetch from a specific remote or URL
result = repo.fetch("upstream")
result = repo.fetch("https://github.com/user/project.git")

# Shallow fetch
result = repo.fetch(depth=5)

repo.close()
```

### Push

Push refs and objects to a remote:

```python
repo = SqliteRepo("project.db")

# Push to "origin"
repo.push("origin", "refs/heads/main")

# Push multiple refspecs
repo.push("origin", ["refs/heads/main", "refs/heads/develop"])

repo.close()
```

## Compression

dulwich-sqlite can optionally compress chunk data with zlib.

### Enable Compression

```python
# At creation time
repo = SqliteRepo.init_bare("my-repo.db", compress=True)

# Or on an existing repo
repo.enable_compression()
```

### Disable Compression

```python
repo.disable_compression()
```

### What Compression Affects

- Compression applies to **new chunks** written after it's enabled
- Existing chunks are **not** re-compressed or decompressed
- A single database can contain both compressed and uncompressed chunks (mixed mode)
- Only chunked blobs (>4 KB) are affected; inline blobs and non-blob objects are stored as-is
- Chunk SHA-256 hashes are computed on the **raw** data, so deduplication works correctly across compression modes

### Trade-offs

| | Compression Off | Compression On |
|---|---|---|
| Database size | Larger | ~40-60% smaller for text |
| SQL search (`LIKE`) | Works directly on all data | Only works on uncompressed chunks; compressed chunks require Python-side decompression |
| Write speed | Faster | Slightly slower (zlib overhead) |
| Read speed | Direct | Decompression on read for compressed chunks |
