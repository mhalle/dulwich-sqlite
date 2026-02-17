"""Demo of dulwich-sqlite: a full Git repo in a single SQLite file."""

import os
import tempfile

from dulwich.objects import Blob, Commit, Tree
from dulwich.repo import MemoryRepo

from dulwich_sqlite import SqliteRepo

db = os.path.join(tempfile.mkdtemp(), "demo.db")

print("=== 1. Create a bare repo ===")
repo = SqliteRepo.init_bare(db)
print(f"Created: {db}")
print(f"Description: {repo.get_description()}")
print()

print("=== 2. Build a commit history ===")
# First commit
blob1 = Blob.from_string(b"hello world\n")
repo.object_store.add_object(blob1)

tree1 = Tree()
tree1.add(b"greeting.txt", 0o100644, blob1.id)
repo.object_store.add_object(tree1)

c1 = Commit()
c1.tree = tree1.id
c1.author = c1.committer = b"Alice <alice@example.com>"
c1.author_time = c1.commit_time = 1700000000
c1.author_timezone = c1.commit_timezone = 0
c1.encoding = b"UTF-8"
c1.message = b"Initial commit"
repo.object_store.add_object(c1)
repo.refs[b"refs/heads/main"] = c1.id
repo.refs.set_symbolic_ref(b"HEAD", b"refs/heads/main")
print(f"  {c1.id.decode()[:12]}  {c1.message.decode()}")

# Second commit
blob2 = Blob.from_string(b"goodbye world\n")
repo.object_store.add_object(blob2)

tree2 = Tree()
tree2.add(b"greeting.txt", 0o100644, blob1.id)
tree2.add(b"farewell.txt", 0o100644, blob2.id)
repo.object_store.add_object(tree2)

c2 = Commit()
c2.tree = tree2.id
c2.parents = [c1.id]
c2.author = c2.committer = b"Bob <bob@example.com>"
c2.author_time = c2.commit_time = 1700001000
c2.author_timezone = c2.commit_timezone = 0
c2.encoding = b"UTF-8"
c2.message = b"Add farewell"
repo.object_store.add_object(c2)
repo.refs[b"refs/heads/main"] = c2.id
print(f"  {c2.id.decode()[:12]}  {c2.message.decode()}")
print()

print("=== 3. Walk the log ===")
sha = repo.refs[b"refs/heads/main"]
while sha:
    commit = repo.object_store[sha]
    print(f"  {sha.decode()[:12]}  {commit.message.decode().strip()}")
    sha = commit.parents[0] if commit.parents else None
print()

print("=== 4. Read tree at HEAD ===")
head_commit = repo.object_store[repo.refs[b"refs/heads/main"]]
tree = repo.object_store[head_commit.tree]
for item in tree.items():
    print(f"  {oct(item.mode)}  {item.sha.decode()[:12]}  {item.path.decode()}")
print()

print("=== 5. Branch operations ===")
repo.refs[b"refs/heads/feature"] = c1.id
branches = sorted(
    r.decode() for r in repo.refs.allkeys() if r.startswith(b"refs/heads/")
)
print(f"  Branches: {branches}")
del repo.refs[b"refs/heads/feature"]
branches = sorted(
    r.decode() for r in repo.refs.allkeys() if r.startswith(b"refs/heads/")
)
print(f"  After delete: {branches}")
print()

print("=== 6. Persistence — close and reopen ===")
repo.close()
repo2 = SqliteRepo(db)
head = repo2.refs[b"refs/heads/main"]
c = repo2.object_store[head]
print(f"  Reopened. HEAD -> {head.decode()[:12]}  {c.message.decode().strip()}")
print(f"  Objects in store: {sum(1 for _ in repo2.object_store)}")
repo2.close()
print()

print("=== 7. Fetch from a MemoryRepo into SqliteRepo ===")
db2 = os.path.join(tempfile.mkdtemp(), "fetched.db")
source = MemoryRepo.init_bare([], {})
b = Blob.from_string(b"fetched content")
source.object_store.add_object(b)
t = Tree()
t.add(b"file.txt", 0o100644, b.id)
source.object_store.add_object(t)
fc = Commit()
fc.tree = t.id
fc.author = fc.committer = b"Carol <carol@example.com>"
fc.author_time = fc.commit_time = 1700002000
fc.author_timezone = fc.commit_timezone = 0
fc.encoding = b"UTF-8"
fc.message = b"Remote commit"
source.object_store.add_object(fc)
source.refs[b"refs/heads/main"] = fc.id

target = SqliteRepo.init_bare(db2)
source.fetch(target)
print(f"  Fetched into: {db2}")
print(f"  Objects: {sum(1 for _ in target.object_store)}")
fetched_commit = target.object_store[fc.id]
print(f"  Commit: {fc.id.decode()[:12]}  {fetched_commit.message.decode().strip()}")
fetched_blob = target.object_store[b.id]
print(f"  Blob:   {b.id.decode()[:12]}  {fetched_blob.data.decode().strip()}")
target.close()
print()

print("=== 8. What's in the SQLite file? ===")
import sqlite3

conn = sqlite3.connect(db)
for table in ["objects", "refs", "named_files"]:
    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"  {table}: {count} rows")
conn.close()
print(f"  File size: {os.path.getsize(db):,} bytes")
