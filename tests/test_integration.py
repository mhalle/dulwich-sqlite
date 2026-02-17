"""End-to-end integration tests."""

import time

from dulwich.objects import Blob, Commit, Tree
from dulwich.repo import MemoryRepo

from dulwich_sqlite import SqliteRepo


class TestIntegration:
    def test_add_and_retrieve_blob(self, sqlite_repo):
        b = Blob.from_string(b"hello world")
        sqlite_repo.object_store.add_object(b)
        assert b.id in sqlite_repo.object_store
        retrieved = sqlite_repo.object_store[b.id]
        assert retrieved.data == b"hello world"

    def test_persistence(self, tmp_db_path):
        repo = SqliteRepo.init_bare(tmp_db_path)
        b = Blob.from_string(b"persistent data")
        repo.object_store.add_object(b)
        blob_id = b.id
        repo.close()

        repo2 = SqliteRepo(tmp_db_path)
        assert blob_id in repo2.object_store
        retrieved = repo2.object_store[blob_id]
        assert retrieved.data == b"persistent data"
        repo2.close()

    def test_commit_workflow(self, sqlite_repo):
        # Create a blob
        b = Blob.from_string(b"file content")
        sqlite_repo.object_store.add_object(b)

        # Create a tree
        t = Tree()
        t.add(b"file.txt", 0o100644, b.id)
        sqlite_repo.object_store.add_object(t)

        # Create a commit
        c = Commit()
        c.tree = t.id
        c.author = c.committer = b"Test User <test@example.com>"
        c.commit_time = c.author_time = int(time.time())
        c.commit_timezone = c.author_timezone = 0
        c.encoding = b"UTF-8"
        c.message = b"Initial commit"
        sqlite_repo.object_store.add_object(c)

        # Set the ref
        sqlite_repo.refs[b"refs/heads/master"] = c.id

        # Verify
        assert sqlite_repo.refs[b"refs/heads/master"] == c.id
        retrieved_commit = sqlite_repo.object_store[c.id]
        assert retrieved_commit.message == b"Initial commit"
        assert retrieved_commit.tree == t.id

    def test_branch_operations(self, sqlite_repo):
        b = Blob.from_string(b"content")
        sqlite_repo.object_store.add_object(b)

        t = Tree()
        t.add(b"file.txt", 0o100644, b.id)
        sqlite_repo.object_store.add_object(t)

        c = Commit()
        c.tree = t.id
        c.author = c.committer = b"Test <test@test.com>"
        c.commit_time = c.author_time = int(time.time())
        c.commit_timezone = c.author_timezone = 0
        c.encoding = b"UTF-8"
        c.message = b"commit"
        sqlite_repo.object_store.add_object(c)

        # Create branches
        sqlite_repo.refs[b"refs/heads/master"] = c.id
        sqlite_repo.refs[b"refs/heads/dev"] = c.id

        assert sqlite_repo.refs[b"refs/heads/master"] == c.id
        assert sqlite_repo.refs[b"refs/heads/dev"] == c.id

        # Delete branch
        del sqlite_repo.refs[b"refs/heads/dev"]
        assert b"refs/heads/dev" not in sqlite_repo.refs.allkeys()

    def test_fetch_from_memory_repo(self, sqlite_repo):
        # Set up source MemoryRepo with a commit
        source = MemoryRepo.init_bare([], {})

        b = Blob.from_string(b"fetch me")
        source.object_store.add_object(b)

        t = Tree()
        t.add(b"file.txt", 0o100644, b.id)
        source.object_store.add_object(t)

        c = Commit()
        c.tree = t.id
        c.author = c.committer = b"Test <test@test.com>"
        c.commit_time = c.author_time = int(time.time())
        c.commit_timezone = c.author_timezone = 0
        c.encoding = b"UTF-8"
        c.message = b"source commit"
        source.object_store.add_object(c)
        source.refs[b"refs/heads/master"] = c.id

        # Fetch into sqlite repo (source.fetch pushes into target)
        source.fetch(sqlite_repo)

        # Verify objects transferred
        assert c.id in sqlite_repo.object_store
        assert t.id in sqlite_repo.object_store
        assert b.id in sqlite_repo.object_store

        retrieved = sqlite_repo.object_store[c.id]
        assert retrieved.message == b"source commit"

    def test_multiple_objects_iteration(self, sqlite_repo):
        blobs = []
        for i in range(10):
            b = Blob.from_string(f"blob {i}".encode())
            sqlite_repo.object_store.add_object(b)
            blobs.append(b)

        stored_ids = set(sqlite_repo.object_store)
        for b in blobs:
            assert b.id in stored_ids
