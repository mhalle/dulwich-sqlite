"""Tests for SqliteRepo."""

import os
import sqlite3

import pytest
from dulwich.errors import NotGitRepository
from dulwich.objects import ZERO_SHA, Blob

from dulwich_sqlite import SqliteRepo


class TestSqliteRepo:
    def test_init_bare(self, tmp_db_path):
        repo = SqliteRepo.init_bare(tmp_db_path)
        try:
            assert repo.bare is True
            assert os.path.exists(tmp_db_path)
        finally:
            repo.close()

    def test_reopen(self, tmp_db_path):
        repo = SqliteRepo.init_bare(tmp_db_path)
        repo.close()
        repo2 = SqliteRepo(tmp_db_path)
        assert repo2.bare is True
        repo2.close()

    def test_description(self, sqlite_repo):
        assert sqlite_repo.get_description() == b"Unnamed repository"
        sqlite_repo.set_description(b"My test repo")
        assert sqlite_repo.get_description() == b"My test repo"

    def test_config(self, sqlite_repo):
        config = sqlite_repo.get_config()
        assert config.get(b"core", b"bare") == b"true"

    def test_named_files(self, sqlite_repo):
        sqlite_repo._put_named_file("test.txt", b"hello")
        f = sqlite_repo.get_named_file("test.txt")
        assert f is not None
        assert f.read() == b"hello"

    def test_named_file_missing(self, sqlite_repo):
        assert sqlite_repo.get_named_file("nonexistent") is None

    def test_del_named_file(self, sqlite_repo):
        sqlite_repo._put_named_file("test.txt", b"hello")
        sqlite_repo._del_named_file("test.txt")
        assert sqlite_repo.get_named_file("test.txt") is None

    def test_open_index_raises(self, sqlite_repo):
        from dulwich.errors import NoIndexPresent

        with pytest.raises(NoIndexPresent):
            sqlite_repo.open_index()

    def test_context_manager(self, tmp_db_path):
        with SqliteRepo.init_bare(tmp_db_path) as repo:
            assert repo.bare is True

    def test_config_persists(self, tmp_db_path):
        repo = SqliteRepo.init_bare(tmp_db_path)
        config = repo.get_config()
        assert config.get(b"core", b"bare") == b"true"
        repo.close()

        repo2 = SqliteRepo(tmp_db_path)
        config2 = repo2.get_config()
        assert config2.get(b"core", b"bare") == b"true"
        repo2.close()

    def test_open_uninitialized_db_raises(self, tmp_db_path):
        # Create an empty SQLite file (no schema)
        conn = sqlite3.connect(tmp_db_path)
        conn.execute("CREATE TABLE unrelated (id INTEGER)")
        conn.commit()
        conn.close()

        with pytest.raises(NotGitRepository):
            SqliteRepo(tmp_db_path)

    def test_open_nonexistent_file_raises(self, tmp_path):
        # sqlite3.connect auto-creates files, but our schema check should fail
        db_path = str(tmp_path / "does_not_exist.db")
        with pytest.raises(NotGitRepository):
            SqliteRepo(db_path)

    def test_read_reflog(self, sqlite_repo):
        blob = Blob.from_string(b"data")
        sqlite_repo.object_store.add_object(blob)
        sqlite_repo.refs.set_if_equals(
            b"refs/heads/main", None, blob.id, message=b"branch: Created"
        )
        entries = list(sqlite_repo.read_reflog(b"refs/heads/main"))
        assert len(entries) == 1
        assert entries[0].old_sha == ZERO_SHA
        assert entries[0].new_sha == blob.id
        assert entries[0].message == b"branch: Created"
        assert entries[0].committer == b"dulwich-sqlite <dulwich-sqlite@localhost>"

    def test_read_reflog_multiple_entries(self, sqlite_repo):
        blob1 = Blob.from_string(b"one")
        blob2 = Blob.from_string(b"two")
        sqlite_repo.object_store.add_objects([(blob1, None), (blob2, None)])
        sqlite_repo.refs.set_if_equals(
            b"refs/heads/main", None, blob1.id, message=b"init"
        )
        sqlite_repo.refs.set_if_equals(
            b"refs/heads/main", blob1.id, blob2.id, message=b"update"
        )
        entries = list(sqlite_repo.read_reflog(b"refs/heads/main"))
        assert len(entries) == 2
        assert entries[0].new_sha == blob1.id
        assert entries[1].old_sha == blob1.id
        assert entries[1].new_sha == blob2.id

    def test_open_non_sqlite_file_raises(self, tmp_path):
        db_path = str(tmp_path / "not_a_db.db")
        with open(db_path, "w") as f:
            f.write("This is not a SQLite database.")
        with pytest.raises(NotGitRepository):
            SqliteRepo(db_path)

    def test_open_future_schema_version_raises(self, tmp_path):
        db_path = str(tmp_path / "future.db")
        repo = SqliteRepo.init_bare(db_path)
        repo._conn.execute(
            "UPDATE metadata SET value = '99' WHERE key = 'schema_version'"
        )
        repo._conn.commit()
        repo.close()
        with pytest.raises(NotGitRepository, match="Unsupported schema version"):
            SqliteRepo(db_path)

    def test_read_reflog_empty(self, sqlite_repo):
        entries = list(sqlite_repo.read_reflog(b"refs/heads/nonexistent"))
        assert entries == []
