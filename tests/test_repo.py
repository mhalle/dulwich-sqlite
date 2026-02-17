"""Tests for SqliteRepo."""

import os
import sqlite3

import pytest
from dulwich.errors import NotGitRepository

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
