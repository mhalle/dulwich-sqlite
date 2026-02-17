"""Shared fixtures for dulwich-sqlite tests."""

import os
import tempfile

import pytest

from dulwich_sqlite import SqliteRepo


@pytest.fixture
def tmp_db_path(tmp_path):
    """Return a path to a temporary SQLite database file."""
    return str(tmp_path / "test.db")


@pytest.fixture
def sqlite_repo(tmp_db_path):
    """Create and return an initialized SqliteRepo, closing it after the test."""
    repo = SqliteRepo.init_bare(tmp_db_path)
    yield repo
    repo.close()
