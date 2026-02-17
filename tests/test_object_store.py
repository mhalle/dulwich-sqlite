"""Tests for SqliteObjectStore using Dulwich's ObjectStoreTests mixin."""

import sqlite3
from unittest import TestCase

import pytest
from dulwich.objects import Blob
from dulwich.tests.test_object_store import ObjectStoreTests

from dulwich_sqlite._schema import init_db
from dulwich_sqlite.object_store import SqliteObjectStore


class SqliteObjectStoreTests(ObjectStoreTests, TestCase):
    """Run the standard Dulwich ObjectStore test suite against SqliteObjectStore."""

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        init_db(self.conn)
        self.store = SqliteObjectStore(self.conn)

    def tearDown(self):
        self.store.close()
        self.conn.close()


class TestGetObjectSize:
    @pytest.fixture
    def store(self):
        conn = sqlite3.connect(":memory:")
        init_db(conn)
        s = SqliteObjectStore(conn)
        yield s
        s.close()
        conn.close()

    def test_get_object_size(self, store):
        blob = Blob.from_string(b"hello world")
        store.add_object(blob)
        assert store.get_object_size(blob.id) == 11

    def test_get_object_size_matches_get_raw(self, store):
        blob = Blob.from_string(b"some longer content here")
        store.add_object(blob)
        _, data = store.get_raw(blob.id)
        assert store.get_object_size(blob.id) == len(data)

    def test_get_object_size_empty(self, store):
        blob = Blob.from_string(b"")
        store.add_object(blob)
        assert store.get_object_size(blob.id) == 0

    def test_get_object_size_missing(self, store):
        with pytest.raises(KeyError):
            store.get_object_size(b"a" * 40)
