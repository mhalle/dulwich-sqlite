"""Tests for SqliteObjectStore using Dulwich's ObjectStoreTests mixin."""

import sqlite3
from unittest import TestCase

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
