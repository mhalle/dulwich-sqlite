"""Tests for SqliteRefsContainer."""

import sqlite3
import threading

import pytest
from dulwich.objects import ZERO_SHA
from dulwich.refs import SYMREF

from dulwich_sqlite._schema import init_db
from dulwich_sqlite.refs import SqliteRefsContainer


@pytest.fixture
def refs_container():
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    container = SqliteRefsContainer(conn)
    yield container
    conn.close()


@pytest.fixture
def logged_refs():
    """RefsContainer with a logger that records calls."""
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    log = []

    def logger(ref, old_sha, new_sha, committer, timestamp, timezone, message):
        log.append((ref, old_sha, new_sha, committer, timestamp, timezone, message))

    container = SqliteRefsContainer(conn, logger=logger)
    yield container, log
    conn.close()


class TestSqliteRefsContainer:
    def test_allkeys_empty(self, refs_container):
        assert refs_container.allkeys() == set()

    def test_set_and_read_ref(self, refs_container):
        sha = b"a" * 40
        assert refs_container.set_if_equals(b"refs/heads/master", None, sha)
        assert refs_container.read_loose_ref(b"refs/heads/master") == sha

    def test_allkeys_after_add(self, refs_container):
        sha = b"a" * 40
        refs_container.set_if_equals(b"refs/heads/master", None, sha)
        refs_container.set_if_equals(b"refs/heads/dev", None, sha)
        assert refs_container.allkeys() == {b"refs/heads/master", b"refs/heads/dev"}

    def test_set_if_equals_cas_success(self, refs_container):
        old_sha = b"a" * 40
        new_sha = b"b" * 40
        refs_container.set_if_equals(b"refs/heads/master", None, old_sha)
        assert refs_container.set_if_equals(b"refs/heads/master", old_sha, new_sha)
        assert refs_container.read_loose_ref(b"refs/heads/master") == new_sha

    def test_set_if_equals_cas_failure(self, refs_container):
        old_sha = b"a" * 40
        new_sha = b"b" * 40
        wrong_sha = b"c" * 40
        refs_container.set_if_equals(b"refs/heads/master", None, old_sha)
        assert not refs_container.set_if_equals(
            b"refs/heads/master", wrong_sha, new_sha
        )
        assert refs_container.read_loose_ref(b"refs/heads/master") == old_sha

    def test_add_if_new_success(self, refs_container):
        sha = b"a" * 40
        assert refs_container.add_if_new(b"refs/heads/master", sha)
        assert refs_container.read_loose_ref(b"refs/heads/master") == sha

    def test_add_if_new_already_exists(self, refs_container):
        sha1 = b"a" * 40
        sha2 = b"b" * 40
        refs_container.add_if_new(b"refs/heads/master", sha1)
        assert not refs_container.add_if_new(b"refs/heads/master", sha2)
        assert refs_container.read_loose_ref(b"refs/heads/master") == sha1

    def test_remove_if_equals_success(self, refs_container):
        sha = b"a" * 40
        refs_container.set_if_equals(b"refs/heads/master", None, sha)
        assert refs_container.remove_if_equals(b"refs/heads/master", sha)
        assert refs_container.read_loose_ref(b"refs/heads/master") is None

    def test_remove_if_equals_failure(self, refs_container):
        sha = b"a" * 40
        wrong_sha = b"b" * 40
        refs_container.set_if_equals(b"refs/heads/master", None, sha)
        assert not refs_container.remove_if_equals(b"refs/heads/master", wrong_sha)
        assert refs_container.read_loose_ref(b"refs/heads/master") == sha

    def test_remove_if_equals_unconditional(self, refs_container):
        sha = b"a" * 40
        refs_container.set_if_equals(b"refs/heads/master", None, sha)
        assert refs_container.remove_if_equals(b"refs/heads/master", None)
        assert refs_container.read_loose_ref(b"refs/heads/master") is None

    def test_set_symbolic_ref(self, refs_container):
        sha = b"a" * 40
        refs_container.set_if_equals(b"refs/heads/master", None, sha)
        refs_container.set_symbolic_ref(b"HEAD", b"refs/heads/master")
        val = refs_container.read_loose_ref(b"HEAD")
        assert val == SYMREF + b"refs/heads/master"

    def test_get_packed_refs_empty(self, refs_container):
        assert refs_container.get_packed_refs() == {}

    def test_get_peeled_missing(self, refs_container):
        assert refs_container.get_peeled(b"refs/heads/master") is None

    def test_read_loose_ref_missing(self, refs_container):
        assert refs_container.read_loose_ref(b"refs/heads/nonexistent") is None


class TestReflog:
    """Verify that reflog entries are written on ref mutations."""

    def test_set_if_equals_logs(self, logged_refs):
        container, log = logged_refs
        sha = b"a" * 40
        container.set_if_equals(
            b"refs/heads/main", None, sha, message=b"branch: Created"
        )
        assert len(log) == 1
        ref, old, new, _, _, _, msg = log[0]
        assert ref == b"refs/heads/main"
        assert old == ZERO_SHA
        assert new == sha
        assert msg == b"branch: Created"

    def test_set_if_equals_cas_logs(self, logged_refs):
        container, log = logged_refs
        old_sha = b"a" * 40
        new_sha = b"b" * 40
        container.set_if_equals(b"refs/heads/main", None, old_sha, message=b"init")
        container.set_if_equals(
            b"refs/heads/main", old_sha, new_sha, message=b"update"
        )
        assert len(log) == 2
        assert log[1][1] == old_sha
        assert log[1][2] == new_sha
        assert log[1][6] == b"update"

    def test_add_if_new_logs(self, logged_refs):
        container, log = logged_refs
        sha = b"a" * 40
        container.add_if_new(b"refs/heads/feature", sha, message=b"branch: Created")
        assert len(log) == 1
        assert log[0][0] == b"refs/heads/feature"
        assert log[0][1] == ZERO_SHA
        assert log[0][2] == sha

    def test_add_if_new_duplicate_no_log(self, logged_refs):
        container, log = logged_refs
        sha1 = b"a" * 40
        sha2 = b"b" * 40
        container.add_if_new(b"refs/heads/feature", sha1, message=b"first")
        container.add_if_new(b"refs/heads/feature", sha2, message=b"second")
        # Only the first (successful) call should log
        assert len(log) == 1

    def test_remove_if_equals_logs(self, logged_refs):
        container, log = logged_refs
        sha = b"a" * 40
        container.set_if_equals(b"refs/heads/main", None, sha, message=b"init")
        log.clear()
        container.remove_if_equals(b"refs/heads/main", sha, message=b"branch: Deleted")
        assert len(log) == 1
        assert log[0][1] == sha
        assert log[0][2] == ZERO_SHA

    def test_set_symbolic_ref_logs(self, logged_refs):
        container, log = logged_refs
        sha = b"a" * 40
        container.set_if_equals(b"refs/heads/main", None, sha, message=b"init")
        log.clear()
        container.set_symbolic_ref(b"HEAD", b"refs/heads/main", message=b"checkout")
        assert len(log) == 1
        assert log[0][0] == b"HEAD"
        assert log[0][6] == b"checkout"

    def test_unconditional_set_logs_correct_old_value(self, logged_refs):
        """Unconditional set_if_equals logs the actual old value atomically."""
        container, log = logged_refs
        sha1 = b"a" * 40
        sha2 = b"b" * 40
        container.set_if_equals(b"refs/heads/main", None, sha1, message=b"first")
        container.set_if_equals(b"refs/heads/main", None, sha2, message=b"second")
        assert len(log) == 2
        assert log[1][1] == sha1
        assert log[1][2] == sha2

    def test_no_log_without_message(self, logged_refs):
        """RefsContainer._log skips logging when message is None."""
        container, log = logged_refs
        sha = b"a" * 40
        container.set_if_equals(b"refs/heads/main", None, sha)
        assert len(log) == 0


class TestConcurrentCAS:
    """Verify CAS atomicity with concurrent writers on separate connections."""

    def test_set_if_equals_concurrent(self, tmp_path):
        db_path = str(tmp_path / "cas.db")
        conn_setup = sqlite3.connect(db_path)
        init_db(conn_setup)
        # Seed the ref
        old_sha = b"a" * 40
        conn_setup.execute(
            "INSERT INTO refs (name, value) VALUES (?, ?)",
            (b"refs/heads/master", old_sha),
        )
        conn_setup.commit()
        conn_setup.close()

        results = [None, None]
        barrier = threading.Barrier(2)

        def cas_writer(idx, new_sha):
            conn = sqlite3.connect(db_path, timeout=10)
            conn.execute("PRAGMA busy_timeout=10000")
            container = SqliteRefsContainer(conn)
            barrier.wait()
            results[idx] = container.set_if_equals(
                b"refs/heads/master", old_sha, new_sha
            )
            conn.close()

        sha_a = b"b" * 40
        sha_b = b"c" * 40
        t1 = threading.Thread(target=cas_writer, args=(0, sha_a))
        t2 = threading.Thread(target=cas_writer, args=(1, sha_b))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # Exactly one writer should succeed
        assert results.count(True) == 1
        assert results.count(False) == 1

    def test_add_if_new_concurrent(self, tmp_path):
        db_path = str(tmp_path / "add_new.db")
        conn_setup = sqlite3.connect(db_path)
        init_db(conn_setup)
        conn_setup.close()

        results = [None, None]
        barrier = threading.Barrier(2)

        def add_writer(idx, sha):
            conn = sqlite3.connect(db_path, timeout=10)
            conn.execute("PRAGMA busy_timeout=10000")
            container = SqliteRefsContainer(conn)
            barrier.wait()
            results[idx] = container.add_if_new(b"refs/heads/new-branch", sha)
            conn.close()

        sha_a = b"a" * 40
        sha_b = b"b" * 40
        t1 = threading.Thread(target=add_writer, args=(0, sha_a))
        t2 = threading.Thread(target=add_writer, args=(1, sha_b))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert results.count(True) == 1
        assert results.count(False) == 1

    def test_remove_if_equals_concurrent(self, tmp_path):
        db_path = str(tmp_path / "remove.db")
        conn_setup = sqlite3.connect(db_path)
        init_db(conn_setup)
        sha = b"a" * 40
        conn_setup.execute(
            "INSERT INTO refs (name, value) VALUES (?, ?)",
            (b"refs/heads/master", sha),
        )
        conn_setup.commit()
        conn_setup.close()

        results = [None, None]
        barrier = threading.Barrier(2)

        def remove_writer(idx):
            conn = sqlite3.connect(db_path, timeout=10)
            conn.execute("PRAGMA busy_timeout=10000")
            container = SqliteRefsContainer(conn)
            barrier.wait()
            results[idx] = container.remove_if_equals(b"refs/heads/master", sha)
            conn.close()

        t1 = threading.Thread(target=remove_writer, args=(0,))
        t2 = threading.Thread(target=remove_writer, args=(1,))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert results.count(True) == 1
        assert results.count(False) == 1
