"""Tests for remote push/fetch/clone operations."""

import os

from dulwich.objects import Blob, Commit, Tree
from dulwich.repo import MemoryRepo, Repo

from dulwich_sqlite import SqliteRepo


def _make_commit(repo, message=b"test commit", parents=None):
    """Helper: create a blob -> tree -> commit in a repo and return commit SHA."""
    blob = Blob.from_string(b"content for " + message)
    repo.object_store.add_object(blob)
    tree = Tree()
    tree.add(b"file.txt", 0o100644, blob.id)
    repo.object_store.add_object(tree)
    commit = Commit()
    commit.tree = tree.id
    commit.author = commit.committer = b"Test User <test@example.com>"
    commit.author_time = commit.commit_time = 1234567890
    commit.author_timezone = commit.commit_timezone = 0
    commit.encoding = b"UTF-8"
    commit.message = message
    commit.parents = parents or []
    repo.object_store.add_object(commit)
    return commit.id


def _init_git_bare(path):
    """Create a bare on-disk git repo (mkdir + Repo.init_bare)."""
    os.mkdir(path)
    return Repo.init_bare(path)


class TestFetch:
    def test_fetch_with_configured_remote(self, tmp_path):
        """Fetch from a configured remote updates refs/remotes/origin/."""
        git_path = str(tmp_path / "source.git")
        source = _init_git_bare(git_path)
        commit_id = _make_commit(source, b"initial commit")
        source.refs[b"refs/heads/main"] = commit_id
        source.refs.set_symbolic_ref(b"HEAD", b"refs/heads/main")
        source.close()

        db_path = str(tmp_path / "target.db")
        target = SqliteRepo.clone_from(git_path, db_path)

        # Remote tracking ref should be set by porcelain.fetch
        assert target.refs[b"refs/remotes/origin/main"] == commit_id
        # Local branch created by clone_from
        assert target.refs[b"refs/heads/main"] == commit_id
        assert commit_id in target.object_store
        target.close()

    def test_fetch_from_configured_remote_by_name(self, tmp_path):
        """fetch('origin') resolves URL from config and imports refs."""
        git_path = str(tmp_path / "source.git")
        source = _init_git_bare(git_path)
        c1 = _make_commit(source, b"first")
        source.refs[b"refs/heads/main"] = c1
        source.close()

        db_path = str(tmp_path / "target.db")
        target = SqliteRepo.clone_from(git_path, db_path)
        assert target.refs[b"refs/remotes/origin/main"] == c1

        # Add another commit to source
        source = Repo(git_path)
        c2 = _make_commit(source, b"second", parents=[c1])
        source.refs[b"refs/heads/main"] = c2
        source.close()

        # Fetch by remote name — refs should update
        target.fetch("origin")
        assert target.refs[b"refs/remotes/origin/main"] == c2
        assert c2 in target.object_store
        target.close()

    def test_fetch_default_remote(self, tmp_path):
        """fetch() with no args defaults to the tracking remote."""
        git_path = str(tmp_path / "source.git")
        source = _init_git_bare(git_path)
        c1 = _make_commit(source, b"first")
        source.refs[b"refs/heads/main"] = c1
        source.close()

        db_path = str(tmp_path / "target.db")
        target = SqliteRepo.clone_from(git_path, db_path)

        source = Repo(git_path)
        c2 = _make_commit(source, b"second", parents=[c1])
        source.refs[b"refs/heads/main"] = c2
        source.close()

        # No remote_location — should default to "origin"
        target.fetch()
        assert target.refs[b"refs/remotes/origin/main"] == c2
        target.close()

    def test_fetch_from_memory_repo(self, tmp_path):
        """Fetch from a MemoryRepo into SQLite repo via BaseRepo.fetch."""
        source = MemoryRepo.init_bare([], {})
        commit_id = _make_commit(source, b"memory commit")
        source.refs[b"refs/heads/main"] = commit_id

        db_path = str(tmp_path / "target.db")
        target = SqliteRepo.init_bare(db_path)
        source.fetch(target)

        assert commit_id in target.object_store
        target.close()

    def test_fetch_incremental(self, tmp_path):
        """Second fetch only transfers new objects."""
        git_path = str(tmp_path / "source.git")
        source = _init_git_bare(git_path)
        c1 = _make_commit(source, b"first")
        source.refs[b"refs/heads/main"] = c1
        source.close()

        db_path = str(tmp_path / "target.db")
        target = SqliteRepo.clone_from(git_path, db_path)
        assert c1 in target.object_store

        # Add another commit to source
        source = Repo(git_path)
        c2 = _make_commit(source, b"second", parents=[c1])
        source.refs[b"refs/heads/main"] = c2
        source.close()

        target.fetch("origin")
        assert c2 in target.object_store
        assert target.refs[b"refs/remotes/origin/main"] == c2
        target.close()


class TestPush:
    def test_push_to_configured_remote(self, tmp_path):
        """Push to a configured remote by name."""
        git_path = str(tmp_path / "remote.git")
        _init_git_bare(git_path).close()

        db_path = str(tmp_path / "source.db")
        source = SqliteRepo.init_bare(db_path)
        commit_id = _make_commit(source, b"sqlite commit")
        source.refs[b"refs/heads/main"] = commit_id

        # Configure remote
        config = source.get_config()
        config.set((b"remote", b"origin"), b"url", git_path.encode())
        config.set(
            (b"remote", b"origin"), b"fetch",
            b"+refs/heads/*:refs/remotes/origin/*",
        )
        source._save_config()

        source.push("origin", refspecs=["refs/heads/main"])
        source.close()

        target = Repo(git_path)
        assert target.refs[b"refs/heads/main"] == commit_id
        assert commit_id in target.object_store
        target.close()

    def test_push_with_explicit_url(self, tmp_path):
        """Push using a direct URL instead of a remote name."""
        db_path = str(tmp_path / "source.db")
        source = SqliteRepo.init_bare(db_path)
        commit_id = _make_commit(source, b"sqlite commit")
        source.refs[b"refs/heads/main"] = commit_id

        git_path = str(tmp_path / "target.git")
        _init_git_bare(git_path).close()

        source.push(git_path, refspecs=["refs/heads/main"])
        source.close()

        target = Repo(git_path)
        assert target.refs[b"refs/heads/main"] == commit_id
        assert commit_id in target.object_store
        target.close()

    def test_push_multiple_refs(self, tmp_path):
        """Push multiple branches."""
        db_path = str(tmp_path / "source.db")
        source = SqliteRepo.init_bare(db_path)
        c1 = _make_commit(source, b"main commit")
        c2 = _make_commit(source, b"feature commit")
        source.refs[b"refs/heads/main"] = c1
        source.refs[b"refs/heads/feature"] = c2

        git_path = str(tmp_path / "target.git")
        _init_git_bare(git_path).close()

        source.push(
            git_path,
            refspecs=["refs/heads/main", "refs/heads/feature"],
        )
        source.close()

        target = Repo(git_path)
        assert target.refs[b"refs/heads/main"] == c1
        assert target.refs[b"refs/heads/feature"] == c2
        target.close()


class TestCloneFrom:
    def test_clone_sets_up_remote_config(self, tmp_path):
        """clone_from creates [remote "origin"] config."""
        git_path = str(tmp_path / "source.git")
        source = _init_git_bare(git_path)
        commit_id = _make_commit(source, b"clone me")
        source.refs[b"refs/heads/main"] = commit_id
        source.close()

        db_path = str(tmp_path / "cloned.db")
        cloned = SqliteRepo.clone_from(git_path, db_path)

        config = cloned.get_config()
        section = (b"remote", b"origin")
        assert config.get(section, b"url") == git_path.encode()
        assert config.get(section, b"fetch") == b"+refs/heads/*:refs/remotes/origin/*"
        cloned.close()

    def test_clone_creates_remote_tracking_refs(self, tmp_path):
        """clone_from populates refs/remotes/origin/."""
        git_path = str(tmp_path / "source.git")
        source = _init_git_bare(git_path)
        commit_id = _make_commit(source, b"clone me")
        source.refs[b"refs/heads/main"] = commit_id
        source.close()

        db_path = str(tmp_path / "cloned.db")
        cloned = SqliteRepo.clone_from(git_path, db_path)

        assert cloned.refs[b"refs/remotes/origin/main"] == commit_id
        assert commit_id in cloned.object_store
        cloned.close()

    def test_clone_creates_local_branch_and_head(self, tmp_path):
        """clone_from creates a local branch and sets HEAD."""
        git_path = str(tmp_path / "source.git")
        source = _init_git_bare(git_path)
        commit_id = _make_commit(source, b"clone me")
        source.refs[b"refs/heads/main"] = commit_id
        source.refs.set_symbolic_ref(b"HEAD", b"refs/heads/main")
        source.close()

        db_path = str(tmp_path / "cloned.db")
        cloned = SqliteRepo.clone_from(git_path, db_path)

        # Local branch should exist
        assert cloned.refs[b"refs/heads/main"] == commit_id
        # HEAD should be symbolic ref to the local branch
        assert cloned.refs.read_loose_ref(b"HEAD") == b"ref: refs/heads/main"
        cloned.close()

    def test_clone_with_branch(self, tmp_path):
        """clone_from with branch= checks out the requested branch."""
        git_path = str(tmp_path / "source.git")
        source = _init_git_bare(git_path)
        c1 = _make_commit(source, b"main commit")
        c2 = _make_commit(source, b"dev commit")
        source.refs[b"refs/heads/main"] = c1
        source.refs[b"refs/heads/dev"] = c2
        source.refs.set_symbolic_ref(b"HEAD", b"refs/heads/main")
        source.close()

        db_path = str(tmp_path / "cloned.db")
        cloned = SqliteRepo.clone_from(git_path, db_path, branch="dev")

        assert cloned.refs[b"refs/heads/dev"] == c2
        assert cloned.refs.read_loose_ref(b"HEAD") == b"ref: refs/heads/dev"
        cloned.close()

    def test_clone_with_compression(self, tmp_path):
        """Clone with zlib compression enabled."""
        git_path = str(tmp_path / "source.git")
        source = _init_git_bare(git_path)
        commit_id = _make_commit(source, b"compress me")
        source.refs[b"refs/heads/main"] = commit_id
        source.close()

        db_path = str(tmp_path / "cloned.db")
        cloned = SqliteRepo.clone_from(git_path, db_path, compress=True)

        assert commit_id in cloned.object_store
        assert cloned.object_store._compression == "zlib"
        cloned.close()

    def test_clone_with_custom_origin_name(self, tmp_path):
        """clone_from respects a custom remote name."""
        git_path = str(tmp_path / "source.git")
        source = _init_git_bare(git_path)
        commit_id = _make_commit(source, b"clone me")
        source.refs[b"refs/heads/main"] = commit_id
        source.close()

        db_path = str(tmp_path / "cloned.db")
        cloned = SqliteRepo.clone_from(git_path, db_path, origin="upstream")

        config = cloned.get_config()
        assert config.get((b"remote", b"upstream"), b"url") == git_path.encode()
        assert cloned.refs[b"refs/remotes/upstream/main"] == commit_id
        cloned.close()


class TestRoundTrip:
    def test_sqlite_to_git_to_sqlite(self, tmp_path):
        """Push from SQLite to git, then clone back into a new SQLite repo."""
        # Create source SQLite repo with commits
        db1_path = str(tmp_path / "source.db")
        source = SqliteRepo.init_bare(db1_path)
        c1 = _make_commit(source, b"first")
        c2 = _make_commit(source, b"second", parents=[c1])
        source.refs[b"refs/heads/main"] = c2

        # Push to conventional git repo
        git_path = str(tmp_path / "relay.git")
        relay = _init_git_bare(git_path)
        relay.refs.set_symbolic_ref(b"HEAD", b"refs/heads/main")
        relay.close()
        source.push(git_path, refspecs=["refs/heads/main"])
        source.close()

        # Clone into a new SQLite repo
        db2_path = str(tmp_path / "dest.db")
        dest = SqliteRepo.clone_from(git_path, db2_path)

        assert dest.refs[b"refs/remotes/origin/main"] == c2
        assert dest.refs[b"refs/heads/main"] == c2
        assert c1 in dest.object_store
        assert c2 in dest.object_store
        dest.close()
