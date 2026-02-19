"""SQLite-backed refs container for Dulwich."""

import sqlite3
from collections.abc import Callable

from dulwich.objects import ZERO_SHA, ObjectID
from dulwich.refs import RefsContainer, Ref, SYMREF


class SqliteRefsContainer(RefsContainer):
    """Refs container backed by a SQLite database."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        logger: Callable | None = None,
    ) -> None:
        super().__init__(logger=logger)
        self._conn = conn

    def allkeys(self) -> set[Ref]:
        rows = self._conn.execute("SELECT name FROM refs").fetchall()
        return {bytes(row[0]) for row in rows}

    def read_loose_ref(self, name: Ref) -> bytes | None:
        row = self._conn.execute(
            "SELECT value FROM refs WHERE name = ?", (name,)
        ).fetchone()
        if row is None:
            return None
        return bytes(row[0])

    def get_packed_refs(self) -> dict[Ref, ObjectID]:
        return {}

    def set_symbolic_ref(
        self,
        name: Ref,
        other: Ref,
        committer: bytes | None = None,
        timestamp: int | None = None,
        timezone: int | None = None,
        message: bytes | None = None,
    ) -> None:
        new = SYMREF + other
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            old = self.follow(name)[-1]
            self._conn.execute(
                "INSERT OR REPLACE INTO refs (name, value) VALUES (?, ?)",
                (name, new),
            )
            self._conn.commit()
        except BaseException:
            self._conn.rollback()
            raise
        self._log(
            name,
            old,
            new,
            committer=committer,
            timestamp=timestamp,
            timezone=timezone,
            message=message,
        )

    def set_if_equals(
        self,
        name: Ref,
        old_ref: ObjectID | None,
        new_ref: ObjectID,
        committer: bytes | None = None,
        timestamp: int | None = None,
        timezone: int | None = None,
        message: bytes | None = None,
    ) -> bool:
        self._check_refname(name)
        if old_ref is None:
            # Unconditional set — grab old value for logging, then upsert.
            # BEGIN IMMEDIATE ensures the read and write are atomic.
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT value FROM refs WHERE name = ?", (name,)
                ).fetchone()
                old = bytes(row[0]) if row is not None else None
                self._conn.execute(
                    "INSERT OR REPLACE INTO refs (name, value) VALUES (?, ?)",
                    (name, new_ref),
                )
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise
        else:
            # Atomic compare-and-swap: UPDATE only the row matching both
            # name and expected old value in a single statement.
            old = old_ref
            cursor = self._conn.execute(
                "UPDATE refs SET value = ? WHERE name = ? AND value = ?",
                (new_ref, name, old_ref),
            )
            self._conn.commit()
            if cursor.rowcount == 0:
                # Either the ref doesn't exist, or its value didn't match.
                # Check if the caller expected ZERO_SHA (i.e. ref absent):
                if old_ref == ZERO_SHA:
                    # Ref should not exist — try atomic insert.
                    try:
                        self._conn.execute(
                            "INSERT INTO refs (name, value) VALUES (?, ?)",
                            (name, new_ref),
                        )
                        self._conn.commit()
                        old = None
                    except sqlite3.IntegrityError:
                        return False
                else:
                    return False
        self._log(
            name,
            old,
            new_ref,
            committer=committer,
            timestamp=timestamp,
            timezone=timezone,
            message=message,
        )
        return True

    def add_if_new(
        self,
        name: Ref,
        ref: ObjectID,
        committer: bytes | None = None,
        timestamp: int | None = None,
        timezone: int | None = None,
        message: bytes | None = None,
    ) -> bool:
        # Atomic insert — relies on PRIMARY KEY constraint to reject
        # duplicates without a separate SELECT.
        try:
            self._conn.execute(
                "INSERT INTO refs (name, value) VALUES (?, ?)",
                (name, ref),
            )
            self._conn.commit()
        except sqlite3.IntegrityError:
            return False
        self._log(
            name,
            None,
            ref,
            committer=committer,
            timestamp=timestamp,
            timezone=timezone,
            message=message,
        )
        return True

    def remove_if_equals(
        self,
        name: Ref,
        old_ref: ObjectID | None,
        committer: bytes | None = None,
        timestamp: int | None = None,
        timezone: int | None = None,
        message: bytes | None = None,
    ) -> bool:
        if old_ref is None:
            # Unconditional delete — grab old value for logging.
            # BEGIN IMMEDIATE ensures the read and write are atomic.
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT value FROM refs WHERE name = ?", (name,)
                ).fetchone()
                old = bytes(row[0]) if row is not None else None
                self._conn.execute("DELETE FROM refs WHERE name = ?", (name,))
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise
        else:
            # Atomic compare-and-delete in a single statement.
            old = old_ref
            cursor = self._conn.execute(
                "DELETE FROM refs WHERE name = ? AND value = ?",
                (name, old_ref),
            )
            self._conn.commit()
            if cursor.rowcount == 0:
                return False
        if old is not None:
            self._log(
                name,
                old,
                None,
                committer=committer,
                timestamp=timestamp,
                timezone=timezone,
                message=message,
            )
        return True

    def get_peeled(self, name: Ref) -> ObjectID | None:
        row = self._conn.execute(
            "SELECT value FROM peeled_refs WHERE name = ?", (name,)
        ).fetchone()
        if row is None:
            return None
        return bytes(row[0])
