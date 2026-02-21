"""SQLite schema definitions for dulwich-sqlite."""

import sqlite3

SCHEMA_VERSION = "1"

PRAGMAS = [
    "PRAGMA journal_mode=WAL",
    "PRAGMA synchronous=NORMAL",
    "PRAGMA busy_timeout=5000",
]

CREATE_TABLES = [
    """
    CREATE TABLE IF NOT EXISTS objects (
        sha BLOB PRIMARY KEY NOT NULL,
        type_num INTEGER NOT NULL,
        data BLOB,
        chunk_refs BLOB,
        total_size INTEGER,
        compression TEXT NOT NULL DEFAULT 'none',
        sha_hex TEXT GENERATED ALWAYS AS (lower(hex(sha))) VIRTUAL,
        type_name TEXT GENERATED ALWAYS AS (
            CASE type_num
                WHEN 1 THEN 'commit'
                WHEN 2 THEN 'tree'
                WHEN 3 THEN 'blob'
                WHEN 4 THEN 'tag'
            END
        ) VIRTUAL,
        size_bytes INTEGER GENERATED ALWAYS AS (total_size) VIRTUAL,
        is_chunked INTEGER GENERATED ALWAYS AS (data IS NULL) VIRTUAL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS chunks (
        chunk_sha BLOB PRIMARY KEY NOT NULL,
        data BLOB NOT NULL,
        compression TEXT NOT NULL DEFAULT 'none',
        raw_size INTEGER,
        chunk_sha_hex TEXT GENERATED ALWAYS AS (lower(hex(chunk_sha))) VIRTUAL,
        stored_size INTEGER GENERATED ALWAYS AS (length(data)) VIRTUAL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS refs (
        name BLOB PRIMARY KEY NOT NULL,
        value BLOB NOT NULL,
        name_hex TEXT GENERATED ALWAYS AS (hex(name)) VIRTUAL,
        value_hex TEXT GENERATED ALWAYS AS (hex(value)) VIRTUAL,
        name_text TEXT GENERATED ALWAYS AS (cast(name AS TEXT)) VIRTUAL,
        value_text TEXT GENERATED ALWAYS AS (cast(value AS TEXT)) VIRTUAL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS peeled_refs (
        name BLOB PRIMARY KEY NOT NULL,
        value BLOB NOT NULL,
        name_hex TEXT GENERATED ALWAYS AS (hex(name)) VIRTUAL,
        value_hex TEXT GENERATED ALWAYS AS (hex(value)) VIRTUAL,
        name_text TEXT GENERATED ALWAYS AS (cast(name AS TEXT)) VIRTUAL,
        value_text TEXT GENERATED ALWAYS AS (cast(value AS TEXT)) VIRTUAL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS named_files (
        path TEXT PRIMARY KEY NOT NULL,
        contents BLOB NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS metadata (
        key TEXT PRIMARY KEY NOT NULL,
        value TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS reflog (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ref_name BLOB NOT NULL,
        old_sha BLOB NOT NULL,
        new_sha BLOB NOT NULL,
        committer BLOB NOT NULL,
        timestamp INTEGER NOT NULL,
        timezone INTEGER NOT NULL,
        message BLOB NOT NULL,
        ref_name_text TEXT GENERATED ALWAYS AS (cast(ref_name AS TEXT)) VIRTUAL,
        old_sha_text TEXT GENERATED ALWAYS AS (cast(old_sha AS TEXT)) VIRTUAL,
        new_sha_text TEXT GENERATED ALWAYS AS (cast(new_sha AS TEXT)) VIRTUAL,
        committer_text TEXT GENERATED ALWAYS AS (cast(committer AS TEXT)) VIRTUAL,
        message_text TEXT GENERATED ALWAYS AS (cast(message AS TEXT)) VIRTUAL,
        datetime_text TEXT GENERATED ALWAYS AS (datetime(timestamp, 'unixepoch')) VIRTUAL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_reflog_ref ON reflog (ref_name, id)",
]


def init_db(conn: sqlite3.Connection) -> None:
    """Initialize the database schema."""
    for pragma in PRAGMAS:
        conn.execute(pragma)
    for stmt in CREATE_TABLES:
        conn.execute(stmt)
    conn.execute(
        "INSERT OR IGNORE INTO metadata (key, value) VALUES (?, ?)",
        ("schema_version", SCHEMA_VERSION),
    )
    conn.execute(
        "INSERT OR IGNORE INTO metadata (key, value) VALUES (?, ?)",
        ("compression", "none"),
    )
    conn.commit()


def apply_pragmas(conn: sqlite3.Connection) -> None:
    """Apply PRAGMAs to an existing connection."""
    for pragma in PRAGMAS:
        conn.execute(pragma)

