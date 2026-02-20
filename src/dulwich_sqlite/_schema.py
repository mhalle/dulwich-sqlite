"""SQLite schema definitions for dulwich-sqlite."""

import sqlite3
import struct

SCHEMA_VERSION = "11"

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



def migrate_v3_to_v4(conn: sqlite3.Connection) -> None:
    """Migrate a v3 database to v4 schema.

    Recreates the objects table (SQLite can't ALTER COLUMN to drop NOT NULL),
    and creates the new chunks/object_chunks tables.
    """
    conn.execute("ALTER TABLE objects RENAME TO _objects_v3")
    conn.execute(
        """
        CREATE TABLE objects (
            sha TEXT PRIMARY KEY NOT NULL,
            type_num INTEGER NOT NULL,
            data BLOB,
            total_size INTEGER,
            type_name TEXT GENERATED ALWAYS AS (
                CASE type_num
                    WHEN 1 THEN 'commit'
                    WHEN 2 THEN 'tree'
                    WHEN 3 THEN 'blob'
                    WHEN 4 THEN 'tag'
                END
            ) VIRTUAL,
            size_bytes INTEGER GENERATED ALWAYS AS (
                CASE WHEN data IS NOT NULL THEN length(data) ELSE total_size END
            ) VIRTUAL
        )
        """
    )
    conn.execute(
        "INSERT INTO objects (sha, type_num, data) "
        "SELECT sha, type_num, data FROM _objects_v3"
    )
    conn.execute("DROP TABLE _objects_v3")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS chunks (
            chunk_sha TEXT PRIMARY KEY NOT NULL,
            data BLOB NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS object_chunks (
            object_sha TEXT NOT NULL,
            chunk_index INTEGER NOT NULL,
            chunk_sha TEXT NOT NULL,
            PRIMARY KEY (object_sha, chunk_index)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_object_chunks_chunk ON object_chunks (chunk_sha)"
    )

    conn.execute(
        "UPDATE metadata SET value = '4' WHERE key = 'schema_version'",
    )
    conn.commit()


def migrate_v4_to_v5(conn: sqlite3.Connection) -> None:
    """Migrate a v4 database to v5 schema.

    Adds the compression column to the chunks table and compression metadata.
    """
    conn.execute(
        "ALTER TABLE chunks ADD COLUMN compression TEXT NOT NULL DEFAULT 'none'"
    )
    conn.execute(
        "INSERT OR IGNORE INTO metadata (key, value) VALUES (?, ?)",
        ("compression", "none"),
    )
    conn.execute(
        "UPDATE metadata SET value = '5' WHERE key = 'schema_version'",
    )
    conn.commit()


def migrate_v5_to_v6(conn: sqlite3.Connection) -> None:
    """Migrate a v5 database to v6 schema.

    Adds generated convenience columns to objects, chunks, and reflog.
    """
    conn.execute(
        "ALTER TABLE objects ADD COLUMN "
        "is_chunked INTEGER GENERATED ALWAYS AS (data IS NULL) VIRTUAL"
    )
    conn.execute(
        "ALTER TABLE chunks ADD COLUMN "
        "stored_size INTEGER GENERATED ALWAYS AS (length(data)) VIRTUAL"
    )
    conn.execute(
        "ALTER TABLE reflog ADD COLUMN "
        "old_sha_text TEXT GENERATED ALWAYS AS (cast(old_sha AS TEXT)) VIRTUAL"
    )
    conn.execute(
        "ALTER TABLE reflog ADD COLUMN "
        "new_sha_text TEXT GENERATED ALWAYS AS (cast(new_sha AS TEXT)) VIRTUAL"
    )
    conn.execute(
        "ALTER TABLE reflog ADD COLUMN "
        "committer_text TEXT GENERATED ALWAYS AS (cast(committer AS TEXT)) VIRTUAL"
    )
    conn.execute(
        "ALTER TABLE reflog ADD COLUMN "
        "datetime_text TEXT GENERATED ALWAYS AS (datetime(timestamp, 'unixepoch')) VIRTUAL"
    )
    conn.execute(
        "UPDATE metadata SET value = '6' WHERE key = 'schema_version'",
    )
    conn.commit()


def migrate_v6_to_v7(conn: sqlite3.Connection) -> None:
    """Migrate a v6 database to v7 schema.

    Replaces text SHA columns in object_chunks with integer rowid references
    to the objects and chunks tables.
    """
    conn.execute(
        """
        CREATE TABLE object_chunks_new (
            object_id INTEGER NOT NULL,
            chunk_index INTEGER NOT NULL,
            chunk_id INTEGER NOT NULL,
            PRIMARY KEY (object_id, chunk_index)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO object_chunks_new (object_id, chunk_index, chunk_id)
        SELECT o.rowid, oc.chunk_index, c.rowid
        FROM object_chunks oc
        JOIN objects o ON o.sha = oc.object_sha
        JOIN chunks c ON c.chunk_sha = oc.chunk_sha
        """
    )
    conn.execute("DROP INDEX IF EXISTS idx_object_chunks_chunk")
    conn.execute("DROP TABLE object_chunks")
    conn.execute("ALTER TABLE object_chunks_new RENAME TO object_chunks")
    conn.execute(
        "CREATE INDEX idx_object_chunks_chunk ON object_chunks (chunk_id)"
    )
    conn.execute(
        "UPDATE metadata SET value = '7' WHERE key = 'schema_version'",
    )
    conn.commit()


def migrate_v7_to_v8(conn: sqlite3.Connection) -> None:
    """Migrate a v7 database to v8 schema.

    Adds compression column to objects table and changes size_bytes to use
    total_size instead of length(data), since inline objects may now be compressed.
    """
    # Add compression column
    conn.execute(
        "ALTER TABLE objects ADD COLUMN compression TEXT NOT NULL DEFAULT 'none'"
    )
    # Backfill total_size for inline objects
    conn.execute(
        "UPDATE objects SET total_size = length(data) WHERE data IS NOT NULL AND total_size IS NULL"
    )
    # Recreate objects table to change size_bytes generated column
    conn.execute("ALTER TABLE objects RENAME TO _objects_v7")
    conn.execute(
        """
        CREATE TABLE objects (
            sha TEXT PRIMARY KEY NOT NULL,
            type_num INTEGER NOT NULL,
            data BLOB,
            total_size INTEGER,
            compression TEXT NOT NULL DEFAULT 'none',
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
        """
    )
    conn.execute(
        "INSERT INTO objects (sha, type_num, data, total_size, compression) "
        "SELECT sha, type_num, data, total_size, compression FROM _objects_v7"
    )
    # Update object_chunks references to use new rowids
    conn.execute(
        """
        UPDATE object_chunks SET object_id = (
            SELECT o_new.rowid FROM objects o_new
            JOIN _objects_v7 o_old ON o_old.sha = o_new.sha
            WHERE o_old.rowid = object_chunks.object_id
        )
        """
    )
    conn.execute("DROP TABLE _objects_v7")
    conn.execute(
        "UPDATE metadata SET value = '8' WHERE key = 'schema_version'",
    )
    conn.commit()


def migrate_v8_to_v9(conn: sqlite3.Connection) -> None:
    """Migrate a v8 database to v9 schema.

    Replaces the object_chunks table with a packed chunk_refs BLOB column
    on the objects table. Each chunked object stores its ordered chunk rowids
    as little-endian 8-byte unsigned integers.
    """
    conn.execute("ALTER TABLE objects ADD COLUMN chunk_refs BLOB")

    # Fetch all object_chunks rows grouped by object_id, ordered by chunk_index
    rows = conn.execute(
        "SELECT object_id, chunk_id FROM object_chunks ORDER BY object_id, chunk_index"
    ).fetchall()

    # Group by object_id and pack chunk_ids
    if rows:
        current_obj = rows[0][0]
        chunk_ids: list[int] = []
        for obj_id, chunk_id in rows:
            if obj_id != current_obj:
                packed = struct.pack(f'<{len(chunk_ids)}Q', *chunk_ids)
                conn.execute(
                    "UPDATE objects SET chunk_refs = ? WHERE rowid = ?",
                    (packed, current_obj),
                )
                current_obj = obj_id
                chunk_ids = []
            chunk_ids.append(chunk_id)
        # Flush last group
        if chunk_ids:
            packed = struct.pack(f'<{len(chunk_ids)}Q', *chunk_ids)
            conn.execute(
                "UPDATE objects SET chunk_refs = ? WHERE rowid = ?",
                (packed, current_obj),
            )

    conn.execute("DROP INDEX IF EXISTS idx_object_chunks_chunk")
    conn.execute("DROP TABLE IF EXISTS object_chunks")
    conn.execute(
        "UPDATE metadata SET value = '9' WHERE key = 'schema_version'",
    )
    conn.commit()


def migrate_v9_to_v10(conn: sqlite3.Connection) -> None:
    """Migrate a v9 database to v10 schema.

    Converts TEXT SHA columns to BLOB for both objects and chunks tables,
    adds generated hex columns for queryability, and re-encodes chunk_refs
    from fixed 8-byte LE integers to delta-zigzag-varint format.
    """
    from .object_store import pack_chunk_refs

    # 1. Rebuild chunks table: TEXT PK → BLOB PK
    conn.execute(
        """
        CREATE TABLE chunks_new (
            chunk_sha BLOB PRIMARY KEY NOT NULL,
            data BLOB NOT NULL,
            compression TEXT NOT NULL DEFAULT 'none',
            chunk_sha_hex TEXT GENERATED ALWAYS AS (lower(hex(chunk_sha))) VIRTUAL,
            stored_size INTEGER GENERATED ALWAYS AS (length(data)) VIRTUAL
        )
        """
    )
    # Preserve rowids explicitly so chunk_refs still point correctly
    for row in conn.execute(
        "SELECT rowid, chunk_sha, data, compression FROM chunks"
    ).fetchall():
        rowid, chunk_sha_text, data, compression = row
        chunk_sha_bin = bytes.fromhex(chunk_sha_text)
        conn.execute(
            "INSERT INTO chunks_new (rowid, chunk_sha, data, compression) VALUES (?, ?, ?, ?)",
            (rowid, chunk_sha_bin, data, compression),
        )
    conn.execute("DROP TABLE chunks")
    conn.execute("ALTER TABLE chunks_new RENAME TO chunks")

    # 2. Rebuild objects table: TEXT PK → BLOB PK, re-encode chunk_refs
    conn.execute(
        """
        CREATE TABLE objects_new (
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
        """
    )
    for row in conn.execute(
        "SELECT sha, type_num, data, chunk_refs, total_size, compression FROM objects"
    ).fetchall():
        sha_text, type_num, data, chunk_refs_blob, total_size, compression = row
        sha_bin = bytes.fromhex(sha_text)
        # Re-encode chunk_refs from fixed 8-byte LE to delta-varint
        new_chunk_refs = None
        if chunk_refs_blob is not None:
            old_blob = bytes(chunk_refs_blob)
            n = len(old_blob) // 8
            rowids = list(struct.unpack(f'<{n}Q', old_blob))
            new_chunk_refs = pack_chunk_refs(rowids)
        conn.execute(
            "INSERT INTO objects_new (sha, type_num, data, chunk_refs, total_size, compression) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (sha_bin, type_num, data, new_chunk_refs, total_size, compression),
        )
    conn.execute("DROP TABLE objects")
    conn.execute("ALTER TABLE objects_new RENAME TO objects")

    # 3. Update schema version
    conn.execute(
        "UPDATE metadata SET value = '10' WHERE key = 'schema_version'",
    )
    conn.commit()


def migrate_v10_to_v11(conn: sqlite3.Connection) -> None:
    """Migrate a v10 database to v11 schema.

    Adds raw_size column to chunks table to enable efficient byte range reads.
    Backfills raw_size for all existing chunks (uncompressed: length(data),
    compressed: decompress and measure).
    """
    conn.execute("ALTER TABLE chunks ADD COLUMN raw_size INTEGER")

    # Backfill uncompressed chunks via SQL
    conn.execute(
        "UPDATE chunks SET raw_size = length(data) WHERE compression = 'none'"
    )

    # Backfill compressed chunks via Python-side decompression
    rows = conn.execute(
        "SELECT rowid, data, compression FROM chunks WHERE compression != 'none'"
    ).fetchall()
    if rows:
        import zlib

        # Load zstd dicts from named_files if needed
        zstd_dicts_by_id: dict[int, object] = {}
        has_zstd = any(r[2] == "zstd" for r in rows)
        if has_zstd:
            import zstandard

            for path in (
                "_zstd_dict_commit",
                "_zstd_dict_tree",
                "_zstd_dict_chunk",
                "_zstd_dict",
            ):
                dict_row = conn.execute(
                    "SELECT contents FROM named_files WHERE path = ?", (path,)
                ).fetchone()
                if dict_row is not None:
                    d = zstandard.ZstdCompressionDict(bytes(dict_row[0]))
                    zstd_dicts_by_id[d.dict_id()] = d

        for rowid, data, compression in rows:
            raw_data = bytes(data)
            if compression == "zlib":
                raw_data = zlib.decompress(raw_data)
            elif compression == "zstd":
                import zstandard

                params = zstandard.get_frame_parameters(raw_data)
                dict_data = zstd_dicts_by_id.get(params.dict_id)
                if dict_data is not None:
                    dctx = zstandard.ZstdDecompressor(dict_data=dict_data)
                else:
                    dctx = zstandard.ZstdDecompressor()
                raw_data = dctx.decompress(raw_data)
            conn.execute(
                "UPDATE chunks SET raw_size = ? WHERE rowid = ?",
                (len(raw_data), rowid),
            )

    conn.execute(
        "UPDATE metadata SET value = '11' WHERE key = 'schema_version'",
    )
    conn.commit()
