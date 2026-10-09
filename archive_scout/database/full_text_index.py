"""Portable contentless FTS updates without rereading an overwritten payload.

FTS row IDs identify token versions. Only the current mapping is searchable.
This works on the older SQLite versions shipped with supported Python builds,
without keeping another full copy of every document body. Explicit Repair or
Compact rebuilds remove superseded postings.
"""
from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path

from ..document_store import document_body
from ..content import decode_bytes, parse_page, looks_textual_bytes


def index_signature(title: str, body: str, original_url: str) -> bytes:
    digest = hashlib.sha256()
    for value in (title, body, original_url):
        data = value.encode("utf-8", errors="replace")
        digest.update(str(len(data)).encode("ascii"))
        digest.update(b":")
        digest.update(data)
    return digest.digest()


def replace_document_index(database: sqlite3.Connection, document_id: int,
                           title: str, body: str, original_url: str) -> None:
    signature = index_signature(title, body, original_url)
    current = database.execute(
        "SELECT signature FROM document_fts_versions WHERE document_id=?", (document_id,)
    ).fetchone()
    if current and current[0] == signature:
        return
    # Both the mapping change and token insertion belong to the caller's single
    # transaction. A rollback restores the previous searchable version.
    database.execute("DELETE FROM document_fts_versions WHERE document_id=?", (document_id,))
    cursor = database.execute(
        "INSERT INTO document_fts_versions(document_id,signature) VALUES(?,?)",
        (document_id, signature),
    )
    database.execute(
        "INSERT INTO documents_fts(rowid,title,body_text,original_url) VALUES(?,?,?,?)",
        (int(cursor.lastrowid), title, body, original_url),
    )


def rebuild_document_index(database: sqlite3.Connection, batch_size: int = 500, callback=None) -> int:
    enabled = database.execute("SELECT value FROM project_meta WHERE key='fts5'").fetchone()
    if not enabled or enabled[0] != "1":
        return 0
    database.execute("DROP TABLE IF EXISTS documents_fts")
    database.execute("CREATE VIRTUAL TABLE documents_fts USING fts5(title,body_text,original_url,content='')")
    database.execute("DELETE FROM document_fts_versions")
    total = int(database.execute("SELECT COUNT(*) FROM documents d JOIN captures c ON c.id=d.capture_id "
                                "WHERE COALESCE(c.payload_availability,'retained') NOT IN ('discarded','cleanup_pending')").fetchone()[0]) if callback else 0
    cursor = database.execute(
        """SELECT d.*,c.original_url AS capture_original_url,c.mimetype,c.detected_encoding
           FROM documents d JOIN captures c ON c.id=d.capture_id
           WHERE COALESCE(c.payload_availability,'retained') NOT IN ('discarded','cleanup_pending')
           ORDER BY d.id"""
    )
    rebuilt = 0
    while rows := cursor.fetchmany(max(1, int(batch_size))):
        for row in rows:
            document_id = int(row["id"])
            title = str(row["title"] or "")
            original = str(row["capture_original_url"] or "")
            path = Path(str(row["path"] or ""))
            try:
                data = path.read_bytes() if path.is_file() else None
            except OSError:
                data = None
            if data is None:
                body = document_body(row)
            else:
                content_type = str(row["mimetype"] or "")
                if row["detected_encoding"]:
                    content_type += "; charset=" + str(row["detected_encoding"])
                if looks_textual_bytes(data[:16384], content_type):
                    raw = decode_bytes(data, content_type)
                    del data
                    _title, body, _links = parse_page(raw, original)
                else:
                    body = ""
            database.execute(
                "INSERT INTO document_fts_versions(fts_rowid,document_id,signature) VALUES(?,?,?)",
                (document_id, document_id, index_signature(title, body, original)),
            )
            database.execute(
                "INSERT INTO documents_fts(rowid,title,body_text,original_url) VALUES(?,?,?,?)",
                (document_id, title, body, original),
            )
            rebuilt += 1
        if callback:
            from ..events import ProgressEvent
            callback(ProgressEvent("full_text_rebuild", "Rebuilding current full-text index", rebuilt, total))
    return rebuilt
