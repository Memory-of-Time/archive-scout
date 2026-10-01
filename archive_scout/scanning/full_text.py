from __future__ import annotations

import sqlite3

from ..document_store import document_body


def _fts_available(database: sqlite3.Connection) -> bool:
    enabled = database.execute("SELECT value FROM project_meta WHERE key='fts5'").fetchone()
    return bool(enabled and enabled["value"] == "1")


def _simple_snippet(text: str, query: str, width: int = 320) -> str:
    if not text:
        return ""
    terms = [part.strip('"*() ') for part in query.replace('OR', ' ').replace('AND', ' ').split()]
    lowered = text.casefold()
    positions = [lowered.find(term.casefold()) for term in terms if term and lowered.find(term.casefold()) >= 0]
    pos = min(positions) if positions else 0
    start = max(0, pos - width // 3)
    end = min(len(text), start + width)
    prefix = "…" if start else ""
    suffix = "…" if end < len(text) else ""
    return prefix + text[start:end].replace("\n", " ") + suffix


def search_documents(
    database: sqlite3.Connection,
    query: str,
    limit: int = 500,
    field: str = "all",
    domain: str = "",
    from_timestamp: str = "",
    to_timestamp: str = "",
    scan_run_id: int | None = None,
) -> list[dict]:
    if not _fts_available(database):
        raise RuntimeError("SQLite FTS5 is not available in this Python build")
    query = query.strip()
    if not query:
        return []
    field_map = {"title": "title", "body": "body_text", "url": "original_url"}
    fts_query = f"{field_map[field]}:({query})" if field in field_map else query
    clauses = ["documents_fts MATCH ?"]
    params: list[object] = [fts_query]
    if domain.strip():
        clauses.append("LOWER(c.original_url) LIKE ?")
        params.append("%" + domain.casefold().strip() + "%")
    if from_timestamp.strip():
        clauses.append("c.timestamp>=?")
        params.append(from_timestamp.strip())
    if to_timestamp.strip():
        clauses.append("c.timestamp<=?")
        params.append(to_timestamp.strip())
    if scan_run_id is not None:
        clauses.append("EXISTS (SELECT 1 FROM document_matches m WHERE m.document_id=d.id AND m.scan_run_id=?)")
        params.append(int(scan_run_id))
    params.append(max(1, min(int(limit), 10000)))
    rows = database.execute(
        """
        SELECT d.*,c.original_url AS capture_original_url,c.timestamp,c.mimetype,bm25(documents_fts) AS rank
        FROM documents_fts
        JOIN documents d ON d.id=documents_fts.rowid
        JOIN captures c ON c.id=d.capture_id
        WHERE """ + " AND ".join(clauses) + " ORDER BY rank LIMIT ?",
        params,
    ).fetchall()
    results: list[dict] = []
    for row in rows:
        item = dict(row)
        item["original_url"] = str(item.get("capture_original_url") or item.get("original_url") or "")
        body = document_body(row)
        item["body_text"] = body
        item["snippet"] = _simple_snippet(body, query)
        results.append(item)
    return results
