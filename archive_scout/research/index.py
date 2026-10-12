from __future__ import annotations

import json
import sqlite3
import threading
from dataclasses import dataclass
from typing import Callable

from ..analysis.duplicates import cluster_duplicates
from ..config import ProjectConfig
from ..events import ProgressEvent, Stopped
from ..utils import utc_now
from .embeddings import encode_text, vector_bands
from .entities import extract_entities


@dataclass(slots=True)
class ResearchIndexSummary:
    indexed: int = 0
    unchanged: int = 0
    removed: int = 0
    entities: int = 0
    edges: int = 0
    duplicate_groups: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "indexed": self.indexed,
            "unchanged": self.unchanged,
            "removed": self.removed,
            "entities": self.entities,
            "edges": self.edges,
            "duplicate_groups": self.duplicate_groups,
        }


def _emit(callback: Callable[[ProgressEvent], None] | None, event: ProgressEvent) -> None:
    if callback:
        callback(event)


def _document_text(row: sqlite3.Row, excerpt_chars: int) -> str:
    body = str(row["body_text"] or "")
    if len(body) > excerpt_chars * 8:
        body = body[: excerpt_chars * 8]
    return "\n".join(part for part in (str(row["title"] or ""), str(row["original_url"] or ""), body) if part)


def _replace_entities(database: sqlite3.Connection, document_id: int, entities) -> int:
    database.execute("DELETE FROM research_document_entities WHERE document_id=?", (document_id,))
    unique = {(entity.kind, entity.normalized): entity for entity in entities if entity.normalized}
    if not unique:
        return 0
    database.executemany(
        "INSERT OR IGNORE INTO research_entities(kind,value,normalized) VALUES(?,?,?)",
        ((entity.kind, entity.value, entity.normalized) for entity in unique.values()),
    )
    ids: dict[tuple[str, str], int] = {}
    normalized_values = list(dict.fromkeys(entity.normalized for entity in unique.values()))
    for offset in range(0, len(normalized_values), 400):
        chunk = normalized_values[offset:offset + 400]
        placeholders = ",".join("?" for _ in chunk)
        for row in database.execute(
            f"SELECT id,kind,normalized FROM research_entities WHERE normalized IN ({placeholders})",
            tuple(chunk),
        ):
            ids[(str(row["kind"]), str(row["normalized"]))] = int(row["id"])
    links = [(document_id, ids[key], 1) for key in unique if key in ids]
    if links:
        database.executemany(
            "INSERT OR IGNORE INTO research_document_entities(document_id,entity_id,occurrence_count) VALUES(?,?,?)",
            links,
        )
    return len(links)


def _document_fingerprint(database: sqlite3.Connection) -> str:
    row = database.execute(
        """
        SELECT COUNT(*),COALESCE(MAX(id),0),COALESCE(MAX(updated_at),''),COALESCE(SUM(size_bytes),0)
        FROM documents WHERE COALESCE(body_text,'')<>''
        """
    ).fetchone()
    return "|".join(str(value or "") for value in row)


def _dependency_fingerprint(database: sqlite3.Connection, document_fingerprint: str) -> str:
    values = [document_fingerprint]
    for table in ("duplicate_groups", "duplicate_members", "provenance_edges", "forum_posts"):
        try:
            row = database.execute(f"SELECT COUNT(*),COALESCE(MAX(rowid),0) FROM {table}").fetchone()
            values.append(f"{table}:{int(row[0])}:{int(row[1])}")
        except sqlite3.DatabaseError:
            values.append(f"{table}:0:0")
    return "|".join(values)


def _meta(database: sqlite3.Connection, key: str) -> str:
    row = database.execute("SELECT value FROM project_meta WHERE key=?", (key,)).fetchone()
    return str(row[0]) if row else ""


def _resolve_hyperlink_batch(
    database: sqlite3.Connection,
    pending: list[tuple[int, str]],
    add_edge,
    now: str,
) -> None:
    if not pending:
        return
    links = list(dict.fromkeys(link for _, link in pending if link))
    resolved: dict[str, int] = {}
    for offset in range(0, len(links), 400):
        chunk = links[offset:offset + 400]
        placeholders = ",".join("?" for _ in chunk)
        for row in database.execute(
            f"SELECT original_url,document_id FROM archive_scout_research_url_first WHERE original_url IN ({placeholders})",
            tuple(chunk),
        ):
            resolved[str(row["original_url"])] = int(row["document_id"])
    seen_pairs: set[tuple[int, int]] = set()
    for source_id, link in pending:
        target_id = resolved.get(link)
        if target_id is None or target_id == source_id:
            continue
        pair = (source_id, target_id)
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)
        add_edge((source_id, target_id, "hyperlink", 1.0, link[:1000], now))
    pending.clear()


def _rebuild_graph(database: sqlite3.Connection, stop_event: threading.Event) -> int:
    database.execute("DELETE FROM research_edges")
    database.execute("DROP TABLE IF EXISTS temp.archive_scout_research_url_first")
    database.execute(
        "CREATE TEMP TABLE archive_scout_research_url_first(original_url TEXT PRIMARY KEY,document_id INTEGER NOT NULL) WITHOUT ROWID"
    )
    # captures_original_idx already provides the desired URL/timestamp order.
    # INSERT OR IGNORE therefore records the earliest saved document for each URL
    # in one indexed pass rather than one SELECT for every extracted hyperlink.
    database.execute(
        """
        INSERT OR IGNORE INTO archive_scout_research_url_first(original_url,document_id)
        SELECT c.original_url,d.id
        FROM captures c JOIN documents d ON d.capture_id=c.id
        ORDER BY c.original_url,c.timestamp,c.id
        """
    )

    now = utc_now()
    edge_rows: list[tuple[int, int, str, float, str, str]] = []

    def flush_edges() -> None:
        if not edge_rows:
            return
        database.executemany(
            "INSERT OR IGNORE INTO research_edges(source_document_id,target_document_id,edge_type,weight,evidence,created_at) VALUES(?,?,?,?,?,?)",
            edge_rows,
        )
        edge_rows.clear()

    def add_edge(edge: tuple[int, int, str, float, str, str]) -> None:
        edge_rows.append(edge)
        if len(edge_rows) >= 5000:
            flush_edges()

    pending_links: list[tuple[int, str]] = []
    for row in database.execute("SELECT id,links_json FROM documents WHERE COALESCE(links_json,'')<>'' ORDER BY id"):
        if stop_event.is_set():
            raise Stopped
        source_id = int(row["id"])
        try:
            links = json.loads(str(row["links_json"] or "[]"))
        except Exception:
            links = []
        for link in list(dict.fromkeys(str(value) for value in links[:300] if value)):
            pending_links.append((source_id, link))
        if len(pending_links) >= 5000:
            _resolve_hyperlink_batch(database, pending_links, add_edge, now)
    _resolve_hyperlink_batch(database, pending_links, add_edge, now)

    for row in database.execute(
        """
        SELECT dm.document_id,dg.representative_document_id,dm.similarity,dg.method
        FROM duplicate_members dm JOIN duplicate_groups dg ON dg.id=dm.group_id
        WHERE dm.document_id<>dg.representative_document_id
        """
    ):
        add_edge((int(row[0]), int(row[1]), "duplicate:" + str(row[3]), float(row[2]), "", now))

    for row in database.execute(
        "SELECT source_document_id,mirror_document_id,method,similarity FROM provenance_edges"
    ):
        if int(row[0]) != int(row[1]):
            add_edge((int(row[0]), int(row[1]), "provenance:" + str(row[2]), float(row[3]), "", now))

    for row in database.execute(
        """
        SELECT fp.document_id,firsts.representative_document_id,fp.thread_id
        FROM forum_posts fp
        JOIN (SELECT thread_id,MIN(document_id) AS representative_document_id FROM forum_posts GROUP BY thread_id) firsts
          ON firsts.thread_id=fp.thread_id
        WHERE fp.document_id<>firsts.representative_document_id
        """
    ):
        add_edge((int(row[0]), int(row[1]), "forum_thread", 0.9, f"thread:{int(row[2])}", now))
    flush_edges()
    database.execute("DROP TABLE archive_scout_research_url_first")
    return int(database.execute("SELECT COUNT(*) FROM research_edges").fetchone()[0])


def build_research_index(
    config: ProjectConfig,
    database: sqlite3.Connection,
    stop_event: threading.Event | None = None,
    callback: Callable[[ProgressEvent], None] | None = None,
) -> ResearchIndexSummary:
    config = config.normalized()
    research = config.research.normalized()
    stop_event = stop_event or threading.Event()
    total = int(database.execute("SELECT COUNT(*) FROM documents WHERE COALESCE(body_text,'')<>''").fetchone()[0])
    summary = ResearchIndexSummary()
    _emit(callback, ProgressEvent("research_index", f"Building local Research Intelligence index for {total:,} documents…", 0, total))

    processed = 0
    pending_writes = 0
    rows = database.execute(
        """
        SELECT d.id,d.title,d.body_text,d.links_json,d.content_hash,c.original_url,c.timestamp,
               rv.content_hash AS research_content_hash,rv.backend AS research_backend,rv.dimensions AS research_dimensions
        FROM documents d
        JOIN captures c ON c.id=d.capture_id
        LEFT JOIN research_vectors rv ON rv.document_id=d.id
        WHERE COALESCE(d.body_text,'')<>'' ORDER BY d.id
        """
    )
    for row in rows:
        if stop_event.is_set():
            if pending_writes:
                database.commit()
            raise Stopped
        document_id = int(row["id"])
        content_hash = str(row["content_hash"] or "")
        existing_backend = str(row["research_backend"] or "")
        desired_backend = research.vector_backend
        backend_matches = (
            (desired_backend == "local-hash" and existing_backend.startswith("local-hash"))
            or (desired_backend == "fastembed" and existing_backend.startswith("fastembed"))
        )
        if str(row["research_content_hash"] or "") == content_hash and backend_matches:
            summary.unchanged += 1
        else:
            text = _document_text(row, research.excerpt_chars)
            backend_name, vector = encode_text(text, research.vector_backend, research.vector_dimensions)
            try:
                links = list(json.loads(str(row["links_json"] or "[]")))
            except Exception:
                links = []
            now = utc_now()
            database.execute(
                """
                INSERT INTO research_vectors(document_id,content_hash,backend,dimensions,vector_blob,norm,token_count,indexed_at)
                VALUES(?,?,?,?,?,?,?,?)
                ON CONFLICT(document_id) DO UPDATE SET
                  content_hash=excluded.content_hash,backend=excluded.backend,dimensions=excluded.dimensions,
                  vector_blob=excluded.vector_blob,norm=excluded.norm,token_count=excluded.token_count,indexed_at=excluded.indexed_at
                """,
                (document_id, content_hash, backend_name, len(vector.values), vector.blob, vector.norm, vector.token_count, now),
            )
            database.execute("DELETE FROM research_vector_bands WHERE document_id=?", (document_id,))
            database.executemany(
                "INSERT INTO research_vector_bands(document_id,band,bucket) VALUES(?,?,?)",
                ((document_id, band, bucket) for band, bucket in vector_bands(vector.values)),
            )
            if research.entity_extraction:
                entities = extract_entities(
                    str(row["title"] or ""), str(row["body_text"] or ""), str(row["original_url"] or ""), links
                )
                summary.entities += _replace_entities(database, document_id, entities)
            summary.indexed += 1
            pending_writes += 1
            if pending_writes >= 128:
                database.commit()
                pending_writes = 0
        processed += 1
        if processed % 250 == 0 or processed == total:
            _emit(callback, ProgressEvent(
                "research_index",
                f"Research Intelligence indexed {processed:,}/{total:,} documents ({summary.indexed:,} updated; {summary.unchanged:,} unchanged)",
                processed,
                total,
            ))
    if pending_writes:
        database.commit()

    # Remove stale vectors entirely inside SQLite. Keeping Python sets of every
    # indexed/live document made a refresh consume memory proportional to the
    # whole project even when nothing changed.
    summary.removed = int(database.execute(
        """
        SELECT COUNT(*)
        FROM research_vectors rv
        LEFT JOIN documents d ON d.id=rv.document_id
        WHERE d.id IS NULL OR COALESCE(d.body_text,'')=''
        """
    ).fetchone()[0])
    if summary.removed:
        with database:
            database.execute(
                """
                DELETE FROM research_document_entities
                WHERE document_id IN (
                    SELECT rde.document_id
                    FROM research_document_entities rde
                    LEFT JOIN documents d ON d.id=rde.document_id
                    WHERE d.id IS NULL OR COALESCE(d.body_text,'')=''
                )
                """
            )
            database.execute(
                """
                DELETE FROM research_vectors
                WHERE document_id IN (
                    SELECT rv.document_id
                    FROM research_vectors rv
                    LEFT JOIN documents d ON d.id=rv.document_id
                    WHERE d.id IS NULL OR COALESCE(d.body_text,'')=''
                )
                """
            )

    document_fingerprint = _document_fingerprint(database)
    previous_duplicate_fingerprint = _meta(database, "research_duplicate_fingerprint")
    if research.duplicate_clustering and (summary.indexed or summary.removed or previous_duplicate_fingerprint != document_fingerprint):
        _emit(callback, ProgressEvent("research_index", "Clustering exact and near-duplicate documents…", processed, total))
        duplicate_summary = cluster_duplicates(database)
        summary.duplicate_groups = duplicate_summary.exact_groups + duplicate_summary.near_groups
        with database:
            database.execute(
                "INSERT OR REPLACE INTO project_meta(key,value) VALUES('research_duplicate_fingerprint',?)",
                (document_fingerprint,),
            )
    else:
        summary.duplicate_groups = int(database.execute("SELECT COUNT(*) FROM duplicate_groups").fetchone()[0])

    graph_fingerprint = _dependency_fingerprint(database, document_fingerprint)
    graph_is_current = _meta(database, "research_graph_fingerprint") == graph_fingerprint
    if graph_is_current and not summary.indexed and not summary.removed:
        summary.edges = int(database.execute("SELECT COUNT(*) FROM research_edges").fetchone()[0])
    else:
        _emit(callback, ProgressEvent("research_index", "Building evidence relationships…", processed, total))
        with database:
            summary.edges = _rebuild_graph(database, stop_event)
            database.execute(
                "INSERT OR REPLACE INTO project_meta(key,value) VALUES('research_graph_fingerprint',?)",
                (graph_fingerprint,),
            )

    with database:
        database.execute("INSERT OR REPLACE INTO project_meta(key,value) VALUES('research_index_version','2')")
        database.execute("INSERT OR REPLACE INTO project_meta(key,value) VALUES('research_indexed_at',?)", (utc_now(),))
    _emit(callback, ProgressEvent(
        "research_index",
        f"Research Intelligence ready: {total:,} documents, {summary.duplicate_groups:,} duplicate groups, {summary.edges:,} evidence edges.",
        total,
        total,
        summary.to_dict(),
    ))
    return summary
