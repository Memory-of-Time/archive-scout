from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
import threading
import tempfile
from pathlib import Path
from typing import Callable

from ..database.connection import DATABASE_NAME
from ..database.repositories import get_or_create_media_target, get_or_create_target
from ..events import ProgressEvent, Stopped
from .files import copy_verified, file_hash, DiskIdentityMap
from ..media.downloader import media_path
from ..utils import atomic_write_text, utc_now


def _fingerprint(path: Path, source_root: Path | None = None) -> str:
    """Hash a consistent SQLite backup, including any previously uncheckpointed WAL.

    The file's mtime/size cannot identify a changing SQLite database. The path
    scopes otherwise-identical snapshots from different source projects.
    """
    digest = hashlib.sha256(str((source_root or path).resolve()).encode("utf-8", "replace"))
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()[:24]


def _snapshot_source(source_db_path: Path, destination: Path) -> None:
    # SQLite's online backup API provides a logically consistent snapshot of
    # the main database AND its WAL, even while the source receives writes.
    source = sqlite3.connect(source_db_path.resolve().as_uri() + "?mode=ro", uri=True, timeout=60)
    try:
        backup = sqlite3.connect(destination)
        try:
            source.backup(backup, pages=1000, sleep=0.05)
        finally:
            backup.close()
    finally:
        source.close()


def _source_file(source_root: Path, value: str) -> Path | None:
    source_root = source_root.resolve()
    path = Path(value)
    candidate = path if path.is_absolute() else source_root / path
    try:
        resolved = candidate.resolve()
    except (OSError, RuntimeError):
        return None
    if resolved != source_root and source_root not in resolved.parents:
        return None
    return resolved


def _copy_file(source: Path | None, destination_root: Path, category: str, fingerprint: str, expected_hash: str = "", expected_size=None, stop_event=None) -> Path | None:
    if source is None or not source.exists() or not source.is_file():
        return None
    digest = hashlib.sha256(str(source).encode("utf-8", "replace")).hexdigest()[:16]
    destination = destination_root / category / "merged" / fingerprint / f"{digest}_{source.name}"
    destination.parent.mkdir(parents=True, exist_ok=True)
    return copy_verified(source,destination,expected_hash,expected_size,stop_event)


def _table_exists(database: sqlite3.Connection, table: str) -> bool:
    return database.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None


def _columns(database: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in database.execute(f"PRAGMA table_info({table})")}


def _value(row: sqlite3.Row, columns: set[str], name: str, default=None):
    return row[name] if name in columns else default


def _rebuild_fts(database: sqlite3.Connection) -> None:
    enabled = database.execute("SELECT value FROM project_meta WHERE key='fts5'").fetchone()
    if not enabled or str(enabled["value"]) != "1":
        return
    database.execute("DELETE FROM documents_fts")
    database.execute(
        """
        INSERT INTO documents_fts(rowid,title,body_text,original_url)
        SELECT d.id,COALESCE(d.title,''),COALESCE(d.body_text,''),c.original_url
        FROM documents d JOIN captures c ON c.id=d.capture_id
        """
    )


def merge_projects(
    destination_root: Path,
    source_root: Path,
    database: sqlite3.Connection,
    stop_event: threading.Event | None = None,
    callback: Callable[[ProgressEvent], None] | None = None,
) -> dict[str, int]:
    destination_root = destination_root.absolute()
    source_root = source_root.absolute()
    if destination_root.resolve() == source_root.resolve():
        raise ValueError("source and destination projects must be different")
    source_db_path = source_root / DATABASE_NAME
    if not source_db_path.exists():
        raise FileNotFoundError(f"Archive Scout database not found: {source_db_path}")
    source_key = str(source_root.resolve())
    # Snapshot files live outside the source project so a live project need not
    # be checkpointed or modified for a merge.
    snapshot_dir = tempfile.TemporaryDirectory(prefix="scout-merge-snapshot-")
    snapshot_path = Path(snapshot_dir.name) / "source.sqlite3"
    try:
        _snapshot_source(source_db_path, snapshot_path)
        fingerprint = _fingerprint(snapshot_path, source_root)
        existing = database.execute(
            "SELECT summary_json FROM project_merges WHERE source_fingerprint=?", (fingerprint,)
        ).fetchone()
        if existing:
            return json.loads(existing["summary_json"] or "{}")
        # Older merges have no stable source->destination scan mapping. Never
        # duplicate their historical scans when a live WAL changes: explicitly
        # require a one-time reconciliation instead of guessing identities.
        old_merges = database.execute(
            "SELECT 1 FROM project_merges WHERE source_path=? LIMIT 1", (source_key,)
        ).fetchone()
        new_links = database.execute(
            "SELECT 1 FROM project_merge_scan_links WHERE source_path=? LIMIT 1", (source_key,)
        ).fetchone()
        if old_merges and not new_links:
            raise RuntimeError(
                "This source was merged by a legacy release without source-run identity mappings. "
                "An updated snapshot cannot be merged safely without historical-run reconciliation; "
                "original project and previously merged data have not been modified."
            )
        source = sqlite3.connect(snapshot_path)
        source.row_factory = sqlite3.Row
    except BaseException:
        snapshot_dir.cleanup()
        raise
    stop_event = stop_event or threading.Event()
    summary = {"captures": 0, "documents": 0, "media": 0, "scan_runs": 0, "matches": 0, "reviews": 0, "notes": 0, "extractions": 0}
    target_map = DiskIdentityMap(database,"target_map")
    capture_map = DiskIdentityMap(database,"capture_map")
    document_map = DiskIdentityMap(database,"document_map")
    media_target_map = DiskIdentityMap(database,"media_target_map")
    media_map = DiskIdentityMap(database,"media_map")
    keyword_set_map = DiskIdentityMap(database,"keyword_set_map")
    scan_map = DiskIdentityMap(database,"scan_map")
    match_map = DiskIdentityMap(database,"match_map")
    tag_map = DiskIdentityMap(database,"tag_map")

    def emit(message: str, completed: int = 0, total: int = 0) -> None:
        if callback:
            callback(ProgressEvent("merge", message, completed, total))

    try:
        with database:
            for row in source.execute("SELECT * FROM targets ORDER BY id"):
                if stop_event.is_set(): raise Stopped
                target_map[int(row["id"])] = get_or_create_target(database, str(row["pattern"]))

            capture_total = int(source.execute("SELECT COUNT(*) FROM captures").fetchone()[0])
            capture_rows = source.execute("SELECT * FROM captures ORDER BY id")
            for index, row in enumerate(capture_rows, 1):
                if stop_event.is_set(): raise Stopped
                if stop_event.is_set():
                    raise Stopped
                target_id = target_map.get(int(row["target_id"])) if row["target_id"] is not None else None
                database.execute(
                    """
                    INSERT OR IGNORE INTO captures(
                        original_url,timestamp,target_id,query_signature,mimetype,statuscode,digest,length,state,
                        download_attempts,http_status,final_url,bytes_saved,created_at,updated_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        row["original_url"], row["timestamp"], target_id, row["query_signature"], row["mimetype"],
                        row["statuscode"], row["digest"], row["length"], row["state"], row["download_attempts"],
                        row["http_status"], row["final_url"], row["bytes_saved"], row["created_at"], row["updated_at"],
                    ),
                )
                merged = database.execute(
                    "SELECT id FROM captures WHERE original_url=? AND timestamp=? AND query_signature=?",
                    (row["original_url"], row["timestamp"], row["query_signature"]),
                ).fetchone()
                capture_map[int(row["id"])] = int(merged["id"])
                summary["captures"] += 1
                if index % 1000 == 0:
                    emit(f"Merged {index:,}/{capture_total:,} captures", index, capture_total)

            for row in source.execute("SELECT * FROM documents ORDER BY id"):
                if stop_event.is_set(): raise Stopped
                old_capture = int(row["capture_id"])
                if old_capture not in capture_map:
                    continue
                existing_doc = database.execute(
                    "SELECT id,content_hash,path,size_bytes FROM documents WHERE capture_id=?",
                    (capture_map[old_capture],)
                ).fetchone()
                if existing_doc:
                    document_id = int(existing_doc["id"])
                    actual_size,actual_hash = file_hash(existing_doc['path'],stop_event)
                    if actual_hash != str(existing_doc['content_hash']) or actual_size != int(existing_doc['size_bytes']):
                        raise RuntimeError('Existing merge destination evidence failed validation')
                    if (row["content_hash"] and existing_doc["content_hash"] and
                            str(row["content_hash"]) != str(existing_doc["content_hash"])):
                        raise RuntimeError(
                            "Merge source changed the raw hash of an existing capture. "
                            "Automatic replacement would invalidate historical scan evidence; "
                            "destination has not been modified."
                        )
                else:
                    # Repeated merges of a changing WAL must never recopy every
                    # previously imported file into a new fingerprint directory.
                    source_path = _source_file(source_root, str(row["path"]))
                    destination_path = _copy_file(source_path, destination_root, "captures", fingerprint, str(row["content_hash"] or ""), int(row["size_bytes"]), stop_event)
                    reconstructed = destination_path is None
                    if reconstructed:
                        destination_path = (
                            destination_root / "captures" / "merged" / fingerprint / f"recovered_{int(row['id'])}.txt"
                        )
                        atomic_write_text(destination_path, str(row["body_text"] or ""))
                    cursor = database.execute(
                        """
                        INSERT INTO documents(
                            capture_id,path,title,body_text,links_json,content_hash,normalized_hash,size_bytes,created_at,updated_at
                        ) VALUES(?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            capture_map[old_capture], str(destination_path), row["title"], row["body_text"], row["links_json"],
                            row["content_hash"], row["normalized_hash"], row["size_bytes"], row["created_at"], row["updated_at"],
                        ),
                    )
                    document_id = int(cursor.lastrowid)
                    database.execute("UPDATE captures SET document_id=?,state='downloaded' WHERE id=?", (document_id, capture_map[old_capture]))
                    # Indexed FTS insertion is proportional to newly imported
                    # documents; rebuilding every existing row grows with the
                    # destination on every incremental merge.
                    fts = database.execute("SELECT value FROM project_meta WHERE key='fts5'").fetchone()
                    if fts and fts[0] == '1':
                        database.execute("""INSERT INTO documents_fts(rowid,title,body_text,original_url)
                            VALUES(?,?,?,?)""", (document_id,row["title"],row["body_text"],
                                                  source.execute("SELECT original_url FROM captures WHERE id=?",
                                                      (old_capture,)).fetchone()[0]))
                    if reconstructed:
                        # A derived text reconstruction cannot be misrepresented
                        # as the original raw-byte SHA-256 capture.
                        database.execute("""INSERT OR REPLACE INTO capture_routing
                            (capture_id,resource_class,evidence,confident,routing,updated_at)
                            VALUES(?,'text','merge:derived_text_original_raw_unavailable',0,'downloaded_scanned',?)""",
                            (capture_map[old_capture],utc_now()))
                document_map[int(row["id"])] = document_id
                summary["documents"] += 1

            if _table_exists(source, "media_targets"):
                for row in source.execute("SELECT * FROM media_targets ORDER BY id"):
                    if stop_event.is_set(): raise Stopped
                    media_target_map[int(row["id"])] = get_or_create_media_target(database, str(row["pattern"]))
            if _table_exists(source, "media_captures"):
                for row in source.execute("SELECT * FROM media_captures ORDER BY id"):
                    if stop_event.is_set(): raise Stopped
                    target_id = media_target_map.get(int(row["target_id"])) if row["target_id"] is not None else None
                    source_document_id = document_map.get(int(row["source_document_id"])) if row["source_document_id"] is not None else None
                    source_path = _source_file(source_root, str(row["path"])) if row["path"] else None
                    destination_path = media_path(destination_root, row) if source_path else None
                    if source_path and source_path.exists() and source_path.is_file() and destination_path is not None:
                        destination_path.parent.mkdir(parents=True, exist_ok=True)
                        if destination_path.is_file() and file_hash(destination_path,stop_event)[1] != str(row['content_hash'] or ''):
                            from ..media.downloader import _disambiguate_media_path
                            destination_path = _disambiguate_media_path(destination_path,row)
                        copy_verified(source_path,destination_path,str(row['content_hash'] or ''),int(row['bytes_saved'] or 0),stop_event)
                    media_file_missing = bool(row["path"]) and (destination_path is None or not destination_path.exists())
                    database.execute(
                        """
                        INSERT OR IGNORE INTO media_captures(
                            original_url,timestamp,target_id,source_document_id,source_type,query_signature,media_kind,extension,
                            mimetype,statuscode,digest,length,state,download_attempts,path,http_status,final_url,bytes_saved,
                            content_hash,created_at,updated_at
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            row["original_url"], row["timestamp"], target_id, source_document_id, row["source_type"],
                            row["query_signature"], row["media_kind"], row["extension"], row["mimetype"], row["statuscode"],
                            row["digest"], row["length"], "pending" if media_file_missing else row["state"],
                            0 if media_file_missing else row["download_attempts"], str(destination_path) if destination_path else None,
                            row["http_status"], row["final_url"], 0 if media_file_missing else row["bytes_saved"],
                            row["content_hash"], row["created_at"], row["updated_at"],
                        ),
                    )
                    merged = database.execute(
                        "SELECT id FROM media_captures WHERE original_url=? AND timestamp=? AND query_signature=?",
                        (row["original_url"], row["timestamp"], row["query_signature"]),
                    ).fetchone()
                    media_map[int(row["id"])] = int(merged["id"])
                    summary["media"] += 1

            keyword_columns = _columns(source, "keyword_sets")
            for row in source.execute("SELECT * FROM keyword_sets ORDER BY id"):
                if stop_event.is_set(): raise Stopped
                rules_json = _value(row, keyword_columns, "rules_json")
                if not rules_json:
                    rules_json = _value(row, keyword_columns, "keywords_json", "[]")
                database.execute(
                    """
                    INSERT OR IGNORE INTO keyword_sets(name,fingerprint,keywords_json,rules_json,created_at,updated_at)
                    VALUES(?,?,?,?,?,?)
                    """,
                    (row["name"], row["fingerprint"], row["keywords_json"], rules_json, row["created_at"], row["updated_at"]),
                )
                merged = database.execute("SELECT id FROM keyword_sets WHERE fingerprint=?", (row["fingerprint"],)).fetchone()
                keyword_set_map[int(row["id"])] = int(merged["id"])

            scan_columns = _columns(source, "scan_runs")
            for row in source.execute("SELECT * FROM scan_runs ORDER BY id"):
                if stop_event.is_set(): raise Stopped
                existing_link = database.execute(
                    "SELECT destination_run_id FROM project_merge_scan_links "
                    "WHERE source_path=? AND source_run_id=?", (source_key, int(row["id"]))
                ).fetchone()
                if existing_link:
                    linked_id = int(existing_link["destination_run_id"])
                    scan_map[int(row["id"])] = linked_id
                    database.execute("""UPDATE scan_runs SET status=?,completed_at=?,
                        document_count=?,match_count=?,duration_seconds=? WHERE id=?""",
                        (row["status"],row["completed_at"],
                         int(_value(row, scan_columns, "document_count", 0) or 0),
                         int(_value(row, scan_columns, "match_count", 0) or 0),
                         float(_value(row, scan_columns, "duration_seconds", 0) or 0),linked_id))
                    continue
                cursor = database.execute(
                    """
                    INSERT INTO scan_runs(
                        keyword_set_id,name,status,minimum_score,started_at,completed_at,source_operation,
                        document_count,match_count,duration_seconds,metadata_json
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        keyword_set_map[int(row["keyword_set_id"])], f"{row['name']} [merged]", row["status"], row["minimum_score"],
                        row["started_at"], row["completed_at"], "merged",
                        int(_value(row, scan_columns, "document_count", 0) or 0),
                        int(_value(row, scan_columns, "match_count", 0) or 0),
                        float(_value(row, scan_columns, "duration_seconds", 0) or 0),
                        _value(row, scan_columns, "metadata_json", "{}"),
                    ),
                )
                scan_map[int(row["id"])] = int(cursor.lastrowid)
                database.execute("""INSERT INTO project_merge_scan_links
                    (source_path,source_run_id,destination_run_id) VALUES(?,?,?)""",
                    (source_key,int(row["id"]),int(cursor.lastrowid)))
                summary["scan_runs"] += 1

            match_columns = _columns(source, "document_matches")
            for row in source.execute("SELECT * FROM document_matches ORDER BY id"):
                if stop_event.is_set(): raise Stopped
                old_document = int(row["document_id"])
                if old_document not in document_map:
                    continue
                cursor = database.execute(
                    """
                    INSERT INTO document_matches(
                        scan_run_id,document_id,score,hits_json,fields_json,snippets_json,interesting_links_json,
                        excluded,required_missing,proximity_json,created_at,updated_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(scan_run_id,document_id) DO UPDATE SET
                        score=excluded.score,hits_json=excluded.hits_json,
                        fields_json=excluded.fields_json,snippets_json=excluded.snippets_json,
                        interesting_links_json=excluded.interesting_links_json,
                        excluded=excluded.excluded,required_missing=excluded.required_missing,
                        proximity_json=excluded.proximity_json,updated_at=excluded.updated_at
                    """,
                    (
                        scan_map[int(row["scan_run_id"])], document_map[old_document], row["score"], row["hits_json"],
                        row["fields_json"], row["snippets_json"], row["interesting_links_json"],
                        int(_value(row, match_columns, "excluded", 0) or 0),
                        int(_value(row, match_columns, "required_missing", 0) or 0),
                        _value(row, match_columns, "proximity_json", "{}"), row["created_at"], row["updated_at"],
                    ),
                )
                merged = database.execute(
                    "SELECT id FROM document_matches WHERE scan_run_id=? AND document_id=?",
                    (scan_map[int(row["scan_run_id"])], document_map[old_document]),
                ).fetchone()
                match_map[int(row["id"])] = int(merged["id"])
                summary["matches"] += 1

            if _table_exists(source, "reviews"):
                for row in source.execute("SELECT * FROM reviews ORDER BY id"):
                    if stop_event.is_set(): raise Stopped
                    if int(row["match_id"]) not in match_map:
                        continue
                    database.execute(
                        """
                        INSERT INTO reviews(match_id,status,reviewer,reviewed_at) VALUES(?,?,?,?)
                        ON CONFLICT(match_id) DO UPDATE SET
                            status=CASE WHEN reviews.status='unreviewed' THEN excluded.status ELSE reviews.status END,
                            reviewer=COALESCE(reviews.reviewer,excluded.reviewer),
                            reviewed_at=COALESCE(reviews.reviewed_at,excluded.reviewed_at)
                        """,
                        (match_map[int(row["match_id"])], row["status"], row["reviewer"], row["reviewed_at"]),
                    )
                    summary["reviews"] += 1
            if _table_exists(source, "notes"):
                for row in source.execute("SELECT * FROM notes ORDER BY id"):
                    if stop_event.is_set(): raise Stopped
                    match_id = match_map.get(int(row["match_id"])) if row["match_id"] is not None else None
                    capture_id = capture_map.get(int(row["capture_id"])) if row["capture_id"] is not None else None
                    if match_id is None and capture_id is None:
                        continue
                    if database.execute(
                        "SELECT 1 FROM project_merge_note_links WHERE source_path=? AND source_note_id=?",
                        (source_key,int(row["id"]))
                    ).fetchone():
                        continue
                    cursor = database.execute(
                        "INSERT INTO notes(match_id,capture_id,text,author,created_at,updated_at) VALUES(?,?,?,?,?,?)",
                        (match_id, capture_id, row["text"], row["author"], row["created_at"], row["updated_at"]),
                    )
                    database.execute("INSERT INTO project_merge_note_links VALUES(?,?,?)",
                                     (source_key,int(row["id"]),int(cursor.lastrowid)))
                    summary["notes"] += 1
            if _table_exists(source, "tags"):
                for row in source.execute("SELECT * FROM tags ORDER BY id"):
                    if stop_event.is_set(): raise Stopped
                    database.execute("INSERT OR IGNORE INTO tags(name) VALUES(?)", (row["name"],))
                    tag_map[int(row["id"])] = int(database.execute("SELECT id FROM tags WHERE name=?", (row["name"],)).fetchone()["id"])
            if _table_exists(source, "match_tags"):
                for row in source.execute("SELECT * FROM match_tags"):
                    if stop_event.is_set(): raise Stopped
                    if int(row["match_id"]) in match_map and int(row["tag_id"]) in tag_map:
                        database.execute(
                            "INSERT OR IGNORE INTO match_tags(match_id,tag_id) VALUES(?,?)",
                            (match_map[int(row["match_id"])], tag_map[int(row["tag_id"])]),
                        )
            if _table_exists(source, "extractions"):
                columns = {item[1] for item in source.execute("PRAGMA table_info(extractions)")}
                for row in source.execute("SELECT * FROM extractions ORDER BY id"):
                    if stop_event.is_set(): raise Stopped
                    if int(row["document_id"]) not in document_map:
                        continue
                    if database.execute(
                        "SELECT 1 FROM project_merge_extraction_links WHERE source_path=? AND source_extraction_id=?",
                        (source_key,int(row["id"]))
                    ).fetchone():
                        continue
                    cursor = database.execute(
                        """
                        INSERT INTO extractions(document_id,extractor_name,extractor_type,field,value,context,start_offset,end_offset,created_at)
                        VALUES(?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            document_map[int(row["document_id"])], row["extractor_name"],
                            row["extractor_type"] if "extractor_type" in columns else "regex",
                            row["field"] if "field" in columns else "body", row["value"], row["context"],
                            row["start_offset"] if "start_offset" in columns else None,
                            row["end_offset"] if "end_offset" in columns else None, row["created_at"],
                        ),
                    )
                    database.execute("INSERT INTO project_merge_extraction_links VALUES(?,?,?)",
                                     (source_key,int(row["id"]),int(cursor.lastrowid)))
                    summary["extractions"] += 1

            database.execute(
                "INSERT INTO project_merges(source_path,source_fingerprint,merged_at,summary_json) VALUES(?,?,?,?)",
                (str(source_root), fingerprint, utc_now(), json.dumps(summary, sort_keys=True)),
            )
        emit(f"Merged project: {summary['documents']:,} documents and {summary['reviews']:,} reviews")
        return summary
    finally:
        source.close()
        snapshot_dir.cleanup()
