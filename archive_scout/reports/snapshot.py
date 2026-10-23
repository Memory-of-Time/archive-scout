"""Independent, immutable report snapshots over committed project evidence.

The report worker deliberately does not acquire the mutating-operation lease or
call ``open_database``: opening a second migrating/repair connection while the
main worker runs is unsafe. A read-only WAL reader with private TEMP tables
is sufficient for the existing streaming report writers.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import threading
import uuid
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from ..config import ProjectConfig
from ..database.connection import DATABASE_NAME
from ..database.lease import project_lease
from ..events import Stopped
from ..media.reports import generate_media_reports
from ..reports.text import generate_index_reports, generate_reports
from ..utils import atomic_write_text


_REPORT_LOCKS: dict[str, threading.Lock] = {}
_REPORT_LOCKS_MUTEX = threading.Lock()


def _lock_for(root: Path) -> threading.Lock:
    key = str(root.resolve())
    with _REPORT_LOCKS_MUTEX:
        return _REPORT_LOCKS.setdefault(key, threading.Lock())


def _collect_report_files(config: ProjectConfig, db: sqlite3.Connection,
                          scan_run_id: int | None = None,
                          progress=None, stop_event=None) -> dict[str, Path]:
    if stop_event is not None and stop_event.is_set():
        raise Stopped
    paths: dict[str, Path] = {}
    if scan_run_id is None:
        latest = db.execute("SELECT id FROM scan_runs ORDER BY id DESC LIMIT 1").fetchone()
        scan_run_id = int(latest[0]) if latest else None
    elif not db.execute("SELECT 1 FROM scan_runs WHERE id=?", (scan_run_id,)).fetchone():
        raise ValueError(f"scan run {scan_run_id} does not exist")
    if scan_run_id is not None:
        paths.update(generate_reports(config, db, scan_run_id,
                                      progress=progress, stop_event=stop_event))
    else:
        # Valid even for an empty project: an empty inventory is not evidence
        # that any requested CDX query has been completed.
        paths.update(generate_index_reports(config, db, progress=progress,
                                            stop_event=stop_event))
    if db.execute("SELECT 1 FROM media_captures LIMIT 1").fetchone() or db.execute(
            "SELECT 1 FROM media_index_state LIMIT 1").fetchone():
        paths.update(generate_media_reports(config, db, stop_event=stop_event))
    return paths


def generate_project_report_files(config: ProjectConfig, database: sqlite3.Connection,
                                  *, scan_run_id: int | None = None,
                                  progress=None, stop_event=None) -> dict[str, Path]:
    """Legacy idle report regeneration, now covers text/index and media."""
    return _collect_report_files(config, database, scan_run_id, progress, stop_event)


def _export_project_snapshot_locked(config: ProjectConfig, *, scan_run_id: int | None = None,
                            stop_event: threading.Event | None = None,
                            progress=None) -> dict[str, Path]:
    """Export a committed-state snapshot without changing scan/operation state.

    Root files are never replaced. Only the fully rendered private folder is
    published, and failures delete their staging tree. No copy of the user's
    SQLite database or capture payloads is created.
    """
    root = Path(config.output_dir).expanduser().resolve()
    path = root / DATABASE_NAME
    if not path.is_file():
        raise FileNotFoundError(f"project database not found: {path}")
    lock = _lock_for(root)
    if not lock.acquire(blocking=False):
        raise RuntimeError("A report snapshot is already being written for this project")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    token = uuid.uuid4().hex[:10]
    parent = root / "reports" / "manual"
    staging = parent / f".partial-{timestamp}-{token}"
    final = parent / f"{timestamp}-{token}"
    try:
        parent.mkdir(parents=True, exist_ok=True)
        staging.mkdir()
        # A SQLite URI using mode=ro permits TEMP tables but cannot mutate the
        # main DB. query_only=ON is intentionally *not* used: text reports
        # generate deduplicated link rows in a per-connection TEMP table.
        db = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=15)
        try:
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA busy_timeout=15000")
            db.execute("PRAGMA cache_size=-8192")
            db.execute("PRAGMA mmap_size=67108864")
            db.execute("PRAGMA temp_store=FILE")
            db.execute("BEGIN")
            # Pin the WAL read view before collecting any report rows.
            operation = db.execute("SELECT id,mode,status,message FROM operation_runs ORDER BY id DESC LIMIT 1").fetchone()
            scan = db.execute("SELECT id,status FROM scan_runs WHERE id=?", (scan_run_id,)).fetchone() if scan_run_id else db.execute(
                "SELECT id,status FROM scan_runs ORDER BY id DESC LIMIT 1").fetchone()
            captures = int(db.execute("SELECT COUNT(*) FROM captures").fetchone()[0])
            media = int(db.execute("SELECT COUNT(*) FROM media_captures").fetchone()[0])
            documents = int(db.execute("SELECT COUNT(*) FROM documents").fetchone()[0])
            pending = int(db.execute("SELECT COUNT(*) FROM captures WHERE state IN ('pending','error','downloading','downloaded_unscanned')").fetchone()[0])
            index_checkpoints = int(db.execute("SELECT COUNT(*) FROM index_coverage").fetchone()[0]) + int(db.execute(
                "SELECT COUNT(*) FROM index_state").fetchone()[0])
            incomplete_index = int(db.execute("SELECT COUNT(*) FROM index_coverage WHERE complete=0").fetchone()[0]) + int(db.execute(
                "SELECT COUNT(*) FROM index_state WHERE complete=0").fetchone()[0])
            incomplete_media = int(db.execute("SELECT COUNT(*) FROM media_index_state WHERE complete=0").fetchone()[0])
            media_pending = int(db.execute("SELECT COUNT(*) FROM media_captures WHERE state IN ('pending','error','downloading')").fetchone()[0])
            request = db.execute("SELECT value FROM project_meta WHERE key='resume_request'").fetchone()
            backlog = db.execute("SELECT value FROM project_meta WHERE key='resume_backlog'").fetchone()
            unfinished_requests = int(request is not None) + (len(json.loads(backlog[0])) if backlog else 0)
            embedded_pending = int(db.execute("""SELECT COUNT(*) FROM media_discovery_queue
                WHERE state IN ('pending','error','resolving','blocked')
                OR (state='unavailable' AND COALESCE(last_error,'')!='')""").fetchone()[0])
            # Clone settings, redirecting *only* output paths to staging.
            temporary_config = replace(config, output_dir=staging)
            paths = _collect_report_files(temporary_config, db, scan_run_id, progress, stop_event)
            if stop_event is not None and stop_event.is_set():
                raise Stopped
            manifest = {
                "format": "archive-scout-report-snapshot-v1",
                "generated_utc": datetime.now(timezone.utc).isoformat(),
                "project": str(root),
                "scope": {"targets": list(config.targets), "from_date": config.from_date,
                          "to_date": config.to_date,
                          "selected_keyword_sets": [item.name for item in config.selected_keyword_sets()]},
                "snapshot_consistency": "single SQLite read transaction over committed rows",
                "operation": ({"id": int(operation[0]), "mode": operation[1], "status": operation[2], "message": operation[3]} if operation else None),
                "scan": ({"id": int(scan[0]), "status": scan[1]} if scan else None),
                "counts": {"indexed_captures": captures, "media_captures": media,
                           "saved_documents": documents, "unfinished_text_captures": pending,
                           "unfinished_media_captures": media_pending,
                           "index_checkpoints": index_checkpoints,
                           "incomplete_index_checkpoints": incomplete_index,
                           "incomplete_media_checkpoints": incomplete_media,
                           "unfinished_requests": unfinished_requests,
                           "unfinished_embedded_lookups": embedded_pending},
                "complete": bool(index_checkpoints and operation and operation[1] != "report" and
                                 operation[2] == "complete" and
                                 (scan is None or scan[1] == "complete") and
                                 not (pending or media_pending or incomplete_index or incomplete_media
                                      or unfinished_requests or embedded_pending)),
                "files": sorted(str(value.relative_to(staging)) for value in paths.values() if value.is_file()),
                "note": "A successful report export does not establish inventory, acquisition, or scan completeness."
            }
            if not paths:
                manifest["note"] += " No report outputs are enabled."
            atomic_write_text(staging / "manifest.json", json.dumps(manifest, indent=2, ensure_ascii=False) + "\n")
            db.rollback()  # End our read snapshot; never mutate project DB.
        finally:
            db.close()
        os.replace(staging, final)
        return {"snapshot_folder": final, "manifest": final / "manifest.json"}
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    finally:
        lock.release()


def export_project_snapshot(config: ProjectConfig, *, scan_run_id: int | None = None,
                            stop_event: threading.Event | None = None,
                            progress=None) -> dict[str, Path]:
    """One report snapshot per project, including across independent GUI processes."""
    root = Path(config.output_dir).expanduser().resolve()
    # Distinct from archive_scout.lock: active indexing/acquisition must be able
    # to continue while a report reader exports committed state.
    with project_lease(root, lock_name="archive_scout_report.lock"):
        return _export_project_snapshot_locked(config, scan_run_id=scan_run_id,
                                               stop_event=stop_event, progress=progress)
