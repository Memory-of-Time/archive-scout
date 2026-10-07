from __future__ import annotations

import sqlite3
import os
from pathlib import Path
from typing import Callable

from ..classification import RESOURCE_CLASSIFIER_REVISION, classify_capture_inventory
from ..database.full_text_index import rebuild_document_index
from ..events import ProgressEvent
from ..projects.backups import create_project_backup
from ..utils import atomic_write_text, utc_now


def rebuild_full_text_index(database: sqlite3.Connection, batch_size: int = 500, callback=None) -> int:
    return rebuild_document_index(database, batch_size, callback)


def repair_project(
    root: Path,
    database: sqlite3.Connection,
    callback: Callable[[ProgressEvent], None] | None = None,
    *,
    keep_backups: int = 5,
    backup_max_mb: float = 1024.0,
) -> Path:
    """Repair resumable state without deleting historical scan/review evidence."""
    root = Path(root)
    backup = create_project_backup(root, reason="before_repair", keep=keep_backups, max_mb=backup_max_mb)
    actions: list[str] = [f"Backup created: {backup}"]
    if callback:
        callback(ProgressEvent("repair", "Created a safety backup before repair."))

    integrity = database.execute("PRAGMA integrity_check").fetchone()
    if not integrity or str(integrity[0]).casefold() != "ok":
        raise RuntimeError(f"SQLite integrity check failed: {integrity}")
    actions.append("SQLite integrity check: ok")

    now = utc_now()
    with database:
        capture_reset = database.execute("UPDATE captures SET state='pending' WHERE state='downloading'").rowcount
        scan_capture_reset = database.execute("UPDATE captures SET state='downloaded_unscanned' WHERE state='scanning'").rowcount
        media_reset = database.execute("UPDATE media_captures SET state='pending' WHERE state='downloading'").rowcount
        scan_reset = database.execute("UPDATE scan_runs SET status='interrupted' WHERE status='running'").rowcount
        # Do not mark the repair operation that owns this database connection as stale.
        operation_reset = database.execute(
            """UPDATE operation_runs SET status='interrupted',updated_at=?,completed_at=?,message='Recovered by project repair'
               WHERE status='running' AND (process_id IS NULL OR process_id<>?)""",
            (now, now, os.getpid()),
        ).rowcount

        # The capture manifest is authoritative for payload availability. Missing
        # retained bytes are queued for reacquisition, but documents/matches/notes
        # remain intact so repair never destroys human research history.
        missing_retained = 0
        last_id = 0
        while True:
            rows = database.execute(
                """SELECT c.id,c.original_url,c.local_path,c.payload_availability,c.cleanup_pending,c.document_id,
                          d.path AS document_path
                   FROM captures c LEFT JOIN documents d ON d.id=c.document_id
                   WHERE c.id>? ORDER BY c.id LIMIT 500""",
                (last_id,),
            ).fetchall()
            if not rows:
                break
            last_id = int(rows[-1]["id"])
            for row in rows:
                availability = str(row["payload_availability"] or "not_acquired")
                if availability == "discarded" or int(row["cleanup_pending"] or 0):
                    continue
                path_value = str(row["local_path"] or row["document_path"] or "").strip()
                if availability in {"retained", "retained_unscanned", "spooled_unscanned", "cleanup_pending"} or path_value:
                    path = Path(path_value) if path_value else None
                    if path is None or not path.is_file() or path.stat().st_size == 0:
                        database.execute(
                            """UPDATE captures
                               SET state='pending',payload_availability='not_acquired',local_path=NULL,cleanup_pending=0,updated_at=?
                               WHERE id=?""",
                            (now, int(row["id"])),
                        )
                        missing_retained += 1

        reclassified = 0
        signatures = [
            str(row[0]) for row in database.execute(
                "SELECT DISTINCT query_signature FROM captures WHERE resource_classifier_revision<?",
                (RESOURCE_CLASSIFIER_REVISION,),
            )
        ]
        for query_signature in signatures:
            before = int(database.execute(
                "SELECT COUNT(*) FROM captures WHERE query_signature=? AND resource_classifier_revision<?",
                (query_signature, RESOURCE_CLASSIFIER_REVISION),
            ).fetchone()[0])
            classify_capture_inventory(database, query_signature)
            reclassified += before

        # Rebuild the contentless/external-content FTS representation wholesale;
        # never issue unsupported per-row DELETE statements against FTS5.
        rebuilt = rebuild_full_text_index(database, callback=callback)
        database.execute(
            "INSERT INTO repair_actions(action,details,created_at) VALUES(?,?,?)",
            ("repair", f"capture_reset={capture_reset}; media_reset={media_reset}; missing={missing_retained}; reclassified={reclassified}; fts={rebuilt}", utc_now()),
        )

    retained_parts = 0
    for folder in (root / "captures", root / "media"):
        if folder.exists():
            retained_parts += sum(1 for path in folder.rglob("*.part") if path.is_file())

    database.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    database.execute("PRAGMA optimize")
    database.commit()
    actions.extend(
        [
            f"Text captures reset from downloading: {capture_reset}",
            f"Text captures reset from scanning: {scan_capture_reset}",
            f"Media captures reset from downloading: {media_reset}",
            f"Scan runs marked interrupted: {scan_reset}",
            f"Abandoned operation runs marked interrupted: {operation_reset}",
            f"Missing retained payloads queued for reacquisition without deleting scan/review evidence: {missing_retained}",
            f"Stale capture classifications refreshed: {reclassified}",
            f"Full-text rows rebuilt (discarded bodies excluded): {rebuilt}",
            f"Resumable .part files retained: {retained_parts}",
            "WAL checkpoint and SQLite optimize completed",
        ]
    )
    report = root / "reports" / "repair.txt"
    atomic_write_text(report, "Archive Scout project repair\n\n" + "\n".join(actions) + "\n")
    if callback:
        callback(ProgressEvent("repair", f"Repair complete. Report written to {report}"))
    return report
