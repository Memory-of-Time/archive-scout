from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Callable

from ..events import ProgressEvent
from ..projects.backups import create_project_backup
from ..utils import atomic_write_text, utc_now


def rebuild_full_text_index(database: sqlite3.Connection, batch_size: int = 500) -> int:
    enabled = database.execute("SELECT value FROM project_meta WHERE key='fts5'").fetchone()
    if not enabled or enabled[0] != "1":
        return 0
    database.execute("DELETE FROM documents_fts")
    cursor = database.execute(
        """
        SELECT d.id,d.title,d.body_text,c.original_url
        FROM documents d JOIN captures c ON c.id=d.capture_id ORDER BY d.id
        """
    )
    rebuilt = 0
    while True:
        rows = cursor.fetchmany(max(1, int(batch_size)))
        if not rows:
            break
        database.executemany(
            "INSERT INTO documents_fts(rowid,title,body_text,original_url) VALUES(?,?,?,?)",
            (
                (row["id"], row["title"] or "", row["body_text"] or "", row["original_url"] or "")
                for row in rows
            ),
        )
        rebuilt += len(rows)
    return rebuilt


def repair_project(
    root: Path,
    database: sqlite3.Connection,
    callback: Callable[[ProgressEvent], None] | None = None,
    *,
    keep_backups: int = 5,
) -> Path:
    root = Path(root)
    backup = create_project_backup(root, reason="before_repair", keep=keep_backups)
    actions: list[str] = [f"Backup created: {backup}"]
    if callback:
        callback(ProgressEvent("repair", "Created a safety backup before repair."))

    integrity = database.execute("PRAGMA integrity_check").fetchone()
    if not integrity or str(integrity[0]).casefold() != "ok":
        raise RuntimeError(f"SQLite integrity check failed: {integrity}")
    actions.append("SQLite integrity check: ok")

    with database:
        capture_reset = database.execute("UPDATE captures SET state='pending' WHERE state='downloading'").rowcount
        media_reset = database.execute("UPDATE media_captures SET state='pending' WHERE state='downloading'").rowcount
        scan_reset = database.execute("UPDATE scan_runs SET status='interrupted' WHERE status='running'").rowcount
        operation_reset = database.execute(
            "UPDATE operation_runs SET status='interrupted',updated_at=?,completed_at=?,message='Recovered by project repair' WHERE status='running'",
            (utc_now(), utc_now()),
        ).rowcount
        missing = 0
        last_id = 0
        while True:
            rows = database.execute(
                "SELECT d.id,d.path,d.capture_id FROM documents d WHERE d.id>? ORDER BY d.id LIMIT 500",
                (last_id,),
            ).fetchall()
            if not rows:
                break
            last_id = int(rows[-1]["id"])
            invalid = []
            for row in rows:
                path = Path(row["path"])
                if not path.is_file() or path.stat().st_size == 0:
                    invalid.append((int(row["id"]), int(row["capture_id"])))
            for document_id, capture_id in invalid:
                database.execute(
                    "UPDATE captures SET state='pending',document_id=NULL,updated_at=? WHERE id=?",
                    (utc_now(), capture_id),
                )
                database.execute("DELETE FROM documents_fts WHERE rowid=?", (document_id,))
                database.execute("DELETE FROM documents WHERE id=?", (document_id,))
            missing += len(invalid)
        rebuilt = rebuild_full_text_index(database)
        database.execute("INSERT INTO repair_actions(action,details,created_at) VALUES(?,?,?)", ("repair", f"capture_reset={capture_reset}; media_reset={media_reset}; missing={missing}; fts={rebuilt}", utc_now()))

    removed_parts = 0
    for folder in (root / "captures", root / "media"):
        if not folder.exists():
            continue
        for path in folder.rglob("*.part"):
            path.unlink(missing_ok=True)
            removed_parts += 1

    database.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    database.execute("PRAGMA optimize")
    database.commit()
    actions.extend(
        [
            f"Text captures reset from downloading: {capture_reset}",
            f"Media captures reset from downloading: {media_reset}",
            f"Scan runs marked interrupted: {scan_reset}",
            f"Operation runs marked interrupted: {operation_reset}",
            f"Missing or empty documents queued for redownload: {missing}",
            f"Full-text rows rebuilt: {rebuilt}",
            f"Temporary .part files removed: {removed_parts}",
            "WAL checkpoint and SQLite optimize completed",
        ]
    )
    report = root / "reports" / "repair.txt"
    atomic_write_text(report, "Scout project repair\n\n" + "\n".join(actions) + "\n")
    if callback:
        callback(ProgressEvent("repair", f"Repair complete. Report written to {report}"))
    return report
