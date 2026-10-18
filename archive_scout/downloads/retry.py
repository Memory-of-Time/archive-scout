from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Callable

from ..config import ProjectConfig
from ..events import ProgressEvent
from ..scanning.jobs import ScanJob
from ..scanning.rescanner import rescan_documents, rescan_keyword_sets
from .downloader import download_archive


def retry_error_urls(
    config: ProjectConfig,
    database: sqlite3.Connection,
    scan_run_id: int,
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None,
    scan_jobs: list[ScanJob] | None = None,
) -> None:
    clauses = ["e.resolved=0", "e.ignored=0", "e.retryable=1", "e.capture_id IS NOT NULL"]
    params: list[object] = []
    if config.retry_error_categories:
        clauses.append("e.category IN (" + ",".join("?" for _ in config.retry_error_categories) + ")")
        params.extend(config.retry_error_categories)
    database.execute("DROP TABLE IF EXISTS temp.archive_scout_retry_selection")
    if config.retry_capture_ids:
        database.execute(
            "CREATE TEMP TABLE archive_scout_retry_selection(id INTEGER PRIMARY KEY) WITHOUT ROWID"
        )
        database.executemany(
            "INSERT OR IGNORE INTO archive_scout_retry_selection(id) VALUES(?)",
            ((int(value),) for value in config.retry_capture_ids),
        )
        clauses.append(
            "EXISTS (SELECT 1 FROM archive_scout_retry_selection s WHERE s.id=e.capture_id)"
        )
    database.execute("DROP TABLE IF EXISTS temp.archive_scout_retry_work")
    database.execute("""CREATE TEMP TABLE archive_scout_retry_work(
        capture_id INTEGER PRIMARY KEY, document_id INTEGER, local_only INTEGER NOT NULL
    ) WITHOUT ROWID""")
    cursor = database.execute(
        """SELECT e.capture_id,MAX(e.document_id) AS document_id,
            GROUP_CONCAT(DISTINCT e.operation) AS operations,MAX(d.path) AS path
        FROM errors e LEFT JOIN documents d ON d.id=e.document_id
        WHERE """ + " AND ".join(clauses) + " GROUP BY e.capture_id ORDER BY e.capture_id",
        params,
    )
    while True:
        if stop_event.is_set():
            from ..events import Stopped
            raise Stopped
        rows = cursor.fetchmany(500)
        if not rows:
            break
        classified = []
        for row in rows:
            document_id = int(row["document_id"]) if row["document_id"] is not None else None
            path = Path(row["path"]) if row["path"] else None
            operations = {value for value in str(row["operations"] or "").split(",") if value}
            local = bool(document_id and path and path.exists() and operations
                         and operations.issubset({"scan", "parse"}))
            classified.append((int(row["capture_id"]), document_id, int(local)))
        database.executemany("INSERT OR REPLACE INTO archive_scout_retry_work VALUES(?,?,?)", classified)
    jobs = scan_jobs or [ScanJob.create(scan_run_id, config.keyword_set_name, config.keywords)]
    n_local = int(database.execute("SELECT COUNT(*) FROM archive_scout_retry_work WHERE local_only=1").fetchone()[0])
    n_net = int(database.execute("SELECT COUNT(*) FROM archive_scout_retry_work WHERE local_only=0").fetchone()[0])
    if callback:
        callback(ProgressEvent("retry", f"Retrying {n_net:,} downloads and {n_local:,} local scans"))
    # Stable selection is in TEMP SQLite, independent of changing error states.
    # Each operation gets a bounded list, not O(project error count) RAM.
    for local, batch_limit in ((1, 256), (0, 1000)):
        last_id = 0
        while True:
            if stop_event.is_set():
                from ..events import Stopped
                raise Stopped
            rows = database.execute("""SELECT capture_id,document_id FROM archive_scout_retry_work
                WHERE local_only=? AND capture_id>? ORDER BY capture_id LIMIT ?""",
                (local,last_id,batch_limit)).fetchall()
            if not rows:
                break
            last_id = int(rows[-1]["capture_id"])
            ids = [int(row["document_id"] if local else row["capture_id"]) for row in rows]
            if local:
                if len(jobs) == 1:
                    rescan_documents(database, jobs[0].scan_run_id, jobs[0].rules,
                                     stop_event, callback, ids, workers=config.workers)
                else:
                    rescan_keyword_sets(database, jobs, stop_event, callback, ids,
                                        workers=config.workers)
            else:
                if len(jobs) == 1:
                    download_archive(config, database, scan_run_id, stop_event, callback,
                        states=("error", "pending", "downloaded"), capture_ids=ids)
                else:
                    download_archive(config, database, scan_run_id, stop_event, callback,
                        states=("error", "pending", "downloaded"), capture_ids=ids,
                        scan_jobs=jobs)
