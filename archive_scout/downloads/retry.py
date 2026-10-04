from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Callable

from ..config import ProjectConfig
from ..events import ProgressEvent
from ..scanning.jobs import ScanJob
from ..scanning.rescanner import rescan_documents, rescan_keyword_sets
from .downloader import download_archive, download_archive_only


def retry_error_urls(
    config: ProjectConfig,
    database: sqlite3.Connection,
    scan_run_id: int,
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None,
    scan_jobs: list[ScanJob] | None = None,
) -> None:
    retry_clause = "(e.retryable=1 OR e.category='external_redirect_blocked')" if config.download_external_redirects else "e.retryable=1"
    clauses = ["e.resolved=0", "e.ignored=0", retry_clause, "e.capture_id IS NOT NULL"]
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
    rows = database.execute(
        """
        SELECT e.capture_id,MAX(e.document_id) AS document_id,GROUP_CONCAT(DISTINCT e.operation) AS operations,MAX(d.path) AS path
        FROM errors e LEFT JOIN documents d ON d.id=e.document_id
        WHERE """ + " AND ".join(clauses) + " GROUP BY e.capture_id ORDER BY e.capture_id",
        params,
    )
    local_document_ids: list[int] = []
    download_capture_ids: list[int] = []
    for row in rows:
        capture_id = int(row["capture_id"])
        document_id = int(row["document_id"]) if row["document_id"] is not None else None
        path = Path(row["path"]) if row["path"] else None
        operations = {value for value in str(row["operations"] or "").split(",") if value}
        if document_id and path and path.exists() and operations and operations.issubset({"scan", "parse"}):
            local_document_ids.append(document_id)
        else:
            download_capture_ids.append(capture_id)
    jobs = scan_jobs or [ScanJob.create(scan_run_id, config.keyword_set_name, config.keywords)]
    if callback:
        callback(ProgressEvent("retry", f"Retrying {len(download_capture_ids):,} downloads and {len(local_document_ids):,} local scans"))
    if local_document_ids:
        if len(jobs) == 1:
            rescan_documents(database, jobs[0].scan_run_id, jobs[0].rules, stop_event, callback, local_document_ids, workers=config.workers, report_config=config.report)
        else:
            rescan_keyword_sets(database, jobs, stop_event, callback, local_document_ids, workers=config.workers, report_config=config.report)
    if download_capture_ids:
        if len(jobs) == 1:
            download_archive(
                config, database, scan_run_id, stop_event, callback,
                states=("error", "pending", "downloaded"), capture_ids=download_capture_ids,
            )
        else:
            download_archive(
                config, database, scan_run_id, stop_event, callback,
                states=("error", "pending", "downloaded"), capture_ids=download_capture_ids, scan_jobs=jobs,
            )


def retry_error_downloads(
    config: ProjectConfig,
    database: sqlite3.Connection,
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None,
) -> dict[str, int | float]:
    """Retry only retryable capture acquisition failures, without scanning.

    This preserves a download-only project's contract: no keyword set, document,
    match, report-enrichment, or Research Intelligence work is introduced just
    because a replay GET previously failed.
    """
    retry_clause = "(e.retryable=1 OR e.category='external_redirect_blocked')" if config.download_external_redirects else "e.retryable=1"
    clauses = ["e.resolved=0", "e.ignored=0", retry_clause, "e.capture_id IS NOT NULL"]
    params: list[object] = []
    if config.retry_error_categories:
        clauses.append("e.category IN (" + ",".join("?" for _ in config.retry_error_categories) + ")")
        params.extend(config.retry_error_categories)
    if config.retry_capture_ids:
        ids = [int(value) for value in config.retry_capture_ids]
        clauses.append("e.capture_id IN (" + ",".join("?" for _ in ids) + ")")
        params.extend(ids)
    rows = database.execute(
        "SELECT DISTINCT e.capture_id FROM errors e WHERE " + " AND ".join(clauses) + " ORDER BY e.capture_id",
        params,
    ).fetchall()
    capture_ids = [int(row[0]) for row in rows]
    if callback:
        callback(ProgressEvent("download_retry", f"Retrying {len(capture_ids):,} retryable text acquisition error(s) without scanning."))
    if not capture_ids:
        return {"queued": 0, "downloaded": 0, "skipped": 0, "errors": 0, "elapsed": 0.0}
    # Retry selection is explicit.  Restore only these capture rows to pending;
    # permanent/ignored errors remain untouched and automatic retry loops do not
    # reinterpret redirect-policy failures.
    placeholders = ",".join("?" for _ in capture_ids)
    with database:
        database.execute(
            f"UPDATE captures SET state='pending',updated_at=datetime('now') WHERE id IN ({placeholders})",
            capture_ids,
        )
    acquisition_config = config.normalized()
    if acquisition_config.text_retention == "discard_after_scan":
        # Acquisition-only retry has no scan completion that could authorize a
        # payload deletion.  Preserve the successfully recovered file.
        from dataclasses import replace
        acquisition_config = replace(acquisition_config, text_retention="keep").normalized()
    return download_archive_only(
        acquisition_config, database, stop_event, callback,
        states=("pending", "error"), capture_ids=capture_ids,
    )
