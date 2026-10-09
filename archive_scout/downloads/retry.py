from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Callable

from ..config import ProjectConfig
from ..events import ProgressEvent, Stopped
from ..scanning.jobs import ScanJob
from ..scanning.workers import scanner_workers, scanner_options
from ..scanning.rescanner import rescan_documents, rescan_keyword_sets
from .downloader import download_archive, download_archive_only, _scan_pending_captures


class _RetryCaptureIds:
    """Reiterable SQLite selection; no project-sized Python ID list."""

    def __init__(self, database: sqlite3.Connection, count: int, table: str = "archive_scout_download_retry_work"):
        self.database = database
        self.count = count
        self.table = table

    def __len__(self):
        return self.count

    def __iter__(self):
        return (int(row[0]) for row in self.database.execute(
            f"SELECT id FROM {self.table} ORDER BY id"
        ))


def retry_error_urls(
    config: ProjectConfig,
    database: sqlite3.Connection,
    scan_run_id: int,
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None,
    scan_jobs: list[ScanJob] | None = None,
) -> None:
    retry_clause = ("1=1" if config.retry_include_unavailable else
                    "(e.retryable=1 OR e.category='external_redirect_blocked')" if config.download_external_redirects else "e.retryable=1")
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
        SELECT e.capture_id,MAX(e.document_id) AS document_id,GROUP_CONCAT(DISTINCT e.operation) AS operations,MAX(COALESCE(d.path,c.local_path)) AS path
        FROM errors e JOIN captures c ON c.id=e.capture_id LEFT JOIN documents d ON d.id=e.document_id
        WHERE """ + " AND ".join(clauses) + " GROUP BY e.capture_id ORDER BY e.capture_id",
        params,
    )
    names = ("archive_scout_retry_local_docs", "archive_scout_retry_local_captures", "archive_scout_retry_downloads")
    for name in names:
        database.execute(f"DROP TABLE IF EXISTS temp.{name}")
        database.execute(f"CREATE TEMP TABLE {name}(id INTEGER PRIMARY KEY) WITHOUT ROWID")
    batches = {name: [] for name in names}
    def flush():
        for name, values in batches.items():
            if values:
                database.executemany(f"INSERT OR IGNORE INTO {name}(id) VALUES(?)", values)
                values.clear()
    for row in rows:
        if stop_event.is_set():
            raise Stopped
        capture_id = int(row["capture_id"])
        document_id = int(row["document_id"]) if row["document_id"] is not None else None
        path = Path(row["path"]) if row["path"] else None
        operations = {value for value in str(row["operations"] or "").split(",") if value}
        local = path and path.is_file() and operations and operations.issubset({"scan", "parse"})
        name = names[0] if local and document_id else names[1] if local else names[2]
        batches[name].append((document_id if local and document_id else capture_id,))
        if sum(map(len, batches.values())) >= 256:
            flush()
    flush()
    selections = [_RetryCaptureIds(database, int(database.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]), name)
                  for name in names]
    local_document_ids, local_capture_ids, download_capture_ids = selections
    jobs = scan_jobs or [ScanJob.create(scan_run_id, config.keyword_set_name, config.keywords)]
    if callback:
        callback(ProgressEvent("retry", f"Retrying {len(download_capture_ids):,} downloads and {len(local_document_ids):,} local scans"))
    if local_document_ids:
        if len(jobs) == 1:
            rescan_documents(database, jobs[0].scan_run_id, jobs[0].rules, stop_event, callback, local_document_ids, workers=scanner_workers(config.scan_workers), report_config=config.report, **scanner_options(config))
        else:
            rescan_keyword_sets(database, jobs, stop_event, callback, local_document_ids, workers=scanner_workers(config.scan_workers), report_config=config.report, **scanner_options(config))
    if local_capture_ids:
        with database:
            database.execute("UPDATE captures SET state='downloaded_unscanned' WHERE id IN (SELECT id FROM archive_scout_retry_local_captures)")
        _scan_pending_captures(config, database, jobs, stop_event, callback, capture_ids=local_capture_ids)
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
    retry_clause = ("1=1" if config.retry_include_unavailable else
                    "(e.retryable=1 OR e.category='external_redirect_blocked')" if config.download_external_redirects else "e.retryable=1")
    clauses = ["e.resolved=0", "e.ignored=0", retry_clause, "e.capture_id IS NOT NULL"]
    params: list[object] = []
    if config.retry_error_categories:
        clauses.append("e.category IN (" + ",".join("?" for _ in config.retry_error_categories) + ")")
        params.extend(config.retry_error_categories)
    if config.retry_capture_ids:
        database.execute("DROP TABLE IF EXISTS temp.archive_scout_download_retry_filter")
        database.execute("CREATE TEMP TABLE archive_scout_download_retry_filter(id INTEGER PRIMARY KEY) WITHOUT ROWID")
        database.executemany("INSERT OR IGNORE INTO archive_scout_download_retry_filter(id) VALUES(?)",
                             ((int(value),) for value in config.retry_capture_ids))
        clauses.append("EXISTS (SELECT 1 FROM archive_scout_download_retry_filter s WHERE s.id=e.capture_id)")
    database.execute("DROP TABLE IF EXISTS temp.archive_scout_download_retry_work")
    database.execute("CREATE TEMP TABLE archive_scout_download_retry_work(id INTEGER PRIMARY KEY) WITHOUT ROWID")
    database.execute(
        "INSERT INTO archive_scout_download_retry_work(id) SELECT DISTINCT e.capture_id FROM errors e WHERE "
        + " AND ".join(clauses), params,
    )
    count = int(database.execute("SELECT COUNT(*) FROM archive_scout_download_retry_work").fetchone()[0])
    capture_ids = _RetryCaptureIds(database, count)
    if callback:
        callback(ProgressEvent("download_retry", f"Retrying {len(capture_ids):,} retryable text acquisition error(s) without scanning."))
    if not capture_ids:
        return {"queued": 0, "downloaded": 0, "skipped": 0, "errors": 0, "elapsed": 0.0}
    # Retry selection is explicit.  Restore only these capture rows to pending;
    # permanent/ignored errors remain untouched and automatic retry loops do not
    # reinterpret redirect-policy failures.
    with database:
        database.execute(
            "UPDATE captures SET state='pending',updated_at=datetime('now') "
            "WHERE id IN (SELECT id FROM archive_scout_download_retry_work)",
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
