from __future__ import annotations

import sqlite3
import json
import hashlib
import threading
import time
from dataclasses import replace
from pathlib import Path
from typing import Callable

from .cdx.client import RateLimitDeferred
from .cdx.indexer import index_archive
from .cdx.parameters import cdx_query_signature
from .database.classification import classify_indexed_captures
from .analysis.workflow import run_analysis
from .config import KeywordSetConfig, ProjectConfig, save_project_config
from .database.connection import open_database
from .database.lease import guard_project
from .database.repositories import (finish_scan_run, get_or_create_keyword_set, latest_scan_run, start_scan_run, start_operation_run, finish_operation_run, update_operation_run)
from .downloads.downloader import download_archive
from .downloads.retry import retry_error_urls
from .events import ConnectivityPaused, ProgressEvent, Stopped
from .media.downloader import download_media, retry_media_errors
from .media.indexer import index_external_embedded_media, index_media
from .media.reports import generate_media_reports
from .projects.integrity import check_project_integrity
from .projects.backups import create_project_backup
from .projects.repair import repair_project
from .projects.diagnostics import export_diagnostics
from .projects.importers import import_text_folder
from .constants import VERSION
from .projects.merge import merge_projects
from .reports.text import generate_index_reports, generate_reports
from .research.index import build_research_index
from .scanning.jobs import ScanJob
from .scanning.rescanner import rescan_keyword_sets

from .scanning.hitlist import load_hitlist, search_with_hitlist

class IncompleteCoverage(RuntimeError):
    """Files/reports are retained, but scan completeness could not be established."""
    def __init__(self, message: str, paths: dict[str, Path]):
        super().__init__(message)
        self.paths = paths


SUPPORTED_MODES = {
    "hitlist",
    "all", "external_media_after_scan", "index", "download", "download_only", "retry_download_errors", "resume", "rescan", "retry_errors", "report", "integrity",
    "repair", "backup", "diagnostics", "import_folder",
    "media_all", "media_index", "media_download", "media_retry",
    "analysis", "research_index", "forum_rebuild", "merge_project",
}


def is_recoverable_pause(exc: BaseException) -> bool:
    """Stable integration contract for UI/bot wrappers around run_project()."""
    return isinstance(exc, (ConnectivityPaused, RateLimitDeferred))


def emit(callback: Callable[[ProgressEvent], None] | None, event: ProgressEvent) -> None:
    if callback:
        callback(event)


def _scan_scope_identity(config: ProjectConfig) -> str:
    """Stable inventory scope; excludes request pacing and project location."""
    targets = sorted((target, cdx_query_signature(config.for_target(target)))
                     for target in config.targets)
    data = {"targets": targets, "from": config.from_date, "to": config.to_date}
    return hashlib.sha256(json.dumps(data, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _reusable_interrupted_scan_run(database: sqlite3.Connection, config: ProjectConfig,
                                   keyword_set_id: int, scope_id: str,
                                   *, include_complete: bool = False,
                                   rescan_only: bool = False) -> int | None:
    """Reuse compatible interrupted or completed scans, retaining earlier matches.

    Pre-1.2.1 jobs lacked a scope fingerprint. For them, verify that all
    previously scored documents belong to the current target/signature set.
    This check is performed at most once on an explicit Resume, never per URL.
    """
    allowed = ("interrupted", "complete") if include_complete else ("interrupted",)
    candidate = database.execute(
        """SELECT id,metadata_json,status FROM scan_runs
           WHERE keyword_set_id=? AND minimum_score=? AND status IN ("""
        + ",".join("?" for _ in allowed) + """)
           AND source_operation IN (""" + ("'rescan'" if rescan_only else "'all','download','resume','external_media_after_scan'") + """)
           ORDER BY CASE status WHEN 'interrupted' THEN 0 ELSE 1 END,id DESC LIMIT 1""",
        (keyword_set_id, config.minimum_score, *allowed),
    ).fetchone()
    if candidate is None:
        return None
    run_id = int(candidate["id"])
    try:
        metadata = json.loads(candidate["metadata_json"] or "{}")
    except (TypeError, ValueError):
        return None
    if not isinstance(metadata, dict):
        return None
    if metadata.get("scope_identity"):
        if metadata["scope_identity"] != scope_id:
            return None
    else:
        # Legacy scan metadata cannot establish a completed run's scope when
        # it had no matches; only reuse interrupted legacy runs on Resume.
        if candidate["status"] == "complete":
            # A completed legacy run can be reused only if it demonstrably
            # contains scored documents; empty legacy runs lack scope evidence.
            if not database.execute("SELECT 1 FROM document_matches WHERE scan_run_id=? LIMIT 1", (run_id,)).fetchone():
                return None
        accepted = {(target, cdx_query_signature(config.for_target(target)))
                    for target in config.targets}
        if not accepted:
            return None
        # v1.2.0 had no scope marker. Only reuse if its existing matched
        # document references are all in the present scope. Unmatched saved
        # captures keep their independent durable states regardless.
        conditions = " OR ".join("(t.pattern=? AND c.query_signature=?)" for _ in accepted)
        params = [item for pair in sorted(accepted) for item in pair]
        mismatch = database.execute(
            f"""SELECT 1 FROM document_matches dm
                JOIN documents d ON d.id=dm.document_id
                JOIN captures c ON c.id=d.capture_id
                LEFT JOIN targets t ON t.id=c.target_id
                WHERE dm.scan_run_id=? AND NOT ({conditions}) LIMIT 1""",
            (run_id, *params),
        ).fetchone()
        if mismatch is not None:
            return None
    metadata["scope_identity"] = scope_id
    database.execute(
        "UPDATE scan_runs SET status='running',completed_at=NULL,metadata_json=? WHERE id=?",
        (json.dumps(metadata, ensure_ascii=False, sort_keys=True), run_id),
    )
    return run_id


def prepare_scan_jobs(
    database: sqlite3.Connection,
    config: ProjectConfig,
    mode: str,
) -> list[ScanJob]:
    jobs: list[ScanJob] = []
    selected = config.selected_keyword_sets()
    if not selected:
        raise ValueError("select at least one keyword set containing at least one rule")
    seen_keyword_set_ids: set[int] = set()
    for keyword_set in selected:
        keyword_set_id = get_or_create_keyword_set(database, keyword_set.name, keyword_set.rules)
        if keyword_set_id in seen_keyword_set_ids:
            continue
        seen_keyword_set_ids.add(keyword_set_id)
        scope_id = _scan_scope_identity(config)
        run_id = (_reusable_interrupted_scan_run(
            database, config, keyword_set_id, scope_id,
            include_complete=mode in {"all", "download", "resume", "external_media_after_scan"},
            rescan_only=mode == "rescan",
        ) if mode in {"all", "download", "resume", "external_media_after_scan", "rescan"} else None)
        if run_id is None:
            run_id = start_scan_run(
                database,
                keyword_set_id,
                f"{keyword_set.name} ({mode})",
                config.minimum_score,
                mode,
                {"keyword_set": keyword_set.name, "rules": keyword_set.rules,
                 "scope_identity": scope_id,
                 "historical_backfill_complete": False,
                 "historical_backfill_cursor": 0},
            )
        jobs.append(ScanJob.create(run_id, keyword_set.name, keyword_set.rules))
    database.commit()
    return jobs


def scan_prior_documents_once(database: sqlite3.Connection, jobs: list[ScanJob],
                              config: ProjectConfig, stop_event: threading.Event,
                              callback: Callable[[ProgressEvent], None] | None) -> None:
    """Backfill only unscored saved bodies, with durable per-document coverage.

    Scores of zero still produce a document_matches row. A crash or failed
    read leaves that row absent, so the next run can retry it without trusting
    a cursor that might have jumped over incomplete work.
    """
    scopes = sorted((target, cdx_query_signature(config.for_target(target)))
                    for target in config.targets)
    if not scopes:
        return
    rescan_keyword_sets(database, jobs, stop_event, callback,
                        workers=config.workers, missing_only=True,
                        scope_pairs=scopes,
                        empty_message="Existing-document check complete: no outstanding saved-body scans; continuing with this operation.")


def finish_jobs(database: sqlite3.Connection, jobs: list[ScanJob], status: str,
                config: ProjectConfig | None = None) -> None:
    """Do not label a scan complete while a known saved body lacks a score."""
    for job in jobs:
        actual = status
        if status == "complete" and config is not None:
            for target in config.targets:
                signature = cdx_query_signature(config.for_target(target))
                gap = database.execute(
                    """SELECT 1 FROM captures c JOIN targets t ON t.id=c.target_id
                       WHERE t.pattern=? AND c.query_signature=? AND
                        (c.state='downloaded_unscanned' OR
                         (c.state='downloaded' AND (NOT EXISTS (
                             SELECT 1 FROM documents d WHERE d.capture_id=c.id) OR
                             EXISTS (SELECT 1 FROM documents d WHERE d.capture_id=c.id
                             AND NOT EXISTS (SELECT 1 FROM document_matches m
                                 WHERE m.document_id=d.id AND m.scan_run_id=?)))) OR
                         EXISTS (SELECT 1 FROM errors e WHERE e.capture_id=c.id
                             AND e.operation='scan' AND e.resolved=0)) LIMIT 1""",
                    (target, signature, job.scan_run_id),
                ).fetchone()
                if gap:
                    actual = "interrupted"
                    break
        finish_scan_run(database, job.scan_run_id, actual)


def generate_job_reports(config: ProjectConfig, database: sqlite3.Connection, jobs: list[ScanJob],
                         callback: Callable[[ProgressEvent], None] | None = None,
                         stop_event: threading.Event | None = None) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    for index, job in enumerate(jobs, 1):
        if callback:
            callback(ProgressEvent("report", f"Generating reports for scan {job.scan_run_id} ({index}/{len(jobs)})"))
        generated = generate_reports(
            config, database, job.scan_run_id,
            progress=(lambda name, count, done: callback(ProgressEvent(
                "report", f"{'Wrote' if done else 'Writing'} {name}.txt: {count:,} rows"
            ))) if callback else None,
            stop_event=stop_event,
        )
        if index == 1:
            paths.update(generated)
        paths[f"scan_{job.scan_run_id}_folder"] = generated["scan_folder"]
    return paths


def _retryable_text_batches(config, database: sqlite3.Connection, *, batch_size: int = 1000,
                           include_exhausted_pending: bool = False):
    """Yield each eligible capture at most once per pass, without an ID-sized list."""
    # Reuse a bounded, disk-backed scope table so target-specific CDX overrides
    # are not silently filtered out by the global signature on retry.
    database.execute("""CREATE TEMP TABLE IF NOT EXISTS archive_scout_retry_scopes(
        pattern TEXT NOT NULL, signature TEXT NOT NULL,
        PRIMARY KEY(pattern,signature)) WITHOUT ROWID""")
    database.execute("DELETE FROM archive_scout_retry_scopes")
    database.executemany(
        "INSERT OR IGNORE INTO archive_scout_retry_scopes VALUES(?,?)",
        ((target, cdx_query_signature(config.for_target(target))) for target in config.targets),
    )
    last_id = 0
    while True:
        # Explicit retry includes pending captures which a lifetime attempt
        # ceiling prevented from entering the normal queue. Automatic recovery
        # still considers only recorded retryable errors, once per sweep.
        exhausted = " OR (c.state='pending' AND c.download_attempts>=?)" if include_exhausted_pending else ""
        rows = database.execute(
            """SELECT c.id FROM captures c JOIN targets t ON t.id=c.target_id
               WHERE c.id>? AND EXISTS (SELECT 1 FROM archive_scout_retry_scopes s
                   WHERE s.pattern=t.pattern AND s.signature=c.query_signature)
               AND ((c.state='error' AND EXISTS (
                   SELECT 1 FROM errors e WHERE e.capture_id=c.id
                   AND e.resolved=0 AND e.ignored=0 AND e.retryable=1)
                   AND (NOT EXISTS (SELECT 1 FROM text_retry_schedule sched WHERE sched.capture_id=c.id)
                        OR EXISTS (SELECT 1 FROM text_retry_schedule sched WHERE sched.capture_id=c.id
                                   AND sched.next_eligible_at<=?)))"""
            + exhausted + ") ORDER BY c.id LIMIT ?",
            (last_id, time.time(), config.max_attempts, batch_size)
            if include_exhausted_pending else (last_id, time.time(), batch_size),
        ).fetchall()
        if not rows:
            return
        ids = [int(row[0]) for row in rows]
        last_id = ids[-1]
        yield ids


def remaining_acquisition_gaps(config: ProjectConfig, database: sqlite3.Connection) -> int:
    """Count unfinished acquisition in the current target/signature scope.

    Reports may still be generated, but a successful export must not turn
    unresolved downloads into a falsely complete operation status.
    """
    from .downloads.downloader import _populate_download_scopes
    _populate_download_scopes(database, config)
    return int(database.execute(
        """SELECT COUNT(*) FROM captures c JOIN targets t ON t.id=c.target_id
           WHERE EXISTS(SELECT 1 FROM archive_scout_download_scopes s
                WHERE s.pattern=t.pattern AND s.signature=c.query_signature)
             AND (c.state='error' OR (c.state='pending' AND c.download_attempts>=?))""",
        (config.max_attempts,),
    ).fetchone()[0])


def recover_transient_text_errors(
    config: ProjectConfig,
    database: sqlite3.Connection,
    scan_run_id: int,
    jobs: list[ScanJob],
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None,
    *,
    continuous: bool = False,
    acquire_only: bool = False,
) -> int:
    """Fair, bounded-memory retries with autonomous outage recovery.

    Every round walks the eligible failures in ID order once. A persistent run
    remains active through outages, but the entire network workload waits at a
    single interruptible cooldown between rounds. No per-capture sleep and no
    requirement to click Resume when a temporary transport problem clears.
    Explicit retry operations and callers not opting in retain one-sweep mode.
    """
    failures_without_progress = 0
    previous_remaining = None
    while True:
        attempted = 0
        for ids in _retryable_text_batches(config, database):
            if stop_event.is_set():
                raise Stopped
            attempted += len(ids)
            emit(callback, ProgressEvent(
                "download_retry", f"Recovering {len(ids):,} transient text failures (fair retry sweep)"))
            download_archive(config, database, scan_run_id, stop_event, callback,
                             states=("error",), capture_ids=ids, scan_jobs=jobs,
                             acquire_only=acquire_only)
        # Query one bounded batch to test whether retryable work remains. Avoid
        # a project-sized COUNT inside the hot capture-saving loop.
        remaining = next(_retryable_text_batches(config, database, batch_size=1), [])
        if not remaining:
            from .downloads.schedule import next_due
            scheduled_due = next_due(database, config)
            if scheduled_due is None:
                return 0
            if not continuous or not config.network.persistent_retries:
                return 1  # deferred work remains; never declare complete
            if jobs and not acquire_only:
                from .scanning.pending import scan_pending_saved
                for target in config.targets:
                    scan_pending_saved(database, config.for_target(target), jobs,
                                       stop_event, callback, workers=config.workers)
            wait_seconds = min(config.network.retry_max_seconds,
                               max(0.05, scheduled_due - time.time()))
            emit(callback, ProgressEvent("download_retry",
                f"Transient errors are checkpointed; next eligible retry in {wait_seconds:.1f}s"))
            if stop_event.wait(wait_seconds):
                raise Stopped
            continue
        if not continuous or not config.network.persistent_retries:
            return len(remaining)  # some retryable work is still incomplete
        # Local scanner remains eligible while network admission is cooling.
        if jobs and not acquire_only:
            from .scanning.pending import scan_pending_saved
            for target in config.targets:
                scan_pending_saved(database, config.for_target(target), jobs,
                                   stop_event, callback, workers=config.workers)
        if previous_remaining is None or attempted == previous_remaining:
            failures_without_progress += 1
        else:
            failures_without_progress = 0
        previous_remaining = attempted
        from .downloads.schedule import next_due
        earliest = next_due(database, config)
        pause = min(config.network.retry_max_seconds,
                    max(0.05, (earliest-time.time()) if earliest is not None else
                        config.network.retry_base_seconds))
        emit(callback, ProgressEvent(
            "download_retry", f"Temporary download failures remain. Work is checkpointed; "
            f"recovery sweep resumes in {pause:.1f}s. Local scanning is independent."))
        if stop_event.wait(pause):
            raise Stopped




def retry_capture_downloads(config, database, stop_event, callback) -> None:
    """Explicit acquisition-only retry, with no scan jobs or keyword matching."""
    for ids in _retryable_text_batches(config, database, include_exhausted_pending=True):
        if stop_event.is_set():
            raise Stopped
        emit(callback, ProgressEvent("download_retry", f"Retrying {len(ids):,} failed or attempt-exhausted downloads without scanning"))
        download_archive(config, database, 0, stop_event, callback,
                         states=("error", "pending"), capture_ids=ids, acquire_only=True)


@guard_project
def run_project(
    config: ProjectConfig,
    mode: str = "all",
    stop_event: threading.Event | None = None,
    callback: Callable[[ProgressEvent], None] | None = None,
) -> dict[str, Path]:
    config = config.normalized()
    if config.download_scope == "index_only" and mode in {"all", "download_only", "external_media_after_scan"}:
        mode = "index"
    if mode == "external_media_after_scan":
        config.media = replace(
            config.media.normalized(),
            enabled=True,
            discover_embedded=True,
            allow_external_embeds=True,
        )
    if mode not in SUPPORTED_MODES:
        raise ValueError(f"unsupported mode: {mode}")
    if config.from_date > config.to_date:
        raise ValueError("start date must not be later than end date")
    if mode in {"all", "external_media_after_scan", "index", "download_only"} and not config.targets:
        raise ValueError("at least one target is required")
    if mode.startswith("media_") and not (config.media.targets or config.targets):
        raise ValueError("at least one media target or site target is required")
    if mode in {"all", "external_media_after_scan", "download", "resume", "rescan", "retry_errors"} and not config.selected_keyword_sets():
        raise ValueError("select at least one keyword set")
    stop_event = stop_event or threading.Event()
    config.output_dir.mkdir(parents=True, exist_ok=True)
    (config.output_dir / "captures").mkdir(exist_ok=True)
    (config.output_dir / "media" / "images").mkdir(parents=True, exist_ok=True)
    (config.output_dir / "media" / "videos").mkdir(parents=True, exist_ok=True)
    (config.output_dir / "reports").mkdir(exist_ok=True)
    database = open_database(config.output_dir, migrate=True)
    jobs: list[ScanJob] = []
    operation_run_id = start_operation_run(database, mode, VERSION)
    database.commit()
    original_callback = callback
    owner_thread_id = threading.get_ident()
    last_progress_write = 0.0
    last_progress_stage = ""

    def operation_callback(event: ProgressEvent) -> None:
        nonlocal last_progress_write, last_progress_stage
        if threading.get_ident() == owner_thread_id:
            now = time.monotonic()
            completed_boundary = (
                event.current is not None
                and event.total is not None
                and event.total >= 0
                and event.current >= event.total
            )
            stage_changed = bool(event.stage) and event.stage != last_progress_stage
            # UI/bot callbacks still receive every event immediately. Persisted
            # operation progress is rate-limited so a fast download/scan no longer
            # forces a full SQLite commit for every single completed item.
            should_write = stage_changed or completed_boundary or now - last_progress_write >= 0.75
            if should_write:
                update_operation_run(
                    database,
                    operation_run_id,
                    message=event.message,
                    completed=event.current,
                    total=event.total,
                    stage=event.stage,
                )
                database.commit()
                last_progress_write = now
                last_progress_stage = event.stage
        if original_callback:
            original_callback(event)

    callback = operation_callback
    def report_progress(name: str, count: int, done: bool) -> None:
        emit(callback, ProgressEvent(
            "report", f"{'Wrote' if done else 'Writing'} {name}.txt: {count:,} rows"
        ))
    report_finalization_started = False
    try:
        save_project_config(config)
        if mode == "hitlist":
            keywords = load_hitlist(config.hitlist_keywords, config.hitlist_file)
            if not keywords:
                keywords = load_hitlist(config.keywords)
            result = search_with_hitlist(config.output_dir, database, keywords, stop_event, callback)
            finish_operation_run(database, operation_run_id, "complete", "Hitlist search complete")
            database.commit()
            return {"hitlist_csv": Path(result["csv"]), "hitlist_summary": Path(result["summary"])}
        if mode == "backup":
            path = create_project_backup(config.output_dir, reason="manual", keep=config.backup_keep)
            emit(callback, ProgressEvent("backup", f"Backup written to {path}"))
            finish_operation_run(database, operation_run_id, "complete", str(path))
            database.commit()
            return {"backup": path}
        if mode == "repair":
            path = repair_project(config.output_dir, database, callback, keep_backups=config.backup_keep)
            finish_operation_run(database, operation_run_id, "complete", str(path))
            database.commit()
            return {"repair": path}
        if mode == "diagnostics":
            path = export_diagnostics(config.output_dir, database, callback)
            finish_operation_run(database, operation_run_id, "complete", str(path))
            database.commit()
            return {"diagnostics": path}
        if mode == "import_folder":
            source = Path(config.import_source).expanduser()
            if not source.is_dir():
                raise ValueError("choose an existing folder to import")
            if config.auto_backup and (config.output_dir / "archive_scout.sqlite3").exists():
                create_project_backup(config.output_dir, reason="before_import", keep=config.backup_keep)
            imported = import_text_folder(config.output_dir, source, database, stop_event, callback)
            report = config.output_dir / "reports" / "import_summary.txt"
            report.write_text(f"Archive Scout import\n\nSource: {source}\nImported: {imported}\n", encoding="utf-8")
            finish_operation_run(database, operation_run_id, "complete", f"Imported {imported}")
            database.commit()
            return {"import_summary": report}
        if mode == "integrity":
            path = check_project_integrity(config.output_dir, database, callback)
            emit(callback, ProgressEvent("integrity", f"Integrity report written to {path}"))
            finish_operation_run(database, operation_run_id, "complete", str(path))
            database.commit()
            return {"integrity": path}
        if mode == "analysis":
            paths = run_analysis(config, database, stop_event, callback, forum_only=False)
            finish_operation_run(database, operation_run_id, "complete", "Analysis complete")
            database.commit()
            return paths
        if mode == "research_index":
            summary = build_research_index(config, database, stop_event, callback)
            report = config.output_dir / "reports" / "research_index.json"
            import json as _json
            report.write_text(_json.dumps(summary.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
            finish_operation_run(database, operation_run_id, "complete", "Research Intelligence index complete")
            database.commit()
            return {"research_index": report}
        if mode == "forum_rebuild":
            paths = run_analysis(config, database, stop_event, callback, forum_only=True)
            finish_operation_run(database, operation_run_id, "complete", "Forum rebuild complete")
            database.commit()
            return paths
        if mode == "merge_project":
            source = Path(config.analysis.normalized().merge_source).expanduser()
            if not str(source).strip() or not source.exists():
                raise ValueError("choose an existing Archive Scout project folder to merge")
            if config.auto_backup and (config.output_dir / "archive_scout.sqlite3").exists():
                create_project_backup(config.output_dir, reason="before_merge", keep=config.backup_keep)
            summary = merge_projects(config.output_dir, source, database, stop_event, callback)
            merge_report = config.output_dir / "reports" / "merge_summary.txt"
            merge_report.write_text("Archive Scout project merge\n\n" + "\n".join(f"{key}: {value}" for key, value in summary.items()) + "\n", encoding="utf-8")
            finish_operation_run(database, operation_run_id, "complete", str(merge_report))
            database.commit()
            return {"merge_summary": merge_report}
        if mode == "index":
            index_archive(config, database, stop_event, callback)
            for signature in {cdx_query_signature(config.for_target(target)) for target in config.targets}:
                classify_indexed_captures(database, signature, stop_event)
            paths = generate_index_reports(config, database,
                progress=report_progress, stop_event=stop_event)
            paths["project"] = config.output_dir / "project.json"
            finish_operation_run(database, operation_run_id, "complete", "Index complete")
            database.commit()
            return paths
        if mode == "report":
            # Explicit report regeneration must not hide a partially completed
            # scan behind an older successful scan. Keep status visible in the
            # report summary and preserve the scan-specific run directory.
            latest = database.execute("SELECT id FROM scan_runs ORDER BY id DESC LIMIT 1").fetchone()
            existing = int(latest[0]) if latest else None
            if existing is None:
                if database.execute("SELECT COUNT(*) FROM captures").fetchone()[0]:
                    paths = generate_index_reports(config, database,
                        progress=report_progress, stop_event=stop_event)
                else:
                    raise RuntimeError("this project does not contain indexed captures or a completed scan run")
            else:
                paths = generate_reports(
                    config, database, existing,
                    progress=lambda name, count, done: emit(callback, ProgressEvent(
                        "report", f"{'Wrote' if done else 'Writing'} {name}.txt: {count:,} rows"
                    )),
                    stop_event=stop_event,
                )
            emit(callback, ProgressEvent("report", f"Reports written to {config.output_dir / 'reports'}"))
            finish_operation_run(database, operation_run_id, "complete", "Reports regenerated")
            database.commit()
            return paths
        if mode == "media_index":
            index_media(config, database, stop_event, callback)
            paths = generate_media_reports(config, database)
            finish_operation_run(database, operation_run_id, "complete", "Media index complete")
            database.commit()
            return paths
        if mode == "media_download":
            download_media(config, database, stop_event, callback)
            paths = generate_media_reports(config, database)
            finish_operation_run(database, operation_run_id, "complete", "Media download complete")
            database.commit()
            return paths
        if mode == "media_retry":
            retry_media_errors(config, database, stop_event, callback)
            paths = generate_media_reports(config, database)
            finish_operation_run(database, operation_run_id, "complete", "Media retry complete")
            database.commit()
            return paths
        if mode == "media_all":
            index_media(config, database, stop_event, callback)
            download_media(config, database, stop_event, callback)
            paths = generate_media_reports(config, database)
            finish_operation_run(database, operation_run_id, "complete", "Media operation complete")
            database.commit()
            return paths

        # Filesystem and database are not one transaction. Recover complete
        # raw bodies after an interrupted SQLite/FTS commit before network work.
        if mode in {"all", "resume", "download", "external_media_after_scan", "rescan"}:
            from .scanning.pending import adopt_orphaned_bodies
            for target in config.targets:
                adopt_orphaned_bodies(database, config.for_target(target), stop_event, callback, target=target)

        # Scanning retained bodies is local work and must not wait for CDX.
        # On a Resume with an unreachable indexing endpoint, every previously
        # saved capture can still be recovered and scored before the outage.
        if mode in {"all", "external_media_after_scan", "resume"} and database.execute(
            "SELECT 1 FROM captures WHERE state IN ('downloaded','downloaded_unscanned') LIMIT 1"
        ).fetchone():
            jobs = prepare_scan_jobs(database, config, mode)
            scan_prior_documents_once(database, jobs, config, stop_event, callback)
            from .scanning.pending import scan_pending_saved
            for target in config.targets:
                scan_pending_saved(database, config.for_target(target), jobs,
                                   stop_event, callback, workers=config.workers)

        if mode in {"all", "external_media_after_scan", "resume", "download_only"}:
            index_archive(config, database, stop_event, callback)

        # Acquisition-only runs never create scan jobs, even without a keyword set.
        if mode in {"download_only", "retry_download_errors"}:
            if mode == "download_only":
                download_archive(config, database, 0, stop_event, callback,
                                 states=("pending", "error"), acquire_only=True)
                recover_transient_text_errors(config, database, 0, [], stop_event,
                                              callback, continuous=True, acquire_only=True)
            else:
                retry_capture_downloads(config, database, stop_event, callback)
            if mode == "download_only" and config.media.enabled:
                index_media(config, database, stop_event, callback)
                download_media(config, database, stop_event, callback)
            paths = generate_index_reports(config, database,
                operation="download_only", progress=report_progress, stop_event=stop_event)
            paths["project"] = config.output_dir / "project.json"
            if config.media.enabled and mode == "download_only":
                paths.update(generate_media_reports(config, database))
            gaps = remaining_acquisition_gaps(config, database)
            finish_operation_run(database, operation_run_id, "incomplete" if gaps else "complete",
                                 f"Acquisition-only: {gaps:,} unresolved capture(s)" if gaps
                                 else "Acquisition-only operation complete")
            database.commit()
            return paths

        if not jobs:
            jobs = prepare_scan_jobs(database, config, mode)
            if mode in {"all", "download", "resume", "external_media_after_scan"}:
                scan_prior_documents_once(database, jobs, config, stop_event, callback)
        primary_run_id = jobs[0].scan_run_id
        if mode in {"all", "external_media_after_scan"}:
            download_archive(config, database, primary_run_id, stop_event, callback, states=("pending", "error"), scan_jobs=jobs)
        elif mode in {"download", "resume"}:
            download_archive(config, database, primary_run_id, stop_event, callback, states=("pending", "error"), scan_jobs=jobs)
        elif mode == "rescan":
            # Existing document rows and saved-but-unscanned bodies are both
            # part of the searchable inventory.  Scanning documents first
            # avoids reprocessing any newly recovered pending body twice.
            rescan_keyword_sets(database, jobs, stop_event, callback, workers=config.workers,
                                missing_only=all(database.execute(
                                    "SELECT source_operation,status FROM scan_runs WHERE id=?", (job.scan_run_id,)
                                ).fetchone()["source_operation"] == "rescan" and
                                database.execute("SELECT 1 FROM document_matches WHERE scan_run_id=? LIMIT 1", (job.scan_run_id,)).fetchone()
                                for job in jobs))
            from .scanning.pending import scan_pending_saved
            for target in config.targets:
                scan_pending_saved(database, config.for_target(target), jobs,
                                   stop_event, callback, workers=config.workers)
        elif mode == "retry_errors":
            retry_error_urls(config, database, primary_run_id, stop_event, callback, jobs)
            media_error_count = database.execute(
                "SELECT COUNT(*) FROM errors WHERE resolved=0 AND ignored=0 AND retryable=1 AND media_capture_id IS NOT NULL"
            ).fetchone()[0]
            if media_error_count:
                retry_media_errors(
                    config,
                    database,
                    stop_event,
                    callback,
                    config.retry_media_capture_ids or None,
                )
        if mode in {"all", "external_media_after_scan", "download", "resume"}:
            recover_transient_text_errors(config, database, primary_run_id, jobs, stop_event,
                                          callback, continuous=True)
        if mode in {"all", "download", "resume", "external_media_after_scan", "retry_errors"}:
            from .scanning.pending import scan_pending_saved
            for target in config.targets:
                scan_pending_saved(database, config.for_target(target), jobs, stop_event,
                                   callback, workers=config.workers)
        emit(callback, ProgressEvent("scan_finalize", "Scanning finished. Verifying saved-document coverage."))
        finish_jobs(database, jobs, "complete", config)
        database.commit()
        # Reporting may involve project-sized exports; keep the scan results
        # durable and report progress so the GUI does not appear frozen at 100%.
        report_finalization_started = True
        emit(callback, ProgressEvent("report", "Scanning committed. Generating scan reports."))
        paths = generate_job_reports(config, database, jobs, callback=callback, stop_event=stop_event)
        if mode == "retry_errors" and database.execute("SELECT COUNT(*) FROM media_captures").fetchone()[0]:
            paths.update(generate_media_reports(config, database))
        if mode == "external_media_after_scan":
            emit(
                callback,
                ProgressEvent(
                    "media_embed",
                    "Text scanning is complete. Discovering external images/videos in saved pages and resolving their Wayback captures…",
                ),
            )
            media_signature = index_external_embedded_media(config, database, stop_event, callback)
            media_total = int(database.execute(
                "SELECT COUNT(*) FROM media_captures WHERE query_signature=?", (media_signature,)
            ).fetchone()[0])
            pending_media = int(database.execute(
                "SELECT COUNT(*) FROM media_captures WHERE query_signature=? AND state IN ('pending','error')",
                (media_signature,),
            ).fetchone()[0])
            emit(
                callback,
                ProgressEvent(
                    "media_embed",
                    f"External-media indexing complete: {media_total:,} archived media captures resolved; {pending_media:,} need download. Starting streamed downloads…",
                    0,
                    pending_media,
                ),
            )
            download_media(config, database, stop_event, callback)
            paths.update(generate_media_reports(config, database))
        elif mode == "all" and config.media.enabled:
            index_media(config, database, stop_event, callback)
            download_media(config, database, stop_event, callback)
            paths.update(generate_media_reports(config, database))
        if config.research.enabled and config.research.auto_build:
            research_summary = build_research_index(config, database, stop_event, callback)
            research_report = config.output_dir / "reports" / "research_index.json"
            import json as _json
            research_report.write_text(_json.dumps(research_summary.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
            paths["research_index"] = research_report
        emit(callback, ProgressEvent("report", f"Reports written to {config.output_dir / 'reports'}"))
        incomplete_runs = []
        for job in jobs:
            run_state = database.execute(
                "SELECT status FROM scan_runs WHERE id=?", (job.scan_run_id,)
            ).fetchone()
            # Compatibility harnesses can inject synthetic jobs without a
            # persisted scan_runs row. Real jobs always have a durable row.
            if run_state is not None and run_state[0] != "complete":
                incomplete_runs.append(int(job.scan_run_id))
        if incomplete_runs:
            raise IncompleteCoverage(
                f"Reports saved, but scan coverage is incomplete for run(s) "
                f"{', '.join(map(str, incomplete_runs))}. Check unresolved scan errors "
                "and rerun Rescan to recover missing bodies.", paths,
            )
        unresolved = remaining_acquisition_gaps(config, database)
        finish_operation_run(database, operation_run_id,
                             "incomplete" if unresolved else "complete",
                             f"{unresolved:,} unresolved capture(s) remain in the selected scope" if unresolved
                             else "Operation complete")
        database.commit()
        return paths
    except IncompleteCoverage as exc:
        finish_operation_run(database, operation_run_id, "incomplete", str(exc))
        database.commit()
        emit(callback, ProgressEvent("scan_incomplete", str(exc)))
        raise
    except ConnectivityPaused as exc:
        with database:
            database.execute("UPDATE captures SET state='pending' WHERE state='downloading'")
            database.execute("UPDATE media_captures SET state='pending' WHERE state='downloading'")
            finish_jobs(database, jobs, "interrupted")
        finish_operation_run(database, operation_run_id, "paused", str(exc))
        database.commit()
        emit(callback, ProgressEvent("network_paused", str(exc)))
        raise
    except RateLimitDeferred as exc:
        with database:
            database.execute("UPDATE captures SET state='pending' WHERE state='downloading'")
            database.execute("UPDATE media_captures SET state='pending' WHERE state='downloading'")
            finish_jobs(database, jobs, "interrupted")
        finish_operation_run(database, operation_run_id, "interrupted", str(exc))
        database.commit()
        emit(
            callback,
            ProgressEvent(
                "rate_limit",
                f"Wayback stayed rate limited beyond the wait budget. Progress was saved; use Resume later. {exc}",
            ),
        )
        raise
    except Stopped:
        with database:
            database.execute("UPDATE captures SET state='pending' WHERE state='downloading'")
            database.execute("UPDATE media_captures SET state='pending' WHERE state='downloading'")
            finish_jobs(database, jobs, "interrupted")
        finish_operation_run(database, operation_run_id, "interrupted", "Stopped by user")
        database.commit()
        emit(callback, ProgressEvent("stopped", "Stopped. Progress was saved and can be resumed."))
        raise
    except Exception as exc:
        # Never leave active queue rows stranded after a local programming,
        # parsing, database, or filesystem failure. They remain resumable even
        # before the project is reopened and crash-recovery runs.
        with database:
            database.execute("UPDATE captures SET state='pending' WHERE state='downloading'")
            database.execute("UPDATE media_captures SET state='pending' WHERE state='downloading'")
            if jobs:
                # A filesystem/report export failure after the last scan result
                # is NOT a failed scan. Leave an explicit rescan retry point so
                # the same run (and its existing zero-hit/match evidence) can
                # be reused to regenerate the missing reports on next Rescan.
                finish_jobs(database, jobs, "interrupted" if
                            (mode == "rescan" and report_finalization_started) else "failed")
        finish_operation_run(database, operation_run_id, "failed", f"{type(exc).__name__}: {exc}")
        database.commit()
        raise
    finally:
        database.close()
