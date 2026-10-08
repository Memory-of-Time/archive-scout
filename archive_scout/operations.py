from __future__ import annotations

import sqlite3
import json
import threading
import time
from datetime import datetime
from dataclasses import fields, replace
from pathlib import Path
from typing import Callable

from .cdx.client import RateLimitDeferred
from .cdx.indexer import index_archive
from .analysis.workflow import run_analysis
from .config import KeywordSetConfig, ProjectConfig, save_project_config
from .database.connection import open_database
from .database.lease import guard_project
from .database.repositories import (finish_scan_run, get_or_create_keyword_set, latest_scan_run, start_scan_run, start_operation_run, finish_operation_run, update_operation_run)
from .downloads.downloader import download_archive, download_archive_only, recover_pending_discard_cleanup
from .downloads.rate_limit import shared_host_gate
from .downloads.retry import retry_error_urls, retry_error_downloads
from .events import ConnectivityPaused, ProgressEvent, Stopped
from .media.downloader import download_media, retry_media_errors
from .media.indexer import index_external_embedded_media, index_media
from .media.reports import generate_media_reports
from .projects.integrity import check_project_integrity
from .projects.backups import create_project_backup
from .projects.compaction import compact_project_storage
from .projects.repair import repair_project
from .projects.diagnostics import export_diagnostics
from .projects.importers import import_text_folder
from .constants import VERSION
from .projects.merge import merge_projects
from .reports.text import generate_index_reports, generate_reports
from .research.index import build_research_index
from .scanning.jobs import ScanJob
from .scanning.workers import scanner_options
from .scanning.rescanner import rescan_keyword_sets
from .scanning.hitlist import load_hitlist, search_with_hitlist

SUPPORTED_MODES = {
    "all", "external_media_after_scan", "index", "download_only", "download", "resume", "rescan", "retry_errors", "retry_download_errors", "report", "integrity",
    "repair", "backup", "diagnostics", "import_folder",
    "media_all", "media_index", "media_download", "media_retry",
    "analysis", "research_index", "forum_rebuild", "merge_project", "hitlist", "compact",
}


def is_recoverable_pause(exc: BaseException) -> bool:
    """Stable integration contract for UI/bot wrappers around run_project()."""
    return isinstance(exc, (ConnectivityPaused, RateLimitDeferred))


def emit(callback: Callable[[ProgressEvent], None] | None, event: ProgressEvent) -> None:
    if callback:
        callback(event)


def prepare_scan_jobs(
    database: sqlite3.Connection,
    config: ProjectConfig,
    mode: str,
    *,
    reuse_completed: bool = False,
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
        compatible_sources = (mode,) if mode not in {"resume", "download", "all"} else ("all", "external_media_after_scan", "download", "resume", "retry_errors", "rescan")
        placeholders = ",".join("?" for _ in compatible_sources)
        if reuse_completed:
            statuses = ("interrupted", "failed", "complete")
        elif mode == "resume":
            statuses = ("interrupted", "failed")
        else:
            statuses = ("interrupted",)
        status_placeholders = ",".join("?" for _ in statuses)
        existing = database.execute(
            f"""SELECT id FROM scan_runs WHERE keyword_set_id=? AND status IN ({status_placeholders})
                AND minimum_score=? AND source_operation IN ({placeholders}) ORDER BY id DESC LIMIT 1""",
            (keyword_set_id, *statuses, config.minimum_score, *compatible_sources),
        ).fetchone()
        if existing:
            run_id = int(existing["id"])
            database.execute(
                "UPDATE scan_runs SET status='running',completed_at=NULL,name=? WHERE id=?",
                (f"{keyword_set.name} ({mode})", run_id),
            )
        else:
            run_id = start_scan_run(
                database, keyword_set_id, f"{keyword_set.name} ({mode})",
                config.minimum_score, mode,
                {"keyword_set": keyword_set.name, "rules": keyword_set.rules},
            )
        jobs.append(ScanJob.create(run_id, keyword_set.name, keyword_set.rules))
    database.commit()
    return jobs


def finish_jobs(database: sqlite3.Connection, jobs: list[ScanJob], status: str) -> None:
    for job in jobs:
        finish_scan_run(database, job.scan_run_id, status)


def generate_job_reports(config: ProjectConfig, database: sqlite3.Connection, jobs: list[ScanJob], callback=None) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    for index, job in enumerate(jobs, 1):
        generated = generate_reports(config, database, job.scan_run_id, **({"progress_callback": callback} if config.dashboard_eta_enabled else {}))
        if index == 1:
            paths.update(generated)
        paths[f"scan_{job.scan_run_id}_folder"] = generated["scan_folder"]
    return paths


def _secondary_media_config(config: ProjectConfig) -> ProjectConfig:
    """Return an independent follow-up media selection policy.

    Media CDX filters and extra parameters come only from ``MediaConfig``. The
    returned clone also mirrors the effective media collapse into its top-level
    CDX collapse field for backward-compatible integrations, but the primary
    text config is never mutated and media indexing no longer reads text filters.
    """
    normalized = config.normalized()
    media = normalized.media.normalized()
    # Eligibility must be established before the one-snapshot policy is applied.
    # Do not inject server-side collapse=urlkey merely because earliest is selected;
    # local snapshot reconciliation groups eligible rows by the archive urlkey.
    media_collapses = list(media.cdx_collapses)
    media = replace(media, cdx_collapses=media_collapses).normalized()
    return replace(normalized, media=media, cdx_collapses=media_collapses).normalized()

def _pending_media_recovered_text(database: sqlite3.Connection, config: ProjectConfig) -> int:
    return int(database.execute(
        """SELECT COUNT(*) FROM captures
           WHERE state='pending' AND resource_class='text'
             AND classification_reason='media_payload_recovered_text'
             AND timestamp BETWEEN ? AND ?""",
        (config.from_date, config.to_date),
    ).fetchone()[0])


def _run_standard_media_phase(
    config: ProjectConfig,
    database: sqlite3.Connection,
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None,
    *,
    external_only: bool = False,
) -> ProjectConfig:
    media_config = _secondary_media_config(config)
    if external_only:
        index_external_embedded_media(media_config, database, stop_event, callback)
    else:
        index_media(media_config, database, stop_event, callback)
    download_media(media_config, database, stop_event, callback)
    return media_config


@guard_project
def run_project(
    config: ProjectConfig,
    mode: str = "all",
    stop_event: threading.Event | None = None,
    callback: Callable[[ProgressEvent], None] | None = None,
) -> dict[str, Path]:
    config = config.normalized()
    if config.download_scope == "index_only" and mode in {"all", "external_media_after_scan"}:
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
    if mode in {"all", "external_media_after_scan", "download", "rescan", "retry_errors"} and not config.selected_keyword_sets():
        raise ValueError("select at least one keyword set")
    if mode == "download_only" and config.text_retention == "discard_after_scan":
        raise ValueError("Scan and discard cannot be used with download-only; choose Keep downloaded text files")
    stop_event = stop_event or threading.Event()
    emit(callback, ProgressEvent("starting", "Opening project database; large projects may need recovery or migration…"))
    if stop_event.is_set():
        raise Stopped
    config.output_dir.mkdir(parents=True, exist_ok=True)
    (config.output_dir / "captures").mkdir(exist_ok=True)
    (config.output_dir / "media" / "images").mkdir(parents=True, exist_ok=True)
    (config.output_dir / "media" / "videos").mkdir(parents=True, exist_ok=True)
    (config.output_dir / "reports").mkdir(exist_ok=True)
    database = open_database(config.output_dir, migrate=True)
    # Cleanup intent was committed with FULL durability before any unlink. Finish
    # that idempotent work before a new operation can reinterpret payload state.
    recover_pending_discard_cleanup(database, config.output_dir, callback)

    requested_mode = mode
    resumed_progress_stage = ""
    saved_rate_pause_detail: dict[str, object] = {}
    if mode == "resume":
        previous = database.execute(
            """SELECT mode,retention_policy,config_json,progress_json FROM operation_runs
               WHERE status IN ('interrupted','paused','failed','blocked_storage') AND mode<>'resume'
               ORDER BY id DESC LIMIT 1"""
        ).fetchone()
        if previous is not None:
            previous_mode = str(previous["mode"] or "")
            if previous_mode in {
                "all", "external_media_after_scan", "index", "download_only", "download", "rescan",
                "retry_errors", "retry_download_errors", "media_all", "media_index",
                "media_download", "media_retry", "analysis", "research_index",
                "forum_rebuild", "hitlist",
            }:
                mode = previous_mode
            raw_snapshot = str(previous["config_json"] or "").strip()
            if raw_snapshot:
                try:
                    payload = json.loads(raw_snapshot)
                    allowed = {item.name for item in fields(ProjectConfig)}
                    snapshot_values = {key: value for key, value in payload.items() if key in allowed}
                    snapshot_values["output_dir"] = config.output_dir
                    # Pacing is a runtime preference; saved selection and retention stay authoritative.
                    snapshot_values["adaptive_rate_limiting"] = config.adaptive_rate_limiting
                    config = ProjectConfig(**snapshot_values).normalized()
                except (TypeError, ValueError, json.JSONDecodeError):
                    # Older operation rows may not contain a complete snapshot.
                    # Fall back to the current project config, but still preserve
                    # the durable retention policy below.
                    pass
            if str(previous["retention_policy"] or "") in {"keep", "discard_after_scan"}:
                config = replace(config, text_retention=str(previous["retention_policy"])).normalized()
            try:
                previous_progress = json.loads(str(previous["progress_json"] or "{}"))
                resumed_progress_stage = str(previous_progress.get("stage") or "")
                detail = previous_progress.get("detail")
                if isinstance(detail, dict) and detail.get("reason_code") == "service_rate_limit":
                    saved_rate_pause_detail = dict(detail)
            except (TypeError, ValueError, json.JSONDecodeError):
                resumed_progress_stage = ""

    # Protect server-supplied Retry-After across a process restart.  Resume (or
    # another network operation in the same project) must not erase an
    # unexpired service deadline simply because the in-memory host gate is new.
    if not saved_rate_pause_detail:
        paused_row = database.execute(
            """SELECT progress_json FROM operation_runs
               WHERE status='paused' ORDER BY id DESC LIMIT 1"""
        ).fetchone()
        if paused_row is not None:
            try:
                paused_progress = json.loads(str(paused_row["progress_json"] or "{}"))
                detail = paused_progress.get("detail")
                if isinstance(detail, dict) and detail.get("reason_code") == "service_rate_limit":
                    saved_rate_pause_detail = dict(detail)
            except (TypeError, ValueError, json.JSONDecodeError):
                pass

    # Resume restores the original operation contract before validating it. A
    # download-only resume must not suddenly require keyword sets, while a full
    # scan still must have the keyword sets frozen in its saved snapshot.
    if config.download_scope == "index_only" and mode in {"all", "external_media_after_scan"}:
        mode = "index"
    if mode in {"all", "external_media_after_scan", "download", "rescan", "retry_errors"} and not config.selected_keyword_sets():
        database.close()
        raise ValueError("select at least one keyword set")
    if mode == "download_only" and config.text_retention == "discard_after_scan":
        database.close()
        raise ValueError("Scan and discard cannot be used with download-only; choose Keep downloaded text files")

    jobs: list[ScanJob] = []
    operation_run_id = start_operation_run(
        database, mode, VERSION, retention_policy=config.text_retention,
        config_json=json.dumps(config.to_payload(), ensure_ascii=False, sort_keys=True),
    )
    database.commit()
    original_callback = callback
    owner_thread_id = threading.get_ident()
    last_progress_write = 0.0
    last_progress_stage = ""
    progress_persist_interval = 5.0 if mode == "download_only" else 0.75
    from .eta import OperationForecast
    forecast = OperationForecast(database, config, mode) if config.dashboard_eta_enabled else None

    def operation_callback(event: ProgressEvent) -> None:
        nonlocal last_progress_write, last_progress_stage
        if config.dashboard_eta_enabled:
            event.detail = {**(event.detail or {}), "operation_run_id": operation_run_id}
            if threading.get_ident() == owner_thread_id:
                plan = forecast.observe(event)
                if plan is not None:
                    event.detail["eta_plan"] = plan
        if threading.get_ident() == owner_thread_id and event.stage not in {"backup_copy", "backup_compress", "backup_verify", "full_text_rebuild"}:
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
            should_write = stage_changed or completed_boundary or now - last_progress_write >= progress_persist_interval
            if should_write:
                update_operation_run(
                    database,
                    operation_run_id,
                    message=event.message,
                    completed=event.current,
                    total=event.total,
                    stage=event.stage,
                    detail=event.detail,
                )
                database.commit()
                last_progress_write = now
                last_progress_stage = event.stage
        if original_callback:
            original_callback(event)

    callback = operation_callback

    def _reset_transient_inflight() -> None:
        # Lower-level schedulers already preserve validated completions. This is
        # a final durable normalization before an archive-wide wait/probe cycle.
        with database:
            database.execute("UPDATE captures SET state='pending' WHERE state='downloading'")
            database.execute("UPDATE captures SET state='downloaded_unscanned' WHERE state='scanning'")
            database.execute("UPDATE media_captures SET state='pending' WHERE state='downloading'")

    def _wait_for_archive(exc: BaseException, stage: str) -> None:
        if not config.network.persistent_retries:
            raise exc
        _reset_transient_inflight()
        gate = shared_host_gate(config.rate_limit_base_pause, config.rate_limit_max_pause)
        now_epoch = time.time()
        query_pause = getattr(exc, "scope", "host") == "query"
        if query_pause:
            wait_seconds = max(0.01, min(float(config.network.retry_base_seconds), float(config.network.retry_max_seconds)))
            eligible_at = now_epoch + wait_seconds
            reason = "index_response"
            detail = {"reason_code": "index_response_retry", "eligible_at_epoch": eligible_at}
        elif isinstance(exc, RateLimitDeferred):
            eligible_at = float(exc.eligible_at_epoch or 0.0)
            wait_seconds = max(0.0, eligible_at - now_epoch) if eligible_at else max(
                float(config.rate_limit_base_pause), float(config.network.retry_base_seconds)
            )
            reason = "rate_limit"
            detail = exc.to_detail()
        else:
            gate.pause_for_connection_outage(float(config.network.connection_retry_seconds))
            wait_seconds = max(gate.remaining(), float(config.network.connection_retry_seconds))
            eligible_at = now_epoch + wait_seconds
            reason = "connectivity"
            detail = {"reason_code": "archive_connectivity", "eligible_at_epoch": eligible_at}
        detail.update({
            "recovery_stage": stage,
            "waiting_seconds": wait_seconds,
        })
        update_operation_run(
            database, operation_run_id, message=str(exc),
            stage="rate_limit_waiting" if reason == "rate_limit" else "network_waiting", detail=detail,
        )
        database.commit()
        until_text = datetime.fromtimestamp(eligible_at).strftime("%H:%M:%S") if eligible_at else "the next probe"
        emit(callback, ProgressEvent(
            "rate_limit_waiting" if reason == "rate_limit" else "network_waiting",
            f"Waiting to retry {'saved CDX work' if query_pause else 'Internet Archive'} — {exc}. Next check at {until_text}. Progress is saved; this run will continue automatically.",
            detail=detail,
        ))
        deadline = time.monotonic() + wait_seconds
        while True:
            if stop_event.is_set():
                raise Stopped
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            if stop_event.wait(min(1.0, remaining)):
                raise Stopped
        if not query_pause:
            gate.renew_recovery_cycle(getattr(exc, "incident_id", None))
        emit(callback, ProgressEvent(
            "network",
            f"Recovery wait finished; continuing saved {stage} work…",
            detail=detail,
        ))

    def _recovering_call(stage: str, function):
        cycle = 0
        while True:
            if stop_event.is_set():
                raise Stopped
            try:
                return function()
            except (ConnectivityPaused, RateLimitDeferred) as exc:
                cycle += 1
                _wait_for_archive(exc, stage)
                # The exact same durable stage is invoked again. Its queue/checkpoint
                # determines the remaining work; no new operation or scan lineage is
                # created here.
                emit(callback, ProgressEvent("network", f"Automatic archive recovery cycle {cycle:,}: resuming {stage}."))

    def _index_reports(*, complete: bool = True) -> dict[str, Path]:
        progress = {"progress_callback": original_callback} if config.dashboard_eta_enabled else {}
        paths = (generate_index_reports(config, database, **progress) if complete
                 else generate_index_reports(config, database, index_complete=False, **progress))
        detail = {
            "report_files": [str(path) for path in paths.values()],
            "index_complete": complete,
            "reason_code": "reports_written" if paths else "index_reports_disabled",
        }
        if paths:
            names = ", ".join(path.name for path in paths.values())
            message = f"{'Index' if complete else 'Partial index'} reports written to {config.output_dir / 'reports'}: {names}"
        else:
            message = (
                "No compatible index report files are enabled. In Reports, enable All indexed URLs, "
                "Summary, Errors or Site-specific issues. Match reports require a scan."
            )
        # Notify the GUI/CLI without replacing the last indexing counts or a
        # saved service deadline with a report event that has no counters.
        emit(original_callback, ProgressEvent("report", message, detail=detail))
        return paths

    def _partial_index_reports() -> None:
        if mode != "index":
            return
        try:
            _index_reports(complete=False)
        except Exception as report_error:
            # An optional inventory snapshot must never replace the original
            # stop/pause exception or erase its durable operation status.
            emit(original_callback, ProgressEvent(
                "report", f"Index progress is saved, but partial reports could not be written: {report_error}",
                detail={"reason_code": "index_report_write_failed", "index_complete": False},
            ))

    try:
        save_project_config(config)
        network_modes = {
            "all", "external_media_after_scan", "index", "download_only", "download", "resume", "retry_download_errors",
            "media_all", "media_index", "media_download", "media_retry",
        }
        eligible_at = float(saved_rate_pause_detail.get("eligible_at_epoch") or 0.0)
        if eligible_at > time.time():
            # Restore the deadline at actual admission, including mixed retries
            # whose local phase can run immediately.
            shared_host_gate(config.rate_limit_base_pause, config.rate_limit_max_pause).pause_for_rate_limit(
                eligible_at - time.time(), reason="saved service cooldown")
        if mode in network_modes and eligible_at > time.time():
            remaining = max(0.0, eligible_at - time.time())
            _wait_for_archive(
                RateLimitDeferred(
                    f"Wayback service cooldown is still active for about {remaining:.0f}s",
                    status=int(saved_rate_pause_detail.get("http_status") or 429),
                    waited=float(saved_rate_pause_detail.get("waited_seconds") or 0.0),
                    eligible_at_epoch=eligible_at,
                    incident_id=(int(saved_rate_pause_detail["incident_id"]) if saved_rate_pause_detail.get("incident_id") is not None else None),
                ),
                "saved service cooldown",
            )
        if mode == "backup":
            path = create_project_backup(config.output_dir, reason="manual", keep=config.backup_keep, max_mb=config.backup_max_mb,
                                         stop_event=stop_event,
                                         **({"callback": callback} if config.dashboard_eta_enabled else {}))
            emit(callback, ProgressEvent("backup", f"Backup written to {path}"))
            finish_operation_run(database, operation_run_id, "complete", str(path))
            database.commit()
            return {"backup": path}
        if mode == "repair":
            path = repair_project(config.output_dir, database, callback, keep_backups=config.backup_keep, backup_max_mb=config.backup_max_mb)
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
                create_project_backup(config.output_dir, reason="before_import", keep=config.backup_keep, max_mb=config.backup_max_mb)
            imported = import_text_folder(config.output_dir, source, database, stop_event, callback)
            report = config.output_dir / "reports" / "import_summary.txt"
            report.write_text(f"Archive Scout import\n\nSource: {source}\nImported: {imported}\n", encoding="utf-8")
            finish_operation_run(database, operation_run_id, "complete", f"Imported {imported}")
            database.commit()
            return {"import_summary": report}
        if mode == "hitlist":
            keywords = load_hitlist(config.hitlist_keywords, config.hitlist_file)
            if not keywords:
                # A selected keyword set is a convenient fallback for users who
                # want a quick literal check without duplicating their hitlist.
                selected = config.selected_keyword_sets()
                if selected:
                    from .scanning.keywords import parse_keyword_rules
                    values: list[str] = []
                    for keyword_set in selected:
                        for rule in parse_keyword_rules(keyword_set.rules):
                            if (not rule.regex and not rule.case_sensitive and not rule.whole_word
                                    and not rule.excluded and str(rule.expression).strip()):
                                values.append(str(rule.expression))
                    keywords = load_hitlist(values)
            if not keywords:
                raise ValueError("enter at least one literal keyword or choose a hitlist file")
            result = search_with_hitlist(config.output_dir, database, keywords, stop_event, callback)
            finish_operation_run(database, operation_run_id, "complete", f"Hitlist search complete: {result['matches']} matching captures")
            database.commit()
            return {"hitlist_csv": Path(result["csv"]), "hitlist_summary": Path(result["summary"])}
        if mode == "compact":
            result = compact_project_storage(config.output_dir, database, stop_event, callback)
            report = config.output_dir / "reports" / "storage_compaction.txt"
            report.write_text(
                "Archive Scout storage compaction\n\n" +
                "\n".join(f"{key}: {value}" for key, value in sorted(result.items())) + "\n",
                encoding="utf-8",
            )
            finish_operation_run(database, operation_run_id, "complete", str(report))
            database.commit()
            return {"storage_compaction": report}
        if mode == "integrity":
            path = check_project_integrity(config.output_dir, database, callback, stop_event=stop_event)
            emit(callback, ProgressEvent("integrity", f"Integrity report written to {path}"))
            finish_operation_run(database, operation_run_id, "complete", str(path))
            database.commit()
            return {"integrity": path}
        if mode == "analysis":
            paths = _recovering_call("archive analysis external lookup", lambda: run_analysis(config, database, stop_event, callback, forum_only=False))
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
            paths = _recovering_call("forum analysis", lambda: run_analysis(config, database, stop_event, callback, forum_only=True))
            finish_operation_run(database, operation_run_id, "complete", "Forum rebuild complete")
            database.commit()
            return paths
        if mode == "merge_project":
            source = Path(config.analysis.normalized().merge_source).expanduser()
            if not str(source).strip() or not source.exists():
                raise ValueError("choose an existing Archive Scout project folder to merge")
            if config.auto_backup and (config.output_dir / "archive_scout.sqlite3").exists():
                create_project_backup(config.output_dir, reason="before_merge", keep=config.backup_keep, max_mb=config.backup_max_mb)
            summary = merge_projects(config.output_dir, source, database, stop_event, callback)
            merge_report = config.output_dir / "reports" / "merge_summary.txt"
            merge_report.write_text("Archive Scout project merge\n\n" + "\n".join(f"{key}: {value}" for key, value in summary.items()) + "\n", encoding="utf-8")
            finish_operation_run(database, operation_run_id, "complete", str(merge_report))
            database.commit()
            return {"merge_summary": merge_report}
        if mode == "download_only":
            # Keep SQLite as a lightweight durable manifest/resume queue. No
            # scan/document/match/research work is created here. Optional media
            # acquisition uses the same Media-page settings as a full text run,
            # but never creates scan jobs merely to discover embedded media.
            acquisition_config = replace(config, download_scope="all_text")
            _recovering_call("text indexing", lambda: index_archive(acquisition_config, database, stop_event, callback))
            stats = _recovering_call("text acquisition", lambda: download_archive_only(
                acquisition_config, database, stop_event, callback, states=("pending",)
            ))
            paths: dict[str, Path] = {"project": config.output_dir / "project.json"}
            if config.media.enabled:
                emit(
                    callback,
                    ProgressEvent(
                        "media_index",
                        "Text acquisition is complete. Starting the standard supplemental-media phase…",
                    ),
                )
                media_config = _recovering_call("supplemental media", lambda: _run_standard_media_phase(
                    config, database, stop_event, callback, external_only=False
                ))
                # A media attempt can prove that a candidate is genuine in-scope
                # text. Download-only reacquires that text without creating scan
                # jobs, then gives any newly deferred media back to the same
                # standard media engine. Bound the routing loop defensively.
                for _cycle in range(4):
                    recovered = _pending_media_recovered_text(database, config)
                    if not recovered:
                        break
                    emit(callback, ProgressEvent(
                        "download_only",
                        f"Media validation recovered {recovered:,} in-scope text capture(s); acquiring them before media continues.",
                    ))
                    _recovering_call("recovered text acquisition", lambda: download_archive_only(
                        acquisition_config, database, stop_event, callback, states=("pending",)
                    ))
                    media_config = _recovering_call("supplemental media", lambda: _run_standard_media_phase(
                        config, database, stop_event, callback, external_only=False
                    ))
                if _pending_media_recovered_text(database, config):
                    raise RuntimeError("text/media routing did not converge after four bounded recovery cycles")
                paths.update(generate_media_reports(media_config, database))
            emit(
                callback,
                ProgressEvent(
                    "download_only",
                    f"Download-only acquisition complete: {int(stats['downloaded']):,} saved; "
                    f"{int(stats['skipped']):,} non-text skipped; {int(stats['errors']):,} errors. "
                    "Run Search with Hitlist when ready.",
                    int(stats["queued"]), int(stats["queued"]),
                    {"downloaded": int(stats["downloaded"]), "scan_workers": 0},
                ),
            )
            finish_operation_run(database, operation_run_id, "complete", "Download-only acquisition complete")
            database.commit()
            return paths
        if mode == "retry_download_errors":
            stats = _recovering_call("text acquisition retry", lambda: retry_error_downloads(config, database, stop_event, callback))
            paths = {"project": config.output_dir / "project.json"}
            emit(callback, ProgressEvent(
                "download_retry",
                f"Acquisition-only retry complete: {int(stats['downloaded']):,} saved; "
                f"{int(stats['skipped']):,} skipped; {int(stats['errors']):,} errors.",
                int(stats["queued"]), int(stats["queued"]),
            ))
            finish_operation_run(database, operation_run_id, "complete", "Acquisition-only retry complete")
            database.commit()
            return paths
        if mode == "index":
            _recovering_call("text indexing", lambda: index_archive(config, database, stop_event, callback))
            paths = _index_reports()
            paths["project"] = config.output_dir / "project.json"
            finish_operation_run(database, operation_run_id, "complete", "Index complete")
            database.commit()
            return paths
        if mode == "report":
            existing = latest_scan_run(database)
            last_text_run = database.execute(
                """SELECT mode,status FROM operation_runs WHERE id<>?
                   AND mode IN ('index','all','external_media_after_scan','download','resume','rescan','retry_errors')
                   ORDER BY id DESC LIMIT 1""",
                (operation_run_id,),
            ).fetchone()
            if last_text_run is not None and last_text_run["mode"] == "index":
                paths = _index_reports(complete=last_text_run["status"] == "complete")
            elif existing is None:
                if database.execute("SELECT 1 FROM captures LIMIT 1").fetchone():
                    paths = _index_reports()
                else:
                    raise RuntimeError("this project does not contain indexed captures or a completed scan run")
            else:
                paths = generate_reports(config, database, existing, **({"progress_callback": callback} if config.dashboard_eta_enabled else {}))
            if paths:
                emit(callback, ProgressEvent("report", f"Reports written to {config.output_dir / 'reports'}"))
            finish_operation_run(database, operation_run_id, "complete", "Reports regenerated")
            database.commit()
            return paths
        if mode == "media_index":
            _recovering_call("media indexing", lambda: index_media(config, database, stop_event, callback))
            paths = generate_media_reports(config, database)
            finish_operation_run(database, operation_run_id, "complete", "Media index complete")
            database.commit()
            return paths
        if mode == "media_download":
            _recovering_call("media download", lambda: download_media(config, database, stop_event, callback))
            paths = generate_media_reports(config, database)
            finish_operation_run(database, operation_run_id, "complete", "Media download complete")
            database.commit()
            return paths
        if mode == "media_retry":
            _recovering_call("media retry", lambda: retry_media_errors(config, database, stop_event, callback))
            paths = generate_media_reports(config, database)
            finish_operation_run(database, operation_run_id, "complete", "Media retry complete")
            database.commit()
            return paths
        if mode == "media_all":
            _recovering_call("media indexing", lambda: index_media(config, database, stop_event, callback))
            _recovering_call("media download", lambda: download_media(config, database, stop_event, callback))
            paths = generate_media_reports(config, database)
            finish_operation_run(database, operation_run_id, "complete", "Media operation complete")
            database.commit()
            return paths

        if mode in {"all", "external_media_after_scan", "resume"}:
            _recovering_call("text indexing", lambda: index_archive(config, database, stop_event, callback))

        # Resume can re-enter after the original text scan already completed but
        # before/during its requested media phase. In that case do not manufacture
        # a new empty scan run; reuse the completed run only if media later recovers
        # genuine text that must be scanned under the frozen operation contract.
        resume_media_only = False
        if requested_mode == "resume" and mode in {"all", "external_media_after_scan"} and config.media.enabled:
            unfinished_text = int(database.execute(
                """SELECT COUNT(*) FROM captures
                   WHERE timestamp BETWEEN ? AND ?
                     AND state IN ('pending','downloading','downloaded_unscanned','scanning')""",
                (config.from_date, config.to_date),
            ).fetchone()[0])
            resume_media_only = unfinished_text == 0 and latest_scan_run(database) is not None

        scan_incomplete = False
        if not resume_media_only:
            job_mode = "resume" if requested_mode == "resume" else mode
            jobs = prepare_scan_jobs(database, config, job_mode)
            primary_run_id = jobs[0].scan_run_id
            if mode in {"all", "external_media_after_scan"}:
                scan_stats = _recovering_call("text acquisition and scan", lambda: download_archive(config, database, primary_run_id, stop_event, callback, states=("pending",), scan_jobs=jobs))
                scan_incomplete = bool(scan_stats and int(scan_stats.get("scan_errors", 0)))
            elif mode in {"download", "resume"}:
                scan_stats = _recovering_call("text acquisition and scan", lambda: download_archive(config, database, primary_run_id, stop_event, callback, states=("pending",), scan_jobs=jobs))
                scan_incomplete = bool(scan_stats and int(scan_stats.get("scan_errors", 0)))
            elif mode == "rescan":
                rescan_keyword_sets(database, jobs, stop_event, callback, workers=(config.scan_workers or None), report_config=config.report, **scanner_options(config))
            elif mode == "retry_errors":
                _recovering_call("text retry", lambda: retry_error_urls(config, database, primary_run_id, stop_event, callback, jobs))
                media_error_count = database.execute(
                    "SELECT COUNT(*) FROM errors WHERE resolved=0 AND ignored=0 AND "
                    + ("1=1" if config.retry_include_unavailable else "retryable=1") + " AND media_capture_id IS NOT NULL"
                ).fetchone()[0]
                if media_error_count:
                    _recovering_call("media retry", lambda: retry_media_errors(
                        config, database, stop_event, callback, config.retry_media_capture_ids or None,
                    ))

        media_config: ProjectConfig | None = None
        combined_media = (
            mode == "external_media_after_scan"
            or (mode == "all" and config.media.enabled)
        )
        if combined_media:
            emit(callback, ProgressEvent(
                "media_index",
                "Text acquisition and the committed scan backlog are complete. Starting the standard supplemental-media phase…",
            ))
            media_config = _recovering_call("supplemental media", lambda: _run_standard_media_phase(
                config, database, stop_event, callback,
                external_only=(mode == "external_media_after_scan"),
            ))
            for _cycle in range(4):
                recovered = _pending_media_recovered_text(database, config)
                if not recovered:
                    break
                # Genuine text returned by a media attempt belongs back in the
                # text pipeline. Pause media scheduling, reuse the original scan
                # contract, then resume standard media selection for discoveries.
                if not jobs:
                    jobs = prepare_scan_jobs(database, config, mode, reuse_completed=True)
                primary_run_id = jobs[0].scan_run_id
                emit(callback, ProgressEvent(
                    "scan",
                    f"Media validation recovered {recovered:,} in-scope text capture(s); draining that text backlog before media resumes.",
                ))
                _recovering_call("recovered text acquisition and scan", lambda: download_archive(
                    config, database, primary_run_id, stop_event, callback,
                    states=("pending",), scan_jobs=jobs,
                ))
                media_config = _recovering_call("supplemental media", lambda: _run_standard_media_phase(
                    config, database, stop_event, callback,
                    external_only=(mode == "external_media_after_scan"),
                ))
            if _pending_media_recovered_text(database, config):
                raise RuntimeError("text/media routing did not converge after four bounded recovery cycles")

        if jobs:
            finish_jobs(database, jobs, "interrupted" if scan_incomplete else "complete")
            database.commit()
            paths = generate_job_reports(config, database, jobs, **({"callback": callback} if config.dashboard_eta_enabled else {}))
        else:
            paths = {}
            existing = latest_scan_run(database)
            if existing is not None:
                paths.update(generate_reports(config, database, existing))
        if mode == "retry_errors" and database.execute("SELECT COUNT(*) FROM media_captures").fetchone()[0]:
            paths.update(generate_media_reports(config, database))
        if media_config is not None:
            paths.update(generate_media_reports(media_config, database))
        if config.research.enabled and config.research.auto_build:
            if config.text_retention == "discard_after_scan":
                emit(callback, ProgressEvent(
                    "research",
                    "Research Intelligence auto-build was skipped because this operation discarded full text payloads. Reacquire or use retained captures for source-dependent research.",
                ))
            else:
                research_summary = build_research_index(config, database, stop_event, callback)
                research_report = config.output_dir / "reports" / "research_index.json"
                import json as _json
                research_report.write_text(_json.dumps(research_summary.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
                paths["research_index"] = research_report
        emit(callback, ProgressEvent("report", f"Reports written to {config.output_dir / 'reports'}"))
        if scan_incomplete:
            finish_operation_run(
                database, operation_run_id, "interrupted",
                "Acquisition completed, but one or more scan items remain retryable; Resume continues the same scan lineage.",
            )
            emit(callback, ProgressEvent(
                "scan",
                "Acquisition completed, but scan coverage is incomplete. Progress and existing matches were preserved; use Resume to retry the remaining scan items.",
            ))
        else:
            finish_operation_run(database, operation_run_id, "complete", "Operation complete")
        database.commit()
        return paths
    except ConnectivityPaused as exc:
        with database:
            database.execute("UPDATE captures SET state='pending' WHERE state='downloading'")
            database.execute("UPDATE captures SET state='downloaded_unscanned' WHERE state='scanning'")
            database.execute("UPDATE media_captures SET state='pending' WHERE state='downloading'")
            finish_jobs(database, jobs, "interrupted")
        finish_operation_run(database, operation_run_id, "paused", str(exc))
        database.commit()
        _partial_index_reports()
        emit(callback, ProgressEvent("network_paused", str(exc)))
        raise
    except RateLimitDeferred as exc:
        with database:
            database.execute("UPDATE captures SET state='pending' WHERE state='downloading'")
            database.execute("UPDATE captures SET state='downloaded_unscanned' WHERE state='scanning'")
            database.execute("UPDATE media_captures SET state='pending' WHERE state='downloading'")
            finish_jobs(database, jobs, "interrupted")
        detail = exc.to_detail()
        update_operation_run(
            database,
            operation_run_id,
            message=str(exc),
            stage="rate_limit_paused",
            detail=detail,
        )
        finish_operation_run(database, operation_run_id, "paused", str(exc))
        database.commit()
        _partial_index_reports()
        emit(
            callback,
            ProgressEvent(
                "rate_limit_paused",
                f"Archive recovery was configured for one-shot deferral. Progress was saved; use Resume later. {exc}",
                detail=detail,
            ),
        )
        raise
    except Stopped:
        with database:
            database.execute("UPDATE captures SET state='pending' WHERE state='downloading'")
            database.execute("UPDATE captures SET state='downloaded_unscanned' WHERE state='scanning'")
            database.execute("UPDATE media_captures SET state='pending' WHERE state='downloading'")
            finish_jobs(database, jobs, "interrupted")
        finish_operation_run(database, operation_run_id, "interrupted", "Stopped by user")
        database.commit()
        _partial_index_reports()
        emit(callback, ProgressEvent("stopped", "Stopped. Progress was saved and can be resumed."))
        raise
    except Exception as exc:
        # Never leave active queue rows stranded after a local programming,
        # parsing, database, or filesystem failure. They remain resumable even
        # before the project is reopened and crash-recovery runs.
        with database:
            database.execute("UPDATE captures SET state='pending' WHERE state='downloading'")
            database.execute("UPDATE captures SET state='downloaded_unscanned' WHERE state='scanning'")
            database.execute("UPDATE media_captures SET state='pending' WHERE state='downloading'")
            if jobs:
                finish_jobs(database, jobs, "failed")
        finish_operation_run(database, operation_run_id, "failed", f"{type(exc).__name__}: {exc}")
        database.commit()
        raise
    finally:
        # Once worker pools have drained, checkpoint WAL so a clean Pause & Save
        # or normal shutdown has a small, self-contained durable database.
        try:
            if forecast is not None:
                forecast.persist()
            database.commit()
            database.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except sqlite3.Error:
            pass
        database.close()
