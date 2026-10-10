from __future__ import annotations

import concurrent.futures
from collections import deque
import hashlib
import sqlite3
import threading
import time
import urllib.parse
from pathlib import Path
from typing import Callable, Iterator

from ..cdx.client import HttpClient, RateLimitDeferred
from ..cdx.parameters import cdx_query_signature
from ..database.classification import classify_indexed_captures
from ..classification import classify_indexed_resource, classify_payload_kind
from ..config import ProjectConfig
from ..constants import REPLAY_URL
from ..content import classify_replay_content, decode_bytes, is_text_candidate, looks_textual_bytes, parse_page
from ..database.repositories import record_error, record_site_issue, resolve_errors, save_match, upsert_document
from ..events import ProgressEvent, Stopped
from ..parsing.embeds import extract_embed_candidates_fast
from ..site_status import host_from_url, should_surface_site_issue, site_issue_message
from ..scanning.jobs import ScanJob
from ..scanning.keywords import compile_prefilter
from ..scanning.scoring import analyze_content, prepare_analysis_fields
from ..utils import atomic_write_bytes, hash_text, normalize_search, utc_now
from .rate_limit import SharedFixedRateLimiter, shared_host_gate
from .validation import classify_exception
from .redirects import make_replay_redirect_validator
from ..network.transports import RedirectPolicyError


def replay_url(timestamp: str, original: str, modifier: str = "id_") -> str:
    encoded = urllib.parse.quote(original, safe=":/?&=#%+;,[]@!$'()*")
    clean_modifier = modifier if modifier in {"id_", "if_", "oe_"} else "id_"
    return f"{REPLAY_URL}/{timestamp}{clean_modifier}/{encoded}"


def capture_path(root: Path, capture_id: int, timestamp: str, original: str) -> Path:
    digest = hashlib.sha1(original.encode("utf-8", "surrogatepass")).hexdigest()
    return root / "captures" / timestamp[:4] / timestamp[4:6] / f"{capture_id}_{digest}.txt"


def cumulative_download_progress(
    database: sqlite3.Connection,
    config: ProjectConfig,
    queued_total: int,
    capture_ids: list[int] | None = None,
) -> tuple[int, int]:
    """Return completed/total progress across the whole resumable CDX queue."""
    if capture_ids:
        return 0, max(0, int(queued_total))
    signature = cdx_query_signature(config)
    total = int(database.execute(
        "SELECT COUNT(*) FROM captures WHERE query_signature=?", (signature,)
    ).fetchone()[0])
    return max(0, total - int(queued_total)), total


def prepare_download_rows(
    database: sqlite3.Connection,
    config: ProjectConfig,
    patterns,
    states: tuple[str, ...] = ("pending",),
    capture_ids: list[int] | None = None,
) -> tuple[int, Iterator[sqlite3.Row]]:
    """Build a disk-backed download queue and stream rows in bounded batches.

    Candidate IDs are stored in temporary SQLite tables. This avoids both the
    project-sized Python lists used by older releases and SQLite's platform-
    dependent parameter limit when a large retry selection is supplied.
    """
    if not capture_ids:
        classify_indexed_captures(database, cdx_query_signature(config))
    database.execute("DROP TABLE IF EXISTS temp.archive_scout_download_queue")
    database.execute(
        """CREATE TEMP TABLE archive_scout_download_queue(
               id INTEGER PRIMARY KEY,
               priority INTEGER NOT NULL,
               length INTEGER NOT NULL
           ) WITHOUT ROWID"""
    )
    database.execute(
        "CREATE INDEX archive_scout_download_queue_order ON archive_scout_download_queue(priority,length,id)"
    )
    database.execute("DROP TABLE IF EXISTS temp.archive_scout_capture_selection")

    source = "captures c"
    clauses: list[str] = []
    params: list[object] = []
    if capture_ids:
        database.execute(
            "CREATE TEMP TABLE archive_scout_capture_selection(id INTEGER PRIMARY KEY) WITHOUT ROWID"
        )
        database.executemany(
            "INSERT OR IGNORE INTO archive_scout_capture_selection(id) VALUES(?)",
            ((int(value),) for value in capture_ids),
        )
        source += " JOIN archive_scout_capture_selection s ON s.id=c.id"
    else:
        clauses.extend(["c.query_signature=?", "c.download_attempts<?"])
        params.extend([cdx_query_signature(config), config.max_attempts])
    if states:
        clauses.append("c.state IN (" + ",".join("?" for _ in states) + ")")
        params.extend(states)

    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    cursor = database.execute(
        "SELECT c.id,c.original_url,c.mimetype,c.length FROM " + source + where + " ORDER BY c.id",
        params,
    )
    url_prefilter = compile_prefilter(patterns) if patterns else None
    now = utc_now()
    while True:
        chunk = cursor.fetchmany(2000)
        if not chunk:
            break
        selected_ids: list[tuple[int, int, int]] = []
        binary_ids: list[int] = []
        keyword_skips: list[tuple[str, int]] = []
        for row in chunk:
            capture_id = int(row["id"])
            decision = classify_indexed_resource(row["original_url"], row["mimetype"] or "")
            if decision.resource_class in {"image","video","audio","other_binary"} and decision.confident:
                binary_ids.append(capture_id)
                continue
            if decision.resource_class == "media_descriptor" and decision.confident:
                binary_ids.append(capture_id)
                continue
            if config.download_scope == "keyword_urls" and url_prefilter is not None:
                original_url = str(row["original_url"])
                normalized_url = normalize_search(original_url)
                if (
                    not url_prefilter.has_positive_rules
                    or not url_prefilter.matches({"url": original_url}, {"url": normalized_url})
                ):
                    keyword_skips.append((now, capture_id))
                    continue
            length = max(0, int(row["length"] or 0))
            selected_ids.append((capture_id, 1 if length <= 0 else 0, length))
        with database:
            if selected_ids:
                database.executemany(
                    "INSERT OR IGNORE INTO archive_scout_download_queue(id,priority,length) VALUES(?,?,?)",
                    selected_ids,
                )
            if keyword_skips:
                database.executemany(
                    "UPDATE captures SET state='skipped',updated_at=? WHERE id=?",
                    keyword_skips,
                )
                database.executemany(
                    "UPDATE capture_routing SET routing='skipped_url_filter',updated_at=? WHERE capture_id=?",
                    keyword_skips,
                )
            if binary_ids:
                database.executemany(
                    "UPDATE captures SET state='skipped',updated_at=? WHERE id=?",
                    ((now, capture_id) for capture_id in binary_ids),
                )
                database.executemany(
                    """UPDATE capture_routing SET routing=?,updated_at=? WHERE capture_id=?""",
                    (("deferred_to_media" if config.media.enabled else "skipped_non_text", now, capture_id)
                     for capture_id in binary_ids),
                )
    total = int(database.execute(
        "SELECT COUNT(*) FROM archive_scout_download_queue"
    ).fetchone()[0])

    def iter_rows() -> Iterator[sqlite3.Row]:
        last_priority = -1
        last_length = -1
        last_id = 0
        while True:
            batch = database.execute(
                """
                SELECT c.* FROM captures c
                JOIN archive_scout_download_queue q ON q.id=c.id
                WHERE (q.priority,q.length,q.id)>(?,?,?)
                ORDER BY q.priority,q.length,q.id LIMIT 1000
                """,
                (last_priority, last_length, last_id),
            ).fetchall()
            if not batch:
                return
            for row in batch:
                length = max(0, int(row["length"] or 0))
                last_priority = 1 if length <= 0 else 0
                last_length = length
                last_id = int(row["id"])
                yield row

    return total, iter_rows()


def select_download_rows(
    database: sqlite3.Connection,
    config: ProjectConfig,
    patterns,
    states: tuple[str, ...] = ("pending",),
    capture_ids: list[int] | None = None,
) -> list[sqlite3.Row]:
    """Compatibility wrapper for extensions that expect an in-memory list."""
    _total, rows = prepare_download_rows(
        database, config, patterns, states=states, capture_ids=capture_ids
    )
    return list(rows)


class NonTextResponse(Exception):
    """An archived payload was media; this is not a transport or download failure."""
    def __init__(self, resource_class: str, reason: str, content_type: str):
        super().__init__(reason)
        self.resource_class=resource_class
        self.reason=reason
        self.content_type=content_type

def fetch_parse_scan(row: sqlite3.Row, config: ProjectConfig, jobs: list[ScanJob], client: HttpClient, scan_now: bool = True) -> dict:
    original = row["original_url"]
    url = replay_url(row["timestamp"], original)
    if isinstance(client, HttpClient):
        response = client.get(url, config.max_file_bytes,
                              redirect_validator=make_replay_redirect_validator(config, original))
    else:
        # Existing offline integrations can supply a fixture-only client.
        response = client.get(url, config.max_file_bytes)
    content_type = response["headers"].get("content-type") or response["headers"].get("Content-Type") or row["mimetype"] or ""
    data = response["data"]
    decision = classify_payload_kind(data, content_type, original)
    if decision.resource_class not in {"text", "media_descriptor"}:
        raise NonTextResponse(decision.resource_class, decision.reason, content_type)
    raw = decode_bytes(data if scan_now else data[:20000], content_type)
    replay_problem = classify_replay_content(raw, response["final_url"])
    if replay_problem:
        raise RuntimeError(replay_problem)
    if not scan_now:
        path = capture_path(config.output_dir, int(row["id"]), row["timestamp"], original)
        atomic_write_bytes(path, bytes(data))
        return {"capture_id": int(row["id"]), "path": path,
                "bytes_saved": len(data), "content_hash": hashlib.sha256(data).hexdigest(),
                "http_status": response["status"], "final_url": response["final_url"],
                "deferred_scan": True}
    title, visible, links = parse_page(raw, original)
    # Avoid a second full HTML parser pass in the download hot path. The page
    # parser above already found ordinary attributes; this lightweight source
    # pass recovers FlashVars/legacy player config. Dedicated media discovery
    # still performs the exhaustive parser pass later.
    if config.media.enabled and config.media.discover_embedded:
        embed_urls = {candidate.url for candidate in extract_embed_candidates_fast(raw, original)}
        if embed_urls:
            links = sorted(set(links).union(embed_urls))
    prepared_fields, prepared_normalized_fields = prepare_analysis_fields(original, title, visible, raw, links)
    analyses = {
        job.scan_run_id: analyze_content(
            original, title, visible, raw, links, job.patterns, job.prefilter,
            prepared_fields, prepared_normalized_fields,
        )
        for job in jobs
    }
    path = capture_path(config.output_dir, int(row["id"]), row["timestamp"], original)
    atomic_write_bytes(path, bytes(data))
    return {
        "capture_id": int(row["id"]),
        "path": path,
        "title": title,
        "visible": visible,
        "links": links,
        "analyses": analyses,
        "content_hash": hashlib.sha256(bytes(data)).hexdigest(),
        "normalized_hash": hash_text(prepared_normalized_fields["body"]),
        "bytes_saved": len(data),
        "http_status": response["status"],
        "final_url": response["final_url"],
    }


def save_success(database: sqlite3.Connection, result: dict) -> None:
    if result.get("deferred_scan"):
        with database:
            database.execute(
                """UPDATE captures SET state='downloaded_unscanned',http_status=?,final_url=?,
                       bytes_saved=?,updated_at=? WHERE id=?""",
                (result["http_status"],result["final_url"],result["bytes_saved"],utc_now(),result["capture_id"]),
            )
            database.execute(
                """UPDATE capture_routing SET resource_class='text',evidence='payload:validated_text',
                       confident=1,routing='downloaded_awaiting_scan',updated_at=? WHERE capture_id=?""",
                (utc_now(),result["capture_id"]),
            )
        return
    with database:
        document_id = upsert_document(
            database,
            result["capture_id"],
            result["path"],
            result["title"],
            result["visible"],
            result["links"],
            result["content_hash"],
            result["normalized_hash"],
            result["bytes_saved"],
        )
        database.execute(
            "UPDATE captures SET state='downloaded',http_status=?,final_url=?,bytes_saved=?,updated_at=? WHERE id=?",
            (result["http_status"], result["final_url"], result["bytes_saved"], utc_now(), result["capture_id"]),
        )
        for scan_run_id, analysis in result["analyses"].items():
            save_match(database, int(scan_run_id), document_id, analysis)
        resolve_errors(database, capture_id=result["capture_id"], document_id=document_id)


def download_archive(
    config: ProjectConfig,
    database: sqlite3.Connection,
    scan_run_id: int,
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None,
    states: tuple[str, ...] = ("pending",),
    capture_ids: list[int] | None = None,
    scan_jobs: list[ScanJob] | None = None,
) -> None:
    if config.download_scope == "index_only":
        if callback:
            callback(ProgressEvent("download", "Index-only mode selected; downloads skipped."))
        return
    jobs = scan_jobs or [ScanJob.create(scan_run_id, config.keyword_set_name, config.keywords)]
    if not jobs or any(not job.patterns for job in jobs):
        raise ValueError("at least one keyword rule is required")
    combined_patterns = [item for job in jobs for item in job.patterns]
    with database:
        database.execute("UPDATE captures SET state='pending' WHERE state='downloading'")
    total, row_iter = prepare_download_rows(
        database, config, combined_patterns, states=states, capture_ids=capture_ids
    )
    completed_before, cumulative_total = cumulative_download_progress(
        database, config, total, capture_ids
    )
    if not total:
        if callback:
            callback(ProgressEvent(
                "download",
                f"No matching captures remain to download; project progress {completed_before:,}/{cumulative_total:,}.",
                completed_before, cumulative_total,
            ))
        from ..scanning.pending import scan_pending_saved
        scan_pending_saved(database,config,jobs,stop_event,callback)
        return
    limiter = SharedFixedRateLimiter(config.download_delay)
    host_gate = shared_host_gate(config.rate_limit_base_pause, config.rate_limit_max_pause)

    def on_retry(attempt: int, total_attempts: int, reason: str, wait_seconds: float) -> None:
        if callback:
            rate_limited = "all Wayback requests paused" in reason
            stage = "rate_limit" if rate_limited else "download_retry"
            if rate_limited:
                limit = f"/{total_attempts}" if total_attempts else ""
                message = f"{reason}. Shared pause {attempt}{limit} for {wait_seconds:.1f}s; one recovery probe will run next…"
            else:
                message = f"{reason}. Retry {attempt}/{total_attempts} in {wait_seconds:.1f}s…"
            callback(ProgressEvent(stage, message))

    client = HttpClient(
        limiter,
        config.retries,
        max(config.connect_timeout, config.read_timeout),
        config.user_agent,
        stop_event,
        retry_callback=on_retry,
        connect_timeout=config.connect_timeout,
        read_timeout=config.read_timeout,
        pool_size=config.workers,
        host_gate=host_gate,
        rate_limit_attempts=config.rate_limit_attempts,
        rate_limit_max_wait=config.rate_limit_max_wait,
        network_backend=config.network.normalized().backend,
        trust_environment=config.network.normalized().trust_environment,
        network_callback=(lambda message: callback(ProgressEvent("network", message)) if callback else None),
    )
    completed = matched = failures = saved_this_pass = 0
    recent_commits: deque[float] = deque()
    started = time.monotonic()
    max_inflight = max(config.workers, config.workers * 2)
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=config.workers, thread_name_prefix="archive-scout") as pool:
            futures: dict[concurrent.futures.Future, sqlite3.Row] = {}
    
            def submit_available() -> None:
                slots = max_inflight - len(futures)
                if slots <= 0:
                    return
                rows: list[sqlite3.Row] = []
                for _ in range(slots):
                    try:
                        row = next(row_iter)
                    except StopIteration:
                        break
                    if stop_event.is_set():
                        raise Stopped
                    rows.append(row)
                if not rows:
                    return
                now = utc_now()
                with database:
                    database.executemany(
                        "UPDATE captures SET state='downloading',download_attempts=download_attempts+1,updated_at=? WHERE id=?",
                        ((now, int(row["id"])) for row in rows),
                    )
                for row in rows:
                    futures[pool.submit(fetch_parse_scan, row, config, jobs, client, False)] = row
    
            submit_available()
            while futures:
                if stop_event.is_set():
                    for pending in futures:
                        pending.cancel()
                    raise Stopped
                done, _ = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
                for future in done:
                    row = futures.pop(future)
                    try:
                        result = future.result()
                        save_success(database, result)
                        saved_this_pass += 1
                        recent_commits.append(time.monotonic())
                        if "analyses" in result:
                            matched += int(any(
                                int(analysis.get("score") or 0) >= config.minimum_score
                                and not analysis.get("excluded") and not analysis.get("required_missing")
                                for analysis in result["analyses"].values()
                            ))
                    except NonTextResponse as exc:
                        with database:
                            database.execute("UPDATE captures SET state='skipped',updated_at=? WHERE id=?", (utc_now(),row["id"]))
                            database.execute(
                                """INSERT INTO capture_routing(capture_id,resource_class,evidence,confident,routing,updated_at)
                                   VALUES(?,?,?,?,?,?) ON CONFLICT(capture_id) DO UPDATE SET
                                   resource_class=excluded.resource_class,evidence=excluded.evidence,
                                   confident=excluded.confident,routing=excluded.routing,updated_at=excluded.updated_at""",
                                (row["id"], exc.resource_class,exc.reason,1,
                                 "deferred_to_media" if config.media.enabled else "skipped_non_text",utc_now()),
                            )
                            if config.media.enabled and exc.resource_class in {"image","video"}:
                                from ..media.indexer import media_query_signature
                                from ..media.extensions import extension_from_url
                                from ..database.repositories import get_or_create_media_target, upsert_media_captures
                                target = get_or_create_media_target(database, "__deferred_text_replay__")
                                suffix = extension_from_url(str(row["original_url"]))
                                if suffix not in {".png",".jpg",".jpeg",".gif",".webp",".bmp",".avif",".mp4",".webm",".avi",".mov",".mkv",".flv",".swf",".wmv"}:
                                    suffix = ".png" if exc.resource_class == "image" else ".mp4"
                                upsert_media_captures(
                                    database,[({"original": row["original_url"],"timestamp":row["timestamp"],
                                               "mimetype":exc.content_type,"statuscode":row["statuscode"],
                                               "digest":row["digest"],"length":row["length"]},
                                               exc.resource_class,suffix)], target,
                                    media_query_signature(config),source_type="text_replay_deferred",
                                )
                    except RateLimitDeferred:
                        stop_event.set()
                        with database:
                            database.execute(
                                """UPDATE captures SET state='pending',
                                   download_attempts=CASE WHEN download_attempts>0 THEN download_attempts-1 ELSE 0 END,
                                   updated_at=? WHERE state='downloading' OR id=?""",
                                (utc_now(), row["id"]),
                            )
                        for pending in futures:
                            pending.cancel()
                        raise
                    except Stopped:
                        with database:
                            database.execute("UPDATE captures SET state='pending',updated_at=? WHERE id=?", (utc_now(), row["id"]))
                        raise
                    except Exception as exc:
                        failures += 1
                        if isinstance(exc, RedirectPolicyError):
                            category, status, retryable = exc.category, None, False
                        else:
                            category, status, retryable = classify_exception(exc)
                        issue_message = site_issue_message(
                            category, str(row["original_url"]), "text download", status
                        )
                        with database:
                            database.execute(
                                "UPDATE captures SET state='error',http_status=?,final_url=COALESCE(?,final_url),updated_at=? WHERE id=?",
                                (status, getattr(exc, "destination", None), utc_now(), row["id"]),
                            )
                            database.execute(
                                "UPDATE capture_routing SET routing='failed',updated_at=? WHERE capture_id=?",
                                (utc_now(), row["id"]),
                            )
                            record_error(
                                database,
                                "download",
                                category,
                                repr(exc),
                                capture_id=int(row["id"]),
                                http_status=status,
                                retryable=retryable,
                            )
                            if should_surface_site_issue(category):
                                record_site_issue(
                                    database,
                                    host_from_url(str(row["original_url"])),
                                    "text_download",
                                    category,
                                    issue_message,
                                    target=str(row["original_url"]),
                                    http_status=status,
                                )
                        if callback and should_surface_site_issue(category):
                            callback(ProgressEvent("site_issue", issue_message))
                    completed += 1
                    elapsed = max(0.001, time.monotonic() - started)
                    # This measures successfully committed *recent* saves,
                    # not an invocation average diluted by hours of indexing,
                    # waiting, or older network faults. No database reads.
                    now = time.monotonic()
                    while recent_commits and recent_commits[0] <= now - 30.0:
                        recent_commits.popleft()
                    rate = len(recent_commits) / min(30.0, elapsed)
                    invocation_rate = saved_this_pass / elapsed
                    cumulative_completed = min(cumulative_total, completed_before + completed)
                    if callback:
                        callback(
                            ProgressEvent(
                                "download",
                                f"Processed {cumulative_completed:,}/{cumulative_total:,}; "
                                f"this pass {completed:,}/{total:,}; newly saved {saved_this_pass:,}; "
                                f"matches {matched:,}; errors {failures:,}; recent successful saves {rate:.1f}/s (30s); "
                                f"whole-pass average {invocation_rate:.1f}/s",
                                cumulative_completed,
                                cumulative_total,
                                {
                                    "matched": matched,
                                    "failures": failures,
                                    "rate": rate,
                                    "rolling_save_rate_30s": rate,
                                    "invocation_save_rate": invocation_rate,
                                    "new_saves": saved_this_pass,
                                },
                            )
                        )
                    submit_available()
    finally:
        client.close()
    # Decouple complete HTML/source scanning from network request workers.
    from ..scanning.pending import scan_pending_saved
    scan_pending_saved(database,config,jobs,stop_event,callback)
