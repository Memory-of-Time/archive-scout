from __future__ import annotations

import concurrent.futures
import contextlib
import heapq
from collections import deque
import fnmatch
import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Callable

from ..cdx.client import HttpClient, RateLimitDeferred, ReplayRetryScheduled
from ..cdx.parameters import cdx_query_signature
from ..config import ProjectConfig
from ..database.repositories import (
    blocked_site_reasons, get_or_create_target, record_error, record_site_issue,
    save_media_success, upsert_capture,
)
from ..downloads.downloader import make_replay_redirect_validator, replay_url
from ..downloads.rate_limit import (SharedFixedRateLimiter, WAYBACK_REPLAY_RATE_KEY, shared_host_gate)
from ..downloads.validation import classify_exception
from ..content import classify_replay_content, decode_bytes
from ..events import ConnectivityPaused, ProgressEvent, Stopped
from ..site_status import host_from_url, should_surface_site_issue, site_issue_message
from ..storage import url_filename, media_path as storage_media_path, sha256_file
from ..classification import classify_payload_prefix
from ..network.transports import PreviewRejected, is_local_storage_error
from ..resource_detection import payload_media_format
from ..utils import utc_now
from .indexer import media_query_signature, media_signature_is_date_bound
from .extensions import extension_from_url, selected_extensions


def _row_value(row, key: str, default=""):
    try:
        value = row[key]
    except (KeyError, IndexError, TypeError):
        return default
    return default if value is None else value


def media_filename(original_url: str, extension: str = "") -> str:
    del extension
    return url_filename(original_url, "media")


def media_path(
    root: Path, row: sqlite3.Row, preserve_paths: bool = False, *,
    disambiguate: bool = False, detected_extension: str = "", detected_kind: str = "",
) -> Path:
    del preserve_paths
    try:
        timestamp = str(row["timestamp"] or "")
    except (KeyError, IndexError):
        timestamp = ""
    return storage_media_path(
        root, detected_kind or str(row["media_kind"]), str(row["original_url"]), timestamp,
        disambiguate=disambiguate, detected_extension=detected_extension,
    )

def media_replay_url(row: sqlite3.Row) -> str:
    modifier = "oe_" if str(row["extension"] or "").casefold() == ".swf" else "if_"
    return replay_url(str(row["timestamp"]), str(row["original_url"]), modifier=modifier)


def _hash_existing(path: Path) -> tuple[int, str]:
    return sha256_file(path)


def _format_allowed(config: ProjectConfig, kind: str, extension: str) -> bool:
    media = config.media.normalized()
    if kind == "image" and not media.include_images:
        return False
    if kind == "video" and not media.include_videos:
        return False
    if kind not in {"image", "video"}:
        return False
    selected = set(selected_extensions(media))
    excluded = {str(value).casefold() for value in media.exclude_extensions}
    ext = str(extension or "").casefold()
    if ext and ext in excluded:
        return False
    return bool(ext and ext in selected)


def fetch_media(row: sqlite3.Row, config: ProjectConfig, client: HttpClient) -> dict:
    """Fetch one selected media candidate and validate its actual payload format.

    Metadata decides what is worth trying. Only structural replay evidence decides
    what becomes a completed media file. This prevents dynamic endpoints and
    misleading MIME/suffixes from bypassing the user's concrete format filters.
    """
    provisional = media_path(config.output_dir, row, disambiguate=(config.media.snapshot_strategy == "all"))
    provisional.parent.mkdir(parents=True, exist_ok=True)
    if provisional.is_file():
        with provisional.open("rb") as handle:
            prefix = handle.read(64 * 1024)
        content_type = str(_row_value(row, "mimetype", ""))
        kind, detected_extension, _reason = payload_media_format(prefix, content_type)
        if kind in {"image", "video"}:
            detected_extension = detected_extension or str(row["extension"] or "")
        if not _format_allowed(config, str(kind or ""), detected_extension):
            return {"id": int(row["id"]), "kind": "rejected", "reason": "existing_media_failed_validation"}
        size, digest = _hash_existing(provisional)
        return {
            "id": int(row["id"]), "kind": "media", "path": provisional,
            "bytes": size, "hash": digest, "status": 200, "final_url": media_replay_url(row),
            "media_kind": kind, "extension": detected_extension,
        }

    temp = provisional.with_name(provisional.name + ".part")

    def preview_validator(headers: dict[str, str], prefix: bytes) -> str | None:
        content_type = headers.get("content-type") or headers.get("Content-Type") or str(row["mimetype"] or "")
        decision = classify_payload_prefix(prefix, content_type, str(row["original_url"]))
        kind, detected_extension, _reason = payload_media_format(prefix, content_type)
        if decision.resource_class == "text":
            return "text"
        if kind in {"image", "video"}:
            candidate_extension = detected_extension or str(row["extension"] or "")
            if detected_extension and not _format_allowed(config, kind, candidate_extension):
                return f"excluded_media_format:{detected_extension}"
            if kind == "image" and not config.media.include_images:
                return "excluded_media_kind:image"
            if kind == "video" and not config.media.include_videos:
                return "excluded_media_kind:video"
            return None
        if kind in {"audio", "other_binary"} or decision.resource_class in {"audio", "other_binary"}:
            return str(kind or decision.resource_class)
        return None

    try:
        try:
            response = client.download_to_path(
                media_replay_url(row), temp, config.media.max_file_bytes,
                preview_validator=preview_validator,
                redirect_validator=make_replay_redirect_validator(config, str(row["original_url"])),
            )
        except TypeError as exc:
            # Preserve compatibility with lightweight adapters/test doubles that
            # predate the bounded-prefix hook. Built-in transports use the hook;
            # legacy adapters are still validated before finalization below.
            if "preview_validator" not in str(exc) and "redirect_validator" not in str(exc):
                raise
            response = client.download_to_path(
                media_replay_url(row), temp, config.media.max_file_bytes
            )
    except PreviewRejected as exc:
        temp.unlink(missing_ok=True)
        classification = str(exc.classification or "unknown")
        return {
            "id": int(row["id"]),
            "kind": "recovered_text" if classification == "text" else "rejected",
            "reason": classification,
        }

    content_type = response["headers"].get("content-type") or response["headers"].get("Content-Type") or str(row["mimetype"] or "")
    if not int(response["bytes"]):
        temp.unlink(missing_ok=True)
        raise RuntimeError("empty media response")
    preview_bytes = bytes(response["preview"])
    decision = classify_payload_prefix(preview_bytes, str(content_type), str(row["original_url"]))
    if decision.resource_class == "text":
        preview = decode_bytes(preview_bytes, str(content_type))
        replay_problem = classify_replay_content(preview, str(response["final_url"]))
        temp.unlink(missing_ok=True)
        if replay_problem:
            raise RuntimeError(replay_problem)
        return {"id": int(row["id"]), "kind": "recovered_text", "reason": "verified_text_response"}

    kind, detected_extension, reason = payload_media_format(preview_bytes, str(content_type))
    if kind not in {"image", "video"}:
        temp.unlink(missing_ok=True)
        return {"id": int(row["id"]), "kind": "rejected", "reason": f"unresolved_media:{reason or 'unknown'}"}
    detected_extension = detected_extension or str(row["extension"] or "")
    if not _format_allowed(config, kind, detected_extension):
        temp.unlink(missing_ok=True)
        return {"id": int(row["id"]), "kind": "rejected", "reason": f"excluded_media_format:{detected_extension or 'unknown'}"}

    final_path = media_path(
        config.output_dir, row, disambiguate=(config.media.snapshot_strategy == "all"),
        detected_extension=detected_extension, detected_kind=kind,
    )
    final_path.parent.mkdir(parents=True, exist_ok=True)
    if final_path != temp:
        os.replace(temp, final_path)
    return {
        "id": int(row["id"]), "kind": "media", "path": final_path,
        "bytes": int(response["bytes"]), "hash": str(response.get("content_hash") or ""),
        "status": response["status"], "final_url": response["final_url"],
        "media_kind": kind, "extension": detected_extension,
    }


def _target_pattern_covers_url(pattern: str, url: str) -> bool:
    try:
        from urllib.parse import urlsplit
        parsed = urlsplit(url)
        value = (parsed.netloc + parsed.path + (("?" + parsed.query) if parsed.query else "")) if parsed.netloc else url.split("://", 1)[-1]
    except Exception:
        value = url.split("://", 1)[-1]
    return fnmatch.fnmatchcase(value.casefold(), str(pattern or "").casefold())


def _queue_recovered_text(database: sqlite3.Connection, config: ProjectConfig, row: sqlite3.Row) -> bool:
    original = str(row["original_url"] or "")
    target = next((value for value in config.targets if _target_pattern_covers_url(value, original)), None)
    if not target:
        return False
    target_config = config.for_target(target)
    if not (target_config.from_date <= str(row["timestamp"] or "") <= target_config.to_date):
        return False
    target_id = get_or_create_target(database, target, {})
    cdx_row = {
        "urlkey": str(row["urlkey"] or ""), "timestamp": str(row["timestamp"] or ""),
        "original": original, "mimetype": "text/html", "statuscode": str(row["statuscode"] or "200"),
        "digest": str(row["digest"] or ""), "length": str(row["length"] or 0),
    }
    signature = cdx_query_signature(target_config)
    upsert_capture(database, cdx_row, target_id, signature)
    database.execute(
        """UPDATE captures SET state='pending',skip_reason=NULL,resource_class='text',
           classification_reason='media_payload_recovered_text',updated_at=?
           WHERE original_url=? AND timestamp=? AND query_signature=?""",
        (utc_now(), original, str(row["timestamp"] or ""), signature),
    )
    return True


def _promote_next_snapshot(database: sqlite3.Connection, config: ProjectConfig, row: sqlite3.Row) -> int | None:
    strategy = config.media.normalized().snapshot_strategy
    if strategy not in {"earliest", "latest"}:
        return None
    key = str(row["urlkey"] or row["original_url"] or "")
    direction = "ASC" if strategy == "earliest" else "DESC"
    comparison = ">" if strategy == "earliest" else "<"
    candidate = database.execute(
        f"""SELECT id FROM media_captures
            WHERE query_signature=? AND COALESCE(NULLIF(urlkey,''),original_url)=?
              AND state='skipped_strategy' AND timestamp {comparison} ?
            ORDER BY timestamp {direction},id {direction} LIMIT 1""",
        (str(row["query_signature"]), key, str(row["timestamp"])),
    ).fetchone()
    if candidate is None:
        return None
    candidate_id = int(candidate["id"])
    database.execute(
        "UPDATE media_captures SET state='pending',skip_reason=NULL,updated_at=? WHERE id=?",
        (utc_now(), candidate_id),
    )
    return candidate_id


def iter_media_download_rows(
    database: sqlite3.Connection,
    clauses: list[str],
    params: list[object],
    batch_size: int = 1000,
):
    """Stream selected media rows using keyset pagination."""
    where = " AND ".join(clauses)
    total = int(database.execute(
        "SELECT COUNT(*) FROM media_captures WHERE " + where, params
    ).fetchone()[0])

    def rows():
        last_length = 0
        last_id = 0
        while True:
            batch = database.execute(
                "SELECT * FROM media_captures WHERE " + where
                + " AND COALESCE(length,0)>0 AND (COALESCE(length,0),id)>(?,?)"
                + " ORDER BY COALESCE(length,0),id LIMIT ?",
                [*params, last_length, last_id, max(1, int(batch_size))],
            ).fetchall()
            if not batch:
                break
            for row in batch:
                last_length = max(0, int(row["length"] or 0))
                last_id = int(row["id"])
                yield row
        last_id = 0
        while True:
            batch = database.execute(
                "SELECT * FROM media_captures WHERE " + where
                + " AND COALESCE(length,0)<=0 AND id>? ORDER BY id LIMIT ?",
                [*params, last_id, max(1, int(batch_size))],
            ).fetchall()
            if not batch:
                return
            for row in batch:
                last_id = int(row["id"])
                yield row

    return total, rows()


def download_media(
    config: ProjectConfig,
    database: sqlite3.Connection,
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None = None,
    states: tuple[str, ...] = ("pending",),
    media_capture_ids: list[int] | None = None,
    _resolution_depth: int = 0,
) -> None:
    clauses: list[str] = []
    params: list[object] = []
    database.execute("DROP TABLE IF EXISTS temp.archive_scout_media_selection")
    if media_capture_ids:
        database.execute(
            "CREATE TEMP TABLE archive_scout_media_selection(id INTEGER PRIMARY KEY) WITHOUT ROWID"
        )
        database.executemany(
            "INSERT OR IGNORE INTO archive_scout_media_selection(id) VALUES(?)",
            ((int(value),) for value in media_capture_ids),
        )
        clauses.append(
            "EXISTS (SELECT 1 FROM archive_scout_media_selection s WHERE s.id=media_captures.id)"
        )
    else:
        clauses.append("query_signature=?")
        params.append(media_query_signature(config))
        if not media_signature_is_date_bound(config):
            clauses.append("timestamp BETWEEN ? AND ?")
            params.extend([config.from_date, config.to_date])
        clauses.append("download_attempts<?")
        params.append(config.max_attempts)
    if states:
        clauses.append("state IN (" + ",".join("?" for _ in states) + ")")
        params.extend(states)
    media_root = config.output_dir / "media"
    (media_root / "images").mkdir(parents=True, exist_ok=True)
    (media_root / "videos").mkdir(parents=True, exist_ok=True)
    total, row_iter = iter_media_download_rows(database, clauses, params)
    if not total:
        if callback:
            callback(ProgressEvent("media_download", "No media captures to download.", 0, 0))
        return
    limiter = SharedFixedRateLimiter(config.download_delay, key=WAYBACK_REPLAY_RATE_KEY)
    host_gate = shared_host_gate(config.rate_limit_base_pause, config.rate_limit_max_pause)

    def on_retry(attempt: int, total_attempts: int, reason: str, wait_seconds: float) -> None:
        if callback:
            rate_limited = "quota/overload cooldown" in reason or "all Wayback requests paused" in reason
            stage = "rate_limit" if rate_limited else "media_retry"
            if rate_limited:
                limit = f"/{total_attempts}" if total_attempts else ""
                message = f"{reason}. Shared pause {attempt}{limit} for {wait_seconds:.1f}s; one recovery probe will run next…"
            else:
                message = f"{reason}. Retry {attempt}/{total_attempts} in {wait_seconds:.1f}s…"
            callback(ProgressEvent(stage, message))

    client = HttpClient(
        limiter,
        max(5, config.retries),
        max(config.connect_timeout, config.read_timeout),
        config.user_agent,
        stop_event,
        retry_callback=on_retry,
        connect_timeout=config.connect_timeout,
        read_timeout=config.read_timeout,
        pool_size=config.workers,
        host_gate=host_gate,
        rate_limit_base_pause=config.rate_limit_base_pause,
        rate_limit_max_pause=config.rate_limit_max_pause,
        rate_limit_attempts=config.rate_limit_attempts,
        rate_limit_max_wait=config.rate_limit_max_wait,
        network_backend=config.network.normalized().backend,
        trust_environment=config.network.normalized().trust_environment,
        network_callback=(lambda message: callback(ProgressEvent("network", message)) if callback else None),
        connection_failure_pause_threshold=config.network.normalized().connection_failure_pause_threshold,
        connection_retry_seconds=config.network.normalized().connection_retry_seconds,
    )
    cancel_event = threading.Event()
    unsettled_ids: set[int] = set()
    deferred_error = None
    complete = errors = 0
    started = time.monotonic()
    max_inflight = max(config.workers, config.workers * 2)
    blocked_hosts = blocked_site_reasons(database)
    promoted_ids: set[int] = set()

    try:
        with contextlib.ExitStack() as executor_scope:
            pool = executor_scope.enter_context(concurrent.futures.ThreadPoolExecutor(max_workers=config.workers, thread_name_prefix="archive-media"))
            executor_scope.callback(cancel_event.set)
            futures = {}
            ready = deque()
            delayed = []
            retry_sequence = 0
            rows_exhausted = False
            queue_limit = max(64, min(512, config.workers * 16))

            def run_attempt(row, attempt):
                with contextlib.ExitStack() as stack:
                    for name, argument in (("cancellation_scope", cancel_event), ("replay_attempt", attempt)):
                        method = getattr(type(client), name, None)
                        if callable(method):
                            stack.enter_context(method(client, argument))
                    return fetch_media(row, config, client)

            def submit_available() -> None:
                nonlocal complete, errors, rows_exhausted
                now_mono = time.monotonic()
                while delayed and delayed[0][0] <= now_mono:
                    _due, _seq, row, attempt = heapq.heappop(delayed)
                    ready.append((row, attempt))
                fresh_hidden = not any(attempt == 1 for _row, attempt in ready)
                limit = queue_limit + (max_inflight if fresh_hidden else 0)
                while not rows_exhausted and len(ready) + len(delayed) < limit:
                    if stop_event.is_set():
                        raise Stopped
                    try:
                        row = next(row_iter)
                    except StopIteration:
                        rows_exhausted = True
                        break
                    host = host_from_url(str(row["original_url"] or ""))
                    blocked_reason = blocked_hosts.get(host)
                    if blocked_reason:
                        message = site_issue_message(blocked_reason, str(row["original_url"]), "media download")
                        with database:
                            database.execute("UPDATE media_captures SET state='error',updated_at=? WHERE id=?",
                                             (utc_now(), int(row["id"])))
                            record_error(database, "media_download", blocked_reason, message,
                                         media_capture_id=int(row["id"]), retryable=False)
                        complete += 1
                        errors += 1
                        if callback:
                            callback(ProgressEvent("site_issue", message))
                        continue
                    ready.append((row, 1))
                rows = []
                while ready and len(futures) + len(rows) < max_inflight:
                    row, attempt = ready[0]
                    retry_inflight = sum(value[1] > 1 for value in futures.values()) + sum(value[1] > 1 for value in rows)
                    fresh_index = next((i for i, (_row, number) in enumerate(ready) if number == 1), None)
                    if attempt > 1 and fresh_index is not None and retry_inflight >= max(1, config.workers // 4):
                        ready.rotate(-fresh_index)
                    rows.append(ready.popleft())
                if not rows:
                    return
                with database:
                    database.executemany(
                        "UPDATE media_captures SET state='downloading',download_attempts=download_attempts+1,updated_at=? WHERE id=?",
                        ((utc_now(), int(row["id"])) for row, attempt in rows if attempt == 1),
                    )
                for row, attempt in rows:
                    unsettled_ids.add(int(row["id"]))
                    futures[pool.submit(run_attempt, row, attempt)] = (row, attempt)

            while futures or ready or delayed or not rows_exhausted:
                if stop_event.is_set():
                    cancel_event.set()
                    for pending in futures:
                        pending.cancel()
                    raise Stopped
                if deferred_error is None:
                    submit_available()
                if not futures:
                    if deferred_error is not None:
                        raise deferred_error
                    if delayed:
                        stop_event.wait(min(0.05, max(0.0, delayed[0][0] - time.monotonic())))
                        continue
                    if not ready and rows_exhausted:
                        break
                    continue
                done, _ = concurrent.futures.wait(futures, timeout=0.05, return_when=concurrent.futures.FIRST_COMPLETED)
                for future in done:
                    row, attempt = futures.pop(future)
                    try:
                        if future.cancelled():
                            raise Stopped
                        result = future.result()
                        result_kind = str(result.get("kind") or "media")
                        if result_kind == "media":
                            with database:
                                database.execute(
                                    "UPDATE media_captures SET media_kind=?,extension=?,skip_reason=NULL,updated_at=? WHERE id=?",
                                    (str(result.get("media_kind") or row["media_kind"]),
                                     str(result.get("extension") or row["extension"] or ""), utc_now(), int(row["id"])),
                                )
                                save_media_success(
                                    database, result["id"], result["path"], result["bytes"], result["hash"], result["status"], result["final_url"]
                                )
                        elif result_kind == "recovered_text":
                            with database:
                                in_scope = _queue_recovered_text(database, config, row)
                                database.execute(
                                    "UPDATE media_captures SET state='skipped',skip_reason=?,updated_at=? WHERE id=?",
                                    ("recovered_text" if in_scope else "text_response_out_of_scope", utc_now(), int(row["id"])),
                                )
                        else:
                            with database:
                                database.execute(
                                    "UPDATE media_captures SET state='skipped',skip_reason=?,updated_at=? WHERE id=?",
                                    (str(result.get("reason") or "media_payload_rejected")[:240], utc_now(), int(row["id"])),
                                )
                                promoted = _promote_next_snapshot(database, config, row)
                                if promoted is not None:
                                    promoted_ids.add(promoted)
                    except ReplayRetryScheduled as exc:
                        retry_sequence += 1
                        heapq.heappush(delayed, (exc.eligible_at, retry_sequence, row, exc.attempt_number))
                        continue
                    except (RateLimitDeferred, ConnectivityPaused) as exc:
                        if deferred_error is None:
                            deferred_error = exc
                            cancel_event.set()
                        for pending in futures:
                            pending.cancel()
                        continue
                    except Stopped:
                        if deferred_error is not None:
                            continue
                        cancel_event.set()
                        raise
                    except Exception as exc:
                        if is_local_storage_error(exc):
                            # A local disk/filesystem failure is not a reason to
                            # rotate HTTP backends or consume every media row.
                            cancel_event.set()
                            for pending in futures:
                                pending.cancel()
                            with database:
                                database.execute(
                                    "UPDATE media_captures SET state='pending',updated_at=? WHERE state='downloading' OR id=?",
                                    (utc_now(), row["id"]),
                                )
                            raise
                        errors += 1
                        category, status, retryable = classify_exception(exc)
                        issue_message = site_issue_message(
                            category, str(row["original_url"]), "media download", status
                        )
                        with database:
                            database.execute(
                                "UPDATE media_captures SET state='error',http_status=?,updated_at=? WHERE id=?",
                                (status, utc_now(), row["id"]),
                            )
                            record_error(
                                database, "media_download", category, repr(exc), media_capture_id=int(row["id"]),
                                http_status=status, retryable=retryable
                            )
                            if should_surface_site_issue(category):
                                record_site_issue(
                                    database,
                                    host_from_url(str(row["original_url"])),
                                    "media_download",
                                    category,
                                    issue_message,
                                    target=str(row["original_url"]),
                                    http_status=status,
                                )
                        if category in {"wayback_excluded", "robots_blocked"}:
                            blocked_hosts[host_from_url(str(row["original_url"]))] = category
                        if callback and should_surface_site_issue(category):
                            callback(ProgressEvent("site_issue", issue_message))
                    unsettled_ids.discard(int(row["id"]))
                    complete += 1
                    elapsed = max(0.001, time.monotonic() - started)
                    if callback:
                        callback(ProgressEvent(
                            "media_download",
                            f"Media {complete:,}/{total:,}; errors {errors:,}; {complete/elapsed:.1f}/s",
                            complete, total,
                            {"errors": errors},
                        ))
            if deferred_error is not None:
                raise deferred_error
    
    
    finally:
        cancel_event.set()
        # Completed futures above are committed before recovery. Any interrupted
        # logical job remains pending; retries do not consume extra job attempts.
        if unsettled_ids:
            with database:
                database.executemany(
                    "UPDATE media_captures SET state='pending',download_attempts=CASE WHEN download_attempts>0 THEN download_attempts-1 ELSE 0 END,updated_at=? WHERE id=? AND state='downloading'",
                    ((utc_now(), capture_id) for capture_id in unsettled_ids),
                )
        client.close()
    if promoted_ids and not stop_event.is_set() and _resolution_depth < 16:
        download_media(
            config, database, stop_event, callback, states=("pending",),
            media_capture_ids=sorted(promoted_ids), _resolution_depth=_resolution_depth + 1,
        )

def retry_media_errors(
    config: ProjectConfig,
    database: sqlite3.Connection,
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None = None,
    media_capture_ids: list[int] | None = None,
) -> None:
    clauses = ["resolved=0", "ignored=0", "retryable=1", "media_capture_id IS NOT NULL"]
    params: list[object] = []
    selected = media_capture_ids if media_capture_ids is not None else config.retry_media_capture_ids
    database.execute("DROP TABLE IF EXISTS temp.archive_scout_media_retry_selection")
    if selected:
        database.execute(
            "CREATE TEMP TABLE archive_scout_media_retry_selection(id INTEGER PRIMARY KEY) WITHOUT ROWID"
        )
        database.executemany(
            "INSERT OR IGNORE INTO archive_scout_media_retry_selection(id) VALUES(?)",
            ((int(value),) for value in selected),
        )
        clauses.append(
            "EXISTS (SELECT 1 FROM archive_scout_media_retry_selection s WHERE s.id=errors.media_capture_id)"
        )
    ids = [
        int(row[0])
        for row in database.execute(
            "SELECT DISTINCT media_capture_id FROM errors WHERE "
            + " AND ".join(clauses)
            + " ORDER BY media_capture_id",
            params,
        )
    ]
    if callback:
        callback(ProgressEvent("media_retry", f"Retrying {len(ids):,} errored media captures"))
    if ids:
        download_media(config, database, stop_event, callback, states=("error", "pending"), media_capture_ids=ids)
