from __future__ import annotations

import concurrent.futures
import hashlib
import os
import re
import sqlite3
import threading
import time
from pathlib import Path
from typing import Callable
from urllib.parse import urlsplit

from ..cdx.client import HttpClient, RateLimitDeferred
from ..config import ProjectConfig
from ..database.repositories import blocked_site_reasons, record_error, record_site_issue, save_media_success
from ..downloads.downloader import replay_url
from ..downloads.rate_limit import SharedFixedRateLimiter, shared_host_gate
from ..downloads.validation import classify_exception
from ..content import classify_replay_content
from ..events import ProgressEvent, Stopped
from ..site_status import host_from_url, should_surface_site_issue, site_issue_message
from ..utils import utc_now
from .indexer import media_query_signature

INVALID_COMPONENT = re.compile(r'[<>:"/\\|?*\x00-\x1f]')


def _platform_filename(value: str, fallback: str) -> str:
    """Preserve the URL filename verbatim whenever the filesystem permits it.

    URL path segments are normally already percent-escaped, so the common case
    needs no rewriting at all. Only characters that cannot be represented as one
    portable filename are percent-escaped instead of being replaced with opaque
    ids/timestamps.
    """
    value = (value or fallback).strip()
    if not value:
        value = fallback
    out: list[str] = []
    for char in value:
        if INVALID_COMPONENT.fullmatch(char) or ord(char) < 32:
            out.append(f"%{ord(char):02X}")
        else:
            out.append(char)
    result = "".join(out).rstrip(" .")
    return result or fallback


def media_filename(original_url: str, extension: str = "") -> str:
    parsed = urlsplit(original_url)
    # Keep the raw URL spelling (including percent escapes) rather than unquoting
    # it, so photo%20one.jpg remains photo%20one.jpg on disk.
    raw_name = (parsed.path or "").rsplit("/", 1)[-1]
    fallback = "media" + (extension or "")
    return _platform_filename(raw_name, fallback)


def media_path(root: Path, row: sqlite3.Row, preserve_paths: bool = False) -> Path:
    del preserve_paths  # v1.0.5 media layout is deliberately fixed and flat.
    folder = "images" if str(row["media_kind"]).casefold() == "image" else "videos"
    filename = media_filename(str(row["original_url"]), str(row["extension"] or ""))
    return root / "media" / folder / filename


def media_replay_url(row: sqlite3.Row) -> str:
    modifier = "oe_" if str(row["extension"] or "").casefold() == ".swf" else "if_"
    return replay_url(str(row["timestamp"]), str(row["original_url"]), modifier=modifier)


def _hash_existing(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            size += len(chunk)
            digest.update(chunk)
    return size, digest.hexdigest()


def fetch_media(row: sqlite3.Row, config: ProjectConfig, client: HttpClient) -> dict:
    path = media_path(config.output_dir, row)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Match the fast downloader's preflight: an exact destination that already
    # exists is treated as downloaded and is not fetched again.
    if path.is_file():
        size, digest = _hash_existing(path)
        return {
            "id": int(row["id"]),
            "path": path,
            "bytes": size,
            "hash": digest,
            "status": 200,
            "final_url": media_replay_url(row),
        }
    temp = path.with_name(path.name + ".part")
    response = client.download_to_path(
        media_replay_url(row),
        temp,
        config.media.max_file_bytes,
    )
    content_type = (
        response["headers"].get("content-type")
        or response["headers"].get("Content-Type")
        or ""
    ).casefold()
    if not int(response["bytes"]):
        temp.unlink(missing_ok=True)
        raise RuntimeError("empty media response")
    if "text/html" in content_type:
        preview = bytes(response["preview"]).decode("utf-8", "ignore")
        replay_problem = classify_replay_content(preview, str(response["final_url"]))
        if replay_problem or "wayback machine" in preview.casefold() or "not archived" in preview.casefold():
            temp.unlink(missing_ok=True)
            raise RuntimeError(replay_problem or "invalid_wayback_replay")
    os.replace(temp, path)
    return {
        "id": int(row["id"]),
        "path": path,
        "bytes": int(response["bytes"]),
        "hash": str(response["content_hash"]),
        "status": response["status"],
        "final_url": response["final_url"],
    }


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
        clauses.extend(["query_signature=?", "download_attempts<?"])
        params.extend([media_query_signature(config), config.max_attempts])
    if states:
        clauses.append("state IN (" + ",".join("?" for _ in states) + ")")
        params.extend(states)
    # Resolve MIME-vs-extension conflicts from proven text replay evidence
    # before opening media replay slots. Never skip a different timestamp.
    with database:
        database.execute(
            """UPDATE media_captures SET state='skipped' WHERE state='pending'
               AND EXISTS (SELECT 1 FROM captures c JOIN capture_routing r ON r.capture_id=c.id
                 WHERE c.original_url=media_captures.original_url AND c.timestamp=media_captures.timestamp
                   AND r.resource_class='text' AND r.evidence='payload:validated_text')"""
        )
    media_root = config.output_dir / "media"
    (media_root / "images").mkdir(parents=True, exist_ok=True)
    (media_root / "videos").mkdir(parents=True, exist_ok=True)
    total, row_iter = iter_media_download_rows(database, clauses, params)
    if not total:
        if callback:
            callback(ProgressEvent("media_download", "No media captures to download.", 0, 0))
        return
    limiter = SharedFixedRateLimiter(config.download_delay)
    host_gate = shared_host_gate(config.rate_limit_base_pause, config.rate_limit_max_pause)

    def on_retry(attempt: int, total_attempts: int, reason: str, wait_seconds: float) -> None:
        if callback:
            rate_limited = "all Wayback requests paused" in reason
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
        rate_limit_attempts=config.rate_limit_attempts,
        rate_limit_max_wait=config.rate_limit_max_wait,
        network_backend=config.network.normalized().backend,
        trust_environment=config.network.normalized().trust_environment,
        network_callback=(lambda message: callback(ProgressEvent("network", message)) if callback else None),
    )
    complete = errors = 0
    started = time.monotonic()
    max_inflight = max(config.workers, config.workers * 2)
    blocked_hosts = blocked_site_reasons(database)

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=config.workers, thread_name_prefix="archive-media") as pool:
            futures: dict[concurrent.futures.Future, sqlite3.Row] = {}
    
            def submit_available() -> None:
                nonlocal complete, errors
                slots = max_inflight - len(futures)
                if slots <= 0:
                    return
                rows: list[sqlite3.Row] = []
                while len(rows) < slots:
                    try:
                        row = next(row_iter)
                    except StopIteration:
                        break
                    if stop_event.is_set():
                        raise Stopped
                    host = host_from_url(str(row["original_url"] or ""))
                    blocked_reason = blocked_hosts.get(host)
                    if blocked_reason:
                        message = site_issue_message(
                            blocked_reason, str(row["original_url"]), "media download"
                        )
                        with database:
                            database.execute(
                                "UPDATE media_captures SET state='error',updated_at=? WHERE id=?",
                                (utc_now(), int(row["id"])),
                            )
                            record_error(
                                database, "media_download", blocked_reason, message,
                                media_capture_id=int(row["id"]), retryable=False,
                            )
                        complete += 1
                        errors += 1
                        if callback:
                            callback(ProgressEvent("site_issue", message))
                        continue
                    rows.append(row)
                if not rows:
                    return
                now = utc_now()
                with database:
                    database.executemany(
                        "UPDATE media_captures SET state='downloading',download_attempts=download_attempts+1,updated_at=? WHERE id=?",
                        ((now, int(row["id"])) for row in rows),
                    )
                for row in rows:
                    futures[pool.submit(fetch_media, row, config, client)] = row
    
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
                        with database:
                            save_media_success(
                                database, result["id"], result["path"], result["bytes"], result["hash"], result["status"], result["final_url"]
                            )
                    except RateLimitDeferred:
                        stop_event.set()
                        with database:
                            database.execute(
                                """UPDATE media_captures SET state='pending',
                                   download_attempts=CASE WHEN download_attempts>0 THEN download_attempts-1 ELSE 0 END,
                                   updated_at=? WHERE state='downloading' OR id=?""",
                                (utc_now(), row["id"]),
                            )
                        for pending in futures:
                            pending.cancel()
                        raise
                    except Stopped:
                        with database:
                            database.execute("UPDATE media_captures SET state='pending',updated_at=? WHERE id=?", (utc_now(), row["id"]))
                        raise
                    except Exception as exc:
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
                    complete += 1
                    elapsed = max(0.001, time.monotonic() - started)
                    if callback:
                        callback(ProgressEvent(
                            "media_download",
                            f"Media {complete:,}/{total:,}; errors {errors:,}; {complete/elapsed:.1f}/s",
                            complete, total,
                            {"errors": errors},
                        ))
                    submit_available()
    
    
    finally:
        client.close()

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
