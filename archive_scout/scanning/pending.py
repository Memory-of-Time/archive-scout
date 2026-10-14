"""Bounded, restartable, purely local scanning of retained archive bodies."""
from __future__ import annotations

import concurrent.futures
from contextlib import nullcontext
import hashlib
import os
import sqlite3
import threading
import time
from collections import deque
from pathlib import Path
from typing import Callable, Iterator

from ..cdx.parameters import cdx_query_signature
from ..content import decode_bytes, parse_page, classify_replay_content
from ..database.repositories import record_error, resolve_errors, save_match, upsert_document
from ..events import ProgressEvent, Stopped
from ..utils import hash_text, utc_now
from .jobs import ScanJob


class PayloadIntegrityError(OSError):
    """A saved body no longer matches its durable raw-byte checkpoint."""


class ArchivedErrorShell(RuntimeError):
    """Wayback returned an origin/error page instead of the intended body."""
from .scoring import analyze_content, prepare_analysis_fields

SCAN_BYTE_BUDGET = 64 * 1024 * 1024


def _sqlite_write_busy(exc: BaseException) -> bool:
    """SQLite busy/locked, including extended BUSY_SNAPSHOT, is not a scan error.

    Retrying a failed write in the old read transaction can never fix
    BUSY_SNAPSHOT. Roll back the *whole batch* and start a fresh write
    transaction instead of recording a permanent per-capture scan failure.
    """
    if not isinstance(exc, sqlite3.OperationalError):
        return False
    code = getattr(exc, "sqlite_errorcode", None)
    if isinstance(code, int) and code & 0xff in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
        return True
    message = str(exc).lower()
    return "database is locked" in message or "database table is locked" in message


def _scan_one(row: dict, jobs: list[ScanJob]) -> dict:
    path = Path(row["path"])
    data = path.read_bytes()
    actual_hash = hashlib.sha256(data).hexdigest()
    if row.get("expected_hash") and (actual_hash != row["expected_hash"]
            or len(data) != int(row.get("expected_size") or 0)):
        raise PayloadIntegrityError(f"local retained body failed SHA-256/size validation: capture {row['id']}")
    raw = decode_bytes(data, row.get("response_content_type") or row.get("mimetype") or "")
    replay_problem = classify_replay_content(raw, row.get("final_url") or "")
    if replay_problem:
        raise ArchivedErrorShell(replay_problem)
    title, visible, links = parse_page(raw, row["original_url"])
    fields, normalized = prepare_analysis_fields(row["original_url"], title, visible, raw, links)
    analyses = {
        job.scan_run_id: analyze_content(
            row["original_url"], title, visible, raw, links,
            job.patterns, job.prefilter, fields, normalized,
        )
        for job in jobs
    }
    return {
        "capture_id": row["id"], "path": path, "title": title, "visible": visible,
        "links": links, "bytes_saved": len(data),
        "content_hash": actual_hash,
        "normalized_hash": hash_text(normalized["body"]), "analyses": analyses,
    }


def _pending_rows(database: sqlite3.Connection, config) -> Iterator[dict]:
    """Keyset scan over the active inventory; cursor memory is bounded."""
    signature = cdx_query_signature(config)
    targets = list(config.targets)
    if not targets:
        return
    cursor_id = 0
    target_slots = ",".join("?" for _ in targets)
    while True:
        batch = database.execute(
            """SELECT c.id,c.timestamp,c.original_url,c.mimetype,c.length,d.path,
                 v.content_hash AS expected_hash,v.bytes_saved AS expected_size,
                 v.response_content_type AS response_content_type,v.final_url AS final_url
               FROM captures c JOIN targets t ON t.id=c.target_id
               LEFT JOIN documents d ON d.capture_id=c.id
               LEFT JOIN capture_payload_evidence v ON v.capture_id=c.id
               WHERE (c.state='downloaded_unscanned' OR
                     (c.state='downloaded' AND d.id IS NULL))
               AND c.query_signature=? AND t.pattern IN (""" + target_slots + """)
               AND c.id>? ORDER BY c.id LIMIT 500""",
            (signature, *targets, cursor_id),
        ).fetchall()
        if not batch:
            return
        for row in batch:
            entry = dict(row)
            cursor_id = entry["id"]
            if not entry.get("path"):
                from ..downloads.downloader import capture_path
                entry["path"] = str(capture_path(config.output_dir, entry["id"], entry["timestamp"], entry["original_url"]))
            yield entry


def scan_pending_saved(
    database: sqlite3.Connection,
    config,
    jobs: list[ScanJob],
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None = None,
    workers: int | None = None,
    write_lock: threading.Lock | None = None,
) -> int:
    """Scan after acquisition; unaffected by Wayback's request admission clock.

    Bounded futures and approximate in-flight bytes. Exceptionally large files
    are still accepted one at a time rather than truncated or discarded.
    """
    signature = cdx_query_signature(config)
    targets = list(config.targets)
    if not targets:
        return 0
    target_slots = ",".join("?" for _ in targets)
    count = int(database.execute(
        """SELECT COUNT(*) FROM captures c JOIN targets t ON t.id=c.target_id
           LEFT JOIN documents d ON d.capture_id=c.id
           WHERE (c.state='downloaded_unscanned' OR (c.state='downloaded' AND d.id IS NULL))
             AND c.query_signature=? AND t.pattern IN (""" + target_slots + ")",
        (signature, *targets),
    ).fetchone()[0])
    if not count:
        return 0
    worker_count = max(1, min(32, int(workers or min(8, os.cpu_count() or 4))))
    max_inflight = worker_count * 2
    estimated_budget = SCAN_BYTE_BUDGET
    rows = iter(_pending_rows(database, config))
    next_row = next(rows, None)
    completed = 0

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=worker_count, thread_name_prefix="archive-local-scan"
    ) as pool:
        futures: dict[concurrent.futures.Future, tuple[dict, int]] = {}
        in_flight_bytes = 0
        while futures or next_row is not None:
            if stop_event.is_set():
                for future in futures:
                    future.cancel()
                raise Stopped
            while next_row is not None and len(futures) < max_inflight:
                # File bytes are already local; archive CDX lengths can be
                # missing, compressed, or inaccurate. Use actual disk size and
                # reserve decoding/parsing overhead inside the memory budget.
                try:
                    length = Path(next_row["path"]).stat().st_size
                except OSError:
                    length = min(config.max_file_bytes, 8 * 1024 * 1024)
                estimate = max(1, length) * 3
                if futures and in_flight_bytes + estimate > estimated_budget:
                    break
                row = next_row
                # Oversized single bodies are processed without dropping evidence.
                futures[pool.submit(_scan_one, row, jobs)] = (row, estimate)
                in_flight_bytes += estimate
                next_row = next(rows, None)
            if not futures:
                continue
            done, _ = concurrent.futures.wait(
                futures, timeout=0.5, return_when=concurrent.futures.FIRST_COMPLETED,
            )
            if not done:
                continue
            # CPU workers have finished.  Keep their results until the WHOLE
            # SQLite batch has committed, so a busy/locked writer cannot lose
            # an outcome or strand an already-retained body.
            ready = [(future, *futures[future]) for future in done]
            busy_started = None
            busy_attempt = 0
            while True:
                if stop_event.is_set():
                    for future in futures:
                        future.cancel()
                    raise Stopped
                try:
                    # The acquisition owner and scanner coordinate their writes
                    # using a common mutex, while BEGIN IMMEDIATE claims the
                    # SQLite WAL writer slot BEFORE any per-document changes.
                    # It also avoids promoting a stale read snapshot mid-batch.
                    with (write_lock if write_lock is not None else nullcontext()), database:
                        if not database.in_transaction:
                            database.execute("BEGIN IMMEDIATE")
                        for future, row, estimate in ready:
                            database.execute("SAVEPOINT scout_capture_scan")
                            try:
                                result = future.result()
                                document_id = upsert_document(
                                    database, result["capture_id"], result["path"], result["title"],
                                    result["visible"], result["links"], result["content_hash"],
                                    result["normalized_hash"], result["bytes_saved"],
                                )
                                for run_id, analysis in result["analyses"].items():
                                    save_match(database, int(run_id), document_id, analysis)
                                database.execute(
                                    "UPDATE captures SET state='downloaded',updated_at=? WHERE id=?",
                                    (utc_now(), result["capture_id"]),
                                )
                                database.execute(
                                    "UPDATE capture_routing SET routing='downloaded_scanned',updated_at=? WHERE capture_id=?",
                                    (utc_now(), result["capture_id"]),
                                )
                                resolve_errors(database, capture_id=result["capture_id"], document_id=document_id)
                            except Exception as exc:
                                database.execute("ROLLBACK TO SAVEPOINT scout_capture_scan")
                                if _sqlite_write_busy(exc):
                                    # Retry after the outer context rolls back.
                                    # Error logging here would hit the same lock.
                                    raise
                                missing = isinstance(exc, (FileNotFoundError, PayloadIntegrityError, ArchivedErrorShell))
                                if missing:
                                    database.execute(
                                        "UPDATE captures SET state='error',updated_at=? WHERE id=?",
                                        (utc_now(), row["id"]),
                                    )
                                record_error(
                                    database, "scan", ("local_integrity_failure" if isinstance(exc, PayloadIntegrityError)
                                     else "invalid_archive_replay" if isinstance(exc, ArchivedErrorShell)
                                     else "missing_local_file" if missing else "scan_failure"),
                                    repr(exc), capture_id=row["id"],
                                    retryable=not isinstance(exc, ArchivedErrorShell)
                                        or str(exc) == "origin_unavailable",
                                )
                            finally:
                                database.execute("RELEASE SAVEPOINT scout_capture_scan")
                    break
                except sqlite3.OperationalError as exc:
                    if not _sqlite_write_busy(exc):
                        raise
                    busy_attempt += 1
                    if busy_started is None:
                        busy_started = time.monotonic()
                    if callback and (busy_attempt == 3 or busy_attempt % 20 == 0):
                        callback(ProgressEvent("scan", "SQLite writer busy; waiting to commit locally scanned bodies. "
                            "Downloaded files and completed scan work are preserved."))
                    # Interruptible cooldown: an active user Stop is honored,
                    # but a temporary database lock is not an acquisition failure.
                    delay = min(1.0, 0.1 * 2 ** min(4, busy_attempt - 1))
                    if stop_event.wait(delay):
                        raise Stopped
            for future, row, estimate in ready:
                futures.pop(future)
                in_flight_bytes -= estimate
            completed += len(ready)
            if callback and (completed % 100 == 0 or completed == count):
                callback(ProgressEvent(
                    "scan", f"Locally scanned {completed:,}/{count:,} saved bodies",
                    completed, count,
                ))
    return completed


def adopt_orphaned_bodies(database: sqlite3.Connection, config, stop_event: threading.Event,
                          callback=None, *, target: str | None = None) -> int:
    """Recover atomically saved bytes whose DB commit was interrupted.

    Only previous attempted captures are checked; a million untouched URLs
    must not produce a million filesystem stats on every Resume. Files are
    identity-bound to (capture ID, original URL), not guessed by basename.
    """
    from ..downloads.downloader import capture_path
    from ..classification import classify_payload_kind
    from ..content import classify_replay_content, decode_bytes
    from ..cdx.parameters import cdx_query_signature
    target = target or (config.targets[0] if config.targets else None)
    if not target:
        return 0
    sig = cdx_query_signature(config)
    cursor = 0
    adopted = 0
    while True:
        rows = database.execute(
            """SELECT c.id,c.timestamp,c.original_url,c.mimetype,
                       v.content_hash AS expected_hash,v.bytes_saved AS expected_size
                   FROM captures c
                   LEFT JOIN capture_payload_evidence v ON v.capture_id=c.id
               LEFT JOIN documents d ON d.capture_id=c.id
               JOIN targets t ON t.id=c.target_id
               WHERE c.id>? AND c.query_signature=? AND t.pattern=?
                 AND c.state IN ('pending','error') AND c.download_attempts>0
                 AND d.id IS NULL ORDER BY c.id LIMIT 500""",
            (cursor, sig, target),
        ).fetchall()
        if not rows:
            break
        with database:
            for row in rows:
                if stop_event.is_set():
                    raise Stopped
                cursor = int(row["id"])
                path = capture_path(config.output_dir, cursor, row["timestamp"], row["original_url"])
                try:
                    length = path.stat().st_size
                    if not (0 < length <= config.max_file_bytes):
                        continue
                    # Exact capture identity is encoded in the filename, but a
                    # previously committed SHA-256 is stronger and mandatory
                    # whenever available. Do not silently adopt altered bytes.
                    if row["expected_hash"]:
                        if length != int(row["expected_size"] or 0):
                            continue
                        digest = hashlib.sha256()
                        with path.open("rb") as check:
                            for segment in iter(lambda: check.read(1024*1024), b""):
                                digest.update(segment)
                        if digest.hexdigest() != row["expected_hash"]:
                            continue
                    with path.open("rb") as handle:
                        preview = handle.read(min(length, 65536))
                    decision = classify_payload_kind(preview, row["mimetype"] or "", row["original_url"])
                    if decision.resource_class != 'text':
                        continue
                    if classify_replay_content(decode_bytes(preview, row["mimetype"] or ""), ''):
                        continue
                    database.execute(
                        """UPDATE captures SET state='downloaded_unscanned',bytes_saved=?,updated_at=?
                           WHERE id=? AND state IN ('error','pending')""",
                        (length, utc_now(), cursor),
                    )
                    database.execute(
                        """UPDATE capture_routing SET routing='downloaded_awaiting_scan',
                           evidence='validated_local_retained_body',resource_class='text',
                           confident=1,updated_at=? WHERE capture_id=?""",
                        (utc_now(), cursor),
                    )
                    adopted += 1
                except (OSError, ValueError, UnicodeError):
                    continue
    if adopted and callback:
        callback(ProgressEvent('scan', f'Recovered {adopted:,} complete previously saved bodies without network access.'))
    return adopted
