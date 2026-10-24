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
    if not row.get('expected_hash'):
        from ..downloads.checkpoint import prove_file
        if not prove_file(path, row):
            raise PayloadIntegrityError(f"local retained body has no complete-transfer proof: capture {row['id']}")
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


def _reconcile_legacy_pending(database: sqlite3.Connection, config) -> None:
    """Once per connection, migrate legacy saved-without-document states into the
    indexed explicit pending queue. Never sweep completed history at every wake.

    The one-time scan is safe for existing project databases and new target
    signatures: any future save without a document must use
    ``downloaded_unscanned`` at the time of the acquisition commit.
    """
    database.execute("CREATE TEMP TABLE IF NOT EXISTS scout_scan_reconciled(signature TEXT,pattern TEXT,PRIMARY KEY(signature,pattern)) WITHOUT ROWID")
    for target in config.targets:
        signature = cdx_query_signature(config.for_target(target))
        done = database.execute("SELECT 1 FROM scout_scan_reconciled WHERE signature=? AND pattern=?", (signature,target)).fetchone()
        if done:
            continue
        # Update only the legacy gap, without materializing IDs in Python.
        # The indexed hot queue is the *only* discovery path after this.
        with database:
            database.execute("""UPDATE captures SET state='downloaded_unscanned',updated_at=?
                WHERE state='downloaded' AND query_signature=?
                AND EXISTS (SELECT 1 FROM targets t WHERE t.pattern=? AND
                    (t.id=captures.target_id OR EXISTS (SELECT 1 FROM capture_target_membership m
                        WHERE m.capture_id=captures.id AND m.target_id=t.id)))
                AND NOT EXISTS (SELECT 1 FROM documents d WHERE d.capture_id=captures.id)""",
                (utc_now(),signature,target))
            database.execute("INSERT OR IGNORE INTO scout_scan_reconciled(signature,pattern) VALUES(?,?)", (signature,target))


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
            """SELECT c.id,c.timestamp,c.original_url,c.mimetype,c.length,c.digest,d.path,
                 COALESCE(v.content_hash,d.content_hash) AS expected_hash,
                 COALESCE(v.bytes_saved,d.size_bytes) AS expected_size,
                 v.response_content_type AS response_content_type,v.final_url AS final_url
               FROM captures c
               LEFT JOIN documents d ON d.capture_id=c.id
               LEFT JOIN capture_payload_evidence v ON v.capture_id=c.id
               WHERE c.state='downloaded_unscanned'
               AND c.query_signature=? AND EXISTS (SELECT 1 FROM targets t
                   WHERE t.pattern IN (""" + target_slots + """) AND
                   (t.id=c.target_id OR EXISTS (SELECT 1 FROM capture_target_membership m
                       WHERE m.capture_id=c.id AND m.target_id=t.id)))
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
    # The expensive historical gap reconciliation is strictly once per
    # connection/scope, not on every background scanner wake.
    while True:
        try:
            with (write_lock if write_lock is not None else nullcontext()):
                _reconcile_legacy_pending(database, config)
            break
        except sqlite3.OperationalError as exc:
            if not _sqlite_write_busy(exc):
                raise
            database.rollback()  # Retry a fresh write transaction, not BUSY_SNAPSHOT.
            if stop_event.wait(0.15):
                raise Stopped
    # Indexed existence test replaces an unconditional project-wide COUNT.
    has_pending = database.execute(
        """SELECT 1 FROM captures c
           WHERE c.state='downloaded_unscanned'
             AND c.query_signature=? AND EXISTS (SELECT 1 FROM targets t
                 WHERE t.pattern IN (""" + target_slots + """) AND
                 (t.id=c.target_id OR EXISTS (SELECT 1 FROM capture_target_membership m
                   WHERE m.capture_id=c.id AND m.target_id=t.id))) LIMIT 1""",
        (signature, *targets),
    ).fetchone()
    if not has_pending:
        return 0
    count = None  # No count on the acquisition hot path; cursor is bounded.
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
                scanned_paths = []
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
                                # Earlier failures can precede document INSERT.
                                # Requiring its new document ID would strand a
                                # NULL-document error after successful recovery.
                                resolve_errors(database, capture_id=result["capture_id"], operations=("scan","download"))
                                from ..downloads.schedule import clear_retry
                                clear_retry(database, result['capture_id'])
                                scanned_paths.append(Path(result['path']))
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
            from ..downloads.checkpoint import discard_receipt
            for path in scanned_paths:
                discard_receipt(path)
            for future, row, estimate in ready:
                futures.pop(future)
                in_flight_bytes -= estimate
            completed += len(ready)
            if callback and completed % 100 == 0:
                callback(ProgressEvent(
                    "scan", f"Locally scanned {completed:,} saved bodies in this pass",
                    completed, None,
                ))
    return completed


def _record_recovery_gap(database, row, category: str, message: str) -> None:
    capture_id = int(row['id'])
    database.execute("UPDATE captures SET state='error',updated_at=? WHERE id=?", (utc_now(),capture_id))
    record_error(database, 'scan' if row['state']=='downloaded_unscanned' else 'download',
                 category, message, capture_id=capture_id, retryable=True)


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
    from ..downloads.checkpoint import prove_file, discard_receipt, preserve_unverified, sync_directory
    from ..downloads.downloader import _save_payload_evidence
    from ..downloads.schedule import clear_retry
    target = target or (config.targets[0] if config.targets else None)
    if not target:
        return 0
    sig = cdx_query_signature(config)
    cursor = 0
    adopted = 0
    while True:
        rows = database.execute(
            """SELECT c.id,c.timestamp,c.original_url,c.mimetype,c.digest,c.state,
                       v.content_hash AS expected_hash,v.bytes_saved AS expected_size,
                       v.response_content_type,v.final_url,v.response_status
                   FROM captures c
                   LEFT JOIN capture_payload_evidence v ON v.capture_id=c.id
               LEFT JOIN documents d ON d.capture_id=c.id
               WHERE c.id>? AND c.query_signature=? AND EXISTS(SELECT 1 FROM targets t
                 WHERE t.pattern=? AND (t.id=c.target_id OR EXISTS(
                   SELECT 1 FROM capture_target_membership m WHERE m.capture_id=c.id AND m.target_id=t.id)))
                 AND ((c.state IN ('pending','error') AND c.download_attempts>0)
                   OR (c.state='downloaded_unscanned' AND v.capture_id IS NULL))
                 AND d.id IS NULL ORDER BY c.id LIMIT 500""",
            (cursor, sig, target),
        ).fetchall()
        if not rows:
            break
        committed_paths = []
        with database:
            for row in rows:
                if stop_event.is_set():
                    raise Stopped
                cursor = int(row["id"])
                path = capture_path(config.output_dir, cursor, row["timestamp"], row["original_url"])
                try:
                    stage = path.with_name(path.name + '.transfer')
                    if stage.is_file() and 0 < stage.stat().st_size <= config.max_file_bytes:
                        staged_proof = prove_file(stage, row, expected_hash=row['expected_hash'],
                            expected_size=row['expected_size'], receipt_for=path)
                        if staged_proof:
                            os.replace(stage, path)
                            sync_directory(path.parent)
                    length = path.stat().st_size
                    if not (0 < length <= config.max_file_bytes):
                        if row['state'] == 'downloaded_unscanned':
                            raise FileNotFoundError('retained body has invalid size')
                        continue
                    proof = prove_file(path, row, expected_hash=row['expected_hash'],
                                       expected_size=row['expected_size'])
                    if not proof:
                        preserve_unverified(path)
                        _record_recovery_gap(database,row,'local_integrity_unproven',
                            'Saved file has no matching complete-transfer proof; preserved for inspection and queued for reacquisition.')
                        continue
                    with path.open("rb") as handle:
                        preview = handle.read(min(length, 65536))
                    decision = classify_payload_kind(preview, row["mimetype"] or "", row["original_url"])
                    if decision.resource_class != 'text':
                        if row['state']=='downloaded_unscanned':
                            _record_recovery_gap(database,row,'local_integrity_unproven',
                                'Unproven retained body is not text; acquisition must revalidate routing.')
                        continue
                    try:
                        preview_text = decode_bytes(preview, proof.get('response_content_type') or row['mimetype'] or '')
                    except UnicodeDecodeError:
                        # A prefix can split a UTF-8 character or UTF-16 pair.
                        # Full-byte proof is already checked; local scanning
                        # validates the complete representation next.
                        preview_text = ''
                    if classify_replay_content(preview_text, proof.get('final_url') or ''):
                        if row['state']=='downloaded_unscanned':
                            _record_recovery_gap(database,row,'invalid_archive_replay',
                                'Recovered file is an archive error shell, not a completed original.')
                        continue
                    _save_payload_evidence(database, {**proof, 'capture_id':cursor,
                        'response_content_type':proof.get('response_content_type') or row['response_content_type'] or row['mimetype'],
                        'final_url':proof.get('final_url') or row['final_url'] or '',
                        'http_status':proof.get('http_status') or row['response_status'] or 200})
                    database.execute(
                        """UPDATE captures SET state='downloaded_unscanned',bytes_saved=?,updated_at=?
                           WHERE id=? AND state IN ('error','pending','downloaded_unscanned')""",
                        (length, utc_now(), cursor),
                    )
                    database.execute(
                        """UPDATE capture_routing SET routing='downloaded_awaiting_scan',
                           evidence='validated_local_retained_body',resource_class='text',
                           confident=1,updated_at=? WHERE capture_id=?""",
                        (utc_now(), cursor),
                    )
                    adopted += 1
                    clear_retry(database, cursor)
                    resolve_errors(database, capture_id=cursor, operations=('download',))
                    committed_paths.append(path)
                except FileNotFoundError:
                    if row['state'] == 'downloaded_unscanned':
                        _record_recovery_gap(database,row,'missing_local_file',
                            'Unproven retained body is missing; queued for reacquisition.')
                    continue
                except OSError as exc:
                    from ..downloads.downloader import _storage_full, StorageFullError
                    if _storage_full(exc):
                        raise StorageFullError('storage full during retained-body recovery') from exc
                    if row['state']=='downloaded_unscanned':
                        _record_recovery_gap(database,row,'local_integrity_unproven',repr(exc))
                    continue
                except (ValueError, UnicodeError):
                    if row['state']=='downloaded_unscanned':
                        _record_recovery_gap(database,row,'local_integrity_unproven',
                            'Retained body validation failed; reacquisition required.')
                    continue
        # Receipts are removed only AFTER the evidence/state transaction commits.
        for path in committed_paths:
            discard_receipt(path)
    if adopted and callback:
        callback(ProgressEvent('scan', f'Recovered {adopted:,} complete previously saved bodies without network access.'))
    return adopted
