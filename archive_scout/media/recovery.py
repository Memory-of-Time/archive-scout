"""Restart-safe, active-scope media recovery without a sleeping worker per URL."""
from __future__ import annotations

import sqlite3
import threading
import time
from typing import Callable

from ..config import ProjectConfig
from ..events import ProgressEvent, Stopped
from ..utils import utc_now
from .indexer import media_query_signature


def record_media_failure(db: sqlite3.Connection, capture_id: int, category: str,
                         status: int | None, retryable: bool, config: ProjectConfig) -> None:
    if not retryable:
        db.execute("DELETE FROM media_retry_schedule WHERE media_capture_id=?", (capture_id,))
        return
    previous = db.execute("SELECT failure_count FROM media_retry_schedule WHERE media_capture_id=?", (capture_id,)).fetchone()
    failures = min(1000000, int(previous[0]) + 1 if previous else 1)
    network = config.network.normalized()
    delay = min(network.retry_max_seconds, network.retry_base_seconds * (2 ** min(12, failures - 1)))
    delay = max(0.1, delay) * (0.9 + (capture_id % 17) / 85.0)
    db.execute("""INSERT INTO media_retry_schedule(media_capture_id,next_eligible_at,failure_count,last_category,last_status,updated_at)
                  VALUES(?,?,?,?,?,?) ON CONFLICT(media_capture_id) DO UPDATE SET
                  next_eligible_at=excluded.next_eligible_at,failure_count=excluded.failure_count,
                  last_category=excluded.last_category,last_status=excluded.last_status,updated_at=excluded.updated_at""",
               (capture_id, time.time() + delay, failures, category, status, utc_now()))


def clear_media_retry(db: sqlite3.Connection, capture_id: int) -> None:
    db.execute("DELETE FROM media_retry_schedule WHERE media_capture_id=?", (capture_id,))


def recover_active_media(config: ProjectConfig, db: sqlite3.Connection, stop: threading.Event,
                         callback: Callable[[ProgressEvent], None] | None = None,
                         additional_signature: str | None = None) -> None:
    """Only run for a currently indexed and admitted media scope.

    A single bounded ID batch is passed to the existing downloader. The DB owns
    due-time state; no per-failure thread, in-memory heap or busy retry loop.
    """
    from .downloader import download_media
    from ..coverage import require_media_coverage
    if additional_signature is None:
        require_media_coverage(db, config)
    signatures = (additional_signature,) if additional_signature else (media_query_signature(config),)
    placeholders = ','.join('?' for _ in signatures)
    # Legacy retryable active errors are adopted once, preserving their old
    # evidence. Historical signatures are never swept into the current run.
    def adopt_legacy_batch() -> bool:
        outstanding = db.execute(
        "SELECT mc.id, e.category, e.http_status FROM media_captures mc JOIN errors e ON e.media_capture_id=mc.id "
        "WHERE mc.query_signature IN (" + placeholders + ") AND mc.state='error' "
        "AND e.retryable=1 AND e.resolved=0 AND e.ignored=0 "
        "AND NOT EXISTS(SELECT 1 FROM media_retry_schedule s WHERE s.media_capture_id=mc.id) "
        "GROUP BY mc.id LIMIT 256", signatures).fetchall()
        if outstanding:
            with db:
                for cid, category, status in outstanding:
                    record_media_failure(db, int(cid), str(category), status, True, config)
        return bool(outstanding)

    while not stop.is_set():
        # Bounded adoption is repeated; a large legacy error history must not
        # leave everything beyond the first selected page undiscoverable.
        adopted = adopt_legacy_batch()
        now = time.time()
        args = tuple(signatures)
        due = db.execute(
            "SELECT s.media_capture_id FROM media_retry_schedule s JOIN media_captures mc ON mc.id=s.media_capture_id "
            "WHERE mc.query_signature IN (" + placeholders + ") AND mc.state='error' AND s.next_eligible_at<=? "
            "AND EXISTS(SELECT 1 FROM errors e WHERE e.media_capture_id=mc.id "
            "AND e.retryable=1 AND e.resolved=0 AND e.ignored=0) "
            "ORDER BY s.next_eligible_at,s.media_capture_id LIMIT 128", (*args, now)).fetchall()
        if due:
            ids = [int(row[0]) for row in due]
            if callback:
                callback(ProgressEvent('media_retry', f'Automatically recovering {len(ids):,} due media capture(s)'))
            download_media(config, db, stop, callback, states=('error',), media_capture_ids=ids,
                           media_signature=additional_signature)
            continue
        earliest = db.execute(
            "SELECT MIN(s.next_eligible_at) FROM media_retry_schedule s JOIN media_captures mc ON mc.id=s.media_capture_id "
            "WHERE mc.query_signature IN (" + placeholders + ") AND mc.state='error' "
            "AND EXISTS(SELECT 1 FROM errors e WHERE e.media_capture_id=mc.id "
            "AND e.retryable=1 AND e.resolved=0 AND e.ignored=0)", args).fetchone()
        if earliest is None or earliest[0] is None:
            return
        if adopted:
            continue
        wait = min(10.0, max(0.05, float(earliest[0]) - time.time()))
        if callback:
            callback(ProgressEvent('media_retry', f'Media temporarily unavailable; retry eligible in {wait:.1f}s'))
        stop.wait(wait)
    raise Stopped
