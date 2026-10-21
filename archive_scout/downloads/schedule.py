"""Persistent, restart-safe acquisition retry eligibility. All writes use the owner DB connection."""
from __future__ import annotations

import sqlite3
import time
from ..utils import utc_now


def record_failure(database: sqlite3.Connection, capture_id: int, category: str,
                   status: int | None, retryable: bool, base_seconds: float,
                   max_seconds: float) -> None:
    if not retryable:
        database.execute("DELETE FROM text_retry_schedule WHERE capture_id=?", (capture_id,))
        return
    previous = database.execute(
        "SELECT failure_count FROM text_retry_schedule WHERE capture_id=?", (capture_id,)
    ).fetchone()
    count = min(1_000_000, (int(previous[0]) if previous else 0) + 1)
    # Deterministic capture-ID jitter disperses retries across restarts, without
    # a process-local RNG and without a large in-memory scheduling heap.
    backoff = min(max(1.0, max_seconds), max(0.1, base_seconds) * 2 ** min(12, count-1))
    delay = backoff * (0.90 + (capture_id % 17) / 85.0)
    database.execute("""INSERT INTO text_retry_schedule
        (capture_id,next_eligible_at,failure_count,last_category,last_status,updated_at)
        VALUES(?,?,?,?,?,?) ON CONFLICT(capture_id) DO UPDATE SET
        next_eligible_at=excluded.next_eligible_at,
        failure_count=excluded.failure_count,last_category=excluded.last_category,
        last_status=excluded.last_status,updated_at=excluded.updated_at""",
        (capture_id, time.time()+delay, count, category, status, utc_now()))


def clear_retry(database: sqlite3.Connection, capture_id: int) -> None:
    database.execute("DELETE FROM text_retry_schedule WHERE capture_id=?", (capture_id,))


def next_due(database: sqlite3.Connection, config=None) -> float | None:
    if config is not None:
        from .downloader import _populate_download_scopes
        _populate_download_scopes(database, config)
        scope = """ AND EXISTS(SELECT 1 FROM targets t
            JOIN archive_scout_download_scopes sc ON sc.pattern=t.pattern
            WHERE sc.signature=c.query_signature AND (t.id=c.target_id OR EXISTS
             (SELECT 1 FROM capture_target_membership m
              WHERE m.capture_id=c.id AND m.target_id=t.id)))"""
    else:
        scope = ""
    row = database.execute("""SELECT MIN(s.next_eligible_at) FROM text_retry_schedule s
        JOIN captures c ON c.id=s.capture_id WHERE c.state='error' AND EXISTS
        (SELECT 1 FROM errors e WHERE e.capture_id=c.id AND e.retryable=1
         AND e.resolved=0 AND e.ignored=0)""" + scope).fetchone()
    return float(row[0]) if row and row[0] is not None else None
