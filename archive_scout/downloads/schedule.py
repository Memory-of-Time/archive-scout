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
    if _retry_queue_exists(database):
        database.execute('''INSERT OR REPLACE INTO archive_scout_ready_retries
            SELECT capture_id,next_eligible_at FROM text_retry_schedule WHERE capture_id=?''', (capture_id,))


def clear_retry(database: sqlite3.Connection, capture_id: int) -> None:
    database.execute("DELETE FROM text_retry_schedule WHERE capture_id=?", (capture_id,))
    try:
        database.execute('DELETE FROM archive_scout_ready_retries WHERE capture_id=?', (capture_id,))
    except sqlite3.OperationalError as exc:
        if 'no such table' not in str(exc):
            raise


def _retry_queue_exists(database):
    return database.execute("SELECT 1 FROM sqlite_temp_master WHERE name='archive_scout_ready_retries'").fetchone() is not None


_ACTIVE_RETRY = '''EXISTS (SELECT 1 FROM errors e WHERE e.capture_id=c.id
    AND e.operation='download' AND e.retryable=1 AND e.resolved=0 AND e.ignored=0)
    AND EXISTS (SELECT 1 FROM targets t JOIN archive_scout_download_scopes sc
      ON sc.pattern=t.pattern AND sc.signature=c.query_signature AND c.timestamp BETWEEN sc.start AND sc.end
      WHERE t.id=c.target_id OR EXISTS (SELECT 1 FROM capture_target_membership m
        WHERE m.capture_id=c.id AND m.target_id=t.id))'''


def prepare_retry_queue(database):
    """Materialize active, nonqueued retry identities once per acquisition batch.

    Persistent schedules are authoritative and never deleted to filter a scope.
    Owner-side failure commits update this disk-backed TEMP queue incrementally.
    """
    database.execute('''CREATE TEMP TABLE IF NOT EXISTS archive_scout_ready_retries(
        capture_id INTEGER PRIMARY KEY,next_eligible_at REAL NOT NULL) WITHOUT ROWID''')
    database.execute('''CREATE INDEX IF NOT EXISTS archive_scout_ready_retries_due
        ON archive_scout_ready_retries(next_eligible_at,capture_id)''')
    database.execute('DELETE FROM archive_scout_ready_retries')
    database.execute('''INSERT INTO archive_scout_ready_retries
        SELECT sched.capture_id,sched.next_eligible_at FROM text_retry_schedule sched
        CROSS JOIN captures c ON c.id=sched.capture_id
        WHERE c.state='error' AND NOT EXISTS
          (SELECT 1 FROM archive_scout_download_queue q WHERE q.id=c.id) AND ''' + _ACTIVE_RETRY)


def select_due_retry(database, now):
    if not _retry_queue_exists(database):
        prepare_retry_queue(database)
    while True:
        candidate = database.execute('''SELECT capture_id FROM archive_scout_ready_retries
            WHERE next_eligible_at<=? ORDER BY next_eligible_at,capture_id LIMIT 1''', (now,)).fetchone()
        if candidate is None:
            return None
        capture_id = int(candidate[0])
        # Revalidate one identity: another WAL connection may have scanned it.
        row = database.execute('''SELECT c.* FROM captures c
            JOIN text_retry_schedule s ON s.capture_id=c.id
            WHERE c.id=? AND c.state='error' AND s.next_eligible_at<=?
              AND NOT EXISTS(SELECT 1 FROM archive_scout_download_queue q WHERE q.id=c.id)
              AND ''' + _ACTIVE_RETRY, (capture_id, now)).fetchone()
        database.execute('DELETE FROM archive_scout_ready_retries WHERE capture_id=?', (capture_id,))
        if row is not None:
            return row
        # Keep a deadline changed by another connection; remove stale successes.
        database.execute('''INSERT OR IGNORE INTO archive_scout_ready_retries
            SELECT capture_id,next_eligible_at FROM text_retry_schedule
            WHERE capture_id=? AND next_eligible_at>?''', (capture_id, now))


def explicit_error_predicate(config, alias='e'):
    """Only a deliberate ID/category selection may recheck a recorded 404."""
    if config.retry_capture_ids or 'missing_capture' in config.retry_error_categories:
        return f"({alias}.retryable=1 OR {alias}.category='missing_capture')"
    return f'{alias}.retryable=1'


def retry_selection(database: sqlite3.Connection, config, alias: str = "c") -> tuple[str, list]:
    """Bounded SQL selection shared by explicit retry and its recovery turns."""
    clauses, params = [], []
    if config.retry_capture_ids:
        database.execute("CREATE TEMP TABLE IF NOT EXISTS archive_scout_retry_filter(id INTEGER PRIMARY KEY) WITHOUT ROWID")
        database.execute("DELETE FROM archive_scout_retry_filter")
        database.executemany("INSERT OR IGNORE INTO archive_scout_retry_filter VALUES(?)",
                             ((int(value),) for value in config.retry_capture_ids))
        clauses.append(f"EXISTS(SELECT 1 FROM archive_scout_retry_filter f WHERE f.id={alias}.id)")
    if config.retry_error_categories:
        clauses.append(f"""EXISTS(SELECT 1 FROM errors e WHERE e.capture_id={alias}.id
            AND e.resolved=0 AND e.ignored=0 AND """ + explicit_error_predicate(config) + """ AND e.category IN ("""
            + ",".join("?" for _ in config.retry_error_categories) + "))")
        params.extend(config.retry_error_categories)
    return "".join(" AND " + item for item in clauses), params


def next_due(database: sqlite3.Connection, config=None, *, respect_selection=False) -> float | None:
    if config is not None:
        from .downloader import _populate_download_scopes
        _populate_download_scopes(database, config)
        scope = """ AND EXISTS(SELECT 1 FROM targets t
            JOIN archive_scout_download_scopes sc ON sc.pattern=t.pattern
            WHERE sc.signature=c.query_signature AND c.timestamp BETWEEN sc.start AND sc.end AND (t.id=c.target_id OR EXISTS
             (SELECT 1 FROM capture_target_membership m
              WHERE m.capture_id=c.id AND m.target_id=t.id)))"""
    else:
        scope = ""
    selection, params = retry_selection(database, config) if respect_selection and config else ("", [])
    row = database.execute("""SELECT MIN(s.next_eligible_at) FROM text_retry_schedule s
        JOIN captures c ON c.id=s.capture_id WHERE c.state='error' AND EXISTS
        (SELECT 1 FROM errors e WHERE e.capture_id=c.id AND e.retryable=1
         AND e.resolved=0 AND e.ignored=0)""" + scope + selection, params).fetchone()
    return float(row[0]) if row and row[0] is not None else None
