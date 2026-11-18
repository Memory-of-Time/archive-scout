from __future__ import annotations

import json
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import nullcontext

from ..events import Stopped


class TargetQueue:
    """One database owner, a bounded number of ordinary requests, and a disk-backed delay queue."""

    def __init__(self, database, config, tasks, workers, stop, on_idle):
        self.db = database
        self.config = config
        self.tasks = iter(tasks)
        self.workers = workers
        self.stop = stop
        self.on_idle = on_idle
        self.last_idle_due = None
        self.cancel = threading.Event()
        self.pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="cdx-domain")
        self.pending = {}
        self.barrier = None
        self.source_open = True
        self.prefer_fresh = True
        self.queued = 0
        self.suspended = 0
        self.cooling = 0
        self.retrying = 0
        self.deferred_targets = 0
        self.db.execute("""CREATE TEMP TABLE IF NOT EXISTS normal_index_queue(
            id INTEGER PRIMARY KEY, due REAL, target TEXT, target_id INTEGER, kind TEXT,
            year INTEGER, start TEXT, end TEXT, signature TEXT)""")
        self.db.execute("CREATE INDEX IF NOT EXISTS temp.normal_index_due ON normal_index_queue(due,id)")
        self.db.execute("CREATE TEMP TABLE IF NOT EXISTS normal_index_turns(key TEXT PRIMARY KEY, state TEXT) WITHOUT ROWID")
        self.db.execute("CREATE TEMP TABLE IF NOT EXISTS normal_index_deferred(target TEXT PRIMARY KEY) WITHOUT ROWID")
        self.db.execute("CREATE TEMP TABLE IF NOT EXISTS normal_index_spent(target TEXT PRIMARY KEY, seconds REAL, expired INTEGER) WITHOUT ROWID")
        with self.db:
            for table in ("normal_index_queue", "normal_index_turns", "normal_index_deferred", "normal_index_spent"):
                self.db.execute(f"DELETE FROM {table}")

    def remember(self, key, completed, extra, budget, histories):
        if not (completed or extra or budget.attempts or histories):
            return
        cursors = [[start, end, [item.hex() for item in history.recent]]
                   for (start, end), history in histories.items() if history.recent]
        with self.db:
            self.db.execute("INSERT INTO normal_index_turns VALUES(?,?)",
                (key, json.dumps([completed, extra, budget.attempts, budget.elapsed, cursors])))
        self.suspended += 1
        self.retrying += int(bool(budget.attempts))

    def recall(self, key):
        if self.suspended:
            row = self.db.execute("SELECT state FROM normal_index_turns WHERE key=?", (key,)).fetchone()
            if row:
                with self.db:
                    self.db.execute("DELETE FROM normal_index_turns WHERE key=?", (key,))
                self.suspended -= 1
                saved = json.loads(row[0])
                self.retrying -= int(bool(saved[2]))
                return saved
        return 0, 0, 0, 0.0, []

    def defer_target(self, target):
        with self.db:
            added = self.db.execute("INSERT OR IGNORE INTO normal_index_deferred VALUES(?)", (target,)).rowcount
        self.deferred_targets += added

    def was_deferred(self, target):
        return self.deferred_targets and self.db.execute(
            "SELECT 1 FROM normal_index_deferred WHERE target=?", (target,)).fetchone()

    def __bool__(self):
        return bool(self.pending or self.barrier or self.queued or self.source_open)

    def append(self, task, due=0.0):
        target, _, target_id, kind, year, start, end, _, signature = task
        with self.db:
            self.db.execute("INSERT INTO normal_index_queue VALUES(NULL,?,?,?,?,?,?,?,?)",
                            (due, target, target_id, kind, year, start, end, signature))
        self.queued += 1
        self.cooling += int(due > 0)

    def _first_waiting(self):
        if self.queued:
            return self.db.execute("SELECT * FROM normal_index_queue ORDER BY due,id LIMIT 1").fetchone()
        return None

    def take(self):
        while self:
            stopping = self.stop.is_set()
            if stopping and not self.pending:
                raise Stopped
            if self.barrier is not None and not self.pending:
                task, self.barrier = self.barrier, None
                return task, None
            first = self._first_waiting()
            if not stopping and self.barrier is None and len(self.pending) < self.workers:
                if first and first["due"] <= time.monotonic() and (not self.source_open or not self.prefer_fresh):
                    self.prefer_fresh = True
                    with self.db:
                        self.db.execute("DELETE FROM normal_index_queue WHERE id=?", (first["id"],))
                    self.queued -= 1
                    self.cooling -= int(first["due"] > 0)
                    start, end, target = first["start"], first["end"], first["target"]
                    return (target, self.config.index_settings(target), first["target_id"], first["kind"],
                            first["year"], start, end, [(start, end)], first["signature"]), None
                if self.source_open:
                    task = next(self.tasks, None)
                    if task is not None:
                        self.prefer_fresh = False
                        return task, None
                    self.source_open = False
            if self.pending:
                done, _ = wait(self.pending, timeout=0.15, return_when=FIRST_COMPLETED)
                if done:
                    future = next(iter(done))
                    task, started = self.pending.pop(future)
                    return task, (future, started)
            elif first:
                remaining = first["due"] - time.monotonic()
                if remaining > 0 and first["due"] != self.last_idle_due:
                    self.last_idle_due = first["due"]
                    self.on_idle(remaining)
                if remaining > 0:
                    self.stop.wait(min(5.0, remaining))
        return None, None

    def submit(self, task, client, request, current):
        scoped, target = task[1], task[0]
        def fetch():
            context = getattr(client, "cancellation_scope", None)
            with context(self.cancel) if callable(context) else nullcontext():
                return request(client, scoped, target, current)
        started = time.monotonic()
        self.pending[self.pool.submit(fetch)] = (task, started)

    def close(self):
        self.cancel.set()
        self.pool.shutdown(wait=True, cancel_futures=True)
