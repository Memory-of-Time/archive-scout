from __future__ import annotations

import queue
import threading
import time
from typing import Generic, TypeVar

T = TypeVar("T")


class CoalescingEventQueue(Generic[T]):
    """Bound UI event memory while preserving the newest progress update."""

    def __init__(self, max_events: int = 256) -> None:
        if max_events < 2:
            raise ValueError("max_events must be at least 2")
        self._queue: queue.Queue[tuple[str, object]] = queue.Queue(maxsize=max_events)
        self._lock = threading.Lock()
        self._latest_progress: T | None = None
        self._progress_pending = False
        self._acquisition: T | None = None
        self._acquisition_interval = 0.5
        self._last_acquisition = float('-inf')
        self._deferred_control: tuple[str, object] | None = None
        self._dropped_notices = 0

    def set_acquisition_interval(self, seconds: float) -> None:
        if seconds not in (0.0, 0.5, 1.0):
            raise ValueError('acquisition display interval must be every update, 0.5, or 1 second')
        with self._lock:
            self._acquisition_interval = seconds

    def reset_acquisition(self) -> None:
        with self._lock:
            self._acquisition = None
            self._last_acquisition = float('-inf')

    def put_acquisition(self, payload: T) -> None:
        with self._lock:
            self._acquisition = payload

    def put_notice(self, payload: T) -> None:
        # Notices cannot be overwritten by routine progress. The producer never
        # blocks on a full UI queue; durable per-capture errors live in SQLite.
        try:
            self._queue.put_nowait(('notice', payload))
        except queue.Full:
            with self._lock:
                self._dropped_notices += 1

    def put_progress(self, payload: T) -> None:
        with self._lock:
            self._latest_progress = payload
            if self._progress_pending:
                return
            try:
                self._queue.put_nowait(("progress", None))
            except queue.Full:
                self._latest_progress = None
                return
            self._progress_pending = True

    def put(self, item: tuple[str, object]) -> None:
        kind, payload = item
        if kind == "progress":
            self.put_progress(payload)  # type: ignore[arg-type]
            return
        self._queue.put(item)

    def get_nowait(self) -> tuple[str, object]:
        with self._lock:
            item, self._deferred_control = self._deferred_control, None
        if item is None:
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                with self._lock:
                    if self._dropped_notices:
                        count, self._dropped_notices = self._dropped_notices, 0
                        return 'notices_dropped', count
                    if self._acquisition is not None and time.monotonic()-self._last_acquisition >= self._acquisition_interval:
                        payload, self._acquisition = self._acquisition, None
                        self._last_acquisition = time.monotonic()
                        return 'progress', payload
                raise
        kind, payload = item
        if kind in {'complete','stopped','error','deferred','incomplete_result','incomplete'}:
            with self._lock:
                if self._acquisition is not None:
                    latest, self._acquisition = self._acquisition, None
                    self._last_acquisition = time.monotonic()
                    self._deferred_control = item
                    return 'progress', latest
        if kind != "progress":
            return kind, payload
        with self._lock:
            payload = self._latest_progress
            self._latest_progress = None
            self._progress_pending = False
        return kind, payload

    def qsize(self) -> int:
        return self._queue.qsize()
