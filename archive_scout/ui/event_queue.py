from __future__ import annotations

import queue
import threading
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
        kind, payload = self._queue.get_nowait()
        if kind != "progress":
            return kind, payload
        with self._lock:
            payload = self._latest_progress
            self._latest_progress = None
            self._progress_pending = False
        return kind, payload

    def qsize(self) -> int:
        return self._queue.qsize()
