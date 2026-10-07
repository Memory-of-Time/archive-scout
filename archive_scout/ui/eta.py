"""Optional, bounded remaining-time estimates from existing progress events."""
from __future__ import annotations

import math
import time
from collections import deque

from ..events import ProgressEvent


_TELEMETRY = {"network", "retry", "site_issue", "warning", "log"}
_WAITING = {"network_waiting", "rate_limit_waiting", "rate_limit", "network_pause"}


def format_remaining(seconds: float) -> str:
    seconds = max(0, math.ceil(seconds))
    if seconds < 60:
        return f"{seconds}s"
    minutes = math.ceil(seconds / 60)
    if minutes < 60:
        return f"{minutes}m"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h {minutes}m"
    days, hours = divmod(hours, 24)
    return f"{days}d {hours}h"


class OperationEtaTracker:
    """A recent wall-clock rate, with known recovery waits counted once.

    Estimates are for the current phase. Future phases and growing/unknown work
    plans are never presented as a precise whole-run deadline. No SQL, files,
    worker threads, per-item persistence, or fixed 8/s assumption is involved.
    """

    def __init__(self, enabled: bool = False):
        self.enabled = enabled
        self.samples: deque[tuple[float, int]] = deque(maxlen=64)
        self.reset()

    def reset(self, status: str = "Ready") -> None:
        self.samples.clear()
        self.stage = ""
        self.current: int | None = None
        self.total: int | None = None
        self.status = status
        self.run_id: int | None = None
        self.last_time: float | None = None
        self.active_time = 0.0
        self.wait_until: float | None = None
        self.waiting = False
        self.last_progress: float | None = None

    def set_enabled(self, enabled: bool) -> None:
        if bool(enabled) != self.enabled:
            self.enabled = bool(enabled)
            self.reset("Estimating" if enabled else "Ready")

    def finish(self, status: str = "Complete") -> None:
        self.status = status
        self.stage = ""
        self.waiting = False
        self.samples.clear()

    def _elapsed(self, now: float) -> float:
        if self.last_time is None:
            return self.active_time
        start = self.last_time
        if self.waiting:
            if self.wait_until is None:
                return self.active_time
            start = max(start, self.wait_until)
        return self.active_time + max(0.0, now - start)

    def observe(self, event: ProgressEvent, *, now: float | None = None,
                epoch: float | None = None) -> None:
        if not self.enabled:
            return
        now = time.monotonic() if now is None else now
        epoch = time.time() if epoch is None else epoch
        detail = event.detail or {}
        run_id = detail.get("operation_run_id")
        if isinstance(run_id, int):
            if self.run_id is not None and run_id < self.run_id:
                return
            if self.run_id is not None and run_id != self.run_id:
                self.reset("Estimating")
            self.run_id = run_id
        if event.stage in _WAITING:
            self.active_time = self._elapsed(now)
            self.last_time = now
            deadline = detail.get("eligible_at_epoch")
            wait = detail.get("waiting_seconds")
            if isinstance(deadline, (int, float)) and math.isfinite(deadline):
                self.wait_until = now + max(0.0, deadline - epoch)
            elif isinstance(wait, (int, float)) and math.isfinite(wait):
                self.wait_until = now + max(0.0, wait)
            else:
                self.wait_until = None
            self.waiting = True
            return
        if event.stage in _TELEMETRY:
            return
        if event.current is None or event.total is None or event.total < 0:
            if event.stage != self.stage:
                self.samples.clear()
                self.current = self.total = None
                self.active_time = 0.0
            self.stage = event.stage
            self.status = "Estimating"
            self.last_time = now
            self.waiting = False
            self.wait_until = None
            return
        current = max(0, int(event.current))
        total = max(0, int(event.total))
        if event.stage != self.stage or (self.current is not None and current < self.current):
            self.samples.clear()
            self.active_time = 0.0
            self.last_time = now
            self.last_progress = now
        else:
            self.active_time = self._elapsed(now)
        self.waiting = False
        self.wait_until = None
        if self.current is None or current != self.current:
            self.last_progress = now
        self.stage = event.stage
        self.status = "Estimating"
        self.current, self.total = current, total
        self.last_time = now
        # Repeated counters never create completed work or unbounded history.
        if not self.samples or self.samples[-1][1] != current:
            self.samples.append((self.active_time, current))
        while len(self.samples) > 2 and self.active_time - self.samples[1][0] > 60.0:
            self.samples.popleft()

    def seconds_remaining(self, now: float | None = None) -> float | None:
        now = time.monotonic() if now is None else now
        if not self.enabled or self.current is None or self.total is None:
            return None
        if self.current >= self.total:
            return 0.0
        if len(self.samples) < 2:
            return None
        elapsed = self._elapsed(now) - self.samples[0][0]
        completed = self.current - self.samples[0][1]
        if elapsed <= 0 or completed <= 0:
            return None
        remaining = (self.total - self.current) * elapsed / completed
        if self.waiting:
            if self.wait_until is None:
                return None
            remaining += max(0.0, self.wait_until - now)
        return remaining

    def label(self, now: float | None = None) -> str:
        if not self.enabled:
            return "Estimated time remaining: off"
        now = time.monotonic() if now is None else now
        phase = self.stage.replace("_", " ").strip().capitalize()
        if not phase:
            return f"Estimated time remaining: {self.status}"
        remaining = self.seconds_remaining(now)
        if self.waiting:
            if self.wait_until is None:
                return f"{phase}: waiting for recovery; remaining time unknown"
            wait = format_remaining(max(0.0, self.wait_until - now))
            estimate = f"~{format_remaining(remaining)}" if remaining is not None else "Estimating"
            return f"{phase}: {estimate} remaining (current phase); retry check in {wait}"
        if remaining == 0:
            return f"{phase}: phase complete"
        if self.last_progress is not None and now - self.last_progress > 15.0:
            return f"{phase}: Estimating — no recent completed work"
        if remaining is None:
            return f"{phase}: Estimating — waiting for a known total and measured progress"
        return f"{phase}: ~{format_remaining(remaining)} remaining (current phase)"
