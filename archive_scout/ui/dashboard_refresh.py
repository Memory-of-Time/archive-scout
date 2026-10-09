from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class DashboardRefreshController:
    """Pure scheduling state for exact dashboard recounts.

    Tk owns timers/threads; this object only decides whether an automatic exact
    recount is due and coalesces one outstanding request. It is deliberately
    clock-agnostic so tests never need real sleeps or a graphical display.
    """

    mode: str = "auto"
    interval_seconds: int = 10
    last_started: float | None = None
    inflight: bool = False
    generation: int = 0

    def configure(self, mode: str, interval_seconds: int) -> None:
        normalized = str(mode or "auto").casefold()
        if normalized not in {"auto", "manual"}:
            raise ValueError("dashboard refresh mode must be auto or manual")
        self.mode = normalized
        self.interval_seconds = min(3600, max(5, int(interval_seconds)))

    def switch_project(self) -> int:
        self.generation += 1
        self.last_started = None
        self.inflight = False
        return self.generation

    def automatic_due(self, now: float, *, visible: bool, operation_active: bool) -> bool:
        if self.mode != "auto" or not visible or operation_active or self.inflight:
            return False
        return self.last_started is None or now - self.last_started >= self.interval_seconds

    def begin(self, now: float, *, manual: bool = False) -> int | None:
        if self.inflight:
            return None
        if not manual and self.mode != "auto":
            return None
        self.inflight = True
        self.last_started = float(now)
        return self.generation

    def finish(self, generation: int) -> bool:
        if generation != self.generation:
            return False
        self.inflight = False
        return True
