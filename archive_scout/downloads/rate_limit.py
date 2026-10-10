from __future__ import annotations

import contextlib
import random  # retained for historical mock compatibility; no adaptive jitter applied
import threading
import time
from dataclasses import dataclass


_shared_rate_lock = threading.Lock()
_shared_rate_states: dict[str, tuple[threading.Condition, list[float]]] = {}
_shared_host_gates: dict[str, "SharedHostGate"] = {}

from ..events import Stopped


class FixedRateLimiter:
    """A fixed, user-controlled minimum delay between request starts."""

    def __init__(self, delay: float) -> None:
        self.delay = max(0.0, float(delay))
        self.condition = threading.Condition()
        self.next_request = 0.0

    @contextlib.contextmanager
    def slot(self, stop_event: threading.Event):
        while True:
            with self.condition:
                if stop_event.is_set():
                    raise Stopped
                now = time.monotonic()
                wait = max(0.0, self.next_request - now)
                if wait <= 0:
                    self.next_request = now + self.delay
                    break
                self.condition.wait(timeout=min(max(wait, 0.05), 0.5))
        yield

    def wait(self, stop_event: threading.Event) -> None:
        with self.slot(stop_event):
            return


class SharedFixedRateLimiter(FixedRateLimiter):
    """Process-wide request-start spacing for one remote host.

    Each client keeps its own configured delay, but all clients targeting the
    same host advance a single shared next-request clock. This prevents several
    simultaneous projects from multiplying traffic from one machine/VPS IP.
    """

    def __init__(self, delay: float, key: str = "web.archive.org") -> None:
        self.delay = max(0.0, float(delay))
        self.key = str(key or "web.archive.org").casefold()
        with _shared_rate_lock:
            state = _shared_rate_states.get(self.key)
            if state is None:
                state = (threading.Condition(), [0.0])
                _shared_rate_states[self.key] = state
        self.condition, self._next_request_box = state

    @property
    def next_request(self) -> float:
        return self._next_request_box[0]

    @next_request.setter
    def next_request(self, value: float) -> None:
        self._next_request_box[0] = float(value)



@dataclass(frozen=True, slots=True)
class HostPermit:
    generation: int
    probe: bool = False


class SharedHostGate:
    """Coordinates HTTP 429 pauses without rewriting the user's speed settings.

    A 429 response closes a host-wide circuit. Every worker waits for the same
    Retry-After/backoff period. When that period ends, exactly one request is
    allowed through as a recovery probe; the normal queue reopens only after
    that probe receives a non-rate-limited response. This prevents both a
    thundering herd and a cascade of per-URL 429 errors.
    """

    def __init__(
        self,
        base_pause: float = 5.0,
        max_pause: float = 5.0,
        coalesce_seconds: float = 2.0,
        decay_seconds: float = 300.0,
    ) -> None:
        self.base_pause = max(1.0, float(base_pause))
        self.max_pause = max(self.base_pause, float(max_pause))
        self.coalesce_seconds = max(0.1, float(coalesce_seconds))
        self.decay_seconds = max(self.coalesce_seconds, float(decay_seconds))
        self.condition = threading.Condition()
        self.blocked_until = 0.0
        self.last_signal = 0.0
        self.incidents = 0
        self.reason = ""
        self.generation = 0
        self.probe_required = False
        self.probe_inflight = False

    def acquire_request(self, stop_event: threading.Event) -> HostPermit:
        while True:
            with self.condition:
                if stop_event.is_set():
                    raise Stopped
                now = time.monotonic()
                remaining = self.blocked_until - now
                if remaining > 0:
                    self.condition.wait(timeout=min(max(remaining, 0.05), 0.5))
                    continue
                if self.probe_required:
                    if not self.probe_inflight:
                        self.probe_inflight = True
                        return HostPermit(self.generation, True)
                    self.condition.wait(timeout=0.5)
                    continue
                return HostPermit(self.generation, False)

    def permit_is_current(self, permit: HostPermit) -> bool:
        with self.condition:
            if permit.generation != self.generation:
                return False
            if self.blocked_until > time.monotonic():
                return False
            if permit.probe:
                return self.probe_required and self.probe_inflight
            return not self.probe_required

    def finish_request(self, permit: HostPermit, recovered: bool) -> None:
        if not permit.probe:
            return
        with self.condition:
            if permit.generation != self.generation:
                return
            self.probe_inflight = False
            if recovered:
                self.probe_required = False
                self.blocked_until = 0.0
                self.incidents = 0
                self.reason = ""
                self.generation += 1
            self.condition.notify_all()

    def wait(self, stop_event: threading.Event) -> None:
        """Compatibility wait that observes only the active closed period."""
        while True:
            with self.condition:
                if stop_event.is_set():
                    raise Stopped
                remaining = self.blocked_until - time.monotonic()
                if remaining <= 0:
                    return
                self.condition.wait(timeout=min(max(remaining, 0.05), 0.5))

    def pause_for_rate_limit(
        self,
        retry_after: float | None = None,
        reason: str = "HTTP 429",
    ) -> float:
        now = time.monotonic()
        with self.condition:
            new_incident = now - self.last_signal > self.coalesce_seconds
            if now - self.last_signal > self.decay_seconds:
                self.incidents = 0
            if new_incident:
                self.incidents += 1
            self.last_signal = now

            if retry_after is not None and retry_after > 0:
                pause = max(1.0, float(retry_after))
            else:
                # Constant response-side fallback, not a learned/adaptive pause.
                pause = self.base_pause

            self.blocked_until = max(self.blocked_until, now + pause)
            self.reason = reason
            self.probe_required = True
            self.probe_inflight = False
            self.generation += 1
            self.condition.notify_all()
            return max(0.0, self.blocked_until - now)

    def remaining(self) -> float:
        with self.condition:
            return max(0.0, self.blocked_until - time.monotonic())

    def configure(self, base_pause: float, max_pause: float) -> None:
        """Adopt the more conservative pause settings from another client."""
        with self.condition:
            requested_base = max(1.0, float(base_pause))
            requested_max = max(requested_base, float(max_pause))
            self.base_pause = max(self.base_pause, requested_base)
            self.max_pause = max(self.max_pause, requested_max)


def shared_host_gate(
    base_pause: float = 30.0,
    max_pause: float = 300.0,
    key: str = "web.archive.org",
) -> SharedHostGate:
    """Return the process-wide Wayback host gate used by every project."""
    normalized = str(key or "web.archive.org").casefold()
    with _shared_rate_lock:
        gate = _shared_host_gates.get(normalized)
        if gate is None:
            gate = SharedHostGate(base_pause, max_pause)
            _shared_host_gates[normalized] = gate
        else:
            gate.configure(base_pause, max_pause)
        return gate


def reset_shared_traffic_state_for_tests() -> None:
    """Test-only reset for process-wide coordinator state."""
    with _shared_rate_lock:
        _shared_rate_states.clear()
        _shared_host_gates.clear()
