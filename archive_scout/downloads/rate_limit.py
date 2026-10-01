from __future__ import annotations

import contextlib
import random
import threading
import time
from dataclasses import dataclass

from ..constants import WAYBACK_INDEX_MIN_INTERVAL, WAYBACK_REPLAY_MIN_INTERVAL
from ..events import Stopped


WAYBACK_HOST_GATE_KEY = "web.archive.org"
WAYBACK_INDEX_RATE_KEY = "web.archive.org:index"
WAYBACK_REPLAY_RATE_KEY = "web.archive.org:replay"

_shared_rate_lock = threading.Lock()
_shared_rate_states: dict[str, "_SharedRateState"] = {}
_shared_host_gates: dict[str, "SharedHostGate"] = {}


class _SharedRateState:
    def __init__(self, requested_delay: float) -> None:
        self.condition = threading.Condition()
        self.next_request = 0.0
        self.requested_delay = max(0.0, float(requested_delay))
        self.adaptive_delay = self.requested_delay
        self.last_rate_limit = 0.0
        self.healthy_starts = 0


class FixedRateLimiter:
    """A fixed minimum delay between actual network request starts."""

    def __init__(self, delay: float) -> None:
        self.delay = max(0.0, float(delay))
        self.condition = threading.Condition()
        self.next_request = 0.0

    @property
    def effective_delay(self) -> float:
        return self.delay

    @contextlib.contextmanager
    def slot(self, stop_event: threading.Event):
        while True:
            with self.condition:
                if stop_event.is_set():
                    raise Stopped
                now = time.monotonic()
                wait = max(0.0, self.next_request - now)
                if wait <= 0:
                    # Do not accumulate burst credit while idle: the next start is
                    # always scheduled relative to the request that is starting now.
                    self.next_request = now + self.effective_delay
                    break
                self.condition.wait(timeout=min(max(wait, 0.05), 0.5))
        yield

    def wait(self, stop_event: threading.Event) -> None:
        with self.slot(stop_event):
            return

    def note_rate_limit(self) -> None:
        return

    def note_healthy_response(self) -> None:
        return


class SharedFixedRateLimiter(FixedRateLimiter):
    """Process-wide spacing and adaptive recovery for one Wayback traffic pool.

    Index and replay traffic use separate keys.  Every client sharing a key also
    shares one effective interval, so a faster per-target value cannot silently
    weaken a slower setting selected elsewhere in the same process.  After a
    genuine live service throttle, the pool temporarily reopens more slowly and
    eases back toward the requested ceiling only after sustained healthy traffic.
    """

    def __init__(self, delay: float, key: str = WAYBACK_HOST_GATE_KEY) -> None:
        self.key = str(key or WAYBACK_HOST_GATE_KEY).casefold()
        floor = 0.0
        if self.key == WAYBACK_INDEX_RATE_KEY:
            floor = WAYBACK_INDEX_MIN_INTERVAL
        elif self.key == WAYBACK_REPLAY_RATE_KEY:
            floor = WAYBACK_REPLAY_MIN_INTERVAL
        self.delay = max(floor, float(delay), 0.0)
        with _shared_rate_lock:
            state = _shared_rate_states.get(self.key)
            if state is None:
                state = _SharedRateState(self.delay)
                _shared_rate_states[self.key] = state
            else:
                with state.condition:
                    # Preserve slower user choices.  A newly-created faster client
                    # may share the pool, but it cannot lower the active ceiling.
                    state.requested_delay = max(state.requested_delay, self.delay)
                    state.adaptive_delay = max(state.adaptive_delay, state.requested_delay)
                    state.condition.notify_all()
        self._state = state
        self.condition = state.condition

    @property
    def next_request(self) -> float:
        return self._state.next_request

    @next_request.setter
    def next_request(self, value: float) -> None:
        self._state.next_request = float(value)

    @property
    def effective_delay(self) -> float:
        return max(self._state.requested_delay, self._state.adaptive_delay)

    @property
    def requested_delay(self) -> float:
        return self.delay

    def note_rate_limit(self) -> None:
        with self.condition:
            now = time.monotonic()
            baseline = max(self._state.requested_delay, 0.001)
            current = max(self._state.adaptive_delay, baseline)
            # Reopen conservatively after a throttle, without changing project
            # semantics or permanently rewriting the user's requested value.
            self._state.adaptive_delay = min(baseline * 8.0, max(baseline * 2.0, current * 1.5))
            self._state.last_rate_limit = now
            self._state.healthy_starts = 0
            self.condition.notify_all()

    def note_healthy_response(self) -> None:
        with self.condition:
            baseline = self._state.requested_delay
            if self._state.adaptive_delay <= baseline:
                self._state.adaptive_delay = baseline
                return
            now = time.monotonic()
            self._state.healthy_starts += 1
            # Require both time and repeated successful service responses before
            # increasing the effective rate again.  This avoids a one-response
            # snap-back immediately after the recovery probe.
            if now - self._state.last_rate_limit >= 60.0 and self._state.healthy_starts >= 32:
                self._state.adaptive_delay = max(baseline, self._state.adaptive_delay * 0.8)
                self._state.healthy_starts = 0
                self.condition.notify_all()

    def snapshot(self) -> dict[str, float | int]:
        with self.condition:
            return {
                "requested_delay": self.delay,
                "pool_floor_delay": self._state.requested_delay,
                "effective_delay": self.effective_delay,
                "healthy_starts": self._state.healthy_starts,
            }


@dataclass(frozen=True, slots=True)
class HostPermit:
    generation: int
    probe: bool = False


class SharedHostGate:
    """Coordinate live Wayback service throttles across in-process workers.

    A live 429/503 closes the shared gate.  After the cooldown, one logical work
    item is released as the recovery probe.  A 500/502/504 does not count as
    recovery; the probe is returned to the queue after a short bounded pause.
    """

    def __init__(
        self,
        base_pause: float = 60.0,
        max_pause: float = 600.0,
        coalesce_seconds: float = 2.0,
        decay_seconds: float = 600.0,
    ) -> None:
        self.base_pause = max(0.01, float(base_pause))
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
                # Retain incident memory; gradual rate recovery belongs to the
                # corresponding rate pool rather than resetting after one probe.
                self.incidents = max(0, self.incidents - 1)
                self.reason = ""
                self.generation += 1
            else:
                # A fresh 5xx/network failure did not prove recovery.  Avoid a
                # thundering sequence of simultaneous recovery probes.
                self.blocked_until = max(self.blocked_until, time.monotonic() + 5.0)
            self.condition.notify_all()

    def wait(self, stop_event: threading.Event) -> None:
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
                # Retry-After is a minimum server deadline; never jitter below it.
                pause = max(1.0, float(retry_after))
            else:
                exponent = max(0, min(self.incidents - 1, 4))
                pause = min(self.max_pause, self.base_pause * (2**exponent))
                pause *= random.uniform(1.0, 1.1)

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

    def snapshot(self) -> dict[str, float | int | str | bool]:
        with self.condition:
            return {
                "remaining": max(0.0, self.blocked_until - time.monotonic()),
                "incidents": self.incidents,
                "reason": self.reason,
                "probe_required": self.probe_required,
                "probe_inflight": self.probe_inflight,
            }

    def configure(self, base_pause: float, max_pause: float) -> None:
        """Adopt the more conservative pause settings from another client."""
        with self.condition:
            requested_base = max(0.01, float(base_pause))
            requested_max = max(requested_base, float(max_pause))
            self.base_pause = max(self.base_pause, requested_base)
            self.max_pause = max(self.max_pause, requested_max)


def shared_host_gate(
    base_pause: float = 60.0,
    max_pause: float = 600.0,
    key: str = WAYBACK_HOST_GATE_KEY,
) -> SharedHostGate:
    """Return the process-wide Wayback host gate used by every project."""
    normalized = str(key or WAYBACK_HOST_GATE_KEY).casefold()
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
