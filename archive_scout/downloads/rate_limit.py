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
    def __init__(self, requested_delay: float, floor_delay: float = 0.0) -> None:
        self.condition = threading.Condition()
        self.next_request = 0.0
        self.floor_delay = max(0.0, float(floor_delay))
        self.requested_delay = max(0.0, float(requested_delay))
        self.adaptive_delay = self.requested_delay
        self.registrations: dict[int, float] = {}
        self.last_rate_limit = 0.0
        self.last_rate_signal = 0.0
        self.last_incident_id: int | None = None
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

    def note_rate_limit(self, incident_id: int | None = None) -> bool:
        del incident_id
        return False

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
        self._registration_id = id(self)
        self._closed = False
        with _shared_rate_lock:
            state = _shared_rate_states.get(self.key)
            if state is None:
                state = _SharedRateState(self.delay, floor)
                _shared_rate_states[self.key] = state
            with state.condition:
                state.floor_delay = max(state.floor_delay, floor)
                state.registrations[self._registration_id] = self.delay
                # Only *active* clients contribute their requested pacing floor.
                # Service-driven adaptive recovery remains a separate state so a
                # throttle survives operation turnover without a closed slow
                # client permanently constraining a later faster operation.
                state.requested_delay = max(
                    state.floor_delay,
                    max(state.registrations.values(), default=state.floor_delay),
                )
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

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        with self.condition:
            self._state.registrations.pop(self._registration_id, None)
            requested = max(
                self._state.floor_delay,
                max(self._state.registrations.values(), default=self._state.floor_delay),
            )
            self._state.requested_delay = requested
            if self._state.last_rate_limit <= 0.0:
                self._state.adaptive_delay = requested
            else:
                self._state.adaptive_delay = max(requested, self._state.adaptive_delay)
            self.condition.notify_all()

    def note_rate_limit(self, incident_id: int | None = None) -> bool:
        """Apply at most one pacing reduction for a coalesced service incident.

        ``SharedHostGate`` owns the canonical incident identifier.  The optional
        time-based coalescing path is retained for callers/tests that do not yet
        provide that identifier, but production HTTP clients always do.
        """
        with self.condition:
            now = time.monotonic()
            if incident_id is not None:
                if self._state.last_incident_id == int(incident_id):
                    self._state.last_rate_signal = now
                    return False
                self._state.last_incident_id = int(incident_id)
            else:
                if self._state.last_rate_signal > 0.0 and now - self._state.last_rate_signal <= 2.0:
                    self._state.last_rate_signal = now
                    return False
                self._state.last_incident_id = None
            self._state.last_rate_signal = now
            baseline = max(self._state.requested_delay, 0.001)
            current = max(self._state.adaptive_delay, baseline)
            # Reopen conservatively after a throttle, without changing project
            # semantics or permanently rewriting the user's requested value.
            self._state.adaptive_delay = min(baseline * 8.0, max(baseline * 2.0, current * 1.5))
            self._state.last_rate_limit = now
            self._state.healthy_starts = 0
            self.condition.notify_all()
            return True

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
                "last_incident_id": self._state.last_incident_id or 0,
            }


@dataclass(frozen=True, slots=True)
class HostPermit:
    generation: int
    probe: bool = False


class RecoveryDeadlineExceeded(RuntimeError):
    """Raised when a shared service-recovery incident outlives its budget."""

    def __init__(
        self,
        *,
        incident_id: int,
        waited: float,
        eligible_at_epoch: float,
        reason: str,
    ) -> None:
        super().__init__(reason or "Wayback service recovery deadline reached")
        self.incident_id = int(incident_id)
        self.waited = max(0.0, float(waited))
        self.eligible_at_epoch = max(0.0, float(eligible_at_epoch))
        self.reason = str(reason or "")


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
        self.blocked_until_wall = 0.0
        self.last_signal = 0.0
        self.incidents = 0
        self.incident_id = 0
        self.incident_started = 0.0
        self.incident_started_wall = 0.0
        self.reason = ""
        self.generation = 0
        self.probe_required = False
        self.probe_inflight = False
        self.connection_failures = 0
        self.last_connection_failure = 0.0

    def acquire_request(
        self,
        stop_event: threading.Event,
        *,
        deadline: float | None = None,
    ) -> HostPermit:
        while True:
            with self.condition:
                if stop_event.is_set():
                    raise Stopped
                now = time.monotonic()
                if deadline is not None and now >= deadline:
                    waited = max(0.0, now - self.incident_started) if self.incident_started else 0.0
                    raise RecoveryDeadlineExceeded(
                        incident_id=self.incident_id,
                        waited=waited,
                        eligible_at_epoch=self.blocked_until_wall,
                        reason=self.reason,
                    )
                remaining = self.blocked_until - now
                if remaining > 0:
                    timeout = min(max(remaining, 0.05), 0.5)
                    if deadline is not None:
                        timeout = min(timeout, max(0.0, deadline - now))
                    if timeout <= 0:
                        continue
                    self.condition.wait(timeout=timeout)
                    continue
                if self.probe_required:
                    if not self.probe_inflight:
                        self.probe_inflight = True
                        return HostPermit(self.generation, True)
                    timeout = 0.5
                    if deadline is not None:
                        timeout = min(timeout, max(0.0, deadline - now))
                    if timeout <= 0:
                        continue
                    self.condition.wait(timeout=timeout)
                    continue
                return HostPermit(self.generation, False)

    def recovery_deadline(self, max_wait: float) -> float | None:
        """Return the absolute monotonic deadline for the active incident."""
        budget = max(0.0, float(max_wait))
        if budget <= 0:
            return None
        with self.condition:
            now = time.monotonic()
            active = self.probe_required or self.probe_inflight or self.blocked_until > now
            if not active or self.incident_started <= 0:
                return None
            return self.incident_started + budget

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
                self.blocked_until_wall = 0.0
                # Retain incident memory; gradual rate recovery belongs to the
                # corresponding rate pool rather than resetting after one probe.
                self.incidents = max(0, self.incidents - 1)
                self.reason = ""
                self.incident_started = 0.0
                self.incident_started_wall = 0.0
                self.generation += 1
            else:
                # A fresh 5xx/network failure did not prove recovery.  Avoid a
                # thundering sequence of simultaneous recovery probes.
                self.blocked_until = max(self.blocked_until, time.monotonic() + 5.0)
                self.blocked_until_wall = max(self.blocked_until_wall, time.time() + 5.0)
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

    def signal_rate_limit(
        self,
        retry_after: float | None = None,
        reason: str = "HTTP 429",
    ) -> tuple[float, int, float, bool]:
        now = time.monotonic()
        wall_now = time.time()
        with self.condition:
            # A throttle remains one incident until its recovery probe proves the
            # service healthy.  Late responses from requests that were already in
            # flight must not manufacture a fresh incident merely because they
            # arrive outside the short signal-coalescing window.
            active_incident = bool(
                self.incident_id
                and (self.probe_required or self.probe_inflight or self.blocked_until > now or self.incident_started > 0.0)
            )
            new_incident = not active_incident
            if not active_incident and now - self.last_signal > self.decay_seconds:
                self.incidents = 0
            if new_incident:
                self.incidents += 1
                self.incident_id += 1
                self.incident_started = now
                self.incident_started_wall = wall_now
            self.last_signal = now

            if retry_after is not None and retry_after > 0:
                # Retry-After is a minimum server deadline; never jitter below it.
                pause = max(1.0, float(retry_after))
            else:
                exponent = max(0, min(self.incidents - 1, 4))
                pause = min(self.max_pause, self.base_pause * (2**exponent))
                pause *= random.uniform(1.0, 1.1)

            self.blocked_until = max(self.blocked_until, now + pause)
            self.blocked_until_wall = max(self.blocked_until_wall, wall_now + pause)
            self.reason = reason
            self.probe_required = True
            self.probe_inflight = False
            self.generation += 1
            self.condition.notify_all()
            return (
                max(0.0, self.blocked_until - now),
                self.incident_id,
                self.blocked_until_wall,
                new_incident,
            )

    def pause_for_rate_limit(
        self,
        retry_after: float | None = None,
        reason: str = "HTTP 429",
    ) -> float:
        """Compatibility wrapper returning only the effective shared wait."""
        return self.signal_rate_limit(retry_after, reason)[0]

    def remaining(self) -> float:
        with self.condition:
            return max(0.0, self.blocked_until - time.monotonic())

    def snapshot(self) -> dict[str, float | int | str | bool]:
        with self.condition:
            now = time.monotonic()
            return {
                "remaining": max(0.0, self.blocked_until - now),
                "incidents": self.incidents,
                "incident_id": self.incident_id,
                "incident_elapsed": max(0.0, now - self.incident_started) if self.incident_started else 0.0,
                "reason": self.reason,
                "probe_required": self.probe_required,
                "probe_inflight": self.probe_inflight,
                "eligible_at_epoch": self.blocked_until_wall,
            }

    def configure(self, base_pause: float, max_pause: float) -> None:
        """Adopt the more conservative pause settings from another client."""
        with self.condition:
            requested_base = max(0.01, float(base_pause))
            requested_max = max(requested_base, float(max_pause))
            self.base_pause = max(self.base_pause, requested_base)
            self.max_pause = max(self.max_pause, requested_max)

    def note_connection_failure(self, threshold: int) -> tuple[int, bool]:
        """Track a short burst of genuine connection-setup failures.

        This deliberately does not share the HTTP 429/503 gate: a connection
        outage should pause the operation and preserve the queue, not masquerade
        as a server quota incident. Healthy responses reset the streak so one bad
        URL or one backend does not stop an otherwise working run.
        """
        limit = max(2, int(threshold))
        with self.condition:
            now = time.monotonic()
            if self.last_connection_failure and now - self.last_connection_failure > 30.0:
                self.connection_failures = 0
            self.last_connection_failure = now
            self.connection_failures += 1
            return self.connection_failures, self.connection_failures >= limit

    def note_connection_success(self) -> None:
        with self.condition:
            self.connection_failures = 0
            self.last_connection_failure = 0.0


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
