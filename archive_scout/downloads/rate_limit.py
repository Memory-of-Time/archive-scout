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
FIXED_SERVICE_RETRY_SECONDS = 5.0


def saved_service_eligibility(detail: dict, *, adaptive: bool, base_pause: float) -> float:
    """Shorten only a positively identified optional wait when resuming off.

    Older records lack provenance; their deadlines remain authoritative. A
    known Retry-After minimum survives even when optional fallback was longer.
    """
    eligible = float(detail.get("eligible_at_epoch") or 0.0)
    if not adaptive and detail.get("wait_source") == "adaptive_fallback":
        signaled = float(detail.get("rate_limit_signal_at_epoch") or 0.0)
        if signaled > 0:
            eligible = max(float(detail.get("server_eligible_at_epoch") or 0.0),
                           min(eligible, signaled + min(FIXED_SERVICE_RETRY_SECONDS, base_pause)))
    return eligible

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
        self.adaptive_factor = 1.0
        self.adaptive_next_request = 0.0
        self.healthy_since = 0.0
        self.registrations: dict[int, float] = {}
        self.adaptive_registrations: set[int] = set()
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
                self.condition.wait(timeout=min(wait, 0.5))
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
    """Process-wide fixed spacing, with optional experimental adaptive pacing.

    Index and replay traffic use separate keys.  Every client sharing a key also
    shares one effective interval, so a faster per-target value cannot silently
    weaken a slower setting selected elsewhere in the same process.  After a
    genuine live service throttle, opted-in clients temporarily reopen more
    slowly and ease back after sustained healthy traffic. Fixed-only clients
    share the mandatory spacing without inheriting those experimental waits.
    """

    def __init__(self, delay: float, key: str = WAYBACK_HOST_GATE_KEY, *, adaptive: bool = False) -> None:
        self.key = str(key or WAYBACK_HOST_GATE_KEY).casefold()
        self.adaptive = bool(adaptive)
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
                if self.adaptive:
                    state.adaptive_registrations.add(self._registration_id)
                # Only *active* clients contribute their requested pacing floor.
                # Adaptive admission has a separate clock: one opted-in client
                # must never place a fixed-only client behind an adaptive wait.
                state.requested_delay = max(
                    state.floor_delay,
                    max(state.registrations.values(), default=state.floor_delay),
                )
                state.adaptive_delay = state.requested_delay * state.adaptive_factor
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
        if self.adaptive:
            return max(self._state.requested_delay, self._state.adaptive_delay)
        return self._state.requested_delay

    @property
    def requested_delay(self) -> float:
        return self.delay

    @contextlib.contextmanager
    def slot(self, stop_event: threading.Event):
        while True:
            with self.condition:
                if stop_event.is_set():
                    raise Stopped
                now = time.monotonic()
                due = self._state.next_request
                if self.adaptive:
                    due = max(due, self._state.adaptive_next_request)
                wait = max(0.0, due - now)
                if wait <= 0:
                    # Mandatory starts remain spaced across every client. The
                    # optional clock only constrains experimental clients.
                    self._state.next_request = now + self._state.requested_delay
                    if self.adaptive:
                        self._state.adaptive_next_request = now + self.effective_delay
                    break
                self.condition.wait(timeout=min(wait, 0.5))
        yield

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        with self.condition:
            self._state.registrations.pop(self._registration_id, None)
            self._state.adaptive_registrations.discard(self._registration_id)
            requested = max(
                self._state.floor_delay,
                max(self._state.registrations.values(), default=self._state.floor_delay),
            )
            self._state.requested_delay = requested
            if not self._state.adaptive_registrations:
                # Opting out (including operation turnover) leaves no inherited
                # adaptive debt. Live service deadlines belong to SharedHostGate.
                self._state.adaptive_factor = 1.0
                self._state.adaptive_next_request = 0.0
                self._state.last_incident_id = None
                self._state.last_rate_signal = 0.0
                self._state.last_rate_limit = 0.0
                self._state.healthy_starts = 0
                self._state.healthy_since = 0.0
            self._state.adaptive_delay = requested * self._state.adaptive_factor
            self.condition.notify_all()

    def note_rate_limit(self, incident_id: int | None = None) -> bool:
        """Apply at most one pacing reduction for a coalesced service incident.

        ``SharedHostGate`` owns the canonical incident identifier.  The optional
        time-based coalescing path is retained for callers/tests that do not yet
        provide that identifier, but production HTTP clients always do.
        """
        if not self.adaptive or self._closed:
            return False
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
            # Reopen conservatively after a throttle, without changing project
            # semantics or permanently rewriting the user's requested value.
            self._state.adaptive_factor = min(8.0, max(2.0, self._state.adaptive_factor * 1.5))
            self._state.adaptive_delay = baseline * self._state.adaptive_factor
            self._state.last_rate_limit = now
            self._state.healthy_starts = 0
            self._state.healthy_since = 0.0
            self.condition.notify_all()
            return True

    def note_healthy_response(self) -> None:
        if not self.adaptive or self._closed:
            return
        with self.condition:
            baseline = self._state.requested_delay
            if self._state.adaptive_delay <= baseline:
                self._state.adaptive_delay = baseline
                return
            now = time.monotonic()
            if self._state.healthy_starts == 0:
                self._state.healthy_since = now
            self._state.healthy_starts += 1
            # A cooldown is already enforced by the host gate. Require a small
            # sustained sample after reopening, rather than adding another minute
            # and 128 successful CDX responses to every incident.
            if now - self._state.healthy_since >= 5.0 and self._state.healthy_starts >= 8:
                self._state.adaptive_factor = max(1.0, self._state.adaptive_factor / 2.0)
                self._state.adaptive_delay = baseline * self._state.adaptive_factor
                self._state.healthy_starts = 0
                self.condition.notify_all()

    def snapshot(self) -> dict[str, float | int | bool]:
        with self.condition:
            return {
                "requested_delay": self.delay,
                "pool_floor_delay": self._state.requested_delay,
                "effective_delay": self.effective_delay,
                "adaptive_rate_limiting": self.adaptive,
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
        *,
        adaptive: bool = False,
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
        self.recovery_cycle_started = 0.0
        self.connection_outage_cycles = 0
        self.connection_outage_base = 3.0
        self.connection_outage_generation = -1
        self.reason = ""
        self.generation = 0
        self.probe_required = False
        self.probe_inflight = False
        self.adaptive = bool(adaptive)
        self.wait_source = ""
        self.server_until = self.server_until_wall = 0.0
        self.fallback_until = self.fallback_until_wall = 0.0
        self.last_signal_wall = 0.0
        self.fallback_signal = self.fallback_signal_wall = 0.0
        self._fallback_adaptive = False
        self.connection_failures = 0
        self.last_connection_failure = 0.0
        self._default_policy = (self.base_pause, self.max_pause, self.adaptive)
        self._policies: dict[object, tuple[float, float, bool]] = {}
        self._wait_started: float | None = None
        self._wait_seconds = 0.0

    def register_policy(self, base_pause: float, max_pause: float, *, adaptive: bool = False) -> object:
        token = object()
        with self.condition:
            base = max(0.01, float(base_pause))
            self._policies[token] = (base, max(base, float(max_pause)), bool(adaptive))
            self._configure_active_policy()
        return token

    def release_policy(self, token: object) -> None:
        with self.condition:
            self._policies.pop(token, None)
            self._configure_active_policy()

    def _configure_active_policy(self) -> None:
        policies = list(self._policies.values()) or [self._default_policy]
        self.base_pause = max(value[0] for value in policies)
        self.max_pause = max(value[1] for value in policies)
        # A fixed-only client must not inherit another client's optional host
        # delay. Mixed clients still share the server deadline and one probe.
        self.adaptive = all(value[2] for value in policies)
        if not self.adaptive and self._fallback_adaptive:
            self.fallback_until = min(self.fallback_until, self.fallback_signal + self.fixed_retry_seconds)
            self.fallback_until_wall = min(self.fallback_until_wall, self.fallback_signal_wall + self.fixed_retry_seconds)
            self._fallback_adaptive = False
            self._update_service_deadline()
        self.condition.notify_all()

    @property
    def fixed_retry_seconds(self) -> float:
        return min(FIXED_SERVICE_RETRY_SECONDS, self.base_pause)

    def _update_service_deadline(self) -> None:
        self.blocked_until = max(self.server_until, self.fallback_until)
        self.blocked_until_wall = max(self.server_until_wall, self.fallback_until_wall)
        if self.server_until >= self.fallback_until and self.server_until_wall:
            self.wait_source = "server_retry_after"
        else:
            self.wait_source = "adaptive_fallback" if self._fallback_adaptive else "fixed_fallback"

    def _start_wait(self, now: float) -> None:
        if self._wait_started is None:
            self._wait_started = now

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
                if (deadline is not None and now >= deadline
                        and (self.probe_required or self.probe_inflight or self.blocked_until > now)):
                    waited = max(0.0, now - self.incident_started) if self.incident_started else 0.0
                    raise RecoveryDeadlineExceeded(
                        incident_id=self.incident_id,
                        waited=waited,
                        eligible_at_epoch=self.blocked_until_wall,
                        reason=self.reason,
                    )
                remaining = self.blocked_until - now
                if remaining > 0:
                    timeout = min(remaining, 0.5)
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
            return max(self.incident_started, self.recovery_cycle_started) + budget

    def renew_recovery_cycle(self, incident_id: int | None = None) -> None:
        """Renew a coordinator's wait budget without shortening host eligibility."""
        with self.condition:
            if incident_id is not None and incident_id != self.incident_id:
                return
            if self.probe_inflight:
                return
            self.recovery_cycle_started = time.monotonic()
            self.condition.notify_all()

    def pause_for_connection_outage(self, base_seconds: float) -> None:
        """One shared, increasing connection pause, independent of quota pacing."""
        with self.condition:
            now = time.monotonic()
            if self.probe_required or self.probe_inflight or self.blocked_until > now:
                return  # Preserve any live service cooldown and existing probe.
            self.connection_outage_cycles += 1
            self.connection_outage_base = max(1.0, float(base_seconds))
            pause = min(60.0, max(1.0, base_seconds) * 2 ** min(self.connection_outage_cycles - 1, 5) * random.uniform(1.0, 1.1))
            self.incident_id += 1
            self.incident_started = self.recovery_cycle_started = now
            self.incident_started_wall = time.time()
            self.blocked_until = now + pause
            self.blocked_until_wall = time.time() + pause
            self.reason = "connection outage"
            self.wait_source = "connection_recovery"
            self.probe_required = True
            self._start_wait(now)
            self.connection_outage_generation = self.generation
            self.generation += 1
            self.condition.notify_all()

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
            if permit.generation != self.generation or not self.probe_inflight:
                return
            self.probe_inflight = False
            if recovered:
                self._finish_recovery_locked()
            else:
                # A fresh 5xx/network failure did not prove recovery.  Avoid a
                # thundering sequence of simultaneous recovery probes.
                pause = 5.0
                if self.reason == "connection outage":
                    self.connection_outage_cycles += 1
                    pause = min(60.0, self.connection_outage_base * 2 ** min(self.connection_outage_cycles - 1, 5) * random.uniform(1.0, 1.1))
                self.blocked_until = max(self.blocked_until, time.monotonic() + pause)
                self.blocked_until_wall = max(self.blocked_until_wall, time.time() + pause)
                if self.reason != "connection outage":
                    self.fallback_until = self.blocked_until
                    self.fallback_until_wall = self.blocked_until_wall
                    self._fallback_adaptive = False
                    self._update_service_deadline()
                self.generation += 1
            self.condition.notify_all()

    def defer_request(self, permit: HostPermit) -> None:
        """Return an unused probe permit without inventing a network failure."""
        if not permit.probe:
            return
        with self.condition:
            if permit.generation == self.generation and self.probe_inflight:
                self.probe_inflight = False
                self.condition.notify_all()

    def _finish_recovery_locked(self) -> None:
        if self._wait_started is not None:
            self._wait_seconds += max(0.0, time.monotonic() - self._wait_started)
            self._wait_started = None
        self.probe_required = False
        self.probe_inflight = False
        self.blocked_until = 0.0
        self.blocked_until_wall = 0.0
        self.server_until = self.server_until_wall = 0.0
        self.fallback_until = self.fallback_until_wall = 0.0
        self._fallback_adaptive = False
        self.fallback_signal = self.fallback_signal_wall = 0.0
        self.wait_source = ""
        # Quota incident memory decays in the optional adaptive rate pool.
        self.incidents = max(0, self.incidents - 1)
        self.reason = ""
        self.incident_started = 0.0
        self.incident_started_wall = 0.0
        self.recovery_cycle_started = 0.0
        self.connection_outage_cycles = 0
        self.connection_outage_generation = -1
        self.generation += 1

    def wait(self, stop_event: threading.Event) -> None:
        while True:
            with self.condition:
                if stop_event.is_set():
                    raise Stopped
                remaining = self.blocked_until - time.monotonic()
                if remaining <= 0:
                    return
                self.condition.wait(timeout=min(remaining, 0.5))

    def signal_rate_limit(
        self,
        retry_after: float | None = None,
        reason: str = "HTTP 429",
        *,
        permit: HostPermit | None = None,
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
            fresh_probe = bool(permit and permit.probe and permit.generation == self.generation and self.probe_inflight)
            stale = bool(permit and permit.generation != self.generation)
            # An old in-flight response cannot reopen an already recovered gate
            # without a new server deadline. Nor can duplicate no-header replies
            # restart the fallback timer or invalidate the current probe.
            if retry_after is None and ((active_incident and not fresh_probe) or (stale and not active_incident)):
                return max(0.0, self.blocked_until - now), self.incident_id, self.blocked_until_wall, False
            if not active_incident and now - self.last_signal > self.decay_seconds:
                self.incidents = 0
            if new_incident:
                self.incidents += 1
                self.incident_id += 1
                self.incident_started = now
                self.incident_started_wall = wall_now
                self.recovery_cycle_started = now
            elif fresh_probe:
                self.incidents += 1
            self.last_signal = now
            self.last_signal_wall = wall_now

            if retry_after is not None:
                # Retry-After is a minimum server deadline; never jitter below it.
                pause = max(0.0, float(retry_after))
                self.server_until = max(self.server_until, now + pause)
                self.server_until_wall = max(self.server_until_wall, wall_now + pause)
            else:
                if self.adaptive:
                    exponent = max(0, min(self.incidents - 1, 4))
                    pause = min(self.max_pause, self.base_pause * (2**exponent) * random.uniform(1.0, 1.1))
                else:
                    pause = self.fixed_retry_seconds
                self.fallback_until = now + pause
                self.fallback_until_wall = wall_now + pause
                self._fallback_adaptive = self.adaptive
                self.fallback_signal, self.fallback_signal_wall = now, wall_now

            # A duplicate explicit deadline only changes the generation when it
            # actually extends eligibility; otherwise keep the one live probe.
            if active_incident and not fresh_probe and (pause <= 0 or now + pause <= self.blocked_until):
                return max(0.0, self.blocked_until - now), self.incident_id, self.blocked_until_wall, False
            self.blocked_until = max(self.blocked_until, now + pause)
            self.blocked_until_wall = max(self.blocked_until_wall, wall_now + pause)
            # Preserve any connection recovery deadline that was already live.
            # Quota provenance tracks server and optional application waits
            # separately so opting out cannot shorten a Retry-After deadline.
            self.fallback_until = max(self.fallback_until, self.blocked_until if self.reason == "connection outage" else 0.0)
            self.fallback_until_wall = max(self.fallback_until_wall, self.blocked_until_wall if self.reason == "connection outage" else 0.0)
            self._update_service_deadline()
            self.reason = reason
            self.probe_required = True
            self._start_wait(now)
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

    def restore_service_wait(self, detail: dict) -> float:
        """Restore persisted eligibility without turning optional debt into a server wait."""
        with self.condition:
            now, wall_now = time.monotonic(), time.time()
            eligible = saved_service_eligibility(detail, adaptive=self.adaptive, base_pause=self.base_pause)
            if eligible <= wall_now:
                return max(0.0, self.blocked_until - now)
            before = self.blocked_until
            source = detail.get("wait_source")
            signaled = float(detail.get("rate_limit_signal_at_epoch") or 0.0)
            if source in {"adaptive_fallback", "fixed_fallback"} and signaled > 0:
                self.fallback_until = max(self.fallback_until, now + eligible - wall_now)
                self.fallback_until_wall = max(self.fallback_until_wall, eligible)
                self._fallback_adaptive = source == "adaptive_fallback" and self.adaptive
                self.fallback_signal, self.fallback_signal_wall = now + signaled - wall_now, signaled
                server_epoch = float(detail.get("server_eligible_at_epoch") or 0.0)
            else:
                # Unlabelled legacy records can contain server instructions.
                server_epoch = eligible
            if server_epoch > wall_now:
                self.server_until = max(self.server_until, now + server_epoch - wall_now)
                self.server_until_wall = max(self.server_until_wall, server_epoch)
            if self.reason == "connection outage":
                self.fallback_until = max(self.fallback_until, before)
                self.fallback_until_wall = max(self.fallback_until_wall, self.blocked_until_wall)
            self._update_service_deadline()
            if self.blocked_until > before or not self.probe_required:
                if not self.probe_required:
                    self.incident_id += 1
                    self.incident_started = self.recovery_cycle_started = now
                    self.incident_started_wall = wall_now
                self.probe_required, self.probe_inflight = True, False
                self.reason = "saved service cooldown"
                self.generation += 1
                self._start_wait(now)
            self.condition.notify_all()
            return max(0.0, self.blocked_until - now)

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
                "wait_source": self.wait_source,
                "adaptive_rate_limiting": self.adaptive,
                "server_eligible_at_epoch": self.server_until_wall,
                "rate_limit_signal_at_epoch": self.fallback_signal_wall or self.last_signal_wall,
                "probe_required": self.probe_required,
                "probe_inflight": self.probe_inflight,
                "eligible_at_epoch": self.blocked_until_wall,
                "service_wait_seconds": self._wait_seconds + (max(0.0, now - self._wait_started) if self._wait_started is not None else 0.0),
            }

    def configure(self, base_pause: float, max_pause: float, *, adaptive: bool | None = None) -> None:
        """Set the idle policy; active clients register and release their own."""
        with self.condition:
            requested_base = max(0.01, float(base_pause))
            requested_max = max(requested_base, float(max_pause))
            self._default_policy = (requested_base, requested_max, self._default_policy[2] if adaptive is None else bool(adaptive))
            self._configure_active_policy()

    def service_wait_detail(self) -> dict[str, object]:
        """Durable quota eligibility, suitable for progress and user-stop events."""
        state = self.snapshot()
        if (state['reason'] == 'connection outage'
                or not (state['remaining'] or state['probe_required'] or state['probe_inflight'])):
            return {}
        return {'reason_code': 'service_rate_limit',
                'http_status': 503 if '503' in str(state['reason']) else 429,
                **{field: state[field] for field in ('eligible_at_epoch', 'incident_id', 'wait_source',
                    'server_eligible_at_epoch', 'rate_limit_signal_at_epoch', 'adaptive_rate_limiting')}}

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

    def note_connection_success(self, *, permit: HostPermit | None = None, recovered: bool = False) -> bool:
        with self.condition:
            self.connection_failures = 0
            self.last_connection_failure = 0.0
            if (recovered and self.reason == "connection outage" and permit is not None
                    and (permit.generation == self.connection_outage_generation
                         or (permit.probe and permit.generation == self.generation))):
                # Trustworthy progress from a transfer already in flight proves
                # this connection incident recovered. A stale transfer from an
                # earlier incident and any live server deadline remain untouched.
                self._finish_recovery_locked()
                self.condition.notify_all()
                return True
            return False


def shared_host_gate(
    base_pause: float = 60.0,
    max_pause: float = 600.0,
    key: str = WAYBACK_HOST_GATE_KEY,
    *,
    adaptive: bool | None = None,
) -> SharedHostGate:
    """Return the process-wide Wayback host gate used by every project."""
    normalized = str(key or WAYBACK_HOST_GATE_KEY).casefold()
    with _shared_rate_lock:
        gate = _shared_host_gates.get(normalized)
        if gate is None:
            gate = SharedHostGate(base_pause, max_pause, adaptive=bool(adaptive))
            _shared_host_gates[normalized] = gate
        else:
            gate.configure(base_pause, max_pause, adaptive=adaptive)
        return gate


def reset_shared_traffic_state_for_tests() -> None:
    """Test-only reset for process-wide coordinator state."""
    with _shared_rate_lock:
        _shared_rate_states.clear()
        _shared_host_gates.clear()
