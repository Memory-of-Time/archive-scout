"""Fixed request spacing and server eligibility; no learned slowdown policy."""
from __future__ import annotations
import contextlib
import threading
import time
from dataclasses import dataclass
from ..constants import WAYBACK_INDEX_MIN_INTERVAL, WAYBACK_REPLAY_MIN_INTERVAL
from ..events import Stopped

WAYBACK_HOST_GATE_KEY = 'web.archive.org'
WAYBACK_INDEX_RATE_KEY = 'web.archive.org:index'
WAYBACK_REPLAY_RATE_KEY = 'web.archive.org:replay'
FIXED_SERVICE_RETRY_SECONDS = 5.0
_shared_rate_lock = threading.Lock()
_shared_rate_states = {}
_shared_host_gates = {}

class _SharedRateState:
    def __init__(self):
        self.condition = threading.Condition()
        self.next_request = self.last_start = 0.0
        self.clients = {}

class FixedRateLimiter:
    def __init__(self, delay):
        self.delay = max(0.0, float(delay))
        self.condition = threading.Condition()
        self.next_request = 0.0
    @property
    def effective_delay(self):
        return self.delay
    @contextlib.contextmanager
    def slot(self, stop_event):
        while True:
            with self.condition:
                if stop_event.is_set():
                    raise Stopped
                now = time.monotonic()
                remaining = self.next_request - now
                if remaining <= 0:
                    self.next_request = now + self.effective_delay
                    break
                self.condition.wait(timeout=min(0.25, remaining))
        yield
    def wait(self, stop_event):
        with self.slot(stop_event):
            pass
    def note_rate_limit(self, incident_id=None):
        return False
    def note_healthy_response(self):
        pass
    def close(self):
        pass

class SharedFixedRateLimiter(FixedRateLimiter):
    """One fixed clock per pool; only active clients set its floor."""
    def __init__(self, delay, key=WAYBACK_HOST_GATE_KEY):
        self.key = str(key or WAYBACK_HOST_GATE_KEY).casefold()
        floor = {WAYBACK_INDEX_RATE_KEY: WAYBACK_INDEX_MIN_INTERVAL,
                 WAYBACK_REPLAY_RATE_KEY: WAYBACK_REPLAY_MIN_INTERVAL}.get(self.key, 0.0)
        self.delay = max(floor, float(delay), 0.0)
        self._token = object()
        with _shared_rate_lock:
            self._state = _shared_rate_states.setdefault(self.key, _SharedRateState())
        self.condition = self._state.condition
        with self.condition:
            self._state.clients[self._token] = self.delay
            if self._state.last_start:
                self._state.next_request = self._state.last_start + self.effective_delay
            self.condition.notify_all()
    @property
    def next_request(self):
        return self._state.next_request
    @next_request.setter
    def next_request(self, value):
        self._state.next_request = float(value)
        self._state.last_start = float(value) - self.effective_delay
    @property
    def requested_delay(self):
        return self.delay
    @property
    def effective_delay(self):
        return max(self._state.clients.values(), default=self.delay)
    def snapshot(self):
        with self.condition:
            return {'requested_delay': self.delay, 'pool_floor_delay': self.effective_delay,
                    'effective_delay': self.effective_delay, 'active_clients': len(self._state.clients)}
    def close(self):
        with self.condition:
            self._state.clients.pop(self._token, None)
            self._state.next_request = self._state.last_start + self.effective_delay
            self.condition.notify_all()

@dataclass(frozen=True, slots=True)
class HostPermit:
    generation: int
    probe: bool = False

class RecoveryDeadlineExceeded(RuntimeError):
    def __init__(self, *, incident_id, waited, eligible_at_epoch, reason):
        super().__init__(reason)
        self.incident_id, self.waited = incident_id, waited
        self.eligible_at_epoch, self.reason = eligible_at_epoch, reason

def saved_service_eligibility(detail):
    """Preserve server/unknown deadlines; expire known application waits early."""
    deadline = max(float(detail.get('server_eligible_at_epoch') or 0), float(detail.get('eligible_at_epoch') or 0))
    if detail.get('wait_source') in {'fallback', 'application', 'fixed', 'adaptive'}:
        signal = float(detail.get('rate_limit_signal_at_epoch') or 0)
        if signal:
            return max(float(detail.get('server_eligible_at_epoch') or 0), min(deadline, signal + FIXED_SERVICE_RETRY_SECONDS))
    return deadline

class SharedHostGate:
    """One live 429/503 deadline; fixed five seconds when no Retry-After exists.

    No connection-wide circuit, escalating delay, or single-probe recovery phase.
    """
    def __init__(self, base_pause=5.0, max_pause=5.0, **unused):
        self.condition = threading.Condition()
        self.blocked_until = self.blocked_until_wall = 0.0
        self.server_until_wall = self.last_signal_wall = 0.0
        self.incident_id = self.generation = 0
        self.incident_started = 0.0
        self.reason = self.wait_source = ''
    def acquire_request(self, stop_event, *, deadline=None):
        with self.condition:
            while self.blocked_until > time.monotonic():
                if stop_event.is_set():
                    raise Stopped
                now = time.monotonic()
                if deadline is not None and now >= deadline:
                    raise RecoveryDeadlineExceeded(incident_id=self.incident_id, waited=max(0.0, now-self.incident_started),
                        eligible_at_epoch=self.blocked_until_wall, reason=self.reason)
                self.condition.wait(timeout=min(0.25, self.blocked_until-now))
            if stop_event.is_set():
                raise Stopped
            return HostPermit(self.generation)
    def recovery_deadline(self, max_wait):
        return None
    def permit_is_current(self, permit):
        with self.condition:
            return permit.generation == self.generation and self.blocked_until <= time.monotonic()
    def finish_request(self, permit, recovered):
        pass
    def wait(self, stop_event):
        self.acquire_request(stop_event)
    def signal_rate_limit(self, retry_after=None, reason='HTTP 429'):
        now, wall = time.monotonic(), time.time()
        with self.condition:
            new = self.blocked_until <= now
            if new:
                self.incident_id += 1
                self.incident_started = now
            pause = max(0.0, float(retry_after)) if retry_after is not None else FIXED_SERVICE_RETRY_SECONDS
            self.blocked_until = max(self.blocked_until, now+pause)
            self.blocked_until_wall = max(self.blocked_until_wall, wall+pause)
            self.last_signal_wall = wall
            if retry_after is not None:
                self.server_until_wall = max(self.server_until_wall, wall+pause)
            self.wait_source = 'server' if self.server_until_wall >= self.blocked_until_wall else 'fixed'
            self.reason = reason
            self.generation += 1
            self.condition.notify_all()
            return self.remaining(), self.incident_id, self.blocked_until_wall, new
    def pause_for_rate_limit(self, retry_after=None, reason='HTTP 429'):
        return self.signal_rate_limit(retry_after, reason)[0]
    def restore_service_wait(self, detail):
        eligible = saved_service_eligibility(detail)
        with self.condition:
            remaining = max(0.0, eligible-time.time())
            if remaining:
                self.blocked_until = max(self.blocked_until, time.monotonic()+remaining)
                self.blocked_until_wall = max(self.blocked_until_wall, eligible)
                self.server_until_wall = max(self.server_until_wall, float(detail.get('server_eligible_at_epoch') or 0))
                self.last_signal_wall = float(detail.get('rate_limit_signal_at_epoch') or 0)
                self.wait_source = str(detail.get('wait_source') or 'legacy_unknown')
                self.reason = f"HTTP {detail.get('http_status') or 429}"
                self.generation += 1
                self.condition.notify_all()
    def remaining(self):
        with self.condition:
            return max(0.0, self.blocked_until-time.monotonic())
    def snapshot(self):
        with self.condition:
            return {'remaining': self.remaining(), 'incidents': self.incident_id, 'incident_id': self.incident_id,
                    'incident_elapsed': max(0.0, time.monotonic()-self.incident_started) if self.incident_started else 0.0,
                    'reason': self.reason, 'probe_required': False, 'probe_inflight': False,
                    'eligible_at_epoch': self.blocked_until_wall, 'wait_source': self.wait_source,
                    'server_eligible_at_epoch': self.server_until_wall, 'rate_limit_signal_at_epoch': self.last_signal_wall}
    def service_wait_detail(self):
        state = self.snapshot()
        return ({'reason_code': 'service_rate_limit', 'http_status': 503 if '503' in self.reason else 429,
                 **{key: state[key] for key in ('eligible_at_epoch','incident_id','wait_source',
                    'server_eligible_at_epoch','rate_limit_signal_at_epoch')}} if state['remaining'] else {})
    def configure(self, base_pause, max_pause):
        pass

def shared_host_gate(base_pause=5.0, max_pause=5.0, key=WAYBACK_HOST_GATE_KEY):
    normalized = str(key or WAYBACK_HOST_GATE_KEY).casefold()
    with _shared_rate_lock:
        return _shared_host_gates.setdefault(normalized, SharedHostGate())

def reset_shared_traffic_state_for_tests():
    with _shared_rate_lock:
        _shared_rate_states.clear()
        _shared_host_gates.clear()
