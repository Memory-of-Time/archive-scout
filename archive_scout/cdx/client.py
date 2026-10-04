from __future__ import annotations

import contextlib
import random
import re
import threading
import time
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Callable, Iterable, TypeAlias

import httpx
import urllib3

from ..constants import RETRYABLE_STATUS
from ..downloads.rate_limit import FixedRateLimiter, RecoveryDeadlineExceeded, SharedHostGate
from ..events import ConnectivityPaused, Stopped
from ..json_codec import JSONDecodeErrors, loads as json_loads
from ..network.transports import (
    BackendsCoolingDown,
    RedirectPolicyError,
    ResilientTransport,
    InvalidRangeResponse,
    PreviewRejected,
    RequestAdmissionRejected,
    ServiceStatusResponse,
    TransportExhaustedError,
    is_transport_connection_failure,
    is_local_storage_error,
    is_transport_read_timeout,
    is_transport_timeout,
)
from ..runtime import ensure_frozen_bundle_available, frozen_bundle_error_from_exception, is_missing_frozen_bundle_error
from ..utils import clean_space, cdx_request_url


class TransientRequestError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status: int | None = None,
        timed_out: bool = False,
        read_timed_out: bool = False,
        connection_failed: bool = False,
        splittable: bool = False,
        endpoint: str | None = None,
        category: str = "",
        backend: str = "",
    ) -> None:
        super().__init__(message)
        self.status = status
        self.read_timed_out = bool(read_timed_out)
        self.connection_failed = bool(connection_failed)
        self.timed_out = bool(timed_out or read_timed_out)
        self.splittable = splittable
        self.endpoint = endpoint
        self.category = str(category or "")
        self.backend = str(backend or "")


class MalformedCDXResponse(TransientRequestError):
    """A successful CDX response whose body cannot be parsed safely."""


class PermanentRequestError(RuntimeError):
    """A deterministic HTTP rejection that should not enter transient retry loops."""

    def __init__(self, message: str, *, status: int, category: str = "http_client_error") -> None:
        super().__init__(message)
        self.status = int(status)
        self.category = category


class RateLimitDeferred(TransientRequestError):
    """Raised only after an optional server-directed wait budget is exhausted."""

    def __init__(
        self,
        message: str,
        *,
        status: int = 429,
        waited: float = 0.0,
        eligible_at_epoch: float | None = None,
        incident_id: int | None = None,
        reason_code: str = "service_rate_limit",
    ) -> None:
        super().__init__(message, status=status, splittable=False)
        self.waited = float(waited)
        self.eligible_at_epoch = float(eligible_at_epoch) if eligible_at_epoch else None
        self.incident_id = int(incident_id) if incident_id is not None else None
        self.reason_code = str(reason_code or "service_rate_limit")

    def to_detail(self) -> dict[str, object]:
        return {
            "reason_code": self.reason_code,
            "http_status": self.status,
            "waited_seconds": max(0.0, self.waited),
            "eligible_at_epoch": self.eligible_at_epoch,
            "incident_id": self.incident_id,
        }


class _CombinedStopEvent:
    """Event facade set when either the user or operation-local event is set."""

    def __init__(self, *events: threading.Event) -> None:
        self.events = tuple(events)

    def is_set(self) -> bool:
        return any(event.is_set() for event in self.events)

    def wait(self, timeout: float | None = None) -> bool:
        if self.is_set():
            return True
        if timeout is not None and timeout <= 0:
            return self.is_set()
        deadline = None if timeout is None else time.monotonic() + timeout
        while not self.is_set():
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return self.is_set()
                step = min(0.1, remaining)
            else:
                step = 0.1
            self.events[0].wait(step)
        return True


CDXRow: TypeAlias = tuple[str, str, str, str, str, str, str]


@dataclass(slots=True)
class CDXRows:
    """Compact CDX rows in timestamp/original/mimetype/status/digest/length order."""

    rows: list[CDXRow]
    resume_key: str | None = None


def is_timeout_error(exc: BaseException) -> bool:
    if isinstance(exc, TransportExhaustedError):
        return exc.timed_out
    current: BaseException | None = exc
    visited: set[int] = set()
    timeout_types = (
        TimeoutError,
        httpx.TimeoutException,
        urllib3.exceptions.TimeoutError,
        urllib3.exceptions.ReadTimeoutError,
        urllib3.exceptions.ConnectTimeoutError,
    )
    while current is not None and id(current) not in visited:
        visited.add(id(current))
        if isinstance(current, timeout_types):
            return True
        reason = getattr(current, "reason", None)
        if isinstance(reason, BaseException) and reason is not current:
            current = reason
            continue
        current = current.__cause__ or current.__context__
    return False


class HttpClient:
    """Wayback-aware HTTP client with independent connection fallbacks.

    The retry policy, shared 429 circuit, and fixed user-selected pacing remain in
    this class. Actual I/O is delegated to a persistent transport that can switch
    between httpx, urllib3, and the operating system's curl stack after genuine
    connection failures. An HTTP response never causes a backend switch; it is
    handled here so all workers follow the same Wayback policy.
    """

    def __init__(
        self,
        limiter: FixedRateLimiter,
        retries: int,
        timeout: float,
        user_agent: str,
        stop_event: threading.Event,
        retry_callback: Callable[[int, int, str, float], None] | None = None,
        *,
        connect_timeout: float | None = None,
        read_timeout: float | None = None,
        pool_size: int = 4,
        host_gate: SharedHostGate | None = None,
        rate_limit_attempts: int = 0,
        rate_limit_max_wait: float = 0.0,
        network_backend: str = "auto",
        trust_environment: bool = True,
        network_callback: Callable[[str], None] | None = None,
        rate_event_callback: Callable[[dict[str, object]], None] | None = None,
        transport: ResilientTransport | None = None,
        connection_failure_pause_threshold: int = 0,
        connection_retry_seconds: float = 3.0,
    ) -> None:
        self.limiter = limiter
        self.retries = max(1, int(retries))
        self.timeout = max(1.0, float(timeout))
        self.connect_timeout = max(1.0, float(connect_timeout if connect_timeout is not None else timeout))
        self.read_timeout = max(1.0, float(read_timeout if read_timeout is not None else timeout))
        self.user_agent = user_agent
        self.stop_event = stop_event
        self.retry_callback = retry_callback
        self.rate_event_callback = rate_event_callback
        self.connection_failure_pause_threshold = max(0, int(connection_failure_pause_threshold))
        self.connection_retry_seconds = max(0.1, float(connection_retry_seconds))
        self.host_gate = host_gate or SharedHostGate()
        self.rate_limit_attempts = max(0, int(rate_limit_attempts))
        self.rate_limit_max_wait = max(0.0, float(rate_limit_max_wait))
        self.endpoint_lock = threading.Lock()
        self.endpoint_last_success: str | None = None
        self.endpoint_cooldown_until: dict[str, float] = {}
        self.metrics_lock = threading.Lock()
        self._metrics = {
            "logical_requests": 0,
            "request_starts": 0,
            "wire_request_starts": 0,
            "request_completions": 0,
            "request_failures": 0,
            "network_bytes": 0,
            "retry_waits": 0,
            "rate_limit_events": 0,
            "pacing_wait_seconds": 0.0,
            "host_gate_wait_seconds": 0.0,
            "retry_wait_seconds": 0.0,
            "rate_limit_wait_seconds": 0.0,
            "network_seconds": 0.0,
        }
        self.transport = transport or ResilientTransport(
            pool_size=max(1, int(pool_size)),
            connect_timeout=self.connect_timeout,
            read_timeout=self.read_timeout,
            mode=network_backend,
            trust_env=trust_environment,
            callback=network_callback,
        )
        self._permit_local = threading.local()
        self._transport_attempt_hooks = callable(getattr(self.transport, "set_attempt_context_factory", None))
        if self._transport_attempt_hooks:
            self.transport.set_attempt_context_factory(self._wire_attempt)

    def close(self) -> None:
        try:
            self.transport.close()
        finally:
            close_limiter = getattr(self.limiter, "close", None)
            if callable(close_limiter):
                close_limiter()

    def _metric_add(self, name: str, value: float = 1.0) -> None:
        with self.metrics_lock:
            self._metrics[name] = self._metrics.get(name, 0.0) + value

    def metrics_snapshot(self) -> dict[str, float | int]:
        """Return admission/completion timing for this logical HTTP client.

        Executor submission is intentionally not counted as a request start. A
        start is recorded immediately before the transport performs network I/O.
        This makes GUI/automation throughput counters useful when diagnosing
        pacing, 429 backoff, server latency, and local scheduling separately.
        """
        with self.metrics_lock:
            values = dict(self._metrics)
        for key in ("logical_requests", "request_starts", "wire_request_starts", "request_completions", "request_failures", "network_bytes", "retry_waits", "rate_limit_events"):
            values[key] = int(values.get(key, 0))
        # Normalize accumulated duration counters at the public snapshot boundary.
        # time.monotonic() deltas can land a few binary-float ulps below an
        # exact boundary on Windows (for example 0.015 becoming
        # 0.014999999999986358), which makes otherwise-correct metrics behave
        # inconsistently across supported Python/OS combinations. Nanosecond
        # precision is already finer than these operational counters require.
        for key in (
            "pacing_wait_seconds",
            "host_gate_wait_seconds",
            "retry_wait_seconds",
            "rate_limit_wait_seconds",
            "network_seconds",
        ):
            values[key] = round(float(values.get(key, 0.0)), 9)
        return values

    def _active_stop_event(self):
        local_cancel = getattr(self._permit_local, "cancel_event", None)
        if local_cancel is None:
            return self.stop_event
        return _CombinedStopEvent(self.stop_event, local_cancel)

    @contextlib.contextmanager
    def cancellation_scope(self, cancel_event: threading.Event):
        """Make admission, limiter waits, and transport I/O observe local cancel."""
        previous = getattr(self._permit_local, "cancel_event", None)
        self._permit_local.cancel_event = cancel_event
        try:
            yield
        finally:
            if previous is None:
                try:
                    del self._permit_local.cancel_event
                except AttributeError:
                    pass
            else:
                self._permit_local.cancel_event = previous

    def _emit_rate_event(self, phase: str, **detail: object) -> None:
        if self.rate_event_callback is None:
            return
        payload: dict[str, object] = {
            "phase": phase,
            "reason_code": "service_rate_limit",
        }
        payload.update(detail)
        try:
            limiter_snapshot = getattr(self.limiter, "snapshot", lambda: {})()
        except Exception:
            limiter_snapshot = {}
        payload.setdefault("effective_spacing_seconds", limiter_snapshot.get("effective_delay"))
        payload.setdefault("wire_request_starts", self.metrics_snapshot().get("wire_request_starts", 0))
        self.rate_event_callback(payload)

    def _gate_snapshot(self) -> dict[str, object]:
        snapshot = getattr(self.host_gate, "snapshot", None)
        if not callable(snapshot):
            return {}
        try:
            value = snapshot()
        except Exception:
            return {}
        return dict(value) if isinstance(value, dict) else {}

    def _signal_rate_limit(self, status: int, retry_after: float | None, rate_attempt: int) -> tuple[float, int, float]:
        signal = getattr(self.host_gate, "signal_rate_limit", None)
        if callable(signal):
            wait_seconds, incident_id, eligible_at_epoch, _new_incident = signal(retry_after, f"HTTP {status}")
            return float(wait_seconds), int(incident_id), float(eligible_at_epoch)
        # Compatibility for integrations/tests implementing the pre-v1.0.1 gate
        # protocol. Production SharedHostGate always takes the typed path above.
        wait_seconds = float(self.host_gate.pause_for_rate_limit(retry_after, f"HTTP {status}"))
        return wait_seconds, int(rate_attempt), time.time() + max(0.0, wait_seconds)

    def _note_connection_success(self) -> None:
        note = getattr(self.host_gate, "note_connection_success", None)
        if callable(note):
            note()

    def _raise_if_common_connection_outage(self, exc: BaseException) -> None:
        if self.connection_failure_pause_threshold <= 0:
            return
        if not bool(getattr(exc, "connection_failed", False)) and not is_transport_connection_failure(exc):
            return
        note = getattr(self.host_gate, "note_connection_failure", None)
        if not callable(note):
            return
        count, should_pause = note(self.connection_failure_pause_threshold)
        if should_pause:
            raise ConnectivityPaused(
                "Repeated connection failures indicate a common network/Wayback outage "
                f"({count} consecutive connection failures). Untouched work remains pending; "
                "check connectivity and Resume later."
            ) from exc

    def _handle_early_service_status(self, exc: ServiceStatusResponse, rate_attempt: int) -> int:
        """Apply live 429/503 recovery using headers before any response body is read."""
        self._note_connection_success()
        status = int(exc.status)
        retry_after_header = exc.headers.get("retry-after") or exc.headers.get("Retry-After")
        retry_after = parse_retry_after(retry_after_header)
        rate_attempt += 1
        wait_seconds, incident_id, eligible_at_epoch = self._signal_rate_limit(status, retry_after, rate_attempt)
        if hasattr(self.limiter, "note_rate_limit"):
            self.limiter.note_rate_limit(incident_id)
        self._metric_add("rate_limit_events")
        gate_state = self._gate_snapshot()
        self._emit_rate_event(
            "cooldown",
            http_status=status,
            incident_id=incident_id,
            wait_seconds=wait_seconds,
            eligible_at_epoch=eligible_at_epoch,
            attempt=rate_attempt,
            attempt_limit=self.rate_limit_attempts,
        )
        if self.retry_callback and self.rate_event_callback is None:
            self.retry_callback(
                rate_attempt,
                self.rate_limit_attempts,
                f"HTTP {status}; Wayback service quota/overload cooldown active",
                wait_seconds,
            )
        if self.rate_limit_attempts > 0 and rate_attempt >= self.rate_limit_attempts:
            deferred = RateLimitDeferred(
                f"Wayback continued returning HTTP {status} after {rate_attempt} coordinated pauses. Progress was saved for resume.",
                status=status,
                waited=float(gate_state.get("incident_elapsed", 0.0) or 0.0),
                eligible_at_epoch=eligible_at_epoch,
                incident_id=incident_id,
            )
            self._emit_rate_event("paused", **deferred.to_detail())
            raise deferred
        return rate_attempt

    def _wait_for_backend_cooldown(self, exc: BackendsCoolingDown) -> None:
        wait_seconds = max(0.05, float(exc.wait_seconds))
        if self.retry_callback:
            self.retry_callback(1, self.retries, "network backends cooling down", wait_seconds)
        stop_event = self._active_stop_event()
        if stop_event.wait(wait_seconds):
            raise Stopped

    def _acquire_host_permit(self):
        started = time.monotonic()
        before = self._gate_snapshot()
        deadline_getter = getattr(self.host_gate, "recovery_deadline", None)
        deadline = deadline_getter(self.rate_limit_max_wait) if callable(deadline_getter) else None
        try:
            if deadline is None:
                permit = self.host_gate.acquire_request(self._active_stop_event())
            else:
                try:
                    permit = self.host_gate.acquire_request(self._active_stop_event(), deadline=deadline)
                except TypeError:
                    permit = self.host_gate.acquire_request(self._active_stop_event())
        except RecoveryDeadlineExceeded as exc:
            elapsed = time.monotonic() - started
            self._metric_add("host_gate_wait_seconds", elapsed)
            self._metric_add("rate_limit_wait_seconds", elapsed)
            status_match = re.search(r"HTTP\s+(429|503)", exc.reason or "", re.I)
            status = int(status_match.group(1)) if status_match else 429
            deferred = RateLimitDeferred(
                "Wayback service recovery exceeded the configured automatic wait budget. "
                "Progress was saved; Resume will not issue another request before the server cooldown is eligible.",
                status=status,
                waited=exc.waited,
                eligible_at_epoch=exc.eligible_at_epoch,
                incident_id=exc.incident_id,
            )
            self._emit_rate_event("paused", **deferred.to_detail())
            raise deferred from exc
        elapsed = time.monotonic() - started
        self._metric_add("host_gate_wait_seconds", elapsed)
        if float(before.get("remaining", 0.0) or 0.0) > 0 or bool(before.get("probe_required")):
            self._metric_add("rate_limit_wait_seconds", elapsed)
        return permit

    @contextlib.contextmanager
    def _wire_attempt(self):
        """Admit and measure one actual transport/backend/redirect attempt."""
        pace_started = time.monotonic()
        with self.limiter.slot(self._active_stop_event()):
            self._metric_add("pacing_wait_seconds", time.monotonic() - pace_started)
            permit = getattr(self._permit_local, "permit", None)
            if permit is not None and not self.host_gate.permit_is_current(permit):
                raise RequestAdmissionRejected("shared Wayback gate changed before this wire attempt")
            self._metric_add("request_starts")
            self._metric_add("wire_request_starts")
            network_started = time.monotonic()
            try:
                yield
            except BaseException:
                self._metric_add("request_failures")
                raise
            else:
                self._metric_add("request_completions")
            finally:
                self._metric_add("network_seconds", time.monotonic() - network_started)

    def _transport_request(self, url: str, headers: dict[str, str], max_bytes: int):
        stop_event = self._active_stop_event()
        if self._transport_attempt_hooks:
            return self.transport.request(url, headers, max_bytes, stop_event)
        with self._wire_attempt():
            return self.transport.request(url, headers, max_bytes, stop_event)

    def _transport_download(self, url: str, headers: dict[str, str], destination: Path, max_bytes: int, **kwargs):
        stop_event = self._active_stop_event()
        if self._transport_attempt_hooks:
            return self.transport.download(url, headers, destination, max_bytes, stop_event, **kwargs)
        with self._wire_attempt():
            return self.transport.download(url, headers, destination, max_bytes, stop_event, **kwargs)

    @staticmethod
    def _is_archived_memento(headers: dict[str, str], url: str) -> bool:
        if "/web/" not in str(url):
            return False
        lowered = {str(k).casefold(): str(v) for k, v in headers.items()}
        return bool(lowered.get("memento-datetime"))

    @staticmethod
    def _probe_recovered(status: int, *, archived_memento: bool = False) -> bool:
        if archived_memento:
            return True
        return int(status) not in {429, 500, 502, 503, 504}

    def get(self, url: str, max_bytes: int, accept: str = "*/*") -> dict:
        headers = {
            "User-Agent": self.user_agent,
            "Accept": accept,
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Accept-Language": "en-US,en;q=0.8",
        }
        ensure_frozen_bundle_available()
        generic_attempt = 0
        rate_attempt = 0
        self._metric_add("logical_requests")

        while True:
            ensure_frozen_bundle_available()
            permit = self._acquire_host_permit()
            try:
                if not self.host_gate.permit_is_current(permit):
                    self.host_gate.finish_request(permit, recovered=False)
                    continue
                self._permit_local.permit = permit
                try:
                    response = self._transport_request(url, headers, max_bytes)
                finally:
                    self._permit_local.permit = None
                self._metric_add("network_bytes", len(response.data))
                self._note_connection_success()
                status = int(response.status)
                retry_after_header = response.headers.get("retry-after") or response.headers.get("Retry-After")
                archived_memento = self._is_archived_memento(response.headers, response.final_url or url)

                if status in {429, 503} and not archived_memento:
                    retry_after = parse_retry_after(retry_after_header)
                    rate_attempt += 1
                    wait_seconds, incident_id, eligible_at_epoch = self._signal_rate_limit(status, retry_after, rate_attempt)
                    if hasattr(self.limiter, "note_rate_limit"):
                        self.limiter.note_rate_limit(incident_id)
                    self._metric_add("rate_limit_events")
                    gate_state = self._gate_snapshot()
                    rate_detail = {
                        "http_status": status,
                        "incident_id": incident_id,
                        "wait_seconds": wait_seconds,
                        "eligible_at_epoch": eligible_at_epoch,
                        "attempt": rate_attempt,
                        "attempt_limit": self.rate_limit_attempts,
                    }
                    self._emit_rate_event("cooldown", **rate_detail)
                    if self.retry_callback and self.rate_event_callback is None:
                        self.retry_callback(
                            rate_attempt,
                            self.rate_limit_attempts,
                            f"HTTP {status}; Wayback service quota/overload cooldown active",
                            wait_seconds,
                        )
                    attempts_exhausted = self.rate_limit_attempts > 0 and rate_attempt >= self.rate_limit_attempts
                    if attempts_exhausted:
                        deferred = RateLimitDeferred(
                            f"Wayback continued returning HTTP {status} after {rate_attempt} coordinated pauses. Progress was saved for resume.",
                            status=status,
                            waited=float(gate_state.get("incident_elapsed", 0.0) or 0.0),
                            eligible_at_epoch=eligible_at_epoch,
                            incident_id=incident_id,
                        )
                        self._emit_rate_event("paused", **deferred.to_detail())
                        raise deferred
                    continue

                recovered = self._probe_recovered(status, archived_memento=archived_memento)
                self.host_gate.finish_request(permit, recovered=recovered)
                if recovered and hasattr(self.limiter, "note_healthy_response"):
                    self.limiter.note_healthy_response()

                if status >= 400:
                    # A replay URL can faithfully reproduce an origin-side 4xx/5xx.
                    # That historical status is capture content, not evidence that the
                    # live Wayback service is currently throttling or unhealthy.
                    if archived_memento:
                        raise PermanentRequestError(
                            f"Archived origin HTTP {status}: {url}",
                            status=status,
                            category="archived_origin_http_error",
                        )
                    if status not in RETRYABLE_STATUS:
                        runtime_error = str(
                            response.headers.get("x-archive-wayback-runtime-error")
                            or response.headers.get("X-Archive-Wayback-Runtime-Error")
                            or ""
                        )
                        try:
                            body_preview = bytes(response.data[:4096]).decode("utf-8", "ignore")
                        except Exception:
                            body_preview = ""
                        combined = (runtime_error + " " + body_preview).casefold()
                        if status == 403 and "robots.txt" in combined:
                            category = "robots_blocked"
                        elif status == 403 and (
                            "blocked site error" in combined
                            or "excluded from the wayback machine" in combined
                        ):
                            category = "wayback_excluded"
                        elif status == 403:
                            category = "wayback_forbidden"
                        elif status == 404:
                            category = "missing_capture"
                        elif archived_memento:
                            category = "archived_origin_http_error"
                        else:
                            category = "http_client_error"
                        detail = clean_space(runtime_error)[:300]
                        suffix = f" — {detail}" if detail else ""
                        raise PermanentRequestError(
                            f"HTTP {status}: {url}{suffix}", status=status, category=category
                        )
                    generic_attempt += 1
                    if generic_attempt >= self.retries:
                        raise TransientRequestError(
                            f"HTTP {status} after {self.retries} attempts: {url}",
                            status=status,
                            splittable=(not archived_memento and status in {408, 500, 502, 503, 504}),
                        )
                    self.retry_wait(generic_attempt - 1, f"HTTP {status}", parse_retry_after(retry_after_header))
                    continue

                return {
                    "data": response.data,
                    "status": status,
                    "headers": response.headers,
                    "final_url": response.final_url,
                    "backend": response.backend,
                    "elapsed": response.elapsed,
                }
            except ServiceStatusResponse as exc:
                self.host_gate.finish_request(permit, recovered=False)
                rate_attempt = self._handle_early_service_status(exc, rate_attempt)
                continue
            except BackendsCoolingDown as exc:
                self.host_gate.finish_request(permit, recovered=False)
                self._wait_for_backend_cooldown(exc)
                continue
            except RedirectPolicyError:
                self.host_gate.finish_request(permit, recovered=True)
                raise
            except RequestAdmissionRejected:
                self.host_gate.finish_request(permit, recovered=False)
                continue
            except (RateLimitDeferred, Stopped):
                self.host_gate.finish_request(permit, recovered=False)
                raise
            except RuntimeError as exc:
                self.host_gate.finish_request(permit, recovered=False)
                if isinstance(exc, TransientRequestError):
                    raise
                if is_missing_frozen_bundle_error(exc):
                    raise frozen_bundle_error_from_exception(exc) from exc
                if isinstance(exc, TransportExhaustedError):
                    timed_out = is_timeout_error(exc)
                    read_timed_out = bool(getattr(exc, "read_timed_out", False))
                    self._raise_if_common_connection_outage(exc)
                    generic_attempt += 1
                    if generic_attempt >= self.retries:
                        raise TransientRequestError(
                            f"network failure for {url}: {exc}",
                            timed_out=timed_out,
                            read_timed_out=read_timed_out,
                            connection_failed=bool(getattr(exc, "connection_failed", False)),
                            splittable=True,
                        ) from exc
                    reason = "read timeout" if read_timed_out else ("connection timeout" if timed_out else str(exc))
                    self.retry_wait(generic_attempt - 1, reason)
                    continue
                if str(exc).startswith("response exceeds"):
                    raise TransientRequestError(
                        f"CDX response was larger than the safe in-memory budget for {url}: {exc}",
                        splittable=True,
                    ) from exc
                raise
            except (httpx.HTTPError, urllib3.exceptions.HTTPError, TimeoutError, OSError) as exc:
                self.host_gate.finish_request(permit, recovered=False)
                if is_local_storage_error(exc):
                    raise
                if is_missing_frozen_bundle_error(exc):
                    raise frozen_bundle_error_from_exception(exc) from exc
                timed_out = is_timeout_error(exc)
                read_timed_out = is_transport_read_timeout(exc)
                self._raise_if_common_connection_outage(exc)
                generic_attempt += 1
                if generic_attempt >= self.retries:
                    raise TransientRequestError(
                        f"network failure for {url}: {exc}",
                        timed_out=timed_out,
                        read_timed_out=read_timed_out,
                        connection_failed=is_transport_connection_failure(exc),
                        splittable=True,
                    ) from exc
                reason = "read timeout" if read_timed_out else ("connection timeout" if timed_out else str(exc))
                self.retry_wait(generic_attempt - 1, reason)
            except Exception:
                self.host_gate.finish_request(permit, recovered=False)
                raise

    def download_to_path(
        self,
        url: str,
        destination: Path,
        max_bytes: int,
        accept: str = "*/*",
        *,
        compute_hash: bool = True,
        preview_validator: Callable[[dict[str, str], bytes], str | None] | None = None,
        redirect_validator: Callable[[str, str], None] | None = None,
    ) -> dict:
        """Stream a replay response to disk under the shared Wayback policy."""
        headers = {
            "User-Agent": self.user_agent,
            "Accept": accept,
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Accept-Language": "en-US,en;q=0.8",
        }
        ensure_frozen_bundle_available()
        generic_attempt = 0
        rate_attempt = 0
        destination = Path(destination)
        range_restarts = 0
        self._metric_add("logical_requests")

        while True:
            ensure_frozen_bundle_available()
            existing_size = destination.stat().st_size if destination.exists() else 0
            request_headers = dict(headers)
            request_headers["Accept-Encoding"] = "identity"
            if existing_size > 0:
                request_headers["Range"] = f"bytes={existing_size}-"
            permit = self._acquire_host_permit()
            try:
                if not self.host_gate.permit_is_current(permit):
                    self.host_gate.finish_request(permit, recovered=False)
                    continue
                self._permit_local.permit = permit
                try:
                    if compute_hash:
                        response = self._transport_download(
                            url, request_headers, destination, max_bytes,
                            preview_validator=preview_validator, redirect_validator=redirect_validator,
                        )
                    else:
                        response = self._transport_download(
                            url, request_headers, destination, max_bytes,
                            compute_hash=False, preview_validator=preview_validator, redirect_validator=redirect_validator,
                        )
                except TypeError as exc:
                    # Preserve lightweight third-party/test transports that do not
                    # yet implement the optional preview/hash keyword arguments.
                    message = str(exc)
                    if "preview_validator" not in message and "compute_hash" not in message:
                        raise
                    if compute_hash:
                        response = self._transport_download(
                            url, request_headers, destination, max_bytes,
                        )
                    else:
                        try:
                            response = self._transport_download(
                                url, request_headers, destination, max_bytes,
                                compute_hash=False,
                            )
                        except TypeError as nested:
                            if "compute_hash" not in str(nested):
                                raise
                            response = self._transport_download(
                                url, request_headers, destination, max_bytes,
                            )
                finally:
                    self._permit_local.permit = None
                self._metric_add("network_bytes", int(getattr(response, "bytes_written", 0) or 0))
                self._note_connection_success()
                status = int(response.status)
                archived_memento = self._is_archived_memento(response.headers, response.final_url or url)
                if status == 416 and existing_size:
                    raise InvalidRangeResponse("server rejected saved replay offset; restarting complete file")
                if status < 200 and status < 400:
                    raise TransientRequestError(f"unexpected replay HTTP {status}: {url}")

                retry_after_header = response.headers.get("retry-after") or response.headers.get("Retry-After")
                if status in {429, 503} and not archived_memento:
                    destination.unlink(missing_ok=True)
                    retry_after = parse_retry_after(retry_after_header)
                    rate_attempt += 1
                    wait_seconds, incident_id, eligible_at_epoch = self._signal_rate_limit(status, retry_after, rate_attempt)
                    if hasattr(self.limiter, "note_rate_limit"):
                        self.limiter.note_rate_limit(incident_id)
                    self._metric_add("rate_limit_events")
                    gate_state = self._gate_snapshot()
                    self._emit_rate_event(
                        "cooldown",
                        http_status=status,
                        incident_id=incident_id,
                        wait_seconds=wait_seconds,
                        eligible_at_epoch=eligible_at_epoch,
                        attempt=rate_attempt,
                        attempt_limit=self.rate_limit_attempts,
                    )
                    if self.retry_callback and self.rate_event_callback is None:
                        self.retry_callback(
                            rate_attempt, self.rate_limit_attempts,
                            f"HTTP {status}; Wayback service quota/overload cooldown active", wait_seconds,
                        )
                    attempts_exhausted = self.rate_limit_attempts > 0 and rate_attempt >= self.rate_limit_attempts
                    if attempts_exhausted:
                        deferred = RateLimitDeferred(
                            f"Wayback continued returning HTTP {status} after {rate_attempt} coordinated pauses. Progress was saved for resume.",
                            status=status,
                            waited=float(gate_state.get("incident_elapsed", 0.0) or 0.0),
                            eligible_at_epoch=eligible_at_epoch,
                            incident_id=incident_id,
                        )
                        self._emit_rate_event("paused", **deferred.to_detail())
                        raise deferred
                    continue

                recovered = self._probe_recovered(status, archived_memento=archived_memento)
                self.host_gate.finish_request(permit, recovered=recovered)
                if recovered and hasattr(self.limiter, "note_healthy_response"):
                    self.limiter.note_healthy_response()

                if status >= 400 and not archived_memento:
                    destination.unlink(missing_ok=True)
                    if status not in RETRYABLE_STATUS:
                        raise PermanentRequestError(
                            f"HTTP {status}: {url}", status=status,
                            category="missing_capture" if status == 404 else "http_client_error",
                        )
                    generic_attempt += 1
                    if generic_attempt >= self.retries:
                        raise TransientRequestError(
                            f"HTTP {status} after {self.retries} attempts: {url}",
                            status=status, splittable=False,
                        )
                    self.retry_wait(generic_attempt - 1, f"HTTP {status}", parse_retry_after(retry_after_header))
                    continue

                return {
                    "path": response.path,
                    "bytes": response.bytes_written,
                    "content_hash": response.content_hash,
                    "preview": response.preview,
                    "status": status,
                    "headers": response.headers,
                    "final_url": response.final_url,
                    "backend": response.backend,
                    "elapsed": response.elapsed,
                    "archived_origin_status": bool(archived_memento and status >= 400),
                }
            except ServiceStatusResponse as exc:
                self.host_gate.finish_request(permit, recovered=False)
                rate_attempt = self._handle_early_service_status(exc, rate_attempt)
                continue
            except BackendsCoolingDown as exc:
                self.host_gate.finish_request(permit, recovered=False)
                self._wait_for_backend_cooldown(exc)
                continue
            except RedirectPolicyError:
                self.host_gate.finish_request(permit, recovered=True)
                raise
            except PreviewRejected:
                self.host_gate.finish_request(permit, recovered=True)
                destination.unlink(missing_ok=True)
                raise
            except InvalidRangeResponse as exc:
                self.host_gate.finish_request(permit, recovered=False)
                destination.unlink(missing_ok=True)
                range_restarts += 1
                if range_restarts > 1:
                    raise TransientRequestError(str(exc), splittable=False) from exc
                if self.retry_callback:
                    self.retry_callback(1, 1, str(exc), 0.0)
                continue
            except RateLimitDeferred:
                destination.unlink(missing_ok=True)
                self.host_gate.finish_request(permit, recovered=False)
                raise
            except RequestAdmissionRejected:
                self.host_gate.finish_request(permit, recovered=False)
                continue
            except Stopped:
                self.host_gate.finish_request(permit, recovered=False)
                raise
            except RuntimeError as exc:
                self.host_gate.finish_request(permit, recovered=False)
                if isinstance(exc, TransientRequestError):
                    raise
                if is_missing_frozen_bundle_error(exc):
                    destination.unlink(missing_ok=True)
                    raise frozen_bundle_error_from_exception(exc) from exc
                if isinstance(exc, TransportExhaustedError):
                    timed_out = is_timeout_error(exc)
                    read_timed_out = bool(getattr(exc, "read_timed_out", False))
                    self._raise_if_common_connection_outage(exc)
                    generic_attempt += 1
                    if generic_attempt >= self.retries:
                        raise TransientRequestError(
                            f"network failure for {url}: {exc}",
                            timed_out=timed_out, read_timed_out=read_timed_out,
                            connection_failed=bool(getattr(exc, "connection_failed", False)),
                            splittable=False,
                        ) from exc
                    reason = "read timeout" if read_timed_out else ("connection timeout" if timed_out else str(exc))
                    self.retry_wait(generic_attempt - 1, reason)
                    continue
                destination.unlink(missing_ok=True)
                raise
            except (httpx.HTTPError, urllib3.exceptions.HTTPError, TimeoutError, OSError) as exc:
                self.host_gate.finish_request(permit, recovered=False)
                if is_local_storage_error(exc):
                    raise
                if is_missing_frozen_bundle_error(exc):
                    destination.unlink(missing_ok=True)
                    raise frozen_bundle_error_from_exception(exc) from exc
                timed_out = is_timeout_error(exc)
                read_timed_out = is_transport_read_timeout(exc)
                self._raise_if_common_connection_outage(exc)
                generic_attempt += 1
                if generic_attempt >= self.retries:
                    raise TransientRequestError(
                        f"network failure for {url}: {exc}", timed_out=timed_out,
                        read_timed_out=read_timed_out,
                        connection_failed=is_transport_connection_failure(exc),
                        splittable=False,
                    ) from exc
                reason = "read timeout" if read_timed_out else ("connection timeout" if timed_out else str(exc))
                self.retry_wait(generic_attempt - 1, reason)
            except Exception:
                destination.unlink(missing_ok=True)
                self.host_gate.finish_request(permit, recovered=False)
                raise

    def get_json(self, url: str, params: list[tuple[str, str]], max_bytes: int = 64 * 1024 * 1024) -> object:
        return self.get_json_any((url,), params, max_bytes=max_bytes)

    def _ordered_endpoints(self, urls: Iterable[str]) -> list[str]:
        endpoints = list(dict.fromkeys(str(url) for url in urls if str(url).strip()))
        now = time.monotonic()
        with self.endpoint_lock:
            preferred = self.endpoint_last_success
            active = [url for url in endpoints if self.endpoint_cooldown_until.get(url, 0.0) <= now]
        if not active:
            active = endpoints
        if preferred in active:
            active.remove(preferred)
            active.insert(0, preferred)
        return active

    def _remember_endpoint_success(self, endpoint: str) -> None:
        with self.endpoint_lock:
            self.endpoint_last_success = endpoint
            self.endpoint_cooldown_until.pop(endpoint, None)

    def _remember_endpoint_failure(self, endpoint: str) -> None:
        with self.endpoint_lock:
            self.endpoint_cooldown_until[endpoint] = time.monotonic() + 20.0

    @staticmethod
    def _cdx_format_attempts(endpoint: str, prefer_text: bool) -> tuple[str, str]:
        """Try an endpoint's native representation before its fallback.

        Wayback's path-specific Timemap endpoints can ignore ``output=txt`` or
        ``output=json``.  In particular, ``/web/timemap/json`` commonly returns
        JSON even when a caller asked for text.  Trying text first there made a
        valid response look malformed and caused every numbered page to be
        downloaded a second time.  The generic CDX endpoint still honors the
        caller's low-memory text preference.
        """
        path = urllib.parse.urlsplit(endpoint).path.rstrip("/").casefold()
        if path.endswith("/web/timemap/json"):
            return ("json", "text")
        if path.endswith("/web/timemap/cdx"):
            return ("text", "json")
        return ("text", "json") if prefer_text else ("json", "text")

    def get_cdx_any(
        self,
        urls: Iterable[str],
        params: list[tuple[str, str]],
        max_bytes: int = 64 * 1024 * 1024,
        *,
        prefer_text: bool = False,
    ) -> object:
        endpoints = self._ordered_endpoints(urls)
        if not endpoints:
            raise ValueError("at least one endpoint is required")
        failures: list[tuple[str, TransientRequestError]] = []
        text_params = cdx_text_fallback_params(params)

        for endpoint in endpoints:
            attempts = self._cdx_format_attempts(endpoint, prefer_text)
            first_error: BaseException | None = None
            for format_name in attempts:
                request_params = text_params if format_name == "text" else params
                full_url = cdx_request_url(endpoint, request_params)
                try:
                    accept = "text/plain,*/*" if format_name == "text" else "application/json,text/plain,*/*"
                    response = self.get(full_url, max_bytes, accept)
                    payload = parse_cdx_response_data(
                        response["data"], endpoint, request_params, format_name
                    )
                    self._remember_endpoint_success(endpoint)
                    return payload
                except MemoryError as exc:
                    raise TransientRequestError(
                        f"CDX parsing exceeded available memory at {endpoint}; retrying with smaller saved work",
                        splittable=True,
                        endpoint=endpoint,
                    ) from exc
                except MalformedCDXResponse as exc:
                    first_error = first_error or exc
                    if self.retry_callback:
                        other = "JSON" if format_name == "text" else "line-oriented text"
                        self.retry_callback(
                            1, 1,
                            f"CDX {format_name} response was malformed or truncated; retrying as {other}",
                            0.0,
                        )
                    continue
                except RateLimitDeferred:
                    raise
                except TransientRequestError as exc:
                    first_error = first_error or exc
                    # Every configured CDX endpoint uses the same Wayback host.
                    # Once every independent transport fails during connection
                    # setup, trying two more paths on that host only multiplies a
                    # DNS/proxy/TLS failure. Return control to the saved operation
                    # queue immediately so it can retry briefly and pause cleanly.
                    if exc.connection_failed or "safe in-memory budget" in str(exc):
                        self._remember_endpoint_failure(endpoint)
                        exc.endpoint = endpoint
                        raise
                    # A read timeout means Wayback accepted the request but did
                    # not finish the body. Repeating the same expensive query
                    # against every endpoint multiplies the stall; let the page
                    # queue requeue or subdivide it immediately instead.
                    if exc.read_timed_out or exc.timed_out:
                        self._remember_endpoint_failure(endpoint)
                        exc.endpoint = endpoint
                        raise
                    # Other transient failures may be endpoint-specific, so try
                    # the next service without reissuing another representation.
                    break
                except RuntimeError as exc:
                    if str(exc).startswith("HTTP ") or str(exc).startswith("response exceeds"):
                        raise
                    first_error = first_error or exc
                    continue

            self._remember_endpoint_failure(endpoint)
            if isinstance(first_error, TransientRequestError):
                failure = first_error
            else:
                failure = MalformedCDXResponse(
                    f"CDX response was unusable at {endpoint}: {first_error}",
                    splittable=True,
                    endpoint=endpoint,
                )
            failure.endpoint = endpoint
            failures.append((endpoint, failure))
            if self.retry_callback and len(endpoints) > 1:
                self.retry_callback(1, len(endpoints), f"Endpoint unavailable: {endpoint}; trying alternate CDX service", 0.0)

        timed_out = any(exc.timed_out for _, exc in failures)
        splittable = any(exc.splittable for _, exc in failures)
        summary = "; ".join(f"{endpoint}: {exc}" for endpoint, exc in failures)
        raise TransientRequestError(
            f"all CDX endpoints failed: {summary}",
            timed_out=timed_out,
            read_timed_out=any(exc.read_timed_out for _, exc in failures),
            connection_failed=bool(failures) and all(exc.connection_failed for _, exc in failures),
            splittable=splittable or timed_out,
        ) from (failures[-1][1] if failures else None)

    def get_cdx_rows_any(
        self,
        urls: Iterable[str],
        params: list[tuple[str, str]],
        max_bytes: int = 64 * 1024 * 1024,
        *,
        prefer_text: bool = True,
    ) -> CDXRows:
        """Fetch CDX rows without constructing a second list of per-row dicts.

        Large 50,000-row responses previously existed simultaneously as raw
        bytes, one decoded string, a list-of-lists, and a list-of-dicts. That
        multiplication was the main source of the platform-dependent crashes
        near the first large page. This path parses directly into compact tuples.
        """
        # Keep compatibility with callers and tests that replace the public
        # get_cdx_any method. Production uses the compact direct parser below;
        # an overridden legacy method is converted once into compact rows.
        legacy_getter = getattr(type(self), "get_cdx_any")
        if (
            getattr(legacy_getter, "__module__", "") != __name__
            or getattr(legacy_getter, "__name__", "") != "get_cdx_any"
        ):
            payload = self.get_cdx_any(
                urls, params, max_bytes=max_bytes, prefer_text=prefer_text
            )
            return parse_cdx_rows_payload(payload)

        endpoints = self._ordered_endpoints(urls)
        if not endpoints:
            raise ValueError("at least one endpoint is required")
        failures: list[tuple[str, TransientRequestError]] = []
        text_params = cdx_text_fallback_params(params)

        for endpoint in endpoints:
            attempts = self._cdx_format_attempts(endpoint, prefer_text)
            first_error: BaseException | None = None
            for format_name in attempts:
                request_params = text_params if format_name == "text" else params
                full_url = cdx_request_url(endpoint, request_params)
                try:
                    accept = "text/plain,*/*" if format_name == "text" else "application/json,text/plain,*/*"
                    response = self.get(full_url, max_bytes, accept)
                    result = parse_cdx_rows_response_data(
                        response["data"], endpoint, request_params, format_name
                    )
                    self._remember_endpoint_success(endpoint)
                    return result
                except MemoryError as exc:
                    raise TransientRequestError(
                        f"CDX parsing exceeded available memory at {endpoint}; retrying with smaller saved work",
                        splittable=True,
                        endpoint=endpoint,
                    ) from exc
                except MalformedCDXResponse as exc:
                    first_error = first_error or exc
                    if self.retry_callback:
                        other = "JSON" if format_name == "text" else "line-oriented text"
                        self.retry_callback(
                            1, 1,
                            f"CDX {format_name} response was malformed or truncated; retrying as {other}",
                            0.0,
                        )
                    continue
                except RateLimitDeferred:
                    raise
                except TransientRequestError as exc:
                    first_error = first_error or exc
                    if (
                        exc.connection_failed
                        or exc.read_timed_out
                        or exc.timed_out
                        or "safe in-memory budget" in str(exc)
                    ):
                        self._remember_endpoint_failure(endpoint)
                        exc.endpoint = endpoint
                        raise
                    break
                except RuntimeError as exc:
                    if str(exc).startswith("HTTP ") or str(exc).startswith("response exceeds"):
                        raise
                    first_error = first_error or exc
                    continue

            self._remember_endpoint_failure(endpoint)
            failure = first_error if isinstance(first_error, TransientRequestError) else MalformedCDXResponse(
                f"CDX response was unusable at {endpoint}: {first_error}",
                splittable=True,
                endpoint=endpoint,
            )
            failure.endpoint = endpoint
            failures.append((endpoint, failure))
            if self.retry_callback and len(endpoints) > 1:
                self.retry_callback(1, len(endpoints), f"Endpoint unavailable: {endpoint}; trying alternate CDX service", 0.0)

        timed_out = any(exc.timed_out for _, exc in failures)
        summary = "; ".join(f"{endpoint}: {exc}" for endpoint, exc in failures)
        raise TransientRequestError(
            f"all CDX endpoints failed: {summary}",
            timed_out=timed_out,
            read_timed_out=any(exc.read_timed_out for _, exc in failures),
            connection_failed=bool(failures) and all(exc.connection_failed for _, exc in failures),
            splittable=timed_out or any(exc.splittable for _, exc in failures),
        ) from (failures[-1][1] if failures else None)

    def get_json_any(
        self,
        urls: Iterable[str],
        params: list[tuple[str, str]],
        max_bytes: int = 64 * 1024 * 1024,
    ) -> object:
        return self.get_cdx_any(urls, params, max_bytes=max_bytes, prefer_text=False)

    def get_cdx_json_any(
        self,
        urls: Iterable[str],
        params: list[tuple[str, str]],
        max_bytes: int = 64 * 1024 * 1024,
    ) -> object:
        """Fetch one native JSON representation per endpoint.

        Numbered Timemap paging is a JSON protocol.  It must not turn one bad
        page into a second text request for the same page; the page scheduler
        owns retries and durable failed-page state.
        """
        return self._get_cdx_native_json(urls, params, max_bytes, compact=False)

    def get_cdx_json_rows_any(
        self,
        urls: Iterable[str],
        params: list[tuple[str, str]],
        max_bytes: int = 64 * 1024 * 1024,
    ) -> CDXRows:
        """Compact-row native JSON fetch used by numbered Timemap pages."""
        result = self._get_cdx_native_json(urls, params, max_bytes, compact=True)
        if not isinstance(result, CDXRows):
            raise RuntimeError("native CDX JSON row parser returned an unexpected result")
        return result

    def _get_cdx_native_json(
        self,
        urls: Iterable[str],
        params: list[tuple[str, str]],
        max_bytes: int,
        *,
        compact: bool,
    ) -> object | CDXRows:
        endpoints = self._ordered_endpoints(urls)
        if not endpoints:
            raise ValueError("at least one endpoint is required")
        failures: list[tuple[str, TransientRequestError]] = []
        for endpoint in endpoints:
            full_url = cdx_request_url(endpoint, params)
            try:
                response = self.get(full_url, max_bytes, "application/json,*/*")
                payload = parse_json_response(response["data"], endpoint)
                result = parse_cdx_rows_payload(payload) if compact else payload
                self._remember_endpoint_success(endpoint)
                return result
            except MemoryError as exc:
                raise TransientRequestError(
                    f"CDX JSON parsing exceeded available memory at {endpoint}",
                    splittable=True,
                    endpoint=endpoint,
                ) from exc
            except MalformedCDXResponse as exc:
                exc.endpoint = endpoint
                failure = exc
            except RateLimitDeferred:
                raise
            except TransientRequestError as exc:
                exc.endpoint = endpoint
                if (
                    exc.connection_failed
                    or exc.read_timed_out
                    or exc.timed_out
                    or "safe in-memory budget" in str(exc)
                ):
                    self._remember_endpoint_failure(endpoint)
                    raise
                failure = exc
            except RuntimeError as exc:
                if str(exc).startswith("HTTP ") or str(exc).startswith("response exceeds"):
                    raise
                failure = MalformedCDXResponse(
                    f"CDX JSON response was unusable at {endpoint}: {exc}",
                    splittable=True,
                    endpoint=endpoint,
                )
            self._remember_endpoint_failure(endpoint)
            failures.append((endpoint, failure))
            if self.retry_callback and len(endpoints) > 1:
                self.retry_callback(
                    1,
                    len(endpoints),
                    f"Endpoint unavailable: {endpoint}; trying alternate CDX service",
                    0.0,
                )

        summary = "; ".join(f"{endpoint}: {exc}" for endpoint, exc in failures)
        raise TransientRequestError(
            f"all native JSON CDX endpoints failed: {summary}",
            timed_out=any(exc.timed_out for _, exc in failures),
            read_timed_out=any(exc.read_timed_out for _, exc in failures),
            connection_failed=bool(failures) and all(exc.connection_failed for _, exc in failures),
            splittable=any(exc.splittable for _, exc in failures),
        ) from (failures[-1][1] if failures else None)

    def retry_wait(self, attempt: int, reason: str, retry_after: float | None = None) -> None:
        base = max(float(retry_after or 0), min(120.0, 2**attempt))
        # Never schedule before a server-supplied Retry-After deadline.  Jitter
        # is positive-only so multiple clients spread out after that minimum.
        wait_seconds = base * random.uniform(1.0, 1.2)
        self._metric_add("retry_waits")
        self._metric_add("retry_wait_seconds", wait_seconds)
        if self.retry_callback:
            self.retry_callback(attempt + 2, self.retries, reason, wait_seconds)
        stop_event = self._active_stop_event()
        stop_event.wait(wait_seconds)
        if stop_event.is_set():
            raise Stopped



def request_cdx_rows(
    client: object,
    urls: Iterable[str],
    params: list[tuple[str, str]],
    max_bytes: int = 64 * 1024 * 1024,
    *,
    prefer_text: bool = True,
) -> CDXRows:
    """Use the compact row API while retaining compatibility with clients.

    Third-party integrations and the long-standing test/mocking surface may
    implement only ``get_cdx_any``. Converting that legacy payload here keeps
    those clients working while the built-in HttpClient takes the low-memory
    direct parsing path.
    """
    compact_getter = getattr(client, "get_cdx_rows_any", None)
    if callable(compact_getter):
        return compact_getter(
            urls, params, max_bytes=max_bytes, prefer_text=prefer_text
        )
    legacy_getter = getattr(client, "get_cdx_any")
    payload = legacy_getter(
        urls, params, max_bytes=max_bytes, prefer_text=prefer_text
    )
    return parse_cdx_rows_payload(payload)


def _uses_builtin_cdx_getter(client: object) -> bool:
    getter = getattr(type(client), "get_cdx_any", None)
    return (
        getattr(getter, "__module__", "") == __name__
        and getattr(getter, "__name__", "") == "get_cdx_any"
    )


def request_cdx_json_payload(
    client: object,
    urls: Iterable[str],
    params: list[tuple[str, str]],
    max_bytes: int = 64 * 1024 * 1024,
) -> object:
    """Use native JSON in production while retaining the established mock API."""
    strict_getter = getattr(client, "get_cdx_json_any", None)
    if callable(strict_getter) and _uses_builtin_cdx_getter(client):
        return strict_getter(urls, params, max_bytes=max_bytes)
    return client.get_cdx_any(urls, params, max_bytes=max_bytes, prefer_text=False)


def request_cdx_json_rows(
    client: object,
    urls: Iterable[str],
    params: list[tuple[str, str]],
    max_bytes: int = 64 * 1024 * 1024,
) -> CDXRows:
    """Fetch a numbered page as native JSON without representation fallback."""
    strict_getter = getattr(client, "get_cdx_json_rows_any", None)
    if callable(strict_getter) and _uses_builtin_cdx_getter(client):
        return strict_getter(urls, params, max_bytes=max_bytes)
    payload = client.get_cdx_any(urls, params, max_bytes=max_bytes, prefer_text=False)
    return parse_cdx_rows_payload(payload)


def _looks_like_json_container(data: bytes | bytearray | memoryview) -> bool:
    """Return whether a CDX body starts like a JSON table/error object."""
    prefix = bytes(data[:64]).lstrip(b"\xef\xbb\xbf \t\r\n")
    return prefix.startswith((b"[", b"{"))


def parse_cdx_response_data(
    data: bytes,
    endpoint: str,
    params: list[tuple[str, str]],
    requested_format: str,
) -> object:
    """Parse the representation Wayback returned, not merely the one requested.

    Path-specific Timemap services occasionally ignore the ``output`` query
    parameter.  A valid response in the other representation can therefore be
    consumed locally instead of issuing the same expensive CDX request again.
    Bodies which actually look like truncated JSON still raise and use the
    normal representation/endpoint recovery path.
    """
    looks_json = _looks_like_json_container(data)
    if requested_format == "text":
        if looks_json:
            return parse_json_response(data, endpoint)
        return parse_cdx_text_response(data, endpoint, params)
    try:
        return parse_json_response(data, endpoint)
    except MalformedCDXResponse:
        if looks_json:
            raise
        return parse_cdx_text_response(data, endpoint, cdx_text_fallback_params(params))


def parse_cdx_rows_response_data(
    data: bytes | bytearray,
    endpoint: str,
    params: list[tuple[str, str]],
    requested_format: str,
) -> CDXRows:
    """Compact-row equivalent of :func:`parse_cdx_response_data`."""
    looks_json = _looks_like_json_container(data)
    if requested_format == "text":
        if looks_json:
            return parse_cdx_rows_payload(parse_json_response(bytes(data), endpoint))
        return parse_cdx_text_rows(data, endpoint, params)
    try:
        return parse_cdx_rows_payload(parse_json_response(bytes(data), endpoint))
    except MalformedCDXResponse:
        if looks_json:
            raise
        return parse_cdx_text_rows(data, endpoint, cdx_text_fallback_params(params))

def parse_json_response(data: bytes, endpoint: str = "") -> object:
    raw = data.decode("utf-8", "replace").lstrip("\ufeff").strip()
    if not raw:
        raise MalformedCDXResponse(
            f"CDX returned an empty response body from {endpoint or 'unknown endpoint'}",
            splittable=True,
            endpoint=endpoint or None,
        )
    try:
        return json_loads(raw)
    except JSONDecodeErrors as exc:
        around = raw[max(0, exc.pos - 160): exc.pos + 160]
        preview = clean_space(around or raw[:320])
        raise MalformedCDXResponse(
            f"CDX returned malformed JSON from {endpoint} at line {exc.lineno}, "
            f"column {exc.colno}: {preview}",
            splittable=True,
            endpoint=endpoint,
        ) from exc


def _decode_field(value: bytes | bytearray | memoryview) -> str:
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", "replace")
    return value.tobytes().decode("utf-8", "replace")


def _iter_binary_lines(data: bytes | bytearray):
    """Yield lines using the C-level delimiter search, without a full copy."""
    start = 0
    length = len(data)
    while start < length:
        end = data.find(b"\n", start)
        if end < 0:
            yield data[start:]
            return
        yield data[start:end]
        start = end + 1


def parse_cdx_rows_payload(payload: object) -> CDXRows:
    """Convert JSON-style CDX data directly to compact tuples."""
    if payload == []:
        return CDXRows([])
    if isinstance(payload, dict):
        message = str(payload.get("message") or payload.get("error") or payload)
        lowered = message.casefold()
        if "no capture" in lowered or "no result" in lowered or "not found" in lowered:
            return CDXRows([])
        raise RuntimeError(message)
    if not isinstance(payload, list) or not payload:
        raise MalformedCDXResponse("unexpected CDX JSON payload", splittable=True)
    header = payload[0]
    if not isinstance(header, list):
        raise RuntimeError("unexpected CDX response header")
    positions = {str(name): index for index, name in enumerate(header)}
    required = ("timestamp", "original")
    if any(name not in positions for name in required):
        raise RuntimeError("CDX response did not include timestamp and original")
    body = payload[1:]
    resume_key: str | None = None
    if len(body) >= 2 and body[-2] == [] and isinstance(body[-1], list) and len(body[-1]) == 1:
        resume_key = str(body[-1][0])
        body = body[:-2]

    def value(item: list, name: str) -> str:
        index = positions.get(name)
        if index is None or index >= len(item):
            return ""
        return str(item[index] if item[index] is not None else "")

    rows: list[CDXRow] = []
    for item in body:
        if not isinstance(item, list) or len(item) != len(header):
            raise MalformedCDXResponse("incomplete CDX JSON row; page must be retried", splittable=True)
        timestamp = value(item, "timestamp")
        original = value(item, "original")
        if not re.fullmatch(r"\d{14}", timestamp) or not original:
            raise MalformedCDXResponse("invalid CDX timestamp or URL; page must be retried", splittable=True)
        if timestamp and original:
            rows.append(
                (
                    timestamp,
                    original,
                    value(item, "mimetype"),
                    value(item, "statuscode"),
                    value(item, "digest"),
                    value(item, "length"),
                    value(item, "urlkey"),
                )
            )
    return CDXRows(rows, resume_key)


def parse_cdx_text_rows(
    data: bytes | bytearray,
    endpoint: str = "",
    params: list[tuple[str, str]] | None = None,
) -> CDXRows:
    """Parse line-oriented CDX output in one pass with bounded duplication.

    A normal 200 response with no rows is a valid empty result. This matters for
    sparse sites and date windows; treating it as a broken connection caused some
    projects to keep subdividing and retrying work that was already complete.
    """
    if not data:
        return CDXRows([])
    prefix = bytes(data[:1000]).lstrip(b"\xef\xbb\xbf").lower()
    if any(marker in prefix for marker in (b"<!doctype", b"<html", b"bad gateway", b"temporarily unavailable", b"too many requests")):
        preview = clean_space(bytes(data[:320]).decode("utf-8", "replace"))
        raise MalformedCDXResponse(
            f"CDX plain-text response returned an error page from {endpoint}: {preview}",
            splittable=True,
            endpoint=endpoint,
        )
    params = params or []
    if any(key.casefold() == "shownumpages" and value.casefold() == "true" for key, value in params):
        for raw_line in _iter_binary_lines(data):
            token = raw_line.strip().lstrip(b"\xef\xbb\xbf")
            if not token:
                continue
            if token.isdigit():
                return CDXRows([(token.decode("ascii"), "", "", "", "", "", "")])
            break
        raise MalformedCDXResponse(
            f"CDX page-count fallback was not numeric at {endpoint}: {clean_space(bytes(data[:320]).decode('utf-8', 'replace'))}",
            splittable=True,
            endpoint=endpoint,
        )

    rows: list[CDXRow] = []
    malformed_count = 0
    malformed_preview = ""
    after_blank = False
    resume_candidate: bytes | None = None
    first_nonempty = True
    fl_value = next((value for key, value in params if key.casefold() == "fl"), "")
    has_urlkey = fl_value.lstrip().casefold().startswith("urlkey,")
    field_offset = 1 if has_urlkey else 0
    expected_parts = 7 if has_urlkey else 6
    maxsplit = expected_parts - 1
    header_token = b"urlkey" if has_urlkey else b"timestamp"
    for raw_line in _iter_binary_lines(data):
        line = raw_line.strip()
        if first_nonempty:
            line = line.lstrip(b"\xef\xbb\xbf")
        if not line:
            after_blank = True
            continue
        first_nonempty = False
        if resume_candidate is not None:
            malformed_count += 1
            if not malformed_preview:
                malformed_preview = _decode_field(resume_candidate)[:320]
            resume_candidate = None
        parts = line.split(None, maxsplit)
        if after_blank and len(parts) == 1:
            resume_candidate = parts[0]
            after_blank = False
            continue
        after_blank = False
        if len(parts) != expected_parts:
            malformed_count += 1
            if not malformed_preview:
                malformed_preview = _decode_field(line)[:320]
            continue
        timestamp = parts[field_offset]
        if not timestamp.isdigit():
            # Header rows are rare. Test for them only after the numeric fast
            # path fails rather than lower-casing the first field of every row.
            if parts[0].lower() == header_token:
                continue
            malformed_count += 1
            if not malformed_preview:
                malformed_preview = _decode_field(line)[:320]
            continue
        mimetype = parts[field_offset + 1]
        statuscode = parts[field_offset + 2]
        digest = parts[field_offset + 3]
        length = parts[field_offset + 4]
        original = parts[field_offset + 5]
        rows.append((
            _decode_field(timestamp), _decode_field(original), _decode_field(mimetype),
            _decode_field(statuscode), _decode_field(digest), _decode_field(length),
            _decode_field(parts[0]) if has_urlkey else "",
        ))
    if malformed_count:
        raise MalformedCDXResponse(
            f"CDX plain-text response contained {malformed_count} malformed row(s) at {endpoint}: "
            f"{clean_space(malformed_preview)}",
            splittable=True,
            endpoint=endpoint,
        )
    resume_key = _decode_field(resume_candidate) if resume_candidate else None
    return CDXRows(rows, resume_key)


def cdx_text_fallback_params(params: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """Reissue a CDX query in a line-oriented format that survives bad JSON rows."""
    cleaned = [
        (key, value)
        for key, value in params
        if key.casefold() not in {"output", "fl", "gzip"}
    ]
    cleaned.append(("output", "txt"))
    cleaned.append(("gzip", "false"))
    if not any(key.casefold() == "shownumpages" and value.casefold() == "true" for key, value in params):
        # Keeping original last makes split(maxsplit=5) safe even for malformed
        # historical URLs containing literal spaces.
        # urlkey is intentionally retained for robust resumeKey traversal. The
        # Wayback CDX resume token is based on index ordering, and omitting the
        # sort key has historically produced incorrect continuation behavior on
        # some server versions. The parser discards it after continuation is safe.
        cleaned.append(("fl", "urlkey,timestamp,mimetype,statuscode,digest,length,original"))
    return cleaned


def parse_cdx_text_response(
    data: bytes,
    endpoint: str = "",
    params: list[tuple[str, str]] | None = None,
) -> object:
    raw = data.decode("utf-8", "replace").lstrip("\ufeff").replace("\x00", "").strip()
    if not raw:
        params = params or []
        if any(key.casefold() == "shownumpages" and value.casefold() == "true" for key, value in params):
            raise MalformedCDXResponse(
                f"CDX page-count fallback returned an empty body from {endpoint}",
                splittable=True,
                endpoint=endpoint,
            )
        return [["timestamp", "mimetype", "statuscode", "digest", "length", "original"]]
    lowered = raw[:1000].casefold()
    if any(marker in lowered for marker in ("<!doctype", "<html", "bad gateway", "temporarily unavailable", "too many requests")):
        raise MalformedCDXResponse(
            f"CDX plain-text fallback returned an error page from {endpoint}: {clean_space(raw[:320])}",
            splittable=True,
            endpoint=endpoint,
        )
    params = params or []
    if any(key.casefold() == "shownumpages" and value.casefold() == "true" for key, value in params):
        token = next((line.strip() for line in raw.splitlines() if line.strip()), "")
        if token.isdigit():
            return int(token)
        raise MalformedCDXResponse(
            f"CDX page-count fallback was not numeric at {endpoint}: {clean_space(raw[:320])}",
            splittable=True,
            endpoint=endpoint,
        )

    fields = ["timestamp", "mimetype", "statuscode", "digest", "length", "original"]
    blocks = re.split(r"\r?\n[ \t]*\r?\n", raw)
    resume_key: str | None = None
    row_text = raw
    if len(blocks) > 1:
        candidate = blocks[-1].strip()
        if candidate and "\n" not in candidate and len(candidate.split()) == 1:
            resume_key = candidate
            row_text = "\n\n".join(blocks[:-1]).strip()

    rows: list[list[str]] = []
    malformed: list[str] = []
    for line in row_text.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(None, len(fields) - 1)
        if len(parts) != len(fields) or not parts[0].isdigit():
            malformed.append(line)
            continue
        rows.append(parts)
    if malformed:
        raise MalformedCDXResponse(
            f"CDX plain-text fallback contained {len(malformed)} malformed row(s) at {endpoint}: "
            f"{clean_space(malformed[0][:320])}",
            splittable=True,
            endpoint=endpoint,
        )
    payload: list[object] = [fields, *rows]
    if resume_key:
        payload.extend([[], [resume_key]])
    return payload

def parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        parsed = parsedate_to_datetime(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return max(0.0, (parsed - datetime.now(timezone.utc)).total_seconds())
    except Exception:
        return None
