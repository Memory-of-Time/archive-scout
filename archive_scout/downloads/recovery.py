from __future__ import annotations

import time
import heapq

from ..events import ProgressEvent, Stopped


def wake_backend_retries(delayed, ready, client, url_for_row) -> int:
    """Release only local-backend waits made obsolete by a healthy backend.

    Generic retry deadlines (including Retry-After) retain their original
    eligibility. The actual worker still passes through the shared host gate.
    """
    backend_ready = getattr(type(client), "replay_backend_ready", None)
    if not callable(backend_ready):
        return 0
    pending = []
    awakened = 0
    for due, sequence, row, payload, wait_kind in delayed:
        if wait_kind == "backend_cooldown" and backend_ready(client, url_for_row(row)):
            ready.append((row, payload))
            awakened += 1
        else:
            pending.append((due, sequence, row, payload, wait_kind))
    if awakened:
        delayed[:] = pending
        heapq.heapify(delayed)
    return awakened


def wait_for_archive(config, gate, stop_event, callback=None, *, stage="download") -> None:
    """Wait after settled workers, then renew a shared recovery cycle.

    Eligibility is never shortened. The HTTP admission layer, not this wait,
    announces and releases the one real probe.
    """
    reason = str(gate.snapshot().get("reason") or "")
    remaining = gate.remaining()
    detail = {
        "reason_code": "archive_connectivity" if reason == "connection outage" else "service_rate_limit",
        "eligible_at_epoch": time.time() + remaining,
        "incident_id": gate.incident_id,
        "recovery_stage": stage,
        "waiting_seconds": remaining,
    }
    if callback:
        callback(ProgressEvent(
            "network_waiting" if reason == "connection outage" else "rate_limit_waiting",
            f"Waiting for Internet Archive ({reason or 'recovery'}); progress is saved, next eligibility in {remaining:.1f}s.",
            detail=detail,
        ))
    gate.wait(stop_event)
    if stop_event.is_set():
        raise Stopped
    gate.renew_recovery_cycle(int(detail["incident_id"]))
