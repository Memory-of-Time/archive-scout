from __future__ import annotations

import time

from ..events import ProgressEvent, Stopped


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
