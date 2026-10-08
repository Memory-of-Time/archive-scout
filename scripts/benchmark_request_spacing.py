"""Deterministic offline request-admission benchmark for Archive Scout 1.1.1.

This exercises the real shared limiter and host gate using a virtual clock.
It makes NO HTTP calls, performs NO downloads, and measures scheduled starts,
not completed snapshots or live Internet Archive performance.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import math
import sys
import threading
import time
from pathlib import Path
from unittest.mock import patch


class VirtualClock:
    def __init__(self) -> None:
        self.now = 1000.0
        self.wait_calls = 0

    def monotonic(self) -> float:
        return self.now

    def wait(self, timeout: float | None = None) -> bool:
        if timeout is None or timeout <= 0:
            raise AssertionError("virtual wait requires a positive bounded timeout")
        self.wait_calls += 1
        self.now += timeout
        return False


def scenario(rate_module, *, admissions: int, adaptive: bool, throttle_every: int,
             server_wait: float | None = 60.0) -> dict:
    rate_module.reset_shared_traffic_state_for_tests()
    virtual = VirtualClock()
    stop = threading.Event()
    first_start = None
    previous_start = None
    smallest_spacing = math.inf
    largest_spacing = 0.0
    incidents = 0
    optional_rate_changes = 0
    probe_starts = 0
    waiting_seconds = 0.0
    checked_server_deadlines = 0
    next_server_deadline = None
    checked_followup_spacing = 0
    check_next = False
    wall_started = time.perf_counter()
    with patch.object(rate_module.time, "monotonic", virtual.monotonic), patch.object(
        rate_module.random, "uniform", return_value=1.0
    ):
        limiter = rate_module.SharedFixedRateLimiter(
            0.125, rate_module.WAYBACK_REPLAY_RATE_KEY, adaptive=adaptive
        )
        gate = rate_module.SharedHostGate(base_pause=60.0, max_pause=600.0, adaptive=adaptive)
        limiter.condition.wait = virtual.wait
        gate.condition.wait = virtual.wait
        try:
            for index in range(admissions):
                if throttle_every and index and index % throttle_every == 0:
                    incidents += 1
                    pause, incident_id, _eligible_at, _new = gate.signal_rate_limit(
                        server_wait, reason="HTTP 429"
                    )
                    waiting_seconds += pause
                    next_server_deadline = virtual.now + (server_wait if server_wait is not None else pause)
                    optional_rate_changes += int(limiter.note_rate_limit(incident_id))
                permit = gate.acquire_request(stop)
                with limiter.slot(stop):
                    started = virtual.now
                    if first_start is None:
                        first_start = started
                    if previous_start is not None:
                        spacing = started - previous_start
                        if spacing < 0.125 - 1e-9:
                            raise AssertionError(f"request-start burst: spacing={spacing}")
                        smallest_spacing = min(smallest_spacing, spacing)
                        largest_spacing = max(largest_spacing, spacing)
                        if check_next:
                            checked_followup_spacing += 1
                            check_next = False
                    previous_start = started
                    if permit.probe:
                        if next_server_deadline is None or started < next_server_deadline - 1e-9:
                            raise AssertionError("recovery probe started before the server deadline")
                        checked_server_deadlines += 1
                        next_server_deadline = None
                        probe_starts += 1
                        check_next = True
                # Only an injected successful response closes the recovery gate.
                # This is fixture evidence; no HTTP response is obtained.
                gate.finish_request(permit, recovered=True)
                limiter.note_healthy_response()
            elapsed = virtual.now - float(first_start)
            if not adaptive and optional_rate_changes:
                raise AssertionError("disabled adaptive pacing applied a rate reduction")
            if incidents != probe_starts:
                raise AssertionError("expected exactly one recovery probe per fixture incident")
            if checked_followup_spacing != incidents:
                raise AssertionError("expected a post-recovery no-burst check for every incident")
            if checked_server_deadlines != incidents:
                raise AssertionError("expected a server deadline check for every incident")
            if not adaptive:
                expected = (admissions - 1) * 0.125 + incidents * max(0.0, (server_wait if server_wait is not None else 5.0) - 0.125)
                if not math.isclose(elapsed, expected, abs_tol=1e-9):
                    raise AssertionError(f"fixed schedule drift: {elapsed} versus {expected}")
            snapshot = limiter.snapshot()
        finally:
            limiter.close()
    return {
        "admissions": admissions,
        "adaptive_rate_limiting": adaptive,
        "fixture_throttle_every_admissions": throttle_every or None,
        "fixture_server_wait_seconds": server_wait if throttle_every else None,
        "incidents": incidents,
        "single_recovery_probe_starts": probe_starts,
        "post_recovery_no_burst_checks": checked_followup_spacing,
        "server_deadline_checks": checked_server_deadlines if server_wait is not None else 0,
        "fixed_fallback_deadline_checks": checked_server_deadlines if server_wait is None else 0,
        "recovery_deadline_kind": "server_retry_after" if server_wait is not None else "fixed_fallback",
        "optional_adaptive_rate_changes": optional_rate_changes,
        "virtual_scheduled_span_seconds": elapsed,
        "virtual_request_start_rate_per_second": (admissions - 1) / elapsed,
        "minimum_start_spacing_seconds": smallest_spacing,
        "maximum_start_spacing_seconds": largest_spacing,
        "fixture_requested_server_wait_seconds": waiting_seconds,
        "final_effective_delay_seconds": snapshot["effective_delay"],
        "virtual_condition_wait_calls": virtual.wait_calls,
        "benchmark_execution_seconds": time.perf_counter() - wall_started,
        "checks_passed": True,
    }


def reference_comparison(rate_module, reference_tree: Path, admissions: int) -> dict:
    """Load only the inspected v1.0.5 pacing module, never the old app/database."""
    reference = reference_tree / 'archive_scout/downloads/rate_limit.py'
    spec = importlib.util.spec_from_file_location('archive_scout.downloads.v105_pacing_reference', reference)
    old = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = old
    spec.loader.exec_module(old)
    virtual = VirtualClock()
    stop = threading.Event()
    with patch.object(old.time, 'monotonic', virtual.monotonic):
        limiter = old.SharedFixedRateLimiter(.125)
        limiter.condition.wait = virtual.wait
        first = virtual.now
        previous = None
        for _ in range(admissions):
            limiter.wait(stop)
            if previous is not None and virtual.now - previous < .125 - 1e-9:
                raise AssertionError('reference fixed pacing burst')
            previous = virtual.now
        old_span = virtual.now - first
    expected = (admissions - 1) * .125
    if not math.isclose(old_span, expected, abs_tol=1e-9):
        raise AssertionError('reference pacing drift')
    # Repeat failed probes in one incident. No real requests or responses.
    recovery = {}
    for name, module, adaptive in (('v1.0.5', old, False), ('v1.1.1_off', rate_module, False), ('v1.1.1_on', rate_module, True)):
        virtual = VirtualClock()
        with patch.object(module.time, 'monotonic', virtual.monotonic), patch.object(module.random, 'uniform', return_value=1.0):
            gate = module.SharedHostGate(**({'adaptive': adaptive} if module is rate_module else {}))
            pauses = []
            for _ in range(4):
                gate.acquire_request(stop)
                # Production calls pass the current permit; the legacy method
                # has no permit argument. Keep its exact original policy.
                permit = module.HostPermit(gate.generation, gate.probe_inflight)
                pause = (gate.pause_for_rate_limit() if module is old else gate.signal_rate_limit(permit=permit)[0])
                pauses.append(pause)
                virtual.now += pause
            recovery[name] = pauses
    return {'reference_module_sha256': hashlib.sha256(reference.read_bytes()).hexdigest(),
            'reference_version': '1.0.5', 'healthy_admissions': admissions,
            'reference_healthy_span_seconds': old_span, 'expected_fixed_span_seconds': expected,
            'reference_request_start_rate_per_second': (admissions - 1) / old_span,
            'consecutive_headerless_throttle_waits_seconds': recovery,
            'http_attempts': 0, 'checks_passed': True}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-tree", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--admissions", type=int, default=2_000_000)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--reference-tree", type=Path, help="Optional extracted complete v1.0.5 source for fixed-pacing comparison")
    arguments = parser.parse_args()
    if arguments.admissions < 4:
        parser.error("--admissions must be at least four")
    source = arguments.source_tree.resolve()
    if not (source / "archive_scout" / "downloads" / "rate_limit.py").is_file():
        parser.error("--source-tree must contain the Archive Scout source package")
    sys.path.insert(0, str(source))
    from archive_scout.downloads import rate_limit

    # Keep both normal and repeated-pause tests at the requested scale, then
    # compare opt-in versus fixed-only recovery using a smaller identical sample.
    sample = min(arguments.admissions, 20_000)
    interval = max(2, sample // 4)
    results = {
        "validation_kind": "deterministic_offline_virtual_request_admission",
        "limitations": [
            "No HTTP attempts and no snapshots downloaded.",
            "Virtual scheduled request starts are not a live completion rate.",
            "Single-thread fixture verifies spacing and recovery deadlines; concurrency is covered by regression tests.",
            "Server throttles and successful recovery responses are injected fixtures.",
        ],
        "http_attempts": 0,
        "scenarios": [
            scenario(rate_limit, admissions=arguments.admissions, adaptive=False, throttle_every=0),
            scenario(rate_limit, admissions=arguments.admissions, adaptive=False,
                     throttle_every=max(2, arguments.admissions // 20)),
            scenario(rate_limit, admissions=sample, adaptive=False, throttle_every=interval),
            scenario(rate_limit, admissions=sample, adaptive=True, throttle_every=interval),
            scenario(rate_limit, admissions=sample, adaptive=False, throttle_every=interval, server_wait=None),
        ],
    }
    if arguments.reference_tree:
        results['v105_reference_comparison'] = reference_comparison(rate_limit, arguments.reference_tree.resolve(), arguments.admissions)
    serialized = json.dumps(results, indent=2, allow_nan=False) + "\n"
    if arguments.output:
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        arguments.output.write_text(serialized, encoding="utf-8")
    print(serialized, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
