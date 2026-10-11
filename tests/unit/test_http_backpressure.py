from __future__ import annotations

import threading
import time
import unittest

from archive_scout.cdx.client import HttpClient, RateLimitDeferred
from archive_scout.downloads.rate_limit import FixedRateLimiter, HostPermit, SharedHostGate
from archive_scout.network.transports import TransportResponse


class FakeGate:
    def __init__(self) -> None:
        self.pauses: list[tuple[float | None, str]] = []

    def acquire_request(self, stop_event: threading.Event) -> HostPermit:
        if stop_event.is_set():
            raise RuntimeError("unexpected stop")
        return HostPermit(0, False)

    def permit_is_current(self, permit: HostPermit) -> bool:
        return True

    def finish_request(self, permit: HostPermit, recovered: bool) -> None:
        return

    def pause_for_rate_limit(self, retry_after=None, reason="HTTP 429") -> float:
        self.pauses.append((retry_after, reason))
        return 0.0


class FakeTransport:
    def __init__(self, responses: list[TransportResponse]) -> None:
        self.responses = list(responses)
        self.calls = 0

    def request(self, url, headers, max_bytes, stop_event):
        self.calls += 1
        return self.responses.pop(0)

    def close(self) -> None:
        return


def response(status: int, data: bytes = b"", headers: dict[str, str] | None = None) -> TransportResponse:
    return TransportResponse(status, headers or {}, "https://web.archive.org/test", data, "fake", 0.01)


class BackpressureTests(unittest.TestCase):
    def make_client(self, responses, gate, rate_limit_attempts=4):
        transport = FakeTransport(responses)
        client = HttpClient(
            FixedRateLimiter(0),
            retries=2,
            timeout=1,
            user_agent="test",
            stop_event=threading.Event(),
            host_gate=gate,
            rate_limit_attempts=rate_limit_attempts,
            rate_limit_max_wait=60,
            transport=transport,
        )
        return client, transport

    def test_429_pauses_shared_gate_then_retries_same_request(self) -> None:
        gate = FakeGate()
        client, transport = self.make_client(
            [response(429, headers={"Retry-After": "0"}), response(200, b"ok")], gate
        )
        result = client.get("https://web.archive.org/test", 100)
        self.assertEqual(result["data"], b"ok")
        self.assertEqual(transport.calls, 2)
        self.assertEqual(len(gate.pauses), 1)

    def test_unlimited_default_waits_through_repeated_429s(self) -> None:
        gate = FakeGate()
        client, transport = self.make_client(
            [response(429), response(429), response(429), response(200, b"ok")],
            gate,
            rate_limit_attempts=0,
        )
        client.rate_limit_max_wait = 0
        result = client.get("https://web.archive.org/test", 100)
        self.assertEqual(result["data"], b"ok")
        self.assertEqual(transport.calls, 4)
        self.assertEqual(len(gate.pauses), 3)

    def test_persistent_429_can_use_optional_deferral_budget(self) -> None:
        gate = FakeGate()
        client, transport = self.make_client([response(429), response(429)], gate, rate_limit_attempts=2)
        with self.assertRaises(RateLimitDeferred):
            client.get("https://web.archive.org/test", 100)
        self.assertEqual(transport.calls, 2)
        self.assertEqual(len(gate.pauses), 2)

    def test_shared_gate_coalesces_simultaneous_429_signals(self) -> None:
        gate = SharedHostGate(base_pause=1, max_pause=10, coalesce_seconds=10)
        first = gate.pause_for_rate_limit(retry_after=2)
        incidents = gate.incidents
        second = gate.pause_for_rate_limit(retry_after=2)
        self.assertEqual(gate.incidents, incidents)
        self.assertGreater(first, 0)
        self.assertGreater(second, 0)

    def test_stale_permits_are_rejected_and_only_one_recovery_probe_opens(self) -> None:
        gate = SharedHostGate(base_pause=1, max_pause=2)
        stop = threading.Event()
        stale = gate.acquire_request(stop)
        gate.pause_for_rate_limit(retry_after=1)
        self.assertFalse(gate.permit_is_current(stale))
        with gate.condition:
            gate.blocked_until = time.monotonic() - 0.01
            gate.condition.notify_all()
        probe = gate.acquire_request(stop)
        self.assertTrue(probe.probe)
        self.assertTrue(gate.permit_is_current(probe))
        gate.finish_request(probe, recovered=True)
        normal = gate.acquire_request(stop)
        self.assertFalse(normal.probe)
        self.assertTrue(gate.permit_is_current(normal))


if __name__ == "__main__":
    unittest.main()
