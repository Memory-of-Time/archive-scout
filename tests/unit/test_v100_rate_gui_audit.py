from __future__ import annotations

import contextlib
import threading
import unittest
from pathlib import Path
from unittest import mock

import httpx

from archive_scout.cdx.client import HttpClient, PermanentRequestError, RateLimitDeferred
from archive_scout.config import ProjectConfig
from archive_scout.constants import VERSION
from archive_scout.downloads.rate_limit import FixedRateLimiter, SharedHostGate
from archive_scout.network.transports import HttpxBackend, ResilientTransport, TransportResponse
from archive_scout.ui import theme


class CountingLimiter:
    def __init__(self):
        self.admissions = 0

    @contextlib.contextmanager
    def slot(self, _stop_event):
        self.admissions += 1
        yield


def transport_for(backends):
    transport = ResilientTransport.__new__(ResilientTransport)
    transport.backends = backends
    transport.order = list(backends)
    transport.lock = threading.Lock()
    transport.cooldown_until = {}
    transport.last_success = None
    transport.callback = None
    transport.attempt_context_factory = None
    return transport


def client_for(transport, limiter=None):
    return HttpClient(
        limiter or FixedRateLimiter(0), 1, 1, "offline-v100-test", threading.Event(),
        host_gate=SharedHostGate(), transport=transport,
    )


class V100RateAndWindowsGuiAuditTests(unittest.TestCase):
    def test_initial_release_identity_and_safe_rate_floors(self):
        self.assertEqual(VERSION, "1.1.3")
        config = ProjectConfig(Path("."), ["example.com/*"], []).normalized()
        self.assertEqual(config.cdx_delay, 2.5)
        self.assertEqual(config.download_delay, 0.125)
        self.assertEqual(config.rate_limit_base_pause, 5.0)

    def test_faster_target_override_cannot_weaken_shared_pool(self):
        config = ProjectConfig(Path("."), ["example.com/*"], []).normalized()
        config.target_settings = {"example.com/*": {"cdx_delay": 0.01, "download_delay": 0.01}}
        target = config.for_target("example.com/*")
        self.assertGreaterEqual(target.cdx_delay, 2.5)
        self.assertGreaterEqual(target.download_delay, 0.125)

    def test_rate_limit_deferral_escapes_cdx_fallback_helpers_unchanged(self):
        endpoints = [
            "https://web.archive.org/cdx/search/cdx",
            "https://web.archive.org/web/timemap/json",
            "https://web.archive.org/web/timemap/cdx",
        ]
        for method, urls in (
            ("get_cdx_any", endpoints),
            ("get_cdx_rows_any", endpoints),
            ("get_cdx_json_any", endpoints),
            ("get_cdx_json_any", endpoints[1:2]),
        ):
            with self.subTest(method=method, endpoints=len(urls)):
                client = client_for(mock.MagicMock())
                deferred = RateLimitDeferred("cooldown budget exhausted", waited=60)
                client.get = mock.MagicMock(side_effect=deferred)
                with self.assertRaises(RateLimitDeferred) as caught:
                    getattr(client, method)(urls, [("output", "json")])
                self.assertIs(caught.exception, deferred)
                self.assertEqual(client.get.call_count, 1)

    def test_redirect_hops_each_consume_wire_admission(self):
        wire = []

        def response(request):
            wire.append(str(request.url))
            if len(wire) < 3:
                return httpx.Response(302, headers={"Location": f"/redirect-{len(wire)}"})
            return httpx.Response(200, content=b"ok")

        backend = HttpxBackend.__new__(HttpxBackend)
        backend.client = httpx.Client(transport=httpx.MockTransport(response), follow_redirects=False)
        limiter = CountingLimiter()
        client = client_for(transport_for({"httpx": backend}), limiter)
        try:
            client.get("https://web.archive.org/start", 1024)
            metrics = client.metrics_snapshot()
            self.assertEqual(len(wire), 3)
            self.assertEqual(limiter.admissions, 3)
            self.assertEqual(metrics["wire_request_starts"], 3)
            self.assertEqual(metrics["logical_requests"], 1)
        finally:
            client.close()

    def test_backend_fallback_attempts_each_consume_wire_admission(self):
        calls = []

        class Backend:
            def __init__(self, name, fails):
                self.name, self.fails = name, fails

            def request(self, url, headers, max_bytes, stop_event):
                calls.append(self.name)
                if self.fails:
                    raise httpx.ReadError("connection reset after request was sent")
                return TransportResponse(200, {}, url, b"ok", self.name, 0)

            def close(self):
                pass

        limiter = CountingLimiter()
        client = client_for(transport_for({str(i): Backend(str(i), i < 2) for i in range(3)}), limiter)
        try:
            client.get("https://web.archive.org/start", 1024)
            self.assertEqual(calls, ["0", "1", "2"])
            self.assertEqual(limiter.admissions, 3)
            self.assertEqual(client.metrics_snapshot()["wire_request_starts"], 3)
        finally:
            client.close()

    def test_headerless_rate_limit_is_fixed_at_five_seconds(self):
        with mock.patch("archive_scout.downloads.rate_limit.time.monotonic", return_value=1000.0), \
             mock.patch("archive_scout.downloads.rate_limit.time.time", return_value=1000.0):
            gate = SharedHostGate()
            self.assertEqual(gate.pause_for_rate_limit(), 5.0)

    def test_retry_after_is_never_shortened_by_generic_retry_jitter(self):
        client = client_for(mock.MagicMock())
        client.stop_event = mock.MagicMock()
        client.stop_event.is_set.return_value = False
        with mock.patch("archive_scout.cdx.client.random.uniform", return_value=1.0):
            client.retry_wait(0, "HTTP 504", retry_after=100)
        self.assertGreaterEqual(client.stop_event.wait.call_args.args[0], 100.0)

    def test_archived_historical_429_does_not_close_live_host_gate(self):
        class Archived429:
            def request(self, url, headers, max_bytes, stop_event):
                return TransportResponse(
                    429,
                    {"Memento-Datetime": "Mon, 01 Jan 2001 00:00:00 GMT"},
                    url,
                    b"Historical origin response",
                    "mock",
                    0,
                )

            def close(self):
                pass

        client = client_for(Archived429())
        client.rate_limit_attempts = 1
        try:
            with self.assertRaises(PermanentRequestError) as caught:
                client.get("https://web.archive.org/web/20010101000000id_/http://example.com/", 1024)
            self.assertEqual(caught.exception.category, "archived_origin_http_error")
            self.assertEqual(client.host_gate.remaining(), 0.0)
            self.assertEqual(client.metrics_snapshot()["rate_limit_events"], 0)
        finally:
            client.close()

    def test_windows_manifest_and_font_scaling_contract(self):
        root = Path(__file__).resolve().parents[2]
        manifest = (root / "packaging/windows/ArchiveScout.manifest").read_text(encoding="utf-8")
        build = (root / "scripts/build_windows.ps1").read_text(encoding="utf-8")
        source = Path(theme.__file__).read_text(encoding="utf-8")
        self.assertIn("PerMonitorV2", manifest)
        self.assertIn("--manifest packaging/windows/ArchiveScout.manifest", build)
        self.assertNotIn('tk.call("tk", "scaling"', source)
        self.assertIn("TkDefaultFont", source)
        self.assertIn("_windows_high_contrast", source)


if __name__ == "__main__":
    unittest.main()
