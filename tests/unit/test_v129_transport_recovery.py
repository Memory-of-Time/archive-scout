from __future__ import annotations

import hashlib
import sqlite3
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import Mock, patch

import httpx

from archive_scout.cdx.client import HttpClient
from archive_scout.cdx.indexer import save_coverage_state
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.config import ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, upsert_captures
from archive_scout.downloads.rate_limit import reset_shared_traffic_state_for_tests
from archive_scout.downloads.validation import classify_exception
from archive_scout.network.transports import (
    CurlTransportError, PermitRevoked, ResilientTransport, TransportExhaustedError,
    TransportResponse, _pace_redirect_hop,
)
from archive_scout.operations import run_project


BODY = b"<html><body>needle in a validated capture</body></html>"


class TransportRecoveryTests(unittest.TestCase):
    def transport(self, mode="httpx"):
        transport = ResilientTransport(pool_size=4, connect_timeout=1, read_timeout=3,
                                       mode=mode, trust_env=False)
        self.addCleanup(transport.close)
        return transport

    def test_late_results_do_not_replace_newer_recovery_state(self):
        transport = self.transport()
        stop = threading.Event()
        first = transport._await_backend("httpx", stop)
        older = transport._await_backend("httpx", stop)
        transport._backend_result(first, False)
        transport.cooldown_until["httpx"] = time.monotonic() - 1
        probe = transport._await_backend("httpx", stop)
        self.assertTrue(probe.probe)
        transport._backend_result(older, False)
        self.assertEqual(transport.snapshot()["probe_backends"], ("httpx",))
        self.assertIsNone(transport._await_backend("httpx", stop, wait=False))
        transport._backend_result(probe, True)
        transport._backend_result(older, False)
        self.assertNotIn("httpx", transport.cooldown_until)
        self.assertFalse(transport._await_backend("httpx", stop).probe)

    def test_closed_alternate_does_not_extend_a_failed_turn(self):
        transport = self.transport()
        transport.backends["other"] = Mock()
        transport.order.append("other")
        transport.cooldown_until["other"] = time.monotonic() + 20
        with patch.object(transport.backends["httpx"], "request", side_effect=httpx.ConnectError("refused")):
            with self.assertRaises(TransportExhaustedError) as caught:
                transport.request("http://example.test/", {}, 1000, threading.Event())
        self.assertEqual([name for name, _ in caught.exception.failures], ["httpx"])
        transport.backends["other"].request.assert_not_called()

    def test_backend_wait_returns_to_host_gate_when_permit_is_revoked(self):
        transport = self.transport()
        transport.cooldown_until["httpx"] = time.monotonic() + 30
        allowed = threading.Event()
        allowed.set()
        with ThreadPoolExecutor(max_workers=1) as workers:
            turn = workers.submit(transport.request, "http://example.test/", {}, 1000,
                                  threading.Event(), permit_checker=allowed.is_set)
            for _ in range(100):
                if transport.snapshot()["backend_waiters"]:
                    break
                time.sleep(.01)
            self.assertEqual(transport.snapshot()["backend_waiters"], 1)
            allowed.clear()
            with self.assertRaises(PermitRevoked):
                turn.result(timeout=2)
        self.assertEqual(transport.snapshot()["backend_waiters"], 0)

    def test_redirect_uses_its_own_permit_during_concurrent_requests(self):
        transport = self.transport()
        first_entered = threading.Event()
        second_entered = threading.Event()
        first_done = threading.Event()

        def request(url, _headers, _maximum, stop):
            if url.endswith("/first"):
                first_entered.set()
                try:
                    if not second_entered.wait(2):
                        raise AssertionError("second request did not arrive")
                    _pace_redirect_hop(stop)
                finally:
                    first_done.set()
            else:
                second_entered.set()
                if not first_done.wait(2):
                    raise AssertionError("first request did not return")
                _pace_redirect_hop(stop)
            return TransportResponse(200, {}, url, b"ok", "httpx", 0)

        with patch.object(transport.backends["httpx"], "request", side_effect=request):
            with ThreadPoolExecutor(max_workers=2) as workers:
                first = workers.submit(transport.request, "http://example.test/first", {}, 1000,
                                       threading.Event(), permit_checker=lambda: not second_entered.is_set())
                self.assertTrue(first_entered.wait(2))
                second = workers.submit(transport.request, "http://example.test/second", {}, 1000,
                                        threading.Event(), permit_checker=lambda: True)
                with self.assertRaises(PermitRevoked):
                    first.result(timeout=3)
                self.assertEqual(second.result(timeout=3).data, b"ok")

    def server(self):
        held = threading.Event()

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self):
                self.send_response(200)
                self.send_header("Content-Length", str(len(BODY)))
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                if self.path == "/hold":
                    held.wait(5)
                try:
                    self.wfile.write(BODY)
                except OSError:
                    pass

            def log_message(self, *_args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        self.addCleanup(held.set)
        return f"http://127.0.0.1:{server.server_port}", held

    def test_successful_headers_release_probe_while_body_still_waits(self):
        url, held = self.server()
        transport = self.transport()
        transport.cooldown_until["httpx"] = time.monotonic() - 1
        with ThreadPoolExecutor(max_workers=2) as workers:
            slow = workers.submit(transport.request, url + "/hold", {}, 1000, threading.Event())
            try:
                for _ in range(100):
                    if "httpx" not in transport.snapshot()["probe_backends"] and transport.last_success:
                        break
                    time.sleep(.01)
                self.assertFalse(slow.done())
                fast = workers.submit(transport.request, url + "/fast", {}, 1000, threading.Event())
                self.assertEqual(fast.result(timeout=2).data, BODY)
            finally:
                held.set()
            self.assertEqual(slow.result(timeout=2).data, BODY)

    def test_real_pool_wait_is_bounded_and_does_not_cool_the_backend(self):
        url, _held = self.server()
        transport = self.transport("urllib3")
        backend = transport.backends["urllib3"]
        backend.pool_timeout = .1
        transport.backends["alternate"] = Mock()
        transport.order.append("alternate")
        held_responses = []
        try:
            for _ in range(backend.pool_size):
                held_responses.append(backend.pool.request("GET", url + "/hold", preload_content=False))
            with tempfile.TemporaryDirectory() as tmp:
                with self.assertRaises(TransportExhaustedError) as caught:
                    transport.download(url + "/fast", {}, Path(tmp) / "body", 1000, threading.Event())
            self.assertEqual(classify_exception(caught.exception), ("connection_pool_timeout", None, True))
            self.assertEqual(transport.cooldown_until, {})
            transport.backends["alternate"].download.assert_not_called()
        finally:
            for response in held_responses:
                response.close()
                response.release_conn()
        self.assertEqual(transport.request(url + "/fast", {}, 1000, threading.Event()).data, BODY)

    def test_download_and_scan_recovers_all_captures_after_backend_outage(self):
        url, _held = self.server()
        transport = self.transport("auto")
        transport.BACKEND_RECOVERY_SECONDS = .12
        outage_ends = None
        failed = set()
        attempts = threading.Lock()

        def intermittent(name, download):
            def call(*args, **kwargs):
                nonlocal outage_ends
                with attempts:
                    if outage_ends is None:
                        outage_ends = time.monotonic() + .85
                    offline = time.monotonic() < outage_ends
                    if offline:
                        failed.add(name)
                if offline:
                    if name == "curl":
                        raise CurlTransportError(7, "test connection refused")
                    raise httpx.ConnectError("test connection refused")
                return download(*args, **kwargs)
            return call

        for name, backend in transport.backends.items():
            patcher = patch.object(backend, "download", side_effect=intermittent(name, backend.download))
            patcher.start()
            self.addCleanup(patcher.stop)

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            cfg = ProjectConfig(root, targets=["example.test/*"], keywords=["needle"], workers=4,
                                download_delay=.005, from_date="2001", to_date="2001").normalized()
            cfg.network.retry_base_seconds = 1
            cfg.network.retry_max_seconds = 1
            signature = cdx_query_signature(cfg)
            count = 40
            db = open_database(root)
            try:
                target = get_or_create_target(db, cfg.targets[0])
                with db:
                    upsert_captures(db, (("20010101000000", f"http://example.test/{n}", "text/html",
                                          "200", f"fixture-{n}", len(BODY)) for n in range(count)),
                                    target, signature)
                    save_coverage_state(db, target, signature, cfg.from_date, cfg.to_date, None, True, count, None)
            finally:
                db.close()

            reset_shared_traffic_state_for_tests()
            class FaultingClient(HttpClient):
                def __init__(self, *args, **kwargs):
                    super().__init__(*args, transport=transport, **kwargs)

            with patch("archive_scout.downloads.downloader.HttpClient", FaultingClient), \
                    patch("archive_scout.downloads.downloader.replay_url", return_value=url + "/fast"):
                run_project(cfg, "download")

            self.assertEqual(failed, set(transport.backend_names))
            files = list((root / "captures").rglob("*.txt"))
            self.assertEqual(len(files), count)
            self.assertTrue(all(hashlib.sha256(file.read_bytes()).digest() == hashlib.sha256(BODY).digest()
                                for file in files))
            db = sqlite3.connect(root / "archive_scout.sqlite3")
            try:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM captures WHERE state='downloaded'").fetchone()[0], count)
                self.assertEqual(db.execute("SELECT COUNT(*) FROM document_matches WHERE score>0").fetchone()[0], count)
                self.assertEqual(db.execute("SELECT COUNT(*) FROM text_retry_schedule").fetchone()[0], 0)
            finally:
                db.close()
