from __future__ import annotations

import contextlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import shutil
import socket
import tempfile
import threading
import time
import unittest
from unittest import mock

import httpx

from archive_scout.cdx.client import HttpClient, RateLimitDeferred, ReplayRetryScheduled, TransientRequestError
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.config import ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.downloads import downloader
from archive_scout.downloads.rate_limit import FixedRateLimiter, SharedHostGate, reset_shared_traffic_state_for_tests
from archive_scout.events import Stopped
from archive_scout.network import transports as tr
from archive_scout.utils import utc_now


class Backend:
    def __init__(self, name, failure=None):
        self.name, self.failure, self.calls = name, failure, 0

    def request(self, url, *args, **kwargs):
        self.calls += 1
        if self.failure:
            raise self.failure
        return tr.TransportResponse(200, {}, url, b"ok", self.name, 0)

    def close(self):
        pass


def transport(*backends):
    instance = tr.ResilientTransport.__new__(tr.ResilientTransport)
    instance.backends = {b.name: b for b in backends}
    instance.order = list(instance.backends)
    instance.lock = threading.Lock()
    instance.cooldown_until = {}
    instance.last_success = None
    instance.callback = instance.attempt_context_factory = None
    return instance


class SelectionTests(unittest.TestCase):
    def test_response_failure_keeps_pool_eligible_for_healthy_next_url(self):
        reset = httpx.ReadError("body connection reset")
        reset.__cause__ = ConnectionResetError("peer disconnected")
        self.assertFalse(tr.is_transport_connection_failure(reset))
        for failure in (httpx.ReadTimeout("slow"), httpx.RemoteProtocolError("truncated"), httpx.PoolTimeout("busy"), reset):
            with self.subTest(failure=type(failure).__name__):
                primary = Backend("httpx", failure)
                alternate = Backend("urllib3")
                network = transport(primary, alternate)
                with self.assertRaises(tr.TransportExhaustedError):
                    network.request("http://example.com/slow", {}, 1024, threading.Event())
                primary.failure = None
                self.assertEqual(network.request("http://example.com/fast", {}, 1024, threading.Event()).backend, "httpx")
                self.assertEqual(alternate.calls, 0)

    def test_primary_requalifies_and_late_curl_success_cannot_demote_it(self):
        primary = Backend("httpx", httpx.ConnectError("offline"))
        fallback = Backend("curl")
        network = transport(primary, fallback)
        with mock.patch.object(tr.time, "monotonic", return_value=100):
            self.assertEqual(network.request("http://example.com/one", {}, 1024, threading.Event()).backend, "curl")
        primary.failure = None
        with mock.patch.object(tr.time, "monotonic", return_value=131):
            self.assertEqual(network.request("http://example.com/two", {}, 1024, threading.Event()).backend, "httpx")
            network._backend_succeeded("http://example.com/old-curl", "curl")
            self.assertEqual(network.request("http://example.com/three", {}, 1024, threading.Event()).backend, "httpx")
        self.assertEqual(fallback.calls, 1)

    def test_origin_failure_does_not_disable_another_origin(self):
        primary = Backend("httpx", httpx.ConnectError("one origin fails"))
        network = transport(primary)
        with self.assertRaises(tr.TransportExhaustedError):
            network.request("http://first.example/", {}, 1024, threading.Event())
        primary.failure = None
        self.assertEqual(network.request("http://second.example/", {}, 1024, threading.Event()).backend, "httpx")

    def test_requalification_is_single_flight_and_stale_candidates_are_rechecked(self):
        primary, fallback = Backend("httpx"), Backend("curl")
        network = transport(primary, fallback)
        url = "http://example.com/"
        with network.lock:
            state = network._health_locked(url)
            state.last_success = "curl"
            state.cooldown_until["httpx"] = 0
        entered, release = threading.Event(), threading.Event()
        outcomes = []

        def blocked(*args, **kwargs):
            entered.set()
            release.wait(2)
            return tr.TransportResponse(200, {}, url, b"ok", "httpx", 0)

        primary.request = blocked
        worker = threading.Thread(target=lambda: outcomes.append(network.request(url, {}, 1024, threading.Event()).backend))
        worker.start()
        try:
            self.assertTrue(entered.wait(1))
            self.assertEqual(network.request(url, {}, 1024, threading.Event()).backend, "curl")
            self.assertFalse(network._claim_backend(url, "httpx"))
        finally:
            release.set()
            worker.join(2)
        self.assertEqual(outcomes, ["httpx"])
        network._backend_failed(url, "httpx", httpx.ConnectError("cooled after ordering"))
        self.assertFalse(network._claim_backend(url, "httpx"))

    def test_all_cooled_backends_wait_without_new_attempts(self):
        primary = Backend("httpx", httpx.ConnectError("offline"))
        network = transport(primary)
        with self.assertRaises(tr.TransportExhaustedError):
            network.request("http://example.com/", {}, 1024, threading.Event())
        with self.assertRaises(tr.BackendsCoolingDown):
            network.request("http://example.com/", {}, 1024, threading.Event())
        self.assertEqual(primary.calls, 1)

    def test_external_redirect_failure_does_not_become_common_archive_outage(self):
        backend = tr.HttpxBackend.__new__(tr.HttpxBackend)
        def response(request):
            if request.url.host == "web.archive.org":
                return httpx.Response(302, headers={"Location": "http://external.example/"})
            raise httpx.ConnectError("external host is unavailable", request=request)
        backend.client = httpx.Client(transport=httpx.MockTransport(response), follow_redirects=False)
        alternate = Backend("curl")
        network = transport(backend, alternate)
        try:
            with self.assertRaises(tr.TransportExhaustedError) as raised:
                network.request("http://web.archive.org/", {}, 1024, threading.Event())
            self.assertFalse(raised.exception.connection_failed)
            self.assertEqual(alternate.calls, 0)
            self.assertTrue(network._claim_backend("http://web.archive.org/", "httpx"))
        finally:
            network.close()

    def test_curl_ignores_proxy_and_incomplete_headers(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "headers"
            # Preserve wire line endings: Windows text mode would double the CR.
            path.write_bytes(b"HTTP/1.1 200 Connection established\r\n\r\nHTTP/1.1 429 Too Many Requests\r\nRetry-After: 120\r\n")
            self.assertIsNone(tr.CurlBackend._origin_headers(path))
            with path.open("ab") as handle:
                handle.write(b"\r\n")
            status, headers = tr.CurlBackend._origin_headers(path)
            self.assertEqual((status, headers["retry-after"]), (429, "120"))


class FixtureHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def do_GET(self):
        self.server.hits.append(self.path)
        if self.path == "/redirect":
            self.send_response(302)
            self.send_header("Location", "/forbidden")
            self.send_header("Content-Length", "131072")
            self.end_headers()
            self.server.release.wait(3)
            self.close_connection = True
            return
        if self.path.startswith("/web/"):
            data = b"archived origin error evidence"
            self.send_response(429)
            self.send_header("Memento-Datetime", "Mon, 01 Jan 2001 00:00:00 GMT")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        if self.path.startswith("/throttle"):
            self.send_response(503 if "503" in self.path else 429)
            self.send_header("Retry-After", "120")
            self.send_header("Content-Length", "131072")
            self.end_headers()
            self.wfile.write(b"wait")
            self.wfile.flush()
            self.server.release.wait(3)
            self.close_connection = True
            return
        if self.path == "/binary":
            self.send_response(200)
            self.send_header("Content-Length", "131072")
            self.end_headers()
            self.wfile.write(b"\x89PNG\r\n\x1a\n" + b"x" * 16376)
            self.wfile.flush()
            self.server.release.wait(3)
            self.close_connection = True
            return
        if self.path.startswith(("/resume", "/suffix")) and self.headers.get("Range"):
            offset = int(self.headers["Range"].split("=")[1].split("-")[0])
            data = b"T" * (16384 - offset)
            if self.path.startswith("/suffix"):
                data = b"\x89PNG\r\n\x1a\n" + data[8:]
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {offset}-16383/16384")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", "16384")
        self.end_headers()
        self.wfile.write(b"T" * 8192)
        self.wfile.flush()
        self.connection.shutdown(socket.SHUT_RDWR)
        self.close_connection = True


class FixtureServer(ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False


class WireTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = FixtureServer(("127.0.0.1", 0), FixtureHandler)
        cls.server.release = threading.Event()
        cls.server.hits = []
        cls.worker = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.worker.start()
        cls.base = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.release.set()
        cls.server.shutdown()
        cls.server.server_close()
        cls.worker.join(2)

    def backends(self):
        yield "httpx", lambda: tr.HttpxBackend(2, 1, 1, trust_env=False)
        yield "urllib3", lambda: tr.Urllib3Backend(2, 1, 1, trust_env=False)
        if shutil.which("curl"):
            yield "curl", lambda: tr.CurlBackend(1, 1, trust_env=False)

    def test_small_prefix_survives_premature_eof_on_all_backends(self):
        for name, factory in self.backends():
            with self.subTest(backend=name), tempfile.TemporaryDirectory() as directory:
                backend = factory()
                path = Path(directory) / "capture.part"
                try:
                    with self.assertRaises(Exception):
                        backend.download(self.base + "/truncate", {}, path, 200000, threading.Event(),
                                         compute_hash=False, preview_validator=lambda h, b: None)
                    self.assertEqual(path.read_bytes(), b"T" * 8192)
                finally:
                    backend.close()

    def test_retained_prefix_is_range_resumed_without_missing_or_duplicated_bytes(self):
        for name, _factory in self.backends():
            with self.subTest(backend=name), tempfile.TemporaryDirectory() as directory:
                client = HttpClient(FixedRateLimiter(0), 2, 1, "offline fixture", threading.Event(),
                                    network_backend=name, trust_environment=False)
                path = Path(directory) / "capture.part"
                try:
                    with mock.patch.object(client, "retry_wait"):
                        result = client.download_to_path(self.base + "/resume", path, 200000, compute_hash=False)
                    self.assertEqual(path.read_bytes(), b"T" * 16384)
                    self.assertEqual(result["bytes"], 16384)
                finally:
                    client.close()

    def test_live_throttle_survives_a_stalled_body_on_every_backend(self):
        for name, factory in self.backends():
            for status in (429, 503):
                with self.subTest(backend=name, status=status):
                    backend = factory()
                    started = time.monotonic()
                    try:
                        with self.assertRaises(tr.ServiceStatusResponse) as raised:
                            backend.request(self.base + f"/throttle{status}", {}, 200000, threading.Event())
                        self.assertEqual(raised.exception.status, status)
                        self.assertEqual(raised.exception.headers["retry-after"], "120")
                    finally:
                        backend.close()

    @unittest.skipUnless(shutil.which("curl"), "curl is unavailable")
    def test_curl_throttle_updates_shared_gate_without_touching_saved_prefix(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "saved.part"
            path.write_bytes(b"existing prefix")
            gate = SharedHostGate(base_pause=1, max_pause=120)
            client = HttpClient(FixedRateLimiter(0), 2, 1, "offline fixture", threading.Event(),
                                network_backend="curl", trust_environment=False,
                                rate_limit_attempts=1, host_gate=gate)
            try:
                with self.assertRaises(RateLimitDeferred):
                    client.download_to_path(self.base + "/throttle429", path, 200000)
                self.assertEqual(path.read_bytes(), b"existing prefix")
                self.assertGreater(gate.remaining(), 119)
                self.assertEqual(client.metrics_snapshot()["rate_limit_events"], 1)
            finally:
                client.close()

    @unittest.skipUnless(shutil.which("curl"), "curl is unavailable")
    def test_curl_rejects_binary_prefix_before_waiting_for_body(self):
        backend = tr.CurlBackend(1, 1, trust_env=False)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "capture.part"
            try:
                with self.assertRaises(tr.PreviewRejected):
                    backend.download(self.base + "/binary", {}, path, 200000, threading.Event(),
                                     preview_validator=lambda h, b: "image" if b.startswith(b"\x89PNG") else None)
                self.assertFalse(path.exists())
            finally:
                backend.close()

    def test_archived_origin_429_remains_available_without_live_cooldown(self):
        for name, _factory in self.backends():
            with self.subTest(backend=name), tempfile.TemporaryDirectory() as directory:
                client = HttpClient(FixedRateLimiter(0), 1, 1, "offline fixture", threading.Event(),
                                    network_backend=name, trust_environment=False)
                try:
                    path = Path(directory) / "capture.part"
                    result = client.download_to_path(self.base + "/web/20010101000000id_/http://example.com/", path, 1024)
                    self.assertTrue(result["archived_origin_status"])
                    self.assertEqual(path.read_bytes(), b"archived origin error evidence")
                    self.assertEqual(client.metrics_snapshot()["rate_limit_events"], 0)
                finally:
                    client.close()

    @unittest.skipUnless(shutil.which("curl"), "curl is unavailable")
    def test_curl_range_suffix_is_not_mistaken_for_a_binary_file_prefix(self):
        backend = tr.CurlBackend(1, 1, trust_env=False)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "capture.part"
            path.write_bytes(b"T" * 8192)
            try:
                result = backend.download(self.base + "/suffix", {"Range": "bytes=8192-", "Accept-Encoding": "identity"},
                                          path, 200000, threading.Event(),
                                          preview_validator=lambda h, b: "image" if b.startswith(b"\x89PNG") else None)
                self.assertEqual(result.bytes_written, 16384)
                self.assertTrue(path.read_bytes().startswith(b"T" * 8192))
            finally:
                backend.close()

    @unittest.skipUnless(shutil.which("curl"), "curl is unavailable")
    def test_curl_redirect_policy_runs_before_destination_or_stalled_body(self):
        backend = tr.CurlBackend(1, 1, trust_env=False)
        def reject(source, destination):
            raise tr.RedirectPolicyError(source, destination)
        before = self.server.hits.count("/forbidden")
        try:
            with self.assertRaises(tr.RedirectPolicyError):
                backend.request(self.base + "/redirect", {}, 200000, threading.Event(), redirect_validator=reject)
            self.assertEqual(self.server.hits.count("/forbidden"), before)
        finally:
            backend.close()

    def test_urllib3_pool_exhaustion_has_a_bounded_local_wait(self):
        backend = tr.Urllib3Backend(2, 1, 1, trust_env=False)
        held = []
        try:
            for _ in range(2):
                held.append(backend.pool.request("GET", self.base + "/truncate", preload_content=False))
            with self.assertRaises(tr.urllib3.exceptions.EmptyPoolError) as raised:
                backend.request(self.base + "/truncate", {}, 200000, threading.Event())
            self.assertTrue(tr.is_response_failure(raised.exception))
        finally:
            for response in held:
                backend._discard(response)
            backend.close()


class ScheduledRetryTests(unittest.TestCase):
    def setUp(self):
        reset_shared_traffic_state_for_tests()
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.config = ProjectConfig(self.root, ["example.com/*"], [], workers=1, retries=2).normalized()
        self.db = open_database(self.root)

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()
        reset_shared_traffic_state_for_tests()

    def add_capture(self, name):
        stamp = utc_now()
        self.db.execute("""INSERT INTO captures(original_url,timestamp,query_signature,mimetype,statuscode,
                            length,state,resource_class,created_at,updated_at)
                            VALUES(?,'20010101000000',?,'text/plain','200',32,'pending','text',?,?)""",
                        (f"http://example.com/{name}", cdx_query_signature(self.config), stamp, stamp))
        self.db.commit()

    def test_retry_does_not_sleep_in_worker_and_budget_is_preserved(self):
        class Failing:
            def download(self, url, headers, destination, max_bytes, stop_event, **kwargs):
                destination.write_bytes(b"valid prefix")
                raise httpx.ReadTimeout("slow capture")
            def close(self): pass
        client = HttpClient(FixedRateLimiter(0), 2, 1, "offline", threading.Event(), transport=Failing())
        try:
            with mock.patch.object(client, "retry_wait", side_effect=AssertionError("worker slept")):
                with client.replay_attempt(1), self.assertRaises(ReplayRetryScheduled) as raised:
                    client.download_to_path("http://example.com/", self.root / "capture.part", 1024)
                self.assertEqual(raised.exception.attempt_number, 2)
                with client.replay_attempt(2), self.assertRaises(TransientRequestError) as exhausted:
                    client.download_to_path("http://example.com/", self.root / "capture.part", 1024)
                self.assertNotIsInstance(exhausted.exception, ReplayRetryScheduled)
                self.assertEqual((self.root / "capture.part").read_bytes(), b"valid prefix")
        finally:
            client.close()

    def test_one_worker_downloads_fresh_capture_while_retry_is_delayed(self):
        self.add_capture("slow")
        self.add_capture("fast")
        order = []
        def attempt(row, path, config, client, **kwargs):
            name = str(row["original_url"]).rsplit("/", 1)[-1]
            order.append((name, int(row.get("retry_attempt", 1))))
            if name == "slow" and row.get("retry_attempt", 1) == 1:
                raise ReplayRetryScheduled("temporary", 0.1, 2)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"text evidence")
            return {"kind": "downloaded", "path": path, "bytes_saved": 13,
                    "content_hash": "", "http_status": 200, "final_url": "http://example.com/"}
        with mock.patch.object(downloader, "_download_capture", side_effect=attempt):
            result = downloader.download_archive_only(self.config, self.db, threading.Event(), None)
        self.assertEqual(order, [("slow", 1), ("fast", 1), ("slow", 2)])
        self.assertEqual((result["downloaded"], result["errors"]), (2, 0))
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM captures WHERE state='downloaded_unscanned'").fetchone()[0], 2)

    def test_cancel_during_delayed_retry_preserves_pending_work_and_part(self):
        self.add_capture("slow")
        stop = threading.Event()
        def attempt(row, path, config, client, **kwargs):
            part = path.with_name(path.name + ".part")
            part.parent.mkdir(parents=True, exist_ok=True)
            part.write_bytes(b"retained prefix")
            stop.set()
            raise ReplayRetryScheduled("temporary", 30, 2)
        with mock.patch.object(downloader, "_download_capture", side_effect=attempt):
            with self.assertRaises(Stopped):
                downloader.download_archive_only(self.config, self.db, stop, None)
        row = self.db.execute("SELECT state,local_path FROM captures").fetchone()
        self.assertEqual(row["state"], "pending")
        self.assertEqual(Path(row["local_path"] + ".part").read_bytes(), b"retained prefix")
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM errors").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
