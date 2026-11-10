from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
import tempfile
import threading
import time
import unittest
from contextlib import closing
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import Mock, patch

from httpcore import ReadTimeout as HttpcoreReadTimeout

from archive_scout.cdx.client import HttpClient, TransientRequestError
from archive_scout.cdx.indexer import save_coverage_state
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.config import NetworkConfig, ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, upsert_captures
from archive_scout.downloads.downloader import download_archive
from archive_scout.downloads.rate_limit import (
    FixedRateLimiter, reset_shared_traffic_state_for_tests, shared_host_gate,
)
from archive_scout.downloads.validation import classify_exception
from archive_scout.events import Stopped
from archive_scout.network.replay_turn import ReplayTurn, ReplayTurnExpired, replay_turn
from archive_scout.network.transports import _TimedNetworkStream
from archive_scout.operations import run_project


BODY = b"<html><body>needle in the saved capture</body></html>"


class ReplayTurnBudgetTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.root = Path(self.folder.name)
        reset_shared_traffic_state_for_tests()
        self.addCleanup(reset_shared_traffic_state_for_tests)
        self.requests = []
        self.requests_lock = threading.Lock()
        self.disconnect = threading.Event()
        self.addCleanup(self.disconnect.set)
        self.server = self.start_server()

    def start_server(self):
        owner = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self):
                with owner.requests_lock:
                    owner.requests.append((self.path, time.monotonic()))
                try:
                    if self.path == "/control":
                        self.send_response(429)
                        self.send_header("Retry-After", "2")
                        self.send_header("Content-Length", "0")
                        self.end_headers()
                    elif self.path.startswith("/redirect/"):
                        hop = int(self.path.rsplit("/", 1)[1])
                        owner.disconnect.wait(.12)
                        self.send_response(302)
                        self.send_header("Location", f"/redirect/{hop+1}")
                        self.send_header("Content-Length", "0")
                        self.end_headers()
                    elif self.path == "/short":
                        self.send_response(200)
                        self.send_header("Content-Length", "132")
                        self.send_header("Connection", "close")
                        self.end_headers()
                        self.wfile.write(b"x" * 121)
                        self.close_connection = True
                    elif self.path == "/header":
                        self.wfile.write(b"HTTP/1.1 200 OK\r\nX-Drip: ")
                        self.drip(b"x" * 100)
                        self.close_connection = True
                    else:
                        self.send_response(200)
                        self.send_header("Content-Length", str(len(BODY)))
                        self.send_header("Content-Type", "text/html")
                        self.end_headers()
                        if self.path == "/slow":
                            self.drip(BODY)
                        else:
                            if self.path == "/slow-pass":
                                owner.disconnect.wait(.75)
                            self.wfile.write(BODY)
                except OSError:
                    pass

            def drip(self, body):
                for offset in range(0, len(body), 2):
                    if owner.disconnect.wait(.035):
                        break
                    self.wfile.write(body[offset:offset + 2])

            def log_message(self, *_args):
                pass

        class QuietServer(ThreadingHTTPServer):
            def handle_error(self, _request, _address):
                pass

        server = QuietServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return server

    def url(self, path):
        return f"http://127.0.0.1:{self.server.server_port}{path}"

    def client(self, backend, observe=None):
        return HttpClient(FixedRateLimiter(0), 1, 3, "test", threading.Event(),
                          network_backend=backend, trust_environment=False,
                          response_callback=observe)

    def seed(self, paths, workers=1):
        cfg = ProjectConfig(self.root, targets=["example.test/*"], keywords=["needle"],
                            workers=workers, download_delay=.01, from_date="2001", to_date="2001",
                            network=NetworkConfig(backend="httpx", trust_environment=False,
                                                  retry_base_seconds=1, retry_max_seconds=1)).normalized()
        signature = cdx_query_signature(cfg)
        with open_database(self.root) as database:
            target = get_or_create_target(database, cfg.targets[0])
            upsert_captures(database, (
                ("20010101000000", f"http://example.test{path}", "text/html", "200", f"fixture-{n}", 1+n)
                for n, path in enumerate(paths)
            ), target, signature)
            save_coverage_state(database, target, signature, cfg.from_date, cfg.to_date,
                                None, True, len(paths), None)
        database.close()
        return cfg

    def redirect_fixture(self):
        return patch("archive_scout.downloads.downloader.replay_url",
                     side_effect=lambda _time, original: self.url(original.split("example.test", 1)[1]))

    def test_active_budget_interrupts_header_and_body_trickle(self):
        backends = ["httpx", "urllib3"]
        if shutil.which("curl"):
            backends.append("curl")
        for backend in backends:
            client = self.client(backend)
            try:
                for path in ("/header", "/slow"):
                    with self.subTest(backend=backend, path=path):
                        destination = self.root / "expired.body"
                        started = time.monotonic()
                        token = replay_turn.set(ReplayTurn(.22))
                        try:
                            with self.assertRaises(ReplayTurnExpired):
                                client.download_to_path(self.url(path), destination, 1000, retain_partial=False)
                        finally:
                            replay_turn.reset(token)
                        self.assertLess(time.monotonic()-started, 1.2)
                        self.assertFalse(destination.exists())
                        self.assertEqual(client.transport.cooldown_until, {})
                        body = self.root / "next.body"
                        client.download_to_path(self.url("/fast"), body, 1000, retain_partial=False)
                        self.assertEqual(body.read_bytes(), BODY)
            finally:
                client.close()

    def test_failed_response_preserves_http_status_and_received_prefix(self):
        observed = []
        client = self.client("httpx", observed.append)
        try:
            token = replay_turn.set(ReplayTurn(1))
            try:
                with self.assertRaises(TransientRequestError) as caught:
                    client.download_to_path(self.url("/short"), self.root / "short.body", 1000,
                                            retain_partial=False)
            finally:
                replay_turn.reset(token)
            self.assertEqual(classify_exception(caught.exception)[0], "incomplete_representation")
            self.assertEqual([(e["status"], e["decoded_bytes"], e["validation_rejected"])
                              for e in observed], [(200, 121, True)])
            self.assertFalse((self.root / "short.body").exists())
        finally:
            client.close()

    def test_short_pass_yields_to_fresh_work_then_long_retry_saves_and_scans(self):
        cfg = self.seed(["/slow-pass", "/fast"])
        with self.redirect_fixture(), \
                patch("archive_scout.downloads.downloader.TEXT_FIRST_TURN_SECONDS", .5), \
                patch("archive_scout.downloads.downloader.TEXT_RETRY_TURN_SECONDS", 6.0):
            result = run_project(cfg, "download")
        self.assertEqual(result.status, "complete")
        self.assertEqual([p for p, _ in self.requests], ["/slow-pass", "/fast", "/slow-pass"])
        with closing(sqlite3.connect(self.root / "archive_scout.sqlite3")) as database:
            self.assertEqual(database.execute("SELECT state,download_attempts FROM captures ORDER BY id").fetchall(),
                             [("downloaded", 2), ("downloaded", 1)])
            self.assertEqual(database.execute("SELECT COUNT(*) FROM document_matches WHERE score>0").fetchone()[0], 2)
            self.assertEqual(database.execute("SELECT COUNT(*) FROM text_retry_schedule").fetchone()[0], 0)
            self.assertEqual(database.execute("SELECT category,resolved FROM errors").fetchall(),
                             [("replay_deadline", 1)])
        files = list((self.root / "captures").rglob("*.txt"))
        self.assertEqual(len(files), 2)
        self.assertTrue(all(hashlib.sha256(path.read_bytes()).digest() == hashlib.sha256(BODY).digest()
                            for path in files))

    def test_shared_gate_wait_leaves_attempts_untouched_and_resumes(self):
        cfg = self.seed(["/one", "/two", "/three"], workers=3)
        gate = shared_host_gate(cfg.rate_limit_base_pause, cfg.rate_limit_max_pause)
        prepared = threading.Event()
        failures = []

        def callback(event):
            if event.stage == "acquisition_health" and event.message.startswith("Text acquisition build"):
                gate.pause_for_rate_limit(1, "injected shared cooldown")
                prepared.set()

        def run():
            try:
                db = open_database(self.root)
                try:
                    download_archive(cfg, db, 0, threading.Event(), callback, acquire_only=True)
                finally:
                    db.close()
            except Exception as exc:
                failures.append(exc)

        with self.redirect_fixture():
            worker = threading.Thread(target=run)
            worker.start()
            try:
                self.assertTrue(prepared.wait(3))
                time.sleep(.15)
                self.assertEqual(self.requests, [])
                with closing(sqlite3.connect(self.root / "archive_scout.sqlite3")) as database:
                    self.assertEqual(database.execute("SELECT SUM(download_attempts) FROM captures").fetchone()[0], 0)
            finally:
                worker.join(6)
        self.assertFalse(worker.is_alive())
        self.assertEqual(failures, [])
        self.assertEqual(len(self.requests), 3)

    def test_retry_after_is_persisted_beyond_the_per_capture_backoff_limit(self):
        cfg = self.seed(["/control"])
        db = open_database(self.root)
        try:
            started = time.time()
            with self.redirect_fixture():
                download_archive(cfg, db, 0, threading.Event(), None, acquire_only=True)
            due, status = db.execute("SELECT next_eligible_at,last_status FROM text_retry_schedule").fetchone()
            self.assertGreaterEqual(due, started + 2)
            self.assertEqual(status, 429)
            self.assertEqual(db.execute("SELECT download_attempts FROM captures").fetchone()[0], 1)
            self.assertEqual(len(self.requests), 1)
        finally:
            db.close()

    def test_stop_while_shared_gate_is_closed_does_not_spend_attempts(self):
        cfg = self.seed(["/one", "/two"])
        gate = shared_host_gate(cfg.rate_limit_base_pause, cfg.rate_limit_max_pause)
        stop = threading.Event()
        timer = threading.Timer(.2, stop.set)
        db = open_database(self.root)

        def callback(event):
            if event.stage == "acquisition_health" and event.message.startswith("Text acquisition build"):
                gate.pause_for_rate_limit(30, "injected shared cooldown")
                timer.start()

        try:
            with self.redirect_fixture(), self.assertRaises(Stopped):
                download_archive(cfg, db, 0, stop, callback, acquire_only=True)
            self.assertEqual(db.execute("SELECT SUM(download_attempts) FROM captures").fetchone()[0], 0)
            self.assertEqual(self.requests, [])
        finally:
            timer.cancel()
            db.close()

    def test_a_gate_closure_between_admission_and_worker_refunds_unstarted_attempts(self):
        cfg = self.seed(["/one", "/two"])
        gate = shared_host_gate(cfg.rate_limit_base_pause, cfg.rate_limit_max_pause)
        dispatch_slots = gate.dispatch_slots
        stop = threading.Event()
        timer = threading.Timer(.45, stop.set)
        db = open_database(self.root)
        admissions = 0

        def admit(slots):
            nonlocal admissions
            admissions += 1
            if admissions == 1:
                gate.pause_for_rate_limit(30, "injected race")
                timer.start()
                return slots
            return dispatch_slots(slots)

        try:
            with self.redirect_fixture(), patch.object(gate, "dispatch_slots", side_effect=admit), \
                    self.assertRaises(Stopped):
                download_archive(cfg, db, 0, stop, None, acquire_only=True)
            self.assertEqual([tuple(row) for row in db.execute("SELECT state,download_attempts FROM captures ORDER BY id")],
                             [("pending", 0), ("pending", 0)])
            self.assertEqual(db.execute("SELECT COUNT(*) FROM errors").fetchone()[0], 0)
            path = self.root / "diagnostics" / "text-acquisition" / "latest.json"
            summary = json.loads(path.read_text())
            self.assertEqual(summary["acquisition"]["dispatch_deferrals_before_request"], 2)
            self.assertEqual(self.requests, [])
        finally:
            timer.cancel()
            db.close()

    def test_explicit_shared_wait_limit_preserves_unstarted_rows(self):
        cfg = replace(self.seed(["/control", "/fast"]), rate_limit_max_wait=.5)
        notices = []
        db = open_database(self.root)
        try:
            with self.redirect_fixture():
                download_archive(cfg, db, 0, threading.Event(), notices.append, acquire_only=True)
            self.assertEqual(self.requests[0][0], "/control")
            self.assertEqual(len(self.requests), 1)
            self.assertEqual([tuple(row) for row in db.execute("SELECT state,download_attempts FROM captures ORDER BY id")],
                             [("error", 1), ("pending", 0)])
            self.assertTrue(any(event.stage == "download_incomplete" for event in notices))
        finally:
            db.close()

    def test_redirect_hops_share_the_same_active_deadline(self):
        client = self.client("httpx")
        token = replay_turn.set(ReplayTurn(.32))
        try:
            with self.assertRaises(ReplayTurnExpired):
                client.download_to_path(self.url("/redirect/0"), self.root / "redirect.body", 1000,
                                        retain_partial=False)
            self.assertGreaterEqual(len(self.requests), 2)
            self.assertLessEqual(len(self.requests), 3)
            self.assertFalse((self.root / "redirect.body").exists())
        finally:
            replay_turn.reset(token)
            client.close()

    def test_socket_timeout_uses_the_limit_that_was_passed_to_the_socket(self):
        stream = Mock()
        stream.read.side_effect = HttpcoreReadTimeout("simulated socket timeout")
        reader = _TimedNetworkStream(stream)
        turn = ReplayTurn(20)
        turn.start()
        token = replay_turn.set(turn)
        try:
            with self.assertRaises(ReplayTurnExpired):
                reader.read(1024, timeout=180)
            with self.assertRaises(HttpcoreReadTimeout):
                reader.read(1024, timeout=.1)
            self.assertFalse(stream.read.call_args_list[0].kwargs["timeout"] > 20)
            self.assertEqual(stream.read.call_args_list[1].kwargs["timeout"], .1)
        finally:
            replay_turn.reset(token)
