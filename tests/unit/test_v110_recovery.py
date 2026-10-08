from __future__ import annotations

import contextlib
from collections import deque
import heapq
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest import mock

import httpx

from archive_scout.cdx.client import HttpClient, RateLimitDeferred, ReplayRetryScheduled
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.config import ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import upsert_media_capture
from archive_scout.downloads import downloader
from archive_scout.downloads.rate_limit import FixedRateLimiter, SharedHostGate, reset_shared_traffic_state_for_tests
from archive_scout.downloads.recovery import wake_backend_retries
from archive_scout.events import ConnectivityPaused
from archive_scout.media import downloader as media
from archive_scout.network import transports as tr
from archive_scout.utils import utc_now


class Backend:
    def __init__(self, name):
        self.name = name
        self.failure = httpx.ConnectError("offline fixture")
        self.calls = 0

    def request(self, url, headers, max_bytes, stop_event, *, attempt_context_factory=None, **kwargs):
        scope = attempt_context_factory(url) if attempt_context_factory else contextlib.nullcontext()
        with scope as progress:
            self.calls += 1
            if self.failure:
                raise self.failure
            response_headers = {"memento-datetime": "Mon, 01 Jan 2001 00:00:00 GMT"}
            if progress:
                progress(200, response_headers, url, b"fixture evidence")
            return tr.TransportResponse(200, response_headers, url, b"fixture evidence", self.name, 0)

    def download(self, url, headers, destination, max_bytes, stop_event, **kwargs):
        response = self.request(url, headers, max_bytes, stop_event, **kwargs)
        destination.write_bytes(response.data)
        return tr.TransportFileResponse(response.status, response.headers, url, destination,
                                        len(response.data), "", response.data, self.name, 0)

    def close(self):
        pass


def transport(*backends):
    network = tr.ResilientTransport.__new__(tr.ResilientTransport)
    network.backends = {backend.name: backend for backend in backends}
    network.order = list(network.backends)
    network.lock = threading.Lock()
    network.callback = network.attempt_context_factory = None
    network.last_success = None
    return network


class RecoveryAdmissionTests(unittest.TestCase):
    url = "https://web.archive.org/web/20010101000000id_/http://example.com/"

    def test_admitted_probe_bypasses_local_cooldown_without_false_failed_probe(self):
        for download in (False, True):
            with self.subTest(download=download), tempfile.TemporaryDirectory() as directory:
                primary, fallback = Backend("httpx"), Backend("curl")
                network = transport(primary, fallback)
                gate = SharedHostGate()
                with mock.patch.object(tr.time, "monotonic", return_value=100):
                    with self.assertRaises(tr.TransportExhaustedError):
                        network.request(self.url, {}, 1024, threading.Event())
                    gate.pause_for_connection_outage(3)
                primary.failure = None
                client = HttpClient(FixedRateLimiter(0), 3, 1, "offline", threading.Event(),
                                    host_gate=gate, transport=network)
                try:
                    with mock.patch.object(tr.time, "monotonic", return_value=104), \
                            mock.patch.object(client, "_wait_for_backend_cooldown", side_effect=AssertionError("extra cooldown")):
                        if download:
                            with client.replay_attempt():
                                result = client.download_to_path(self.url, Path(directory) / "capture.part", 1024)
                        else:
                            result = client.get(self.url, 1024)
                    self.assertEqual(result["backend"], "httpx")
                    self.assertEqual((primary.calls, fallback.calls), (2, 1))
                    self.assertFalse(gate.snapshot()["probe_required"])
                    self.assertEqual(gate.connection_outage_cycles, 0)
                    self.assertEqual(client.metrics_snapshot()["wire_request_starts"], 1)
                finally:
                    client.close()

    def test_probe_scope_does_not_release_other_origins_or_ordinary_workers(self):
        primary = Backend("httpx")
        network = transport(primary)
        with self.assertRaises(tr.TransportExhaustedError):
            network.request(self.url, {}, 1024, threading.Event())
        with network.recovery_probe(self.url):
            self.assertTrue(network._claim_backend(self.url, "httpx"))
            self.assertFalse(network._claim_backend(self.url, "httpx"))
            network._release_backend(self.url, "httpx")
            # A different worker retains the local cooldown.
            allowed = []
            worker = threading.Thread(target=lambda: allowed.append(network._claim_backend(self.url, "httpx")))
            worker.start(); worker.join(1)
            self.assertEqual(allowed, [False])
        self.assertFalse(network.backend_ready(self.url))
        self.assertTrue(network.backend_ready("https://other.example/"))

    def test_unused_probe_does_not_escalate_when_backend_is_already_probing(self):
        primary = Backend("httpx")
        network = transport(primary)
        gate = SharedHostGate()
        with mock.patch.object(tr.time, "monotonic", return_value=100):
            network._backend_failed(self.url, "httpx", primary.failure)
            network._health_locked(self.url).probing.add("httpx")
            gate.pause_for_connection_outage(3)
        generation, deadline = gate.generation, gate.blocked_until
        client = HttpClient(FixedRateLimiter(0), 3, 1, "offline", threading.Event(),
                            host_gate=gate, transport=network)
        try:
            with mock.patch.object(tr.time, "monotonic", return_value=104), client.replay_attempt():
                with self.assertRaises(ReplayRetryScheduled):
                    client.download_to_path(self.url, Path("unused.part"), 1024)
            self.assertEqual(gate.connection_outage_cycles, 1)
            self.assertEqual((gate.generation, gate.blocked_until), (generation, deadline))
            self.assertTrue(gate.snapshot()["probe_required"])
            self.assertFalse(gate.snapshot()["probe_inflight"])
            self.assertEqual(client.metrics_snapshot()["wire_request_starts"], 0)
        finally:
            client.close()

    def test_ordinary_validated_progress_recovers_connection_only_incident(self):
        gate = SharedHostGate()
        permit = gate.acquire_request(threading.Event())
        client = HttpClient(FixedRateLimiter(0), 3, 1, "offline", threading.Event(), host_gate=gate,
                            transport=transport(Backend("httpx")))
        try:
            client._permit_local.permit = permit
            client._permit_local.logical_url = self.url
            client._permit_local.streaming_download = True
            # Initial healthy headers must not suppress later progress signals.
            client._response_progress(200, {}, self.url, b"validated prefix")
            gate.pause_for_connection_outage(60)
            self.assertGreater(gate.remaining(), 59)
            client._response_progress(200, {}, self.url, b"validated next bytes")
            self.assertEqual(gate.remaining(), 0)
            self.assertFalse(gate.snapshot()["probe_required"])
            self.assertFalse(gate.acquire_request(threading.Event(), deadline=time.monotonic() - 1).probe)
        finally:
            client.close()

    def test_old_success_cannot_clear_later_outage_or_server_deadline(self):
        gate = SharedHostGate()
        old = gate.acquire_request(threading.Event())
        gate.pause_for_connection_outage(60)
        self.assertTrue(gate.note_connection_success(permit=old, recovered=True))
        gate.pause_for_connection_outage(60)
        self.assertFalse(gate.note_connection_success(permit=old, recovered=True))
        self.assertGreater(gate.remaining(), 59)
        current = gate.connection_outage_generation
        gate.signal_rate_limit(120, "HTTP 429")
        deadline = gate.blocked_until
        from archive_scout.downloads.rate_limit import HostPermit
        self.assertFalse(gate.note_connection_success(permit=HostPermit(current, False), recovered=True))
        self.assertEqual(gate.blocked_until, deadline)
        self.assertEqual(gate.snapshot()["reason"], "HTTP 429")

    def test_untrusted_progress_and_external_progress_do_not_release_outage(self):
        gate = SharedHostGate()
        permit = gate.acquire_request(threading.Event())
        gate.pause_for_connection_outage(60)
        client = HttpClient(FixedRateLimiter(0), 3, 1, "offline", threading.Event(), host_gate=gate,
                            transport=transport(Backend("httpx")))
        try:
            client._permit_local.permit = permit
            client._permit_local.logical_url = self.url
            client._permit_local.streaming_download = False
            client._response_progress(200, {}, self.url, b"unvalidated body")
            client._response_progress(503, {}, self.url)
            client._response_progress(200, {"memento-datetime": "fixture"}, "https://external.example/web/fixture")
            self.assertGreater(gate.remaining(), 59)
        finally:
            client.close()

    def test_backend_retry_wakes_early_but_server_retry_deadline_is_preserved(self):
        primary = Backend("httpx")
        network = transport(primary)
        client = HttpClient(FixedRateLimiter(0), 3, 1, "offline", threading.Event(), transport=network)
        try:
            with self.assertRaises(tr.TransportExhaustedError):
                network.request(self.url, {}, 1024, threading.Event())
            with client.replay_attempt():
                with self.assertRaises(ReplayRetryScheduled) as paused:
                    client.download_to_path(self.url, Path("unused.part"), 1024)
            self.assertEqual(paused.exception.wait_kind, "backend_cooldown")
            now = time.monotonic()
            delayed = [(now + 30, 1, {"id": 1}, "path1", "backend_cooldown"),
                       (now + 120, 2, {"id": 2}, "path2", "retry")]
            heapq.heapify(delayed)
            ready = deque()
            self.assertEqual(wake_backend_retries(delayed, ready, client, lambda row: self.url), 0)
            network._backend_succeeded(self.url, "httpx")
            self.assertEqual(wake_backend_retries(delayed, ready, client, lambda row: self.url), 1)
            self.assertEqual(list(ready), [({"id": 1}, "path1")])
            self.assertEqual(delayed, [(now + 120, 2, {"id": 2}, "path2", "retry")])
        finally:
            client.close()

    def test_admission_pause_drains_waiter_without_setting_transport_stop(self):
        gate = SharedHostGate()
        gate.signal_rate_limit(120, "HTTP 503")
        deadline = gate.blocked_until
        stop = threading.Event()
        client = HttpClient(FixedRateLimiter(0), 3, 1, "offline", stop, host_gate=gate,
                            rate_limit_max_wait=900, transport=transport(Backend("httpx")))
        waiting = threading.Event()
        errors = []
        def wait_for_permit():
            waiting.set()
            try:
                client._acquire_host_permit()
            except BaseException as exc:
                errors.append(exc)
        worker = threading.Thread(target=wait_for_permit)
        worker.start()
        try:
            self.assertTrue(waiting.wait(1))
            client.pause_admissions(RateLimitDeferred("service fixture"))
            worker.join(1.5)
            self.assertFalse(worker.is_alive(), "admission drain waited for the server cooldown")
            self.assertEqual(len(errors), 1)
            self.assertIsInstance(errors[0], RateLimitDeferred)
            self.assertFalse(stop.is_set())
            self.assertFalse(client._active_stop_event().is_set())
            self.assertEqual(gate.blocked_until, deadline)
            client.resume_admissions()
            self.assertFalse(client._admission_pause.is_set())
        finally:
            stop.set()
            with gate.condition:
                gate.condition.notify_all()
            worker.join(2)
            client.close()


class HealthySiblingTests(unittest.TestCase):
    def setUp(self):
        reset_shared_traffic_state_for_tests()
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.config = ProjectConfig(self.root, ["example.com/*"], [], workers=2).normalized()
        self.db = open_database(self.root)

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()
        reset_shared_traffic_state_for_tests()

    def add_text(self):
        stamp = utc_now()
        for number in range(2):
            self.db.execute("""INSERT INTO captures(original_url,timestamp,query_signature,mimetype,
                statuscode,length,state,resource_class,created_at,updated_at)
                VALUES(?,'20010101000000',?,'text/plain','200',32,'pending','text',?,?)""",
                (f"http://example.com/{number}.txt", cdx_query_signature(self.config), stamp, stamp))
        self.db.commit()
        return [int(row[0]) for row in self.db.execute("SELECT id FROM captures ORDER BY id")]

    def test_text_recovery_keeps_running_healthy_download_and_partial_pending(self):
        ids = self.add_text()
        started = threading.Event()
        failed = threading.Event()
        attempts = {}
        stop = threading.Event()

        def fetch(row, path, config, client, **kwargs):
            cid = int(row["id"])
            attempts[cid] = attempts.get(cid, 0) + 1
            if cid == ids[0]:
                started.set()
                self.assertTrue(failed.wait(2))
                # Give the coordinator time to process the sibling pause.
                self.assertFalse(kwargs["stop_event"].wait(0.15), "healthy download was cancelled")
            elif attempts[cid] == 1:
                self.assertTrue(started.wait(2))
                failed.set()
                path.parent.mkdir(parents=True, exist_ok=True)
                path.with_suffix(path.suffix + ".part").write_bytes(b"partial evidence")
                raise ConnectivityPaused("offline fixture")
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"evidence")
            return {"kind": "downloaded", "path": path, "bytes_saved": 8,
                    "content_hash": "", "http_status": 200, "final_url": row["original_url"]}

        with mock.patch.object(downloader, "_download_capture", side_effect=fetch):
            result = downloader.download_archive_only(self.config, self.db, stop, None)
        self.assertEqual(result["downloaded"], 2)
        self.assertEqual(attempts[ids[0]], 1)
        self.assertFalse(stop.is_set())
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM errors").fetchone()[0], 0)
        self.assertTrue(any(path.read_bytes() == b"partial evidence" for path in self.root.rglob("*.part")))

    def test_backend_retry_heap_is_released_after_healthy_sibling_for_text_and_media(self):
        self.config.workers = 1
        text_ids = self.add_text()
        text_calls = []
        def text_fetch(row, path, config, client, **kwargs):
            cid = int(row["id"])
            text_calls.append(cid)
            url = downloader.replay_url(str(row["timestamp"]), str(row["original_url"]))
            if cid == text_ids[0] and text_calls.count(cid) == 1:
                with client.transport.lock:
                    state = client.transport._health_locked(url)
                    state.cooldown_until.update({name: time.monotonic() + 30 for name in client.transport.order})
                raise ReplayRetryScheduled("local cooldown", 30, 1, wait_kind="backend_cooldown")
            client.transport._backend_succeeded(url, client.transport.order[0])
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"evidence")
            return {"kind": "downloaded", "path": path, "bytes_saved": 8,
                    "content_hash": "", "http_status": 200, "final_url": row["original_url"]}
        started = time.monotonic()
        with mock.patch.object(downloader, "_download_capture", side_effect=text_fetch):
            result = downloader.download_archive_only(self.config, self.db, threading.Event(), None)
        self.assertLess(time.monotonic() - started, 3)
        self.assertEqual((result["downloaded"], text_calls), (2, [text_ids[0], text_ids[1], text_ids[0]]))
        for number in range(2):
            upsert_media_capture(self.db, {"original": f"http://example.com/retry{number}.jpg",
                "timestamp": "20010101000000", "mimetype": "image/jpeg", "statuscode": "200", "length": "10"},
                None, "sig", "image", ".jpg")
        self.db.commit()
        media_ids = [int(row[0]) for row in self.db.execute("SELECT id FROM media_captures ORDER BY id")]
        media_calls = []
        def media_fetch(row, config, client):
            cid = int(row["id"])
            media_calls.append(cid)
            url = downloader.replay_url(str(row["timestamp"]), str(row["original_url"]))
            if cid == media_ids[0] and media_calls.count(cid) == 1:
                with client.transport.lock:
                    state = client.transport._health_locked(url)
                    state.cooldown_until.update({name: time.monotonic() + 30 for name in client.transport.order})
                raise ReplayRetryScheduled("local cooldown", 30, 2, wait_kind="backend_cooldown")
            client.transport._backend_succeeded(url, client.transport.order[0])
            path = self.root / "media" / "images" / f"{cid}.jpg"
            path.write_bytes(b"JPEG evidence")
            return {"id": cid, "path": str(path), "bytes": 13, "hash": "", "status": 200,
                    "final_url": row["original_url"]}
        started = time.monotonic()
        with mock.patch.object(media, "fetch_media", side_effect=media_fetch):
            media.download_media(self.config, self.db, threading.Event(), media_capture_ids=media_ids)
        self.assertLess(time.monotonic() - started, 3)
        self.assertEqual(media_calls, [media_ids[0], media_ids[1], media_ids[0]])
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM media_captures WHERE state='downloaded'").fetchone()[0], 2)

    def test_text_recovery_drains_only_admissions_then_resumes_same_client(self):
        ids = self.add_text()
        stamp = utc_now()
        self.db.execute("""INSERT INTO captures(original_url,timestamp,query_signature,mimetype,
            statuscode,length,state,resource_class,created_at,updated_at)
            VALUES('http://example.com/waiter.txt','20010101000000',?,'text/plain','200',32,'pending','text',?,?)""",
            (cdx_query_signature(self.config), stamp, stamp))
        self.db.commit()
        waiter_id = int(self.db.execute("SELECT MAX(id) FROM captures").fetchone()[0])
        self.config.workers = 3
        healthy_started, gate_closed, waiter_started = threading.Event(), threading.Event(), threading.Event()
        attempts = {}
        clients = []
        stop = threading.Event()
        def fetch(row, path, config, client, **kwargs):
            cid = int(row["id"])
            attempts[cid] = attempts.get(cid, 0) + 1
            if client not in clients:
                clients.append(client)
            if cid == ids[0]:
                healthy_started.set()
                self.assertTrue(gate_closed.wait(2))
                self.assertFalse(kwargs["stop_event"].wait(0.2), "healthy I/O was cancelled")
            elif cid == ids[1] and attempts[cid] == 1:
                self.assertTrue(healthy_started.wait(2))
                client.host_gate.signal_rate_limit(120, "HTTP 503")
                gate_closed.set()
                self.assertTrue(waiter_started.wait(2))
                raise RateLimitDeferred("service fixture")
            elif cid == waiter_id and attempts[cid] == 1:
                self.assertTrue(gate_closed.wait(2))
                waiter_started.set()
                client._acquire_host_permit()
                raise AssertionError("waiter sent a request during the service deadline")
            else:
                self.assertFalse(client._admission_pause.is_set())
                permit = client._acquire_host_permit()
                client.host_gate.finish_request(permit, recovered=True)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"evidence")
            return {"kind": "downloaded", "path": path, "bytes_saved": 8,
                    "content_hash": "", "http_status": 200, "final_url": row["original_url"]}
        def recovered(config, gate, stop_event, callback, **kwargs):
            # Advance eligibility in this offline test; production retains it.
            self.assertEqual(gate.snapshot()["reason"], "HTTP 503")
            self.assertTrue(clients[0]._admission_pause.is_set())
            gate.blocked_until = time.monotonic() - 1
            gate.renew_recovery_cycle(gate.incident_id)
        started = time.monotonic()
        with mock.patch.object(downloader, "_download_capture", side_effect=fetch), \
                mock.patch.object(downloader, "wait_for_archive", side_effect=recovered):
            result = downloader.download_archive_only(self.config, self.db, stop, None)
        self.assertLess(time.monotonic() - started, 3)
        self.assertEqual(result["downloaded"], 3)
        self.assertEqual(len(clients), 1)
        self.assertEqual(attempts[ids[0]], 1)
        self.assertEqual(attempts[waiter_id], 2)
        self.assertFalse(stop.is_set())

    def test_media_service_pause_does_not_cancel_healthy_incomplete_sibling(self):
        for number in range(2):
            upsert_media_capture(self.db, {"original": f"http://example.com/{number}.jpg",
                "timestamp": "20010101000000", "mimetype": "image/jpeg", "statuscode": "200", "length": "10"},
                None, "sig", "image", ".jpg")
        self.db.commit()
        ids = [int(row[0]) for row in self.db.execute("SELECT id FROM media_captures ORDER BY id")]
        started = threading.Event()
        failed = threading.Event()
        stop = threading.Event()

        def fetch(row, config, client):
            cid = int(row["id"])
            if cid == ids[0]:
                self.assertTrue(started.wait(2))
                failed.set()
                raise RateLimitDeferred("service fixture")
            started.set()
            self.assertTrue(failed.wait(2))
            self.assertFalse(client._active_stop_event().wait(0.15), "healthy media was cancelled")
            path = self.root / "media" / "images" / "evidence.jpg"
            path.write_bytes(b"JPEG evidence")
            return {"id": cid, "path": str(path), "bytes": 13, "hash": "", "status": 200,
                    "final_url": row["original_url"]}

        with mock.patch.object(media, "fetch_media", side_effect=fetch):
            with self.assertRaises(RateLimitDeferred):
                media.download_media(self.config, self.db, stop, media_capture_ids=ids)
        rows = self.db.execute("SELECT state,download_attempts,path FROM media_captures ORDER BY id").fetchall()
        self.assertEqual((rows[0][0], rows[0][1]), ("pending", 0))
        self.assertEqual((rows[1][0], rows[1][1]), ("downloaded", 1))
        self.assertTrue(Path(rows[1][2]).is_file())
        self.assertFalse(stop.is_set())
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM errors").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
