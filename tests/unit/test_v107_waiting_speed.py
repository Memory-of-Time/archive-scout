from __future__ import annotations

import contextlib
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

import httpx

from archive_scout.cdx.client import HttpClient, ReplayRetryScheduled, RateLimitDeferred, TransientRequestError
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.config import ProjectConfig, load_project_config, save_project_config
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import upsert_media_capture
from archive_scout.downloads import downloader, rate_limit as rl
from archive_scout.events import IndexResponsePaused, Stopped
from archive_scout.media import downloader as media
from archive_scout.network import transports as tr
from archive_scout.operations import run_project
from archive_scout.utils import utc_now


class Clock:
    def __init__(self):
        self.now = 100.0
        self.stack = contextlib.ExitStack()

    def __enter__(self):
        self.stack.enter_context(mock.patch.object(rl.time, "monotonic", side_effect=lambda: self.now))
        self.stack.enter_context(mock.patch.object(rl.time, "time", side_effect=lambda: 1000 + self.now))
        self.stack.enter_context(mock.patch.object(rl.random, "uniform", return_value=1.0))
        return self

    def __exit__(self, *args):
        return self.stack.__exit__(*args)

    def advance(self, seconds):
        self.now += seconds


class WaitingPolicyTests(unittest.TestCase):
    def tearDown(self):
        rl.reset_shared_traffic_state_for_tests()

    def test_duplicate_no_header_failure_keeps_original_deadline(self):
        with Clock() as clock:
            gate = rl.SharedHostGate()
            old = gate.acquire_request(threading.Event())
            gate.signal_rate_limit(permit=old)
            clock.advance(4)
            gate.signal_rate_limit(permit=old)
            self.assertEqual(gate.remaining(), 1)
            self.assertEqual(gate.incidents, 1)

    def test_duplicate_without_header_does_not_overwrite_retry_after(self):
        with Clock() as clock:
            gate = rl.SharedHostGate()
            old = gate.acquire_request(threading.Event())
            gate.signal_rate_limit(5, permit=old)
            clock.advance(1)
            gate.signal_rate_limit(permit=old)
            self.assertEqual(gate.remaining(), 4)

    def test_late_duplicate_keeps_probe_current_and_cannot_reopen_recovered_gate(self):
        with Clock() as clock:
            gate = rl.SharedHostGate()
            old = gate.acquire_request(threading.Event())
            gate.signal_rate_limit(permit=old)
            clock.advance(60)
            probe = gate.acquire_request(threading.Event())
            clock.advance(1)
            gate.signal_rate_limit(permit=old)
            self.assertTrue(gate.permit_is_current(probe))
            self.assertTrue(gate.probe_inflight)
            gate.finish_request(probe, recovered=True)
            gate.signal_rate_limit(permit=old)
            self.assertFalse(gate.probe_required)

    def test_later_explicit_deadline_is_honored_without_shortening(self):
        with Clock() as clock:
            gate = rl.SharedHostGate(max_pause=30)
            old = gate.acquire_request(threading.Event())
            gate.signal_rate_limit(120, permit=old)
            clock.advance(10)
            gate.signal_rate_limit(5, permit=old)
            self.assertEqual(gate.remaining(), 110)
            gate.signal_rate_limit(180, permit=old)
            self.assertEqual(gate.remaining(), 180)

    def test_failed_fresh_probes_escalate_once_per_probe_with_bounded_jitter(self):
        with Clock() as clock, mock.patch.object(rl.random, "uniform", return_value=1.1):
            gate = rl.SharedHostGate(base_pause=5, max_pause=12, adaptive=True)
            old = gate.acquire_request(threading.Event())
            gate.signal_rate_limit(permit=old)
            waits = []
            for _ in range(3):
                clock.advance(gate.remaining())
                probe = gate.acquire_request(threading.Event())
                gate.signal_rate_limit(permit=probe)
                waits.append(gate.remaining())
                generation = gate.generation
                gate.signal_rate_limit(permit=probe)
                self.assertEqual(gate.generation, generation)
            self.assertEqual(gate.incidents, 4)
            self.assertAlmostEqual(waits[0], 11)
            self.assertAlmostEqual(waits[1], 12)
            self.assertAlmostEqual(waits[2], 12)

    def test_retry_after_zero_is_distinct_from_missing(self):
        with Clock():
            gate = rl.SharedHostGate()
            gate.signal_rate_limit(0)
            self.assertEqual(gate.remaining(), 0)
            self.assertTrue(gate.acquire_request(threading.Event()).probe)

    def test_late_zero_deadline_and_duplicate_finish_cannot_displace_next_probe(self):
        with Clock() as clock:
            gate = rl.SharedHostGate()
            old = gate.acquire_request(threading.Event())
            gate.signal_rate_limit(0, permit=old)
            probe = gate.acquire_request(threading.Event())
            clock.advance(1)
            gate.signal_rate_limit(0, permit=old)
            self.assertTrue(gate.permit_is_current(probe))
            gate.finish_request(probe, False)
            clock.advance(5)
            next_probe = gate.acquire_request(threading.Event())
            gate.finish_request(probe, False)
            self.assertTrue(gate.permit_is_current(next_probe))

    def test_connection_fallback_jitter_cannot_exceed_sixty_seconds(self):
        with Clock(), mock.patch.object(rl.random, "uniform", return_value=1.1):
            gate = rl.SharedHostGate()
            gate.pause_for_connection_outage(60)
            self.assertEqual(gate.remaining(), 60)

    def test_active_policy_release_preserves_deadline_and_removes_slow_policy(self):
        with Clock():
            gate = rl.shared_host_gate(5, 30)
            slow = gate.register_policy(300, 1800)
            fast = gate.register_policy(5, 30)
            gate.signal_rate_limit(120)
            gate.release_policy(slow)
            self.assertEqual((gate.base_pause, gate.max_pause), (5, 30))
            self.assertEqual(gate.remaining(), 120)
            gate.release_policy(fast)
            self.assertIs(rl.shared_host_gate(3, 15), gate)
            self.assertEqual((gate.base_pause, gate.max_pause), (3, 15))

    def test_client_close_releases_its_policy_registration(self):
        gate = rl.SharedHostGate(5, 30)
        network = mock.Mock()
        network.set_attempt_context_factory = None
        client = HttpClient(rl.FixedRateLimiter(0), 1, 1, "offline", threading.Event(),
                            host_gate=gate, rate_limit_base_pause=300, rate_limit_max_pause=1800, transport=network)
        self.assertEqual(gate.base_pause, 300)
        client.close()
        self.assertEqual(gate.base_pause, 5)
        self.assertFalse(gate._policies)

    def test_closed_slow_limiter_does_not_leave_optional_adaptive_debt(self):
        with Clock() as clock:
            slow = rl.SharedFixedRateLimiter(1, key=rl.WAYBACK_REPLAY_RATE_KEY, adaptive=True)
            slow.note_rate_limit(1)
            slow.close()
            fast = rl.SharedFixedRateLimiter(0.125, key=rl.WAYBACK_REPLAY_RATE_KEY, adaptive=True)
            self.assertEqual(fast.effective_delay, 0.125)
            for _ in range(8):
                clock.advance(1)
                fast.note_healthy_response()
            self.assertEqual(fast.effective_delay, 0.125)
            fast.close()

    def test_index_recovers_after_small_sustained_success_sample(self):
        with Clock() as clock:
            limiter = rl.SharedFixedRateLimiter(2.5, key=rl.WAYBACK_INDEX_RATE_KEY, adaptive=True)
            limiter.note_rate_limit(1)
            for _ in range(7):
                clock.advance(5)
                limiter.note_healthy_response()
            self.assertEqual(limiter.effective_delay, 5)
            clock.advance(5)
            limiter.note_healthy_response()
            self.assertEqual(limiter.effective_delay, 2.5)
            limiter.close()

    def test_rapid_single_success_does_not_snap_back(self):
        with Clock():
            limiter = rl.SharedFixedRateLimiter(0.125, key=rl.WAYBACK_REPLAY_RATE_KEY, adaptive=True)
            limiter.note_rate_limit(1)
            for _ in range(8):
                limiter.note_healthy_response()
            self.assertEqual(limiter.effective_delay, 0.25)
            limiter.close()

    def test_limiter_waits_exact_remaining_time_without_fifty_ms_floor(self):
        with Clock() as clock:
            limiter = rl.FixedRateLimiter(0.125)
            limiter.wait(threading.Event())
            clock.advance(0.120)
            with mock.patch.object(limiter.condition, "wait", side_effect=lambda timeout: clock.advance(timeout)) as wait:
                limiter.wait(threading.Event())
            self.assertAlmostEqual(wait.call_args.kwargs["timeout"], 0.005)

    def test_retry_delay_caps_jitter_and_preserves_explicit_server_delay(self):
        with mock.patch("archive_scout.cdx.client.random.uniform", return_value=1.2):
            self.assertEqual(HttpClient._retry_delay(10, None), 120)
            self.assertEqual(HttpClient._retry_delay(5, 0), 0)
            self.assertEqual(HttpClient._retry_delay(0, 180), 180)

    def test_shorter_pause_policy_roundtrips_without_changing_rate_floors(self):
        with tempfile.TemporaryDirectory() as directory:
            config = ProjectConfig(Path(directory), ["example.com/*"], [],
                                   rate_limit_base_pause=5, rate_limit_max_pause=30).normalized()
            loaded = load_project_config(save_project_config(config))
            self.assertEqual((loaded.rate_limit_base_pause, loaded.rate_limit_max_pause), (5, 30))
            self.assertEqual((loaded.cdx_delay, loaded.download_delay), (2.5, 0.125))

    def test_gate_wall_time_is_not_multiplied_by_workers(self):
        with Clock() as clock:
            gate = rl.SharedHostGate()
            gate.signal_rate_limit()
            clock.advance(60)
            probe = gate.acquire_request(threading.Event())
            clock.advance(2)
            gate.finish_request(probe, True)
            self.assertEqual(gate.snapshot()["service_wait_seconds"], 62)


def network_for(handler):
    backend = tr.HttpxBackend.__new__(tr.HttpxBackend)
    backend.client = httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False)
    network = tr.ResilientTransport.__new__(tr.ResilientTransport)
    network.backends = {"httpx": backend}
    network.order = ["httpx"]
    network.lock = threading.Lock()
    network.cooldown_until = {}
    network.last_success = network.callback = network.attempt_context_factory = None
    return network


class ProbeAndOriginTests(unittest.TestCase):
    def test_repeated_external_connection_failures_do_not_become_archive_outage(self):
        def handler(request):
            if request.url.host == "web.archive.org":
                return httpx.Response(302, headers={"Location": "https://external.example/file"})
            raise httpx.ConnectError("external host unavailable", request=request)
        gate = rl.SharedHostGate()
        client = HttpClient(rl.FixedRateLimiter(0), 1, 1, "offline", threading.Event(), host_gate=gate,
                            connection_failure_pause_threshold=2, transport=network_for(handler))
        try:
            for _ in range(3):
                with self.assertRaises(TransientRequestError):
                    client.get("https://web.archive.org/web/20010101000000/http://example.com/", 1024)
            self.assertEqual(gate.connection_failures, 0)
            self.assertFalse(gate.probe_required)
        finally:
            client.close()

    def test_external_503_does_not_close_archive_gate_or_adapt_archive_pacing(self):
        def handler(request):
            if request.url.host == "web.archive.org":
                return httpx.Response(302, headers={"Location": "https://external.example/file"})
            return httpx.Response(503, headers={"Retry-After": "120"})
        gate = rl.SharedHostGate()
        limiter = rl.FixedRateLimiter(0)
        client = HttpClient(limiter, 1, 1, "offline", threading.Event(), host_gate=gate, transport=network_for(handler))
        try:
            with mock.patch.object(limiter, "note_rate_limit") as adapt:
                with self.assertRaises(TransientRequestError) as raised:
                    client.get("https://web.archive.org/web/20010101000000/http://example.com/", 1024)
                self.assertEqual(raised.exception.category, "external_service_error")
                adapt.assert_not_called()
            self.assertFalse(gate.probe_required)
            self.assertEqual(gate.remaining(), 0)
        finally:
            client.close()

    def test_live_503_waits_without_quota_pacing_penalty(self):
        limiter = rl.FixedRateLimiter(0)
        client = HttpClient(limiter, 1, 1, "offline", threading.Event(), rate_limit_attempts=1,
                            transport=network_for(lambda request: httpx.Response(503, headers={"Retry-After": "3"})))
        try:
            with mock.patch.object(limiter, "note_rate_limit") as adapt:
                with self.assertRaises(RateLimitDeferred):
                    client.get("https://web.archive.org/cdx/search/cdx", 1024)
                adapt.assert_not_called()
            self.assertGreater(client.host_gate.remaining(), 2)
        finally:
            client.close()

    def test_probe_releases_on_valid_cdx_prefix_but_truncation_still_fails(self):
        gate = rl.SharedHostGate()
        gate.signal_rate_limit(0)
        test = self
        class Broken(httpx.SyncByteStream):
            def __iter__(self):
                yield b"20010101000000 http://example.com/ text/html 200 ABC 200\n"
                test.assertFalse(gate.probe_required)
                raise httpx.ReadError("truncated CDX body")
        client = HttpClient(rl.FixedRateLimiter(0), 1, 1, "offline", threading.Event(), host_gate=gate,
                            transport=network_for(lambda request: httpx.Response(200, stream=Broken())))
        try:
            with self.assertRaises(TransientRequestError):
                client.get("https://web.archive.org/cdx/search/cdx", 1024)
        finally:
            client.close()

    def test_replay_headers_release_probe_before_slow_body_finishes(self):
        gate = rl.SharedHostGate()
        gate.signal_rate_limit(0)
        test = self
        class Body(httpx.SyncByteStream):
            def __iter__(self):
                test.assertFalse(gate.probe_required)
                yield b"saved text"
        client = HttpClient(rl.FixedRateLimiter(0), 1, 1, "offline", threading.Event(), host_gate=gate,
                            transport=network_for(lambda request: httpx.Response(200, headers={"Memento-Datetime": "Mon, 01 Jan 2001 00:00:00 GMT"}, stream=Body())))
        try:
            with tempfile.TemporaryDirectory() as directory:
                result = client.download_to_path("https://web.archive.org/web/20010101000000/http://example.com/", Path(directory) / "capture.part", 1024)
                self.assertEqual(result["bytes"], 10)
        finally:
            client.close()


class SchedulingAndRecoveryTests(unittest.TestCase):
    def setUp(self):
        rl.reset_shared_traffic_state_for_tests()
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.config = ProjectConfig(self.root, ["example.com/*"], [], workers=1).normalized()
        self.db = open_database(self.root)

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()
        rl.reset_shared_traffic_state_for_tests()

    def add_media(self, count):
        for number in range(count):
            upsert_media_capture(self.db, {"original": f"http://example.com/{number}.jpg", "timestamp": "20010101000000",
                "mimetype": "image/jpeg", "statuscode": "200", "length": "10"}, None, "sig", "image", ".jpg")
        self.db.commit()
        return [int(row[0]) for row in self.db.execute("SELECT id FROM media_captures ORDER BY id")]

    def media_success(self, row):
        path = self.root / "media" / "images" / (str(row["id"]) + ".jpg")
        path.write_bytes(b"JPEG evidence")
        return {"id": int(row["id"]), "path": str(path), "bytes": 13, "hash": "", "status": 200, "final_url": row["original_url"]}

    def test_media_retry_yields_worker_to_fresh_url_and_counts_one_logical_job(self):
        ids = self.add_media(2)
        order = []
        def fetch(row, config, client):
            attempt = client._permit_local.replay_attempt
            order.append((int(row["id"]), attempt))
            if int(row["id"]) == ids[0] and attempt == 1:
                raise ReplayRetryScheduled("temporary", 0.05, 2)
            return self.media_success(row)
        with mock.patch.object(media, "fetch_media", side_effect=fetch), mock.patch.object(HttpClient, "retry_wait", side_effect=AssertionError("worker slept")):
            media.download_media(self.config, self.db, threading.Event(), media_capture_ids=ids)
        self.assertEqual(order, [(ids[0], 1), (ids[1], 1), (ids[0], 2)])
        rows = self.db.execute("SELECT state,download_attempts FROM media_captures").fetchall()
        self.assertEqual([(row[0], row[1]) for row in rows], [("downloaded", 1), ("downloaded", 1)])

    def test_media_recovery_preserves_user_stop_flag_and_pending_queue(self):
        ids = self.add_media(3)
        stop = threading.Event()
        with mock.patch.object(media, "fetch_media", side_effect=RateLimitDeferred("temporary service pause")):
            with self.assertRaises(RateLimitDeferred):
                media.download_media(self.config, self.db, stop, media_capture_ids=ids)
        self.assertFalse(stop.is_set())
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM media_captures WHERE state='pending'").fetchone()[0], 3)
        self.assertEqual(self.db.execute("SELECT SUM(download_attempts) FROM media_captures").fetchone()[0], 0)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM errors").fetchone()[0], 0)
        with mock.patch.object(media, "fetch_media", side_effect=lambda row, config, client: self.media_success(row)):
            media.download_media(self.config, self.db, stop, media_capture_ids=ids)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM media_captures WHERE state='downloaded'").fetchone()[0], 3)

    def test_media_recovery_commits_other_completed_future_before_returning(self):
        self.config.workers = 2
        ids = self.add_media(2)
        completed = threading.Event()
        stop = threading.Event()
        def fetch(row, config, client):
            if int(row["id"]) == ids[0]:
                if not completed.wait(2):
                    raise AssertionError("second media worker did not run")
                raise RateLimitDeferred("temporary throttle")
            result = self.media_success(row)
            completed.set()
            return result
        with mock.patch.object(media, "fetch_media", side_effect=fetch):
            with self.assertRaises(RateLimitDeferred):
                media.download_media(self.config, self.db, stop, media_capture_ids=ids)
        self.assertFalse(stop.is_set())
        rows = self.db.execute("SELECT state,download_attempts,path FROM media_captures ORDER BY id").fetchall()
        self.assertEqual((rows[0][0], rows[0][1]), ("pending", 0))
        self.assertEqual((rows[1][0], rows[1][1]), ("downloaded", 1))
        self.assertTrue(Path(rows[1][2]).is_file())

    def test_user_stop_during_media_delay_keeps_partial_and_pending(self):
        ids = self.add_media(1)
        stop = threading.Event()
        part = self.root / "test.jpg.part"
        def fetch(row, config, client):
            part.write_bytes(b"partial evidence")
            stop.set()
            raise ReplayRetryScheduled("temporary", 30, 2)
        with mock.patch.object(media, "fetch_media", side_effect=fetch):
            with self.assertRaises(Stopped):
                media.download_media(self.config, self.db, stop, media_capture_ids=ids)
        self.assertEqual(part.read_bytes(), b"partial evidence")
        self.assertEqual(self.db.execute("SELECT state,download_attempts FROM media_captures").fetchone()[0], "pending")

    def test_due_text_retry_batch_cannot_hide_fresh_database_work(self):
        stamp = utc_now()
        for number in range(70):
            self.db.execute("""INSERT INTO captures(original_url,timestamp,query_signature,mimetype,statuscode,length,state,resource_class,created_at,updated_at)
                VALUES(?,'20010101000000',?,'text/plain','200',32,'pending','text',?,?)""",
                (f"http://example.com/{number:03d}.txt", cdx_query_signature(self.config), stamp, stamp))
        self.db.commit()
        order = []
        def fetch(row, path, config, client, **kwargs):
            number = int(str(row["original_url"]).rsplit("/", 1)[1].split(".")[0])
            attempt = int(row.get("retry_attempt", 1))
            order.append((number, attempt))
            if number < 64 and attempt == 1:
                raise ReplayRetryScheduled("temporary", 0, 2)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"evidence")
            return {"kind": "downloaded", "path": path, "bytes_saved": 8, "content_hash": "", "http_status": 200, "final_url": row["original_url"]}
        with mock.patch.object(downloader, "_download_capture", side_effect=fetch):
            result = downloader.download_archive_only(self.config, self.db, threading.Event(), None)
        self.assertEqual(result["downloaded"], 70)
        first_fresh = order.index((64, 1))
        last_retry = max(i for i, (_number, attempt) in enumerate(order) if attempt == 2)
        self.assertLess(first_fresh, last_retry)

    def test_query_specific_cdx_recovery_does_not_close_shared_archive_gate(self):
        config = self.config.normalized()
        config.network.retry_base_seconds = 0.01
        config.network.retry_max_seconds = 0.01
        self.db.close()
        gate = rl.shared_host_gate(config.rate_limit_base_pause, config.rate_limit_max_pause)
        try:
            with mock.patch("archive_scout.operations.index_archive", side_effect=[IndexResponsePaused("malformed CDX for one query"), None]) as index:
                run_project(config, "index", threading.Event())
            self.assertEqual(index.call_count, 2)
            self.assertFalse(gate.probe_required)
            self.assertEqual(gate.remaining(), 0)
        finally:
            self.db = open_database(self.root)


if __name__ == "__main__":
    unittest.main()
