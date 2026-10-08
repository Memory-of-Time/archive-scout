from __future__ import annotations

import contextlib
import json
import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path
from unittest import mock

import httpx

from archive_scout.cdx.client import HttpClient, RateLimitDeferred
from archive_scout.downloads import rate_limit as rl
from archive_scout.network import transports as tr
from archive_scout.config import ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import start_operation_run, update_operation_run, finish_operation_run
from archive_scout.downloads.recovery import wait_for_archive
from archive_scout.operations import run_project
from archive_scout.events import Stopped


class Clock:
    def __init__(self):
        self.now = 1000.0
        self.stack = contextlib.ExitStack()

    def __enter__(self):
        self.stack.enter_context(mock.patch.object(rl.time, 'monotonic', side_effect=lambda: self.now))
        self.stack.enter_context(mock.patch.object(rl.time, 'time', side_effect=lambda: self.now + 10000))
        self.stack.enter_context(mock.patch.object(rl.random, 'uniform', return_value=1.0))
        return self

    def __exit__(self, *args):
        return self.stack.__exit__(*args)

    def wait(self, timeout):
        self.now += timeout
        return False


class CooldownPolicyTests(unittest.TestCase):
    def tearDown(self):
        rl.reset_shared_traffic_state_for_tests()

    def test_default_off_repeated_failed_probes_never_escalate(self):
        with Clock() as clock:
            gate = rl.SharedHostGate()
            for _ in range(12):
                permit = gate.acquire_request(threading.Event())
                gate.signal_rate_limit(permit=permit)
                self.assertEqual(gate.remaining(), 5)
                self.assertEqual(gate.snapshot()['wait_source'], 'fixed_fallback')
                clock.wait(5)
            probe = gate.acquire_request(threading.Event())
            self.assertTrue(probe.probe)
            gate.finish_request(probe, True)
            self.assertFalse(gate.acquire_request(threading.Event()).probe)

    def test_opt_in_retains_bounded_exponential_cooldowns(self):
        with Clock() as clock:
            gate = rl.SharedHostGate(adaptive=True)
            waits = []
            for _ in range(6):
                permit = gate.acquire_request(threading.Event())
                gate.signal_rate_limit(permit=permit)
                waits.append(gate.remaining())
                clock.wait(gate.remaining())
            self.assertEqual(waits, [60, 120, 240, 480, 600, 600])
            self.assertEqual(gate.snapshot()['wait_source'], 'adaptive_fallback')

    def test_fixed_retry_respects_a_shorter_configured_base(self):
        with Clock():
            gate = rl.SharedHostGate(base_pause=0.5)
            gate.signal_rate_limit()
            self.assertEqual(gate.remaining(), 0.5)

    def test_off_preserves_long_server_deadline_above_adaptive_maximum(self):
        with Clock() as clock:
            gate = rl.SharedHostGate(max_pause=600)
            old = gate.acquire_request(threading.Event())
            gate.signal_rate_limit(3600, permit=old)
            clock.wait(2)
            gate.signal_rate_limit(permit=old)
            self.assertEqual(gate.remaining(), 3598)
            self.assertEqual(gate.snapshot()['wait_source'], 'server_retry_after')
            gate.condition.wait = clock.wait
            self.assertTrue(gate.acquire_request(threading.Event()).probe)
            self.assertEqual(clock.now, 4600)

    def test_explicit_zero_is_immediate_single_probe_in_either_mode(self):
        for adaptive in (False, True):
            with self.subTest(adaptive=adaptive), Clock():
                gate = rl.SharedHostGate(adaptive=adaptive)
                gate.signal_rate_limit(0)
                self.assertEqual(gate.remaining(), 0)
                self.assertTrue(gate.acquire_request(threading.Event()).probe)
                self.assertTrue(gate.probe_inflight)

    def test_fixed_client_does_not_inherit_an_active_adaptive_wait(self):
        with Clock() as clock:
            gate = rl.SharedHostGate(adaptive=True)
            enabled = gate.register_policy(60, 600, adaptive=True)
            gate.signal_rate_limit()
            clock.wait(2)
            fixed = gate.register_policy(60, 600, adaptive=False)
            self.assertEqual(gate.remaining(), 3)
            self.assertFalse(gate.adaptive)
            gate.release_policy(fixed)
            self.assertTrue(gate.adaptive)
            self.assertEqual(gate.remaining(), 3)  # Closing off never re-inflates debt.
            gate.release_policy(enabled)

    def test_opt_out_after_fixed_interval_admits_probe_promptly(self):
        with Clock() as clock:
            gate = rl.SharedHostGate(adaptive=True)
            gate.signal_rate_limit()
            clock.wait(20)
            gate.configure(60, 600, adaptive=False)
            self.assertEqual(gate.remaining(), 0)
            self.assertTrue(gate.acquire_request(threading.Event()).probe)

    def test_opt_out_preserves_server_deadline_hidden_under_longer_optional_wait(self):
        with Clock() as clock:
            gate = rl.SharedHostGate(adaptive=True)
            old = gate.acquire_request(threading.Event())
            gate.signal_rate_limit(permit=old)
            clock.wait(1)
            gate.signal_rate_limit(30, permit=old)
            self.assertEqual(gate.remaining(), 59)
            gate.configure(60, 600, adaptive=False)
            self.assertEqual(gate.remaining(), 30)
            self.assertEqual(gate.snapshot()['wait_source'], 'server_retry_after')

    def test_optional_toggle_does_not_shorten_connection_recovery(self):
        with Clock():
            gate = rl.SharedHostGate(adaptive=True)
            gate.pause_for_connection_outage(30)
            gate.configure(60, 600, adaptive=False)
            self.assertEqual(gate.remaining(), 30)
            self.assertEqual(gate.snapshot()['wait_source'], 'connection_recovery')

    def test_late_explicit_zero_does_not_restart_optional_wait_on_opt_out(self):
        with Clock() as clock:
            gate = rl.SharedHostGate(adaptive=True)
            old = gate.acquire_request(threading.Event())
            gate.signal_rate_limit(permit=old)
            clock.wait(10)
            gate.signal_rate_limit(0, permit=old)
            gate.configure(60, 600, adaptive=False)
            self.assertEqual(gate.remaining(), 0)

    def test_restored_optional_wait_remains_optional_on_later_opt_out(self):
        with Clock() as clock:
            gate = rl.SharedHostGate(adaptive=True)
            detail = {'wait_source': 'adaptive_fallback', 'eligible_at_epoch': 11100,
                      'rate_limit_signal_at_epoch': 11000, 'server_eligible_at_epoch': 11020}
            gate.restore_service_wait(detail)
            self.assertEqual(gate.remaining(), 100)
            clock.wait(2)
            gate.configure(60, 600, adaptive=False)
            self.assertEqual(gate.remaining(), 18)

    def test_restart_off_drops_known_expired_optional_debt(self):
        with Clock():
            gate = rl.SharedHostGate()
            detail = {'wait_source': 'adaptive_fallback', 'eligible_at_epoch': 11100,
                      'rate_limit_signal_at_epoch': 10980}
            gate.restore_service_wait(detail)
            self.assertEqual(gate.remaining(), 0)

    def test_restart_off_preserves_server_and_unknown_legacy_waits(self):
        for source in ('server_retry_after', ''):
            with self.subTest(source=source), Clock():
                gate = rl.SharedHostGate()
                gate.restore_service_wait({'wait_source': source, 'eligible_at_epoch': 11120})
                self.assertEqual(gate.remaining(), 120)


def network_for(handler):
    backend = tr.HttpxBackend.__new__(tr.HttpxBackend)
    backend.client = httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False)
    network = tr.ResilientTransport.__new__(tr.ResilientTransport)
    network.backends = {'httpx': backend}
    network.order = ['httpx']
    network.lock = threading.Lock()
    network.cooldown_until = {}
    network.last_success = network.callback = network.attempt_context_factory = None
    return network


class HttpCooldownTests(unittest.TestCase):
    def tearDown(self):
        rl.reset_shared_traffic_state_for_tests()

    def exercise(self, responses, *, adaptive=False, attempt_limit=0):
        events, logs, starts = [], [], []
        iterator = iter(responses)
        with Clock() as clock:
            def handler(request):
                starts.append(clock.now)
                status, headers = next(iterator)
                return httpx.Response(status, headers=headers, content=b'healthy capture')
            limiter = rl.SharedFixedRateLimiter(.125, rl.WAYBACK_REPLAY_RATE_KEY, adaptive=adaptive)
            limiter.condition.wait = clock.wait
            gate = rl.SharedHostGate()
            gate.condition.wait = clock.wait
            client = HttpClient(limiter, 1, 1, 'offline', threading.Event(), host_gate=gate,
                                rate_event_callback=events.append, rate_limit_attempts=attempt_limit,
                                retry_callback=lambda *args: logs.append(args), transport=network_for(handler))
            try:
                try:
                    result = client.get('https://web.archive.org/web/20010101000000id_/http://example.com/', 1024)
                except RateLimitDeferred as exc:
                    result = exc
                metrics = client.metrics_snapshot()
            finally:
                client.close()
        return result, starts, events, metrics

    def test_http_429_and_503_use_fixed_wait_then_resume_at_requested_spacing(self):
        result, starts, events, metrics = self.exercise([(429, {}), (503, {}), (200, {}),])
        self.assertEqual(starts, [1000, 1005, 1010])
        self.assertEqual(metrics['wire_request_starts'], 3)
        cooldowns = [event for event in events if event['phase'] == 'cooldown']
        self.assertEqual([event['wait_seconds'] for event in cooldowns], [5, 5])
        self.assertTrue(all(event['wait_source'] == 'fixed_fallback' for event in cooldowns))
        self.assertTrue(all(event['effective_spacing_seconds'] == .125 for event in cooldowns))

    def test_http_opt_in_escalates_and_records_testing_mode(self):
        _, starts, events, _ = self.exercise([(429, {}), (429, {}), (200, {})], adaptive=True)
        self.assertEqual(starts, [1000, 1060, 1180])
        cooldowns = [event for event in events if event['phase'] == 'cooldown']
        self.assertTrue(all(event['adaptive_rate_limiting'] for event in cooldowns))
        self.assertEqual([event['wait_source'] for event in cooldowns], ['adaptive_fallback'] * 2)

    def test_http_off_honors_and_explains_explicit_server_wait(self):
        _, starts, events, _ = self.exercise([(429, {'Retry-After': '120'}), (200, {})])
        self.assertEqual(starts, [1000, 1120])
        cooldown = next(event for event in events if event['phase'] == 'cooldown')
        self.assertEqual(cooldown['wait_source'], 'server_retry_after')
        self.assertEqual(cooldown['retry_after_seconds'], 120)

    def test_deferred_wait_serializes_provenance_for_restart(self):
        result, starts, _, _ = self.exercise([(429, {})], adaptive=True, attempt_limit=1)
        self.assertIsInstance(result, RateLimitDeferred)
        detail = result.to_detail()
        self.assertEqual(detail['wait_source'], 'adaptive_fallback')
        self.assertEqual(detail['rate_limit_signal_at_epoch'], 11000)
        self.assertEqual(rl.saved_service_eligibility(detail, adaptive=False, base_pause=60), 11005)


class ResumeCooldownTests(unittest.TestCase):
    def tearDown(self):
        rl.reset_shared_traffic_state_for_tests()

    def test_resume_off_drops_known_adaptive_debt_and_preserves_pending_capture(self):
        self.check_resume('adaptive_fallback', signaled=10980, expected_wait=0)

    def test_resume_off_still_waits_for_a_server_deadline(self):
        self.check_resume('server_retry_after', signaled=11000, expected_wait=120)

    def test_resume_off_preserves_unlabelled_legacy_deadlines(self):
        self.check_resume('', signaled=0, expected_wait=120)

    def check_resume(self, source, *, signaled, expected_wait):
        with tempfile.TemporaryDirectory() as directory, Clock() as clock:
            root = Path(directory)
            config = ProjectConfig(root, ['example.com/*'], [], adaptive_rate_limiting=True).normalized()
            database = open_database(root)
            database.execute("""INSERT INTO captures(original_url,timestamp,query_signature,mimetype,state,created_at,updated_at)
                                VALUES('http://example.com/a','20010101000000','fixture','text/plain','pending','now','now')""")
            operation = start_operation_run(database, 'download_only', '1.1.0',
                                            config_json=json.dumps(config.to_payload()))
            update_operation_run(database, operation, stage='rate_limit_waiting',
                                 detail={'reason_code': 'service_rate_limit', 'eligible_at_epoch': 11120,
                                         'wait_source': source, 'rate_limit_signal_at_epoch': signaled})
            finish_operation_run(database, operation, 'paused')
            database.commit()
            database.close()
            stop = threading.Event()
            config = replace(config, adaptive_rate_limiting=False)
            with mock.patch.object(stop, 'wait', side_effect=clock.wait), \
                    mock.patch('archive_scout.operations.index_archive', return_value={}), \
                    mock.patch('archive_scout.operations.download_archive_only', return_value={'queued': 0, 'downloaded': 0, 'skipped': 0, 'errors': 0, 'elapsed': 0}):
                run_project(config, 'resume', stop)
            self.assertEqual(clock.now - 1000, expected_wait)
            database = open_database(root)
            try:
                self.assertEqual(database.execute('SELECT state FROM captures').fetchone()[0], 'pending')
            finally:
                database.close()

    def test_download_recovery_events_keep_provenance_for_restart(self):
        with Clock() as clock:
            gate = rl.SharedHostGate(adaptive=True)
            gate.signal_rate_limit()
            events = []
            gate.condition.wait = clock.wait
            wait_for_archive(ProjectConfig(Path('unused'), [], []), gate, threading.Event(), events.append)
            self.assertEqual(events[0].detail['wait_source'], 'adaptive_fallback')
            self.assertEqual(events[0].detail['rate_limit_signal_at_epoch'], 11000)

    def test_immediate_user_stop_saves_server_deadline_before_periodic_progress(self):
        self.check_stop(server_wait=120, expected_source='server_retry_after')

    def test_immediate_user_stop_saves_optional_wait_as_optional(self):
        self.check_stop(server_wait=None, expected_source='adaptive_fallback')

    def check_stop(self, *, server_wait, expected_source):
        with tempfile.TemporaryDirectory() as directory, Clock():
            config = ProjectConfig(Path(directory), ['example.com/*'], [], adaptive_rate_limiting=True)
            def stop_during_request(*args):
                gate = rl.shared_host_gate(adaptive=True)
                gate.signal_rate_limit(server_wait)
                raise Stopped
            with mock.patch('archive_scout.operations.index_archive', side_effect=stop_during_request), self.assertRaises(Stopped):
                run_project(config, 'download_only', threading.Event())
            database = open_database(Path(directory))
            try:
                row = database.execute('SELECT status,progress_json FROM operation_runs ORDER BY id DESC LIMIT 1').fetchone()
                self.assertEqual(row['status'], 'interrupted')
                detail = json.loads(row['progress_json'])['detail']
                self.assertEqual(detail['wait_source'], expected_source)
                self.assertEqual(detail['eligible_at_epoch'], 11120 if server_wait else 11060)
            finally:
                database.close()


if __name__ == '__main__':
    unittest.main()
