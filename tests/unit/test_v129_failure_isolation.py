from __future__ import annotations

import base64
import errno
import hashlib
import shutil
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import Mock, patch

import httpx

from archive_scout.cdx.client import HttpClient, RateLimitDeferred
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.config import NetworkConfig, ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, upsert_capture, record_error
from archive_scout.downloads.rate_limit import FixedRateLimiter, SharedHostGate
from archive_scout.downloads.schedule import record_failure
from archive_scout.downloads.validation import classify_exception
from archive_scout.network.transports import (
    CurlBackend, ResilientTransport, TransportExhaustedError, PayloadDigestMismatch,
    _backend_received_headers, transport_failure_phase,
)
from archive_scout.operations import recover_transient_text_errors
from archive_scout.scanning.capacity import scan_capacity

BODY = b'<html><body>needle in complete archive payload</body></html>'
BAD = (b'<a target="_blank" href="http://adclient.ifeng.com/event.ng/Type=click&FlightID='
       b'1107&AdID=1056&TargetID=196&Segments=1,16,75,81,147,160&Targets=255,196,72,184&'
       b'Values=34,46,51,85,90,100,110,132,204,235,240,249&RawValues=&Redirect=http://www.'
       b'phoenixtv.com"><img src="http://img.phoenixtv')
EXPECTED = 'CG5PKUSMBFZKJWXPZ5SETGWPV32W3CAW'


class IsolationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.held = threading.Event()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def do_GET(self):
                body = BAD if self.path == '/bad' else BODY
                status = int(self.path[1:]) if self.path in ('/429', '/503') else 200
                self.send_response(status)
                self.send_header('Content-Length', str(len(body)))
                self.send_header('Content-Type', 'text/html')
                if self.path == '/bad':
                    self.send_header('Content-Encoding', 'gzip')
                if status in (429, 503):
                    self.send_header('Retry-After', '2')
                self.end_headers()
                if self.path == '/held':
                    owner.held.wait(5)
                try:
                    self.wfile.write(body)
                except OSError:
                    pass

            def log_message(self, *_):
                pass

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.addCleanup(self.held.set)

    def url(self, path):
        return f'http://127.0.0.1:{self.server.server_port}{path}'

    def client(self, backend='httpx', gate=None):
        client = HttpClient(FixedRateLimiter(0), 1, 2, 'fixture', threading.Event(),
            network_backend=backend, trust_environment=False, host_gate=gate,
            rate_limit_attempts=1)
        self.addCleanup(client.close)
        return client

    def test_real_malformed_phoenix_fixture_remains_unsaved_on_every_backend(self):
        self.assertEqual(len(BAD), 285)
        self.assertEqual(hashlib.sha1(BAD).hexdigest().upper(), 'B377B3F34B3ED02920AA713B1F62D1CEF23A0FBC')
        for backend in ['httpx', 'urllib3'] + (['curl'] if shutil.which('curl') else []):
            with self.subTest(backend=backend):
                client = self.client(backend)
                destination = self.root / backend
                with self.assertRaises(Exception) as failed:
                    client.download_to_path(self.url('/bad'), destination, 10000,
                        expected_digest=EXPECTED, retain_partial=False)
                self.assertEqual(classify_exception(failed.exception)[0], 'payload_digest_mismatch')
                self.assertFalse(destination.exists())
                self.assertEqual(client.transport.cooldown_until, {})
                client.download_to_path(self.url('/good'), destination, 10000, retain_partial=False)
                self.assertEqual(destination.read_bytes(), BODY)

    def test_digest_proven_header_repair_still_works(self):
        digest = base64.b32encode(hashlib.sha1(BAD).digest()).decode()
        client = self.client()
        destination = self.root / 'proven'
        result = client.download_to_path(self.url('/bad'), destination, 10000,
            expected_digest=digest, retain_partial=False)
        self.assertEqual(destination.read_bytes(), BAD)
        self.assertTrue(result['encoding_recovery'])

    def test_host_probe_reopens_at_headers_before_body_on_all_backends(self):
        for backend in ['httpx', 'urllib3'] + (['curl'] if shutil.which('curl') else []):
            with self.subTest(backend=backend):
                self.held.clear()
                gate = SharedHostGate()
                gate.pause_for_rate_limit(1)
                gate.blocked_until = time.monotonic()-1
                client = self.client(backend, gate)
                with ThreadPoolExecutor(max_workers=2) as pool:
                    slow = pool.submit(client.download_to_path, self.url('/held'), self.root / ('slow-'+backend),
                                       10000, retain_partial=False)
                    try:
                        deadline = time.monotonic()+2
                        while gate.snapshot()['probe_required'] and time.monotonic() < deadline:
                            time.sleep(.01)
                        self.assertFalse(gate.snapshot()['probe_required'])
                        self.assertFalse(slow.done())
                        fast = pool.submit(client.download_to_path, self.url('/good'), self.root / ('fast-'+backend),
                                           10000, retain_partial=False)
                        fast.result(timeout=2)
                    finally:
                        self.held.set()
                    slow.result(timeout=3)

    def test_control_headers_do_not_reopen_host_gate(self):
        for status in (429, 503):
            gate = SharedHostGate()
            client = self.client(gate=gate)
            with self.assertRaises(RateLimitDeferred):
                client.download_to_path(self.url('/'+str(status)), self.root / str(status), 10000,
                                        retain_partial=False)
            self.assertTrue(gate.snapshot()['probe_required'])
            self.assertGreater(gate.remaining(), 1)

    def test_body_reset_does_not_cool_backend_or_repeat_other_stacks(self):
        client = self.client()
        transport = client.transport
        transport.backends['alternate'] = Mock()
        transport.order.append('alternate')

        def reset(*args, **kwargs):
            _backend_received_headers(200)
            raise ConnectionResetError(errno.ECONNRESET, 'body connection reset')

        with patch.object(transport.backends['httpx'], 'download', side_effect=reset):
            with self.assertRaises(Exception) as failed:
                client.download_to_path(self.url('/good'), self.root / 'reset', 10000, retain_partial=False)
        self.assertEqual(classify_exception(failed.exception)[0], 'response_transfer')
        self.assertEqual(transport.cooldown_until, {})
        transport.backends['alternate'].download.assert_not_called()
        self.assertFalse((self.root / 'reset').exists())

    def test_setup_refusal_still_cools_backend_and_keeps_phase(self):
        transport = self.client().transport
        with patch.object(transport.backends['httpx'], 'download', side_effect=ConnectionRefusedError(10061, 'actively refused')):
            with self.assertRaises(TransportExhaustedError) as failed:
                transport.download(self.url('/good'), {}, self.root / 'refused', 10000, threading.Event())
        self.assertEqual(classify_exception(failed.exception)[0], 'connection')
        self.assertIn('httpx', transport.cooldown_until)
        self.assertEqual(transport_failure_phase(failed.exception.failures[0][1]), 'connection_refused')

    def test_proxy_connect_headers_are_not_origin_reachability(self):
        path = self.root / 'headers'
        path.write_bytes(b'HTTP/1.1 200 Connection established\r\n\r\n')
        self.assertIsNone(CurlBackend._early_control_status(path, any_status=True))
        path.write_bytes(path.read_bytes() + b'HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\n')
        self.assertEqual(CurlBackend._early_control_status(path, any_status=True)[0], 200)

    def test_recovered_probe_can_continue_but_a_new_pause_revokes_it(self):
        gate = SharedHostGate()
        gate.pause_for_rate_limit(1)
        gate.blocked_until = time.monotonic()-1
        permit = gate.acquire_request(threading.Event())
        gate.finish_request(permit, recovered=True)
        self.assertTrue(gate.permit_is_current(permit))
        gate.pause_for_rate_limit(1)
        self.assertFalse(gate.permit_is_current(permit))

    def test_redirect_target_setup_failure_does_not_inherit_source_headers(self):
        from archive_scout.network.transports import _pace_redirect_hop
        transport = self.client().transport

        def failed_hop(*args, **kwargs):
            _backend_received_headers(302)
            _pace_redirect_hop(threading.Event())
            raise ConnectionRefusedError(errno.ECONNREFUSED, 'target refused')

        with patch.object(transport.backends['httpx'], 'download', side_effect=failed_hop):
            with self.assertRaises(TransportExhaustedError) as failed:
                transport.download(self.url('/good'), {}, self.root / 'redirect', 10000, threading.Event())
        self.assertEqual(classify_exception(failed.exception)[0], 'connection')
        self.assertIn('httpx', transport.cooldown_until)

    def test_measured_cpu_and_memory_cap_local_scan_admission(self):
        with patch('archive_scout.scanning.capacity.os.cpu_count', return_value=16), \
             patch('archive_scout.scanning.capacity.os.sched_getaffinity', return_value={0, 1}, create=True), \
             patch('archive_scout.scanning.capacity.available_memory', return_value=16*1024*1024):
            self.assertEqual(scan_capacity(32, 64*1024*1024), (1, 2*1024*1024))
        with patch('archive_scout.scanning.capacity.available_memory', return_value=None):
            workers, budget = scan_capacity(1, 64*1024*1024)
            self.assertEqual((workers, budget), (1, 64*1024*1024))

    def test_persistent_outage_recovery_survives_no_progress_sweep_limit(self):
        cfg = ProjectConfig(self.root, targets=['example.test/*'], keywords=['needle'],
            from_date='2001', to_date='2001', max_attempts=1,
            network=NetworkConfig(retry_base_seconds=1, retry_max_seconds=1)).normalized()
        with closing(open_database(self.root)) as db:
            target = get_or_create_target(db, cfg.targets[0])
            upsert_capture(db, {'timestamp':'20010101000000', 'original':'http://example.test/a',
                'mimetype':'text/html', 'statuscode':'200', 'length':1}, target, cdx_query_signature(cfg))
            capture_id = db.execute('SELECT id FROM captures').fetchone()[0]
            db.execute("UPDATE captures SET state='error'")
            record_error(db, 'download', 'connection', 'offline', capture_id=capture_id, retryable=True)
            record_failure(db, capture_id, 'connection', None, True, 1, 1)
            db.execute('UPDATE text_retry_schedule SET next_eligible_at=0')
            db.commit()
            calls = []

            def recover(*args, **kwargs):
                calls.append(1)
                if len(calls) == 4:
                    db.execute("UPDATE captures SET state='downloaded'")
                    db.execute('DELETE FROM text_retry_schedule')
                    db.commit()

            class FastStop(threading.Event):
                def wait(self, timeout=None):
                    return self.is_set()

            with patch('archive_scout.operations.download_archive', side_effect=recover):
                result = recover_transient_text_errors(cfg, db, 0, [], FastStop(), None,
                                                       continuous=True, acquire_only=True)
            self.assertEqual(result, 0)
            self.assertEqual(len(calls), 4)

    def test_persistent_media_outage_survives_failed_sweep_limit(self):
        from archive_scout.database.repositories import upsert_media_capture
        from archive_scout.media.indexer import media_query_signature
        from archive_scout.media.recovery import recover_active_media, record_media_failure
        cfg = ProjectConfig(self.root, targets=['example.test/*'], keywords=[], max_attempts=1,
            network=NetworkConfig(retry_base_seconds=1, retry_max_seconds=1)).normalized()
        with closing(open_database(self.root)) as db:
            upsert_media_capture(db, {'original':'http://example.test/a.jpg', 'timestamp':'20010101000000',
                'mimetype':'image/jpeg', 'statuscode':'200', 'digest':'', 'length':10},
                None, media_query_signature(cfg), 'image', '.jpg')
            cid = db.execute('SELECT id FROM media_captures').fetchone()[0]
            db.execute("UPDATE media_captures SET state='error'")
            record_error(db, 'media_download', 'connection', 'offline', media_capture_id=cid, retryable=True)
            record_media_failure(db, cid, 'connection', None, True, cfg)
            db.execute('UPDATE media_retry_schedule SET next_eligible_at=0')
            db.commit()
            calls = []

            def recovered(*args, **kwargs):
                calls.append(1)
                if len(calls) == 4:
                    db.execute("UPDATE media_captures SET state='downloaded'")
                    db.execute('DELETE FROM media_retry_schedule')
                    db.commit()

            class FastStop(threading.Event):
                def wait(self, timeout=None):
                    return self.is_set()

            with patch('archive_scout.media.downloader.download_media', side_effect=recovered):
                recover_active_media(cfg, db, FastStop(), allow_partial_inventory=True)
            self.assertEqual(len(calls), 4)

    def test_media_persistent_recovery_respects_explicit_server_wait_limit(self):
        from archive_scout.media.recovery import recover_active_media
        gate = SharedHostGate()
        gate.pause_for_rate_limit(10)
        cfg = ProjectConfig(self.root, targets=['example.test/*'], keywords=[], rate_limit_max_wait=1).normalized()
        with closing(open_database(self.root)) as db, \
             patch('archive_scout.downloads.rate_limit.shared_host_gate', return_value=gate), \
             patch('archive_scout.media.downloader.download_media') as download:
            events = []
            recover_active_media(cfg, db, threading.Event(), events.append, allow_partial_inventory=True)
            download.assert_not_called()
            self.assertTrue(any('wait limit' in event.message for event in events))
