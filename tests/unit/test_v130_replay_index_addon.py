from __future__ import annotations

import base64
import gzip
import hashlib
import json
import tempfile
import threading
import unittest
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch, Mock

from archive_scout.cdx.client import CDXRows, HttpClient, MalformedCDXResponse
from archive_scout.cdx.indexer import index_archive
from archive_scout.config import ProjectConfig, NetworkConfig, MediaConfig, save_project_config, load_project_config
from archive_scout.database.connection import open_database
from archive_scout.downloads.schedule import record_failure, prepare_retry_queue, select_due_retry
from archive_scout.downloads.watchdog import OwnerWatchdog
from archive_scout.events import Stopped
from archive_scout.media.indexer import index_direct_media, media_query_signature, media_index_state_signature
from archive_scout.network.transports import HttpxBackend, Urllib3Backend, CurlBackend, _decoded_verified_chunks


BODY = b'<html>\r\n<meta http-equiv="refresh" content="0;URL=http://www.ifeng.com/phoenixtv/76561193665298432/index.shtml">\r\n</html>'
DIGEST = 'UK4VJ7QXINJQ2BIFPYSP6B5NHIINZKGN'


class ReplayRecoveryTests(unittest.TestCase):
    def setUp(self):
        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def do_GET(self):
                body = BODY if self.path != '/wrong' else BODY[:-1]+b'!'
                coding = 'identity' if self.path == '/identity' else 'gzip'
                if self.path == '/empty':
                    body = bytes.fromhex('1f8b0800000000000403000000ffff')
                if self.path in {'/gzip', '/gzip-short'}:
                    body = gzip.compress(BODY)
                    if self.path == '/gzip-short':
                        body = body[:-8]
                self.send_response(200)
                self.send_header('Content-Encoding', coding)
                self.send_header('Content-Type', 'text/html')
                self.send_header('Connection', 'close')
                if self.path == '/chunked':
                    self.send_header('Transfer-Encoding', 'chunked')
                else:
                    self.send_header('Content-Length', str(len(body) if self.path == '/gzip' else len(body)+11))
                self.end_headers()
                wire = f'{len(body):x}\r\n'.encode()+body+b'\r\n' if self.path == '/chunked' else body
                self.wfile.write(wire)
                self.wfile.flush()
                self.close_connection = True

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.url = f'http://127.0.0.1:{self.server.server_port}'

    def backends(self):
        return [HttpxBackend(2, 2, 2, trust_env=False),
                Urllib3Backend(2, 2, 2, trust_env=False),
                CurlBackend(2, 2, trust_env=False)]

    def test_sample_ten_body_matches_expected_digest(self):
        self.assertEqual(len(BODY), 121)
        self.assertEqual(base64.b32encode(hashlib.sha1(BODY).digest()).decode(), DIGEST)

    def test_matching_payload_recovers_wrong_length_and_chunk_framing_on_all_backends(self):
        for backend in self.backends():
            try:
                for path in ('/short', '/chunked', '/gzip'):
                    with self.subTest(backend=backend.name, path=path), tempfile.TemporaryDirectory() as temp:
                        dest = Path(temp)/'capture.bin'
                        result = backend.download(self.url+path, {}, dest, 4096, threading.Event(), expected_digest=DIGEST)
                        self.assertEqual(dest.read_bytes(), BODY)
                        self.assertEqual(result.bytes_written, len(BODY))
                        self.assertEqual(result.content_hash, hashlib.sha256(BODY).hexdigest())
                        if path != '/gzip':
                            self.assertIn(DIGEST, result.headers['x-archive-scout-encoding-recovery'])
                            self.assertIn('framing', result.headers['x-archive-scout-encoding-recovery'])
            finally:
                backend.close()

    def test_unproven_empty_and_genuinely_truncated_payloads_are_never_saved(self):
        for backend in self.backends():
            try:
                for path, digest in (('/short', ''), ('/short', 'A'*32), ('/wrong', DIGEST),
                                     ('/empty', '3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ'),
                                     ('/identity', DIGEST), ('/gzip-short', DIGEST)):
                    with self.subTest(backend=backend.name, path=path, digest=digest), tempfile.TemporaryDirectory() as temp:
                        dest = Path(temp)/'capture.bin'
                        with self.assertRaises(Exception):
                            backend.download(self.url+path, {}, dest, 4096, threading.Event(), expected_digest=digest)
                        self.assertFalse(dest.exists())
            finally:
                backend.close()

    def test_limits_cancellation_and_non_eof_failures_cannot_be_recovered(self):
        for backend in self.backends():
            try:
                with self.subTest(backend=backend.name), tempfile.TemporaryDirectory() as temp:
                    dest = Path(temp)/'capture.bin'
                    with self.assertRaises(Exception):
                        backend.download(self.url+'/short', {}, dest, 64, threading.Event(), expected_digest=DIGEST)
                    self.assertFalse(dest.exists())
            finally:
                backend.close()
        def chunks():
            yield BODY
            raise TimeoutError('injected read timeout')
        with self.assertRaises(TimeoutError):
            list(_decoded_verified_chunks(chunks(), {'content-encoding':'gzip'}, threading.Event(), expected_digest=DIGEST))
        stopped = threading.Event(); stopped.set()
        with self.assertRaises(Stopped):
            list(_decoded_verified_chunks([BODY], {'content-encoding':'gzip'}, stopped, expected_digest=DIGEST))


class NoWait(threading.Event):
    def wait(self, timeout=None):
        return self.is_set()


class IndexBudgetTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = open_database(self.root)
        self.addCleanup(self.db.close)
        self.config = ProjectConfig(self.root, ['example.org/*'], ['needle'],
            from_date='2008', to_date='2008', cdx_delay=0, max_attempts=3,
            cdx_collapses=[], network=NetworkConfig(index_strategy='resume', retry_max_seconds=1),
            media=MediaConfig(enabled=True, discover_embedded=False)).normalized()

    def run_index(self, kind, enabled, permanent_failure=False):
        cfg = replace(self.config, skip_slow_index_targets=enabled, index_target_minutes=1)
        clock = [10000.0]
        calls = []
        events = []
        def response(*args, **kwargs):
            calls.append(1)
            clock[0] += 600
            if permanent_failure or len(calls) == 1:
                raise MalformedCDXResponse('injected malformed response', splittable=False)
            return CDXRows([]) if kind == 'text' else ([], True, None)
        function = index_archive if kind == 'text' else index_direct_media
        module = 'archive_scout.cdx.indexer' if kind == 'text' else 'archive_scout.media.indexer'
        request_module = HttpClient if kind == 'text' else __import__(module, fromlist=['_request_media_resume'])
        request_name = 'get_cdx_any' if kind == 'text' else '_request_media_resume'
        with patch.object(request_module, request_name, side_effect=response), \
             patch(module+'.time.monotonic', side_effect=lambda: clock[0]), \
             patch('archive_scout.cdx.indexer.transient_backoff', return_value=0):
            deferred = (function(cfg, self.db, NoWait(), events.append) if kind == 'text' else
                        function(cfg, self.db, Mock(), NoWait(), events.append,
                                 media_query_signature(cfg), media_index_state_signature(cfg)))
        return deferred, calls, events

    def test_text_disabled_budget_allows_recovery_after_ten_minutes(self):
        deferred, calls, events = self.run_index('text', False)
        self.assertFalse(deferred)
        self.assertEqual(len(calls), 2)
        self.assertFalse(any('budget reached' in e.message for e in events))

    def test_media_disabled_budget_allows_recovery_after_ten_minutes(self):
        deferred, calls, events = self.run_index('media', False)
        self.assertFalse(deferred)
        self.assertEqual(len(calls), 2)
        self.assertFalse(any('budget reached' in e.message for e in events))

    def test_enabled_text_budget_retains_incomplete_checkpoint(self):
        deferred, calls, events = self.run_index('text', True)
        self.assertTrue(deferred)
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.db.execute('SELECT complete FROM index_coverage').fetchone()[0], 0)
        self.assertTrue(any('budget reached' in e.message for e in events))

    def test_enabled_media_budget_retains_incomplete_checkpoint(self):
        deferred, calls, events = self.run_index('media', True)
        self.assertTrue(deferred)
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.db.execute('SELECT complete FROM media_index_state').fetchone()[0], 0)
        self.assertTrue(any('time budget reached' in e.message for e in events))

    def test_retry_count_remains_bounded_without_time_limit(self):
        deferred, calls, events = self.run_index('text', False, permanent_failure=True)
        self.assertTrue(deferred)
        self.assertEqual(len(calls), self.config.max_attempts)
        exhausted = [e for e in events if e.stage == 'index_deferred']
        self.assertTrue(exhausted)
        self.assertIsNone(exhausted[-1].detail['active_seconds_limit'])

    def test_budget_setting_roundtrips_off_and_on(self):
        for enabled in (False, True):
            cfg = replace(self.config, skip_slow_index_targets=enabled, index_target_minutes=2.5)
            path = save_project_config(cfg)
            loaded = load_project_config(path)
            self.assertEqual(loaded.skip_slow_index_targets, enabled)
            self.assertEqual(loaded.for_target('example.org/*').skip_slow_index_targets, enabled)
            self.assertEqual(loaded.index_target_minutes, 2.5)

    def test_embedded_lookup_sweep_has_no_hidden_elapsed_cutoff(self):
        from archive_scout.media.indexer import _complete_embedded_media
        cfg = replace(self.config, media=replace(self.config.media, discover_embedded=True))
        signature = media_query_signature(cfg)
        self.db.execute("INSERT INTO media_discovery_queue(original_url,query_signature,state,created_at,updated_at) VALUES('http://example.org/a.jpg',?,'pending','t','t')", (signature,))
        self.db.commit()
        clock = [10000.0]
        calls = []
        def lookup(*args, **kwargs):
            calls.append(1)
            clock[0] += 600
            with self.db:
                self.db.execute('UPDATE media_discovery_queue SET state=?',
                                ('error' if len(calls) == 1 else 'indexed',))
        with patch('archive_scout.media.indexer.index_embedded_media', side_effect=lookup), \
             patch('archive_scout.media.indexer.time.monotonic', side_effect=lambda: clock[0]):
            _complete_embedded_media(cfg, self.db, Mock(), NoWait(), None, signature)
        self.assertEqual(len(calls), 2)
        self.assertEqual(self.db.execute('SELECT state FROM media_discovery_queue').fetchone()[0], 'indexed')

    def test_repeated_bad_payloads_yield_but_remain_due_and_retryable(self):
        from archive_scout.cdx.parameters import cdx_query_signature
        from archive_scout.downloads.downloader import _populate_download_scopes
        self.db.execute("INSERT INTO targets(id,pattern,created_at) VALUES(1,'example.org/*','t')")
        self.db.execute("INSERT INTO captures(id,original_url,timestamp,target_id,query_signature,created_at,updated_at,state) VALUES(1,'http://example.org/a','20080101000000',1,?,'t','t','error')", (cdx_query_signature(self.config),))
        self.db.execute("INSERT INTO errors(capture_id,operation,category,message,retryable,first_seen,last_seen) VALUES(1,'download','incomplete_representation','fixture',1,'t','t')")
        _populate_download_scopes(self.db, self.config)
        with patch('archive_scout.downloads.schedule.time.time', return_value=100):
            for _ in range(3):
                record_failure(self.db, 1, 'incomplete_representation', 200, True, 1, 300)
        row = self.db.execute('SELECT * FROM text_retry_schedule').fetchone()
        self.assertGreaterEqual(row['next_eligible_at'], 154)
        self.db.execute('CREATE TEMP TABLE archive_scout_download_queue(id INTEGER PRIMARY KEY)')
        prepare_retry_queue(self.db)
        self.assertIsNone(select_due_retry(self.db, 153))
        self.assertEqual(select_due_retry(self.db, 200)['id'], 1)
        self.assertEqual(self.db.execute('SELECT state FROM captures').fetchone()[0], 'error')


class WatchdogTests(unittest.TestCase):
    def test_recovered_payload_proofs_are_bounded_in_health_history(self):
        from archive_scout.downloads.diagnostics import AcquisitionDiagnostics
        with tempfile.TemporaryDirectory() as temp:
            diagnostic = AcquisitionDiagnostics(temp, None)
            try:
                for index in range(20):
                    diagnostic.response({'status': 200, 'url': f'https://example.org/{index}?token=secret',
                                         'backend': 'httpx', 'encoding_recovery': 'CDX SHA-1 verified: '+DIGEST})
                snapshot = diagnostic.snapshot()
                self.assertEqual(snapshot['representation_recovery_count'], 20)
                self.assertEqual(len(snapshot['recent_representation_recoveries']), 8)
                self.assertNotIn('secret', json.dumps(snapshot['recent_representation_recoveries']))
                diagnostic.summary(20, force=True)
                saved = json.loads((diagnostic.summary_path/'latest.json').read_text())
                self.assertEqual(saved['representation_recovery_count'], 20)
            finally:
                diagnostic.close()

    def test_stall_reports_are_bounded_throttled_and_do_not_include_locals(self):
        clock = [0.0]
        with tempfile.TemporaryDirectory() as temp:
            monitor = OwnerWatchdog(temp, clock=lambda: clock[0])
            monitor.beat('injected blocked owner')
            self.assertFalse(monitor.check())
            clock[0] = 121
            self.assertTrue(monitor.check())
            clock[0] = 200
            self.assertFalse(monitor.check())
            clock[0] = 421
            self.assertTrue(monitor.check())
            files = list(Path(temp).glob('stall-*.json'))
            self.assertEqual(len(files), 2)
            for file in files:
                self.assertLessEqual(file.stat().st_size, 64*1024)
                data = json.loads(file.read_text())
                self.assertEqual(data['owner_stage'], 'injected blocked owner')
                self.assertTrue(data['threads'][0]['owner'])
                for thread in data['threads']:
                    for frame in thread['stack']:
                        self.assertEqual(set(frame), {'file', 'line', 'function'})
            monitor.beat('recovered')
            self.assertFalse(monitor.check())
            monitor.close()

    def test_disabled_and_closed_watchdogs_produce_no_files_or_threads(self):
        with tempfile.TemporaryDirectory() as temp:
            monitor = OwnerWatchdog(temp, enabled=False)
            monitor.start()
            self.assertIsNone(monitor.thread)
            self.assertFalse(monitor.check())
            self.assertEqual(list(Path(temp).iterdir()), [])
            monitor = OwnerWatchdog(temp)
            monitor.start()
            thread = monitor.thread
            self.assertTrue(thread.is_alive())
            monitor.close()
            self.assertFalse(thread.is_alive())
            self.assertFalse(monitor.check())
