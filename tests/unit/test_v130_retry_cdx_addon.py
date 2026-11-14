from __future__ import annotations

import http.client
import json
import tempfile
import threading
import time
import unittest
from contextlib import closing
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

import httpx
import urllib3

from archive_scout.cdx.client import HttpClient, MalformedCDXResponse, CDXRows
from archive_scout.cdx.indexer import index_archive
from archive_scout.downloads.diagnostics import AcquisitionDiagnostics
from archive_scout.downloads.rate_limit import FixedRateLimiter, SharedHostGate
from archive_scout.downloads.session import begin_session, end_session, acquisition_client, AcquisitionExecutor
from archive_scout.network.transports import CurlTransportError, TransportExhaustedError


class CdxTransferTests(unittest.TestCase):
    def client(self):
        return HttpClient(FixedRateLimiter(0), 1, 2, 'test', threading.Event(),
                          network_backend='httpx', trust_environment=False, host_gate=SharedHostGate())

    def test_wrapped_incomplete_transfers_do_not_multiply_formats_or_endpoints(self):
        for method in ('get_cdx_any', 'get_cdx_rows_any'):
            for raw in (httpx.RemoteProtocolError('peer closed connection without sending complete message body (incomplete chunked read)'),
                        urllib3.exceptions.ProtocolError('connection broken', http.client.IncompleteRead(b'partial', 20)),
                        CurlTransportError(18, 'transfer closed with outstanding read data remaining')):
                with self.subTest(method=method, raw=type(raw).__name__), closing(self.client()) as client:
                    wrapped = TransportExhaustedError('url', [('backend', raw)])
                    error = MalformedCDXResponse('representation failure', splittable=True)
                    error.__cause__ = wrapped
                    with patch.object(client, 'get', side_effect=error) as get:
                        with self.assertRaises(MalformedCDXResponse):
                            getattr(client, method)(['https://web.archive.org/cdx/search/cdx',
                                                    'https://web.archive.org/web/timemap/json'], [('url', 'example.org/*')])
                    self.assertEqual(get.call_count, 1)

    def test_genuine_format_failure_still_gets_a_format_fallback(self):
        payload = json.dumps([['timestamp', 'original', 'mimetype', 'statuscode', 'digest', 'length'],
                              ['20010101000000', 'http://example.org/a', 'text/html', '200', '', '4']]).encode()
        for method in ('get_cdx_any', 'get_cdx_rows_any'):
            with self.subTest(method=method), closing(self.client()) as client:
                with patch.object(client, 'get', side_effect=[MalformedCDXResponse('unexpected field count'), {'data': payload}]) as get:
                    result = getattr(client, method)(['https://web.archive.org/cdx/search/cdx'], [('url', 'example.org/*')])
                self.assertEqual(get.call_count, 2)
                self.assertTrue(result.rows if isinstance(result, CDXRows) else result)

    def test_real_truncated_cdx_response_uses_one_physical_request(self):
        requests = []
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                requests.append(self.path)
                self.send_response(200)
                self.send_header('Content-Length', '1000')
                self.send_header('Connection', 'close')
                self.end_headers()
                self.wfile.write(b'partial cdx')
                self.close_connection = True
            def log_message(self, *args):
                pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with closing(self.client()) as client:
                url = f'http://127.0.0.1:{server.server_port}'
                with self.assertRaises(MalformedCDXResponse):
                    client.get_cdx_rows_any([url+'/cdx/search/cdx', url+'/web/timemap/json'], [('url','example.org/*')])
            self.assertEqual(len(requests), 1)
        finally:
            server.shutdown(); server.server_close(); thread.join(timeout=2)

    def test_failed_target_yields_with_cursor_preserved_then_recovers(self):
        from tests.unit.test_v126_retry_tail import Fixture, FastStop
        fixture = Fixture()
        fixture.setUp()
        try:
            cfg = replace(fixture.cfg, targets=['example.org/*', 'other.org/*'])
            calls = []
            def request(*args, **kwargs):
                params = dict(args[2]); target = params['url']; calls.append((target, params.get('resumeKey')))
                if len(calls) == 1:
                    return CDXRows([('20080101000000', 'http://example.org/a', 'text/html', '200', '', '4')], 'saved-cursor')
                if len(calls) == 2:
                    error = MalformedCDXResponse('incomplete transfer', splittable=True)
                    error.__cause__ = httpx.RemoteProtocolError('incomplete chunked read')
                    raise error
                return CDXRows([])
            with patch.object(HttpClient, 'get_cdx_any', request), \
                 patch('archive_scout.cdx.indexer.transient_backoff', return_value=0):
                self.assertFalse(index_archive(cfg, fixture.db, FastStop()))
            self.assertEqual(calls[:3], [('example.org/*', None), ('example.org/*', 'saved-cursor'), ('other.org/*', None)])
            self.assertEqual(calls[3], ('example.org/*', 'saved-cursor'))
            self.assertEqual(fixture.db.execute('SELECT COUNT(*) FROM captures').fetchone()[0], 1)
        finally:
            fixture.tearDown()


class OperationMetricsTests(unittest.TestCase):
    def test_worker_pool_creation_failure_leaves_scanner_and_captures_recoverable(self):
        from tests.unit.test_v129_admission_recovery import AdmissionTests
        from archive_scout.downloads.downloader import download_archive
        fixture = AdmissionTests(); fixture.setUp()
        try:
            with patch.object(AcquisitionExecutor, '__init__', side_effect=RuntimeError('injected pool startup failure')):
                with self.assertRaisesRegex(RuntimeError, 'injected pool startup failure'):
                    download_archive(fixture.cfg, fixture.db, fixture.run_id, threading.Event(), None,
                                     capture_ids=[1], scan_jobs=fixture.jobs)
            self.assertEqual(len(fixture.requests), 0)
            self.assertEqual(fixture.db.execute("SELECT COUNT(*) FROM captures WHERE state='pending'").fetchone()[0], 67)
            self.assertFalse(any(t.name == 'archive-scout-local-scan-supervisor' for t in threading.enumerate()))
        finally:
            fixture.doCleanups()

    def test_save_age_and_rate_survive_failed_batches_and_can_report_no_save(self):
        clock = [100.0]
        with tempfile.TemporaryDirectory() as temp, patch('archive_scout.downloads.diagnostics.time.monotonic', side_effect=lambda:clock[0]):
            diag = AcquisitionDiagnostics(temp, None)
            self.assertIsNone(diag.save_metrics()['seconds_since_last_save'])
            self.assertEqual(diag.save_age(), 'none in this operation')
            diag.begin_batch(None); diag.saved(); diag.summary(1)
            clock[0] = 110
            diag.begin_batch(None); diag.summary(0)
            self.assertEqual(diag.save_metrics()['seconds_since_last_save'], 10)
            self.assertGreater(diag.save_metrics()['recent_saves_per_second'], 0)
            clock[0] = 200
            diag.begin_batch(None); diag.summary(0)
            self.assertEqual(diag.save_age(), '100s ago')
            self.assertEqual(diag.save_metrics()['recent_saves_per_second'], 0)
            diag.close()
            data = json.loads((Path(temp)/'diagnostics/text-acquisition/latest.json').read_text())
            self.assertEqual(data['new_saves'], 1)
            self.assertEqual(data['operation_save_metrics']['seconds_since_last_save'], 100)

    def test_cached_worker_pool_drains_before_transport_closes(self):
        token = begin_session(); release = threading.Event(); started = threading.Event(); order = []
        class Transport:
            def close(self):
                order.append('transport')
        try:
            acquisition_client('transport', Transport)
            pool, owned = acquisition_client('pool', lambda: AcquisitionExecutor(max_workers=1))
            self.assertFalse(owned)
            again, owned = acquisition_client('pool', lambda: self.fail('pool was rebuilt'))
            self.assertIs(pool, again)
            def task():
                started.set(); release.wait(2); order.append('worker')
            future = pool.submit(task); self.assertTrue(started.wait(2)); release.set()
            end_session(token); token = None
            self.assertTrue(future.done())
            self.assertEqual(order, ['worker', 'transport'])
        finally:
            release.set()
            if token is not None: end_session(token)

    def test_real_selected_batches_reuse_executor_and_keep_operation_saves(self):
        from tests.unit.test_v129_admission_recovery import AdmissionTests
        from archive_scout.downloads.downloader import download_archive
        from archive_scout.network.transports import HttpxBackend
        fixture = AdmissionTests(); fixture.setUp(); token = begin_session(); events = []; pools = []
        original = AcquisitionExecutor.__init__
        def create(pool, *args, **kwargs):
            pools.append(pool); original(pool, *args, **kwargs)
        try:
            with patch.object(AcquisitionExecutor, '__init__', create), \
                 patch('archive_scout.downloads.downloader.replay_url', side_effect=lambda stamp,url:f'http://127.0.0.1:{fixture.server.server_port}/'+url.rsplit('/',1)[1]):
                download_archive(fixture.cfg, fixture.db, fixture.run_id, threading.Event(), events.append, capture_ids=[1], scan_jobs=fixture.jobs)
                with patch.object(HttpxBackend, 'download', side_effect=httpx.ConnectError('injected refusal')):
                    download_archive(fixture.cfg, fixture.db, fixture.run_id, threading.Event(), events.append, capture_ids=[2], scan_jobs=fixture.jobs)
            self.assertEqual(len(pools), 1)
            self.assertEqual(len([e for e in events if e.stage=='download_prepare']), 0)
            self.assertEqual(len([e for e in events if e.stage=='acquisition_health' and 'build:' in e.message]), 1)
            progress = [e for e in events if e.stage=='download']
            self.assertEqual(progress[-1].detail['operation_new_saves'], 1)
            self.assertEqual(progress[-1].detail['new_saves'], 0)
            self.assertTrue(progress[-1].detail['retry_batch'])
            self.assertIn('Retry batch', progress[-1].message)
            self.assertEqual(len([e for e in events if e.stage=='acquisition_health' and 'health:' in e.message]), 0)
            end_session(token); token = None
            final = json.loads((fixture.root/'diagnostics/text-acquisition/latest.json').read_text())
            self.assertEqual(final['new_saves'], 1)
            self.assertIsNotNone(final['operation_save_metrics']['seconds_since_last_save'])
            self.assertEqual(fixture.db.execute("SELECT COUNT(*) FROM captures WHERE state='downloaded'").fetchone()[0], 1)
        finally:
            if token is not None: end_session(token)
            fixture.doCleanups()


class RetrySelectionTests(unittest.TestCase):
    def test_automatic_selection_keeps_scope_legacy_errors_deadlines_and_held_outcomes(self):
        from tests.unit.test_v126_text_retry_decisions import Fixture
        from archive_scout.operations import _retryable_text_batches
        from archive_scout.downloads.schedule import next_due
        fixture = Fixture(); fixture.setUp()
        try:
            due = fixture.seed('http://example.org/due')
            legacy = fixture.seed('http://example.org/legacy')
            future = fixture.seed('http://example.org/future')
            held = fixture.seed('http://example.org/held')
            with fixture.db:
                fixture.db.execute('DELETE FROM text_retry_schedule WHERE capture_id=?', (legacy,))
                fixture.db.execute('UPDATE text_retry_schedule SET next_eligible_at=0 WHERE capture_id=?', (due,))
                fixture.db.execute('UPDATE text_retry_schedule SET next_eligible_at=? WHERE capture_id=?', (time.time()+3600, future))
                fixture.db.execute("UPDATE capture_routing SET routing='failed_nonretryable:missing_capture' WHERE capture_id=?", (held,))
            self.assertEqual(list(_retryable_text_batches(fixture.cfg, fixture.db, automatic=True, batch_size=1)), [[due],[legacy]])
            self.assertEqual(next_due(fixture.db, fixture.cfg), 0)
            with fixture.db:
                fixture.db.execute('UPDATE errors SET resolved=1 WHERE capture_id=?', (due,))
            self.assertGreater(next_due(fixture.db, fixture.cfg), time.time())
        finally:
            fixture.tearDown()
