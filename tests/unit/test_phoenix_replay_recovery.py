import base64
import hashlib
import tempfile
import threading
import time
import unittest
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit
from unittest.mock import patch

import httpx

from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.config import NetworkConfig, ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, record_error, upsert_capture
from archive_scout.downloads import downloader
from archive_scout.downloads.rate_limit import reset_shared_traffic_state_for_tests
from archive_scout.downloads.schedule import clear_retry, record_failure
from archive_scout.network.transports import HttpxBackend
from archive_scout.operations import _retryable_text_batches, recover_transient_text_errors


class ReplayAdmissionTests(unittest.TestCase):
    def test_refusals_then_unusable_responses_do_not_serialize_the_fresh_queue(self):
        reset_shared_traffic_state_for_tests()
        self.addCleanup(reset_shared_traffic_state_for_tests)
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        db = self.enterContext(closing(open_database(root)))
        config = ProjectConfig(root, ['example.test/*'], [], workers=3, download_delay=.025,
                               from_date='2001', to_date='2003', cdx_collapses=[],
                               network=NetworkConfig(backend='httpx', trust_environment=False,
                                                     retry_base_seconds=60, retry_max_seconds=60)).normalized()
        paths = [f'/refused-{n}' for n in range(3)] + [f'/rejected-{n}' for n in range(18)] + ['/healthy']
        payload = b'<html><body>Original snapshot body</body></html>'
        digest = base64.b32encode(hashlib.sha1(payload).digest()).decode()
        signature = cdx_query_signature(config)
        with db:
            target = get_or_create_target(db, config.targets[0])
            for path in paths:
                upsert_capture(db, {'timestamp': '20010101000000', 'original': f'http://example.test{path}',
                                    'mimetype': 'text/html', 'statuscode': '200', 'digest': digest, 'length': 512},
                               target, signature)

        observed = []

        class ReplayHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                observed.append((self.path, time.monotonic()))
                self.send_response(200)
                self.send_header('Content-Length', str(len(payload)))
                self.send_header('Content-Type', 'text/html')
                sent = payload
                if self.path.startswith('/rejected-'):
                    number = int(self.path.rsplit('-', 1)[1])
                    if number % 2:
                        sent = payload[:20]
                    else:
                        self.send_header('Content-Encoding', 'gzip')
                        sent = payload.replace(b'Original', b'Modified')
                self.end_headers()
                try:
                    self.wfile.write(sent)
                except OSError:
                    pass

            def log_message(self, *_args):
                pass

        server = self.enterContext(ThreadingHTTPServer(('127.0.0.1', 0), ReplayHandler))
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        self.addCleanup(server.shutdown)
        stop = threading.Event()
        deadline = threading.Timer(40, stop.set)
        deadline.start()
        self.addCleanup(deadline.cancel)
        original_download = HttpxBackend.download

        def download(backend, url, *args, **kwargs):
            if urlsplit(url).path.startswith('/refused-'):
                stop.wait(5.2)
                raise httpx.ConnectError('Injected connection refusal')
            return original_download(backend, url, *args, **kwargs)

        def replay(_timestamp, original):
            return f'http://127.0.0.1:{server.server_port}' + urlsplit(original).path

        with patch.object(HttpxBackend, 'download', download), patch.object(downloader, 'replay_url', replay):
            downloader.download_archive(config, db, 0, stop, None, acquire_only=True, allow_partial_inventory=True)
        self.assertEqual({path for path, _ in observed}, set(paths[3:]))
        healthy = next(timestamp for path, timestamp in observed if path == '/healthy')
        first_rejected = min(timestamp for path, timestamp in observed if path.startswith('/rejected-'))
        self.assertLess(healthy - first_rejected, 8)
        states = dict(db.execute('SELECT state,COUNT(*) FROM captures GROUP BY state'))
        self.assertEqual(states, {'downloaded_unscanned': 1, 'error': 21})
        self.assertEqual(db.execute('SELECT COUNT(*) FROM text_retry_schedule').fetchone()[0], 21)
        self.assertEqual([row[0] for row in db.execute('SELECT content_hash FROM capture_payload_evidence')],
                         [hashlib.sha256(payload).hexdigest()])


class RetrySweepTests(unittest.TestCase):
    def setUp(self):
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.db = self.enterContext(closing(open_database(root)))
        self.cfg = ProjectConfig(root, ['example.test/*'], [], from_date='2001', to_date='2003',
                                 cdx_collapses=[], max_attempts=1,
                                 network=NetworkConfig(retry_base_seconds=1, retry_max_seconds=1)).normalized()
        self.target_id = get_or_create_target(self.db, self.cfg.targets[0])
        self.signature = cdx_query_signature(self.cfg)

    def failure(self, n, category='read_timeout', *, scheduled=True, deadline=0):
        with self.db:
            original = f'http://example.test/{n}'
            upsert_capture(self.db, {'timestamp': '20010101000000', 'original': original,
                       'mimetype': 'text/html', 'statuscode': '200', 'length': 1}, self.target_id, self.signature)
            cid = int(self.db.execute('SELECT id FROM captures WHERE original_url=?', (original,)).fetchone()[0])
            self.db.execute("UPDATE captures SET state='error' WHERE id=?", (cid,))
            record_error(self.db, 'download', category, 'fixture', capture_id=cid, retryable=True)
            if scheduled:
                record_failure(self.db, cid, category, None, True, 1, 1)
                self.db.execute('UPDATE text_retry_schedule SET next_eligible_at=? WHERE capture_id=?', (deadline, cid))
        return cid

    def test_retry_sweep_revalidates_saved_keys_and_reopens_for_new_errors(self):
        first = self.failure(0)
        due_later = self.failure(1, deadline=4200000000)
        obsolete = self.failure(2)
        legacy = self.failure(3, scheduled=False)
        sweep = _retryable_text_batches(self.cfg, self.db, batch_size=1, automatic=True)
        self.assertEqual(next(sweep), [first])
        with self.db:
            self.db.execute('UPDATE text_retry_schedule SET next_eligible_at=0 WHERE capture_id=?', (due_later,))
            self.db.execute('UPDATE errors SET resolved=1 WHERE capture_id=?', (obsolete,))
        new = self.failure(4)
        self.assertEqual(list(sweep), [[due_later], [legacy]])
        self.assertEqual(list(_retryable_text_batches(self.cfg, self.db, automatic=True)),
                         [[first, due_later, legacy, new]])

    def test_read_timeout_recovery_probes_one_capture_then_drains_all_due_work(self):
        ids = [self.failure(n) for n in range(4)]
        calls = []

        class ImmediateStop(threading.Event):
            def wait(self, timeout=None):
                return self.is_set()

        def retry(*_args, **kwargs):
            selected = kwargs['capture_ids']
            calls.append(selected)
            if len(calls) <= 2:
                return
            with self.db:
                for cid in selected:
                    self.db.execute("UPDATE captures SET state='downloaded' WHERE id=?", (cid,))
                    self.db.execute('UPDATE errors SET resolved=1 WHERE capture_id=?', (cid,))
                    clear_retry(self.db, cid)

        with patch('archive_scout.operations.download_archive', side_effect=retry):
            self.assertEqual(recover_transient_text_errors(self.cfg, self.db, 0, [], ImmediateStop(), None,
                             continuous=True, acquire_only=True), 0)
        self.assertEqual(calls, [ids, [ids[0]], [ids[0]], ids[1:]])
        self.assertEqual(self.db.execute('SELECT count(*) FROM text_retry_schedule').fetchone()[0], 0)

    def test_recovery_probe_with_rejected_body_reopens_the_retry_sweep(self):
        ids = [self.failure(n) for n in range(3)]
        calls = []

        def retry(*_args, **kwargs):
            selected = kwargs['capture_ids']
            calls.append(selected)
            if len(calls) == 1:
                return
            with self.db:
                for cid in selected:
                    if cid == ids[-1]:
                        self.db.execute("UPDATE captures SET state='downloaded' WHERE id=?", (cid,))
                        self.db.execute('UPDATE errors SET resolved=1 WHERE capture_id=?', (cid,))
                        clear_retry(self.db, cid)
                    else:
                        record_failure(self.db, cid, 'payload_digest_mismatch', 200, True, 1, 1)
                        self.db.execute('UPDATE text_retry_schedule SET next_eligible_at=0 WHERE capture_id=?', (cid,))

        stop = threading.Event()
        with patch.object(stop, 'wait', return_value=False), patch('archive_scout.operations.download_archive', side_effect=retry):
            self.assertEqual(recover_transient_text_errors(self.cfg, self.db, 0, [], stop, None,
                             continuous=True, acquire_only=True), 1)
        self.assertEqual(calls[:3], [ids, [ids[0]], ids])
        self.assertEqual([row[0] for row in self.db.execute("SELECT id FROM captures WHERE state='downloaded'")], [ids[-1]])
        self.assertEqual([row[0] for row in self.db.execute('SELECT capture_id FROM text_retry_schedule ORDER BY capture_id')],
                         ids[:2])
