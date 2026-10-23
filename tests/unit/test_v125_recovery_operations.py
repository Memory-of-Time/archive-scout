"""Adversarial recovery and operation handoff checks; all traffic is local."""
from __future__ import annotations

import base64
import hashlib
import json
import shutil
import tempfile
import threading
import time
import unittest
from dataclasses import replace
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import Mock, patch

from archive_scout.cdx.client import HttpClient, TransientRequestError
from archive_scout.cdx.indexer import save_coverage_state
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.config import MediaConfig, NetworkConfig, ProjectConfig
from archive_scout.coverage import IndexCoveragePending, require_embedded_coverage
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import (
    get_or_create_target, get_or_create_keyword_set, get_or_create_media_target, record_error,
    start_scan_run, upsert_capture, upsert_document,
)
from archive_scout.downloads.downloader import (
    _save_payload_evidence, capture_path, download_archive, fetch_retain_streamed,
    prepare_download_rows, save_success,
)
from archive_scout.downloads.redirects import make_replay_redirect_validator
from archive_scout.downloads.schedule import next_due, record_failure
from archive_scout.downloads.validation import classify_exception
from archive_scout.events import Stopped
from archive_scout.downloads.rate_limit import FixedRateLimiter
from archive_scout.media.indexer import media_index_state_signature, media_query_signature
from archive_scout.network.transports import (
    ContentEncodingMismatch, CurlBackend, HttpxBackend, IncompleteRepresentationError,
    PayloadDigestMismatch, RedirectPolicyError, ResilientTransport,
    TransportExhaustedError, TransportFileResponse, Urllib3Backend,
    _decoded_verified_chunks,
)
from archive_scout.operations import (
    _resume_request, _retryable_text_batches, _reusable_interrupted_scan_run,
    remaining_acquisition_gaps, run_project,
)

BODY = b'<html><title>Preserved profile</title><body>needle user profile</body></html>'
DIGEST = base64.b32encode(hashlib.sha1(BODY).digest()).decode()


class ReplayFixture(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    destination_requests = 0

    def do_GET(self):
        if self.path in ('/redirect', '/redirect-missing-location'):
            self.send_response(302)
            if self.path == '/redirect':
                self.send_header('Location', '/destination')
            self.send_header('Content-Length', '1000000')
            self.end_headers()
            # Headers alone must terminate an excluded redirect. Waiting for
            # this body would hit the client's read timeout.
            self.close_connection = True
            return
        if self.path == '/destination':
            type(self).destination_requests += 1
        self.send_response(200)
        self.send_header('Content-Type', 'text/html')
        self.send_header('Content-Length', str(len(BODY)))
        self.send_header('Content-Encoding', 'gzip')
        self.end_headers()
        self.wfile.write(BODY)

    def log_message(self, *args):
        pass


class RecoveryTransport(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(('127.0.0.1', 0), ReplayFixture)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.url = f'http://127.0.0.1:{cls.server.server_port}'

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(2)

    def backends(self):
        yield 'httpx', HttpxBackend(2, 2, 2, trust_env=False)
        yield 'urllib3', Urllib3Backend(2, 2, 2, trust_env=False)
        if shutil.which('curl'):
            yield 'curl', CurlBackend(2, 2, trust_env=False)

    def test_mislabeled_gzip_requires_exact_selected_digest_on_every_backend(self):
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / 'body'
            for name, backend in self.backends():
                try:
                    for digest, expected in (('', ContentEncodingMismatch),
                                             ('A' * 32, PayloadDigestMismatch)):
                        with self.subTest(backend=name, digest=digest):
                            with self.assertRaises(expected):
                                backend.download(self.url+'/profile', {}, dest, 10000,
                                    threading.Event(), expected_digest=digest)
                            self.assertFalse(dest.exists())
                    result = backend.download(self.url+'/profile', {}, dest, 10000,
                        threading.Event(), expected_digest=DIGEST)
                    self.assertEqual(dest.read_bytes(), BODY)
                    self.assertIn(DIGEST, result.headers['x-archive-scout-encoding-recovery'])
                    dest.unlink()
                finally:
                    backend.close()

    def test_sniff_handles_chunk_boundaries_and_still_checks_wire_length(self):
        headers = {'content-encoding': 'gzip', 'content-length': str(len(BODY))}
        self.assertEqual(b''.join(_decoded_verified_chunks(
            [BODY[:1], BODY[1:]], headers, threading.Event(), expected_digest=DIGEST)), BODY)
        with self.assertRaises(IncompleteRepresentationError):
            b''.join(_decoded_verified_chunks([BODY[:-1]], dict(headers), threading.Event(),
                                             expected_digest=DIGEST))

    def test_production_stream_passes_digest_and_commits_recovery_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = ProjectConfig(root, targets=['example.org/*'], keywords=[]).normalized()
            db = open_database(root)
            client = HttpClient(FixedRateLimiter(0), 4, 2, 'ArchiveScoutTest', threading.Event(),
                                network_backend='httpx', trust_environment=False)
            try:
                tid = get_or_create_target(db, cfg.targets[0])
                upsert_capture(db, {'original':'http://example.org/user/a', 'timestamp':'20080101000000',
                    'mimetype':'text/html', 'statuscode':'200', 'digest':DIGEST, 'length':'17'},
                    tid, cdx_query_signature(cfg))
                row = db.execute('SELECT * FROM captures').fetchone()
                record_error(db, 'download', 'content_encoding_mismatch', 'previous failure',
                             capture_id=row['id'], retryable=True)
                with patch('archive_scout.downloads.downloader.replay_url', return_value=self.url+'/profile'):
                    result = fetch_retain_streamed(row, cfg, client)
                save_success(db, result)
                self.assertEqual(result['path'].read_bytes(), BODY)
                self.assertEqual(db.execute('SELECT state FROM captures').fetchone()[0], 'downloaded_unscanned')
                self.assertEqual(db.execute('SELECT COUNT(*) FROM errors WHERE resolved=0').fetchone()[0], 0)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM capture_payload_evidence').fetchone()[0], 1)
            finally:
                client.close()
                db.close()

    def test_redirect_disabled_never_reads_body_or_requests_destination(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = ProjectConfig(Path(tmp), targets=['example.org/*'], keywords=[]).normalized()
            policy = make_replay_redirect_validator(cfg, 'http://example.org/user/a', skip_redirects=True)
            before = ReplayFixture.destination_requests
            for name, backend in self.backends():
                try:
                    for path in ('/redirect', '/redirect-missing-location'):
                        with self.subTest(backend=name, path=path):
                            with self.assertRaises(RedirectPolicyError) as caught:
                                backend.download(self.url+path, {}, Path(tmp)/name,
                                    2000000, threading.Event(), redirect_validator=policy)
                            self.assertEqual(caught.exception.category, 'redirect_skipped')
                            self.assertFalse((Path(tmp)/name).exists())
                finally:
                    backend.close()
            self.assertEqual(ReplayFixture.destination_requests, before)

    def test_plain_and_wrapped_representation_failures_keep_their_category(self):
        for exc, category in ((ContentEncodingMismatch('bad header'), 'content_encoding_mismatch'),
                              (PayloadDigestMismatch('bad bytes'), 'payload_digest_mismatch'),
                              (IncompleteRepresentationError('short body'), 'incomplete_representation')):
            outer = TransientRequestError('representation failure')
            outer.__cause__ = TransportExhaustedError('https://archive.test/', [('httpx', exc)])
            self.assertEqual(classify_exception(outer), (category, None, True))

    def test_stale_route_selection_uses_healthy_backend_without_bypassing_cooldown(self):
        transport = ResilientTransport(pool_size=1, connect_timeout=1, read_timeout=1,
                                       mode='httpx', trust_env=False)
        healthy = Mock()
        transport.backends['healthy'] = healthy
        transport.order.append('healthy')
        transport.cooldown_until['httpx'] = time.monotonic() + 30
        healthy.download.side_effect = lambda url, headers, dest, *args, **kwargs: TransportFileResponse(
            200, {}, url, dest, len(BODY), hashlib.sha256(BODY).hexdigest(), BODY, 'healthy', 0)
        try:
            # Simulate selection made before a peer discovered the failure.
            with patch.object(transport, '_ordered_names', return_value=['httpx']):
                started = time.monotonic()
                with tempfile.TemporaryDirectory() as tmp:
                    result = transport.download(self.url, {}, Path(tmp)/'out', 10000, threading.Event())
                self.assertLess(time.monotonic()-started, 2)
                self.assertEqual(result.backend, 'healthy')
                self.assertGreater(transport.cooldown_until['httpx'], time.monotonic())
        finally:
            transport.close()


class OperationRecovery(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config = ProjectConfig(self.root, targets=['example.org/*'], keywords=['needle'],
                                   from_date='2008', to_date='2008').normalized()
        self.db = open_database(self.root)
        self.addCleanup(self.db.close)

    def seed(self, *, target='example.org/*', state='pending', original='http://example.org/user/a'):
        tid = get_or_create_target(self.db, target)
        sig = cdx_query_signature(self.config.for_target(target))
        upsert_capture(self.db, {'original': original, 'timestamp': '20080101000000',
            'mimetype': 'text/html', 'statuscode': '200', 'digest': DIGEST,
            'length': str(len(BODY))}, tid, sig)
        cid = self.db.execute('SELECT id FROM captures WHERE original_url=?', (original,)).fetchone()[0]
        self.db.execute('UPDATE captures SET state=? WHERE id=?', (state, cid))
        save_coverage_state(self.db, tid, sig, self.config.from_date, self.config.to_date,
                            None, True, 1, None)
        self.db.commit()
        return cid

    def test_redirect_skip_is_durable_exclusion_and_enabling_reopens_only_active_scope(self):
        cid = self.seed()
        error = RedirectPolicyError('redirect excluded', destination='https://web.archive.org/next',
                                    category='redirect_skipped')
        with patch('archive_scout.downloads.downloader.fetch_retain_streamed', side_effect=error):
            download_archive(self.config, self.db, 0, threading.Event(), None, acquire_only=True)
        self.assertEqual(self.db.execute('SELECT state FROM captures WHERE id=?', (cid,)).fetchone()[0], 'skipped')
        self.assertEqual(remaining_acquisition_gaps(self.config, self.db), 0)
        count, rows = prepare_download_rows(self.db, self.config, [])
        self.assertEqual(count, 0)
        count, rows = prepare_download_rows(self.db, replace(self.config, download_external_redirects=True), [])
        self.assertEqual(count, 1)
        self.assertEqual([row['id'] for row in rows], [cid])

    def test_explicit_capture_ids_cannot_escape_active_target_scope(self):
        active = self.seed()
        foreign = self.seed(target='outside.org/*', original='http://outside.org/a')
        count, rows = prepare_download_rows(self.db, self.config, [], capture_ids=[active, foreign])
        self.assertEqual(count, 1)
        self.assertEqual([row['id'] for row in rows], [active])

    def test_legacy_redirect_errors_reenter_only_when_current_policy_can_change_outcome(self):
        cid = self.seed(state='error')
        record_error(self.db, 'download', 'live_redirect_blocked', 'old live redirect',
                     capture_id=cid, retryable=False)
        enabled = replace(self.config, download_external_redirects=True)
        count, rows = prepare_download_rows(self.db, enabled, [], states=('pending','error'))
        self.assertEqual(count, 0)  # enabling following never permits the live web
        count, rows = prepare_download_rows(self.db, self.config, [], states=('pending','error'))
        self.assertEqual(count, 1)  # Off will establish a recorded skip at the first redirect
        self.assertEqual([row['id'] for row in rows], [cid])
        self.assertEqual(self.db.execute('SELECT resolved FROM errors WHERE capture_id=?', (cid,)).fetchone()[0], 0)

    def test_pending_capture_is_unfinished_even_below_attempt_ceiling(self):
        self.seed()
        self.assertEqual(remaining_acquisition_gaps(self.config, self.db), 1)

    def test_retry_selection_and_wait_clock_exclude_unselected_errors(self):
        a = self.seed(state='error')
        b = self.seed(state='error', original='http://example.org/b')
        for cid, category in ((a, 'incomplete_representation'), (b, 'timeout')):
            record_error(self.db, 'download', category, 'temporary', capture_id=cid, retryable=True)
            record_failure(self.db, cid, category, None, True, 10, 30)
        cfg = replace(self.config, retry_capture_ids=[a], retry_error_categories=['incomplete_representation'])
        self.db.execute('UPDATE text_retry_schedule SET next_eligible_at=? WHERE capture_id=?', (time.time()-1, a))
        self.assertEqual(list(_retryable_text_batches(cfg, self.db, respect_selection=True)), [[a]])
        self.db.execute('DELETE FROM text_retry_schedule WHERE capture_id=?', (a,))
        self.assertIsNone(next_due(self.db, cfg, respect_selection=True))
        self.assertIsNotNone(next_due(self.db, cfg))

    def test_resolved_encoding_evidence_survives_reopen_without_open_error(self):
        cid = self.seed()
        _save_payload_evidence(self.db, {'capture_id': cid, 'content_hash': hashlib.sha256(BODY).hexdigest(),
            'bytes_saved': len(BODY), 'response_content_type': 'text/html', 'final_url': 'https://web.archive.org/a',
            'http_status': 200, 'encoding_recovery': 'CDX SHA-1 verified: '+DIGEST})
        self.db.commit()
        with closing(open_database(self.root)) as other:
            row = other.execute("SELECT category,resolved FROM errors WHERE capture_id=?", (cid,)).fetchone()
            self.assertEqual(tuple(row), ('encoding_header_recovered', 1))
            self.assertEqual(other.execute('SELECT bytes_saved FROM capture_payload_evidence').fetchone()[0], len(BODY))

    def test_saved_resume_intent_survives_reporting_and_restores_acquisition_only(self):
        cfg = replace(self.config, keywords=[], keyword_sets=[])
        self.db.execute("INSERT INTO project_meta VALUES('resume_request',?)",
                        (json.dumps({'mode': 'download_only', 'settings': cfg.to_payload()}),))
        self.db.commit()
        result = run_project(cfg, 'report')
        restored, mode = _resume_request(replace(cfg, targets=['other.org/*'], download_external_redirects=True))
        self.assertEqual(mode, 'download_only')
        self.assertEqual(restored.targets, cfg.targets)
        self.assertTrue(restored.download_external_redirects)
        self.assertFalse(restored.selected_keyword_sets())

    def test_resume_keeps_each_media_and_index_mode(self):
        for mode in ('media_index', 'media_all', 'media_download', 'media_retry', 'index', 'rescan'):
            self.db.execute("INSERT OR REPLACE INTO project_meta VALUES('resume_request',?)",
                (json.dumps({'mode': mode, 'settings': self.config.to_payload()}),))
            self.db.commit()
            self.assertEqual(_resume_request(self.config)[1], mode)

    def test_completing_rescan_preserves_an_earlier_unfinished_download(self):
        cfg = replace(self.config, keywords=[], keyword_sets=[])
        self.db.execute("INSERT INTO project_meta VALUES('resume_request',?)",
            (json.dumps({'mode':'download_only', 'settings':cfg.to_payload()}),))
        self.db.commit()
        outcome = run_project(self.config, 'rescan')
        self.assertEqual(outcome.status, 'complete')
        restored, mode = _resume_request(self.config)
        self.assertEqual(mode, 'download_only')
        self.assertFalse(restored.selected_keyword_sets())

    def test_resumed_request_does_not_duplicate_itself_in_backlog(self):
        from archive_scout.operations import _remember_resume_request, _complete_resume_request
        _remember_resume_request(self.db, self.config, 'download_only', 1)
        _remember_resume_request(self.db, self.config, 'rescan', 2)
        _remember_resume_request(self.db, self.config, 'rescan', 3, resumed=True)
        _complete_resume_request(self.db, 3)
        self.db.commit()
        self.assertEqual(_resume_request(self.config)[1], 'download_only')
        self.assertIsNone(self.db.execute("SELECT value FROM project_meta WHERE key='resume_backlog'").fetchone())

    def test_snapshot_cannot_claim_project_complete_with_an_unfinished_request(self):
        from archive_scout.reports.snapshot import export_project_snapshot
        from archive_scout.database.repositories import start_operation_run, finish_operation_run
        self.seed(state='skipped')
        op = start_operation_run(self.db, 'rescan', '1.2.5')
        finish_operation_run(self.db, op, 'complete', 'local task done')
        self.db.execute("INSERT INTO project_meta VALUES('resume_request',?)",
            (json.dumps({'mode':'media_index', 'settings':self.config.to_payload()}),))
        self.db.commit()
        result = export_project_snapshot(self.config)
        manifest = json.loads(result['manifest'].read_text())
        self.assertFalse(manifest['complete'])
        self.assertEqual(manifest['counts']['unfinished_requests'], 1)

    def test_stopped_download_only_resumes_saved_body_without_network_or_keyword_job(self):
        cid = self.seed()
        stopped = threading.Event()
        stopped.set()
        with self.assertRaises(Stopped):
            run_project(replace(self.config, keywords=[]), 'download_only', stopped)
        path = capture_path(self.root, cid, '20080101000000', 'http://example.org/user/a')
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(BODY)
        self.db.execute("UPDATE captures SET state='downloaded_unscanned' WHERE id=?", (cid,))
        _save_payload_evidence(self.db, {'capture_id':cid, 'content_hash':hashlib.sha256(BODY).hexdigest(),
            'bytes_saved':len(BODY), 'response_content_type':'text/html', 'final_url':'https://web.archive.org/a', 'http_status':200})
        self.db.commit()
        with patch('archive_scout.downloads.downloader.fetch_retain_streamed') as network:
            outcome = run_project(replace(self.config, keywords=[]), 'resume')
        network.assert_not_called()
        self.assertEqual(outcome.status, 'complete')
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM scan_runs').fetchone()[0], 0)
        self.assertIsNone(self.db.execute("SELECT value FROM project_meta WHERE key='resume_request'").fetchone())

    def test_embedded_gate_rejects_unprocessed_document_and_failed_lookup(self):
        cid = self.seed(state='downloaded')
        path = self.root/'body.html'
        path.write_bytes(BODY)
        doc = upsert_document(self.db, cid, path, 'profile', 'needle', [],
                              hashlib.sha256(BODY).hexdigest(), 'normalized', len(BODY))
        cfg = replace(self.config, media=MediaConfig(enabled=True, discover_embedded=True))
        with self.assertRaises(IndexCoveragePending):
            require_embedded_coverage(self.db, cfg, 'scope')
        self.db.execute('INSERT INTO media_discovery_documents(query_signature,document_id,content_hash,scanned_at) VALUES(?,?,?,?)',
                         ('scope', doc, hashlib.sha256(BODY).hexdigest(), 'now'))
        require_embedded_coverage(self.db, cfg, 'scope')
        self.db.execute("""INSERT INTO media_discovery_queue(source_document_id,original_url,query_signature,
            state,last_error,created_at,updated_at) VALUES(?,?,?,'unavailable','temporary failure','now','now')""",
            (doc, 'http://example.org/a.mp4', 'scope'))
        with self.assertRaises(IndexCoveragePending):
            require_embedded_coverage(self.db, cfg, 'scope')

    def test_embedded_transient_recovery_has_no_lifetime_attempt_ceiling(self):
        from archive_scout.media.indexer import _complete_embedded_media
        cfg = replace(self.config, max_attempts=2,
            media=MediaConfig(enabled=True, discover_embedded=True),
            network=NetworkConfig(persistent_retries=True, retry_base_seconds=.01))
        self.db.execute("""INSERT INTO media_discovery_queue(original_url,query_signature,state,
            lookup_attempts,created_at,updated_at) VALUES(?,'scope','error',50,'now','now')""",
            ('http://example.org/video.mp4',))
        calls = []
        def lookup(config, client, row, *args):
            calls.append(row['lookup_attempts'])
            return row, [], TransientRequestError('HTTP 504', status=504) if len(calls)==1 else None
        with patch('archive_scout.media.indexer._embedded_lookup', side_effect=lookup):
            _complete_embedded_media(cfg, self.db, Mock(), threading.Event(), None, 'scope')
        self.assertEqual(calls, [50, 51])
        require_embedded_coverage(self.db, cfg, 'scope')

    def test_legacy_resume_handles_1500_scope_pairs(self):
        key = get_or_create_keyword_set(self.db, 'test', ['needle'])
        scan = start_scan_run(self.db, key, 'old', self.config.minimum_score, 'rescan', {})
        self.db.execute("UPDATE scan_runs SET status='interrupted' WHERE id=?", (scan,))
        cfg = replace(self.config, targets=[f'site{i}.example.org/*' for i in range(1500)])
        self.assertEqual(_reusable_interrupted_scan_run(self.db, cfg, key, 'new-scope', rescan_only=True), scan)

    def test_combined_operation_refreshes_discovery_after_text_before_media(self):
        cid = self.seed()
        cfg = replace(self.config, media=MediaConfig(enabled=True, discover_embedded=True))
        indexed_document_counts, acquired_document_counts = [], []

        def index_media(config, db, *args):
            count = db.execute('SELECT COUNT(*) FROM documents').fetchone()[0]
            indexed_document_counts.append(count)
            tid = get_or_create_media_target(db, config.targets[0])
            db.execute("""INSERT OR REPLACE INTO media_index_state(target_id,extension,year,
                query_signature,complete,updated_at) VALUES(?,'__all__',2008,?,1,'now')""",
                (tid, media_index_state_signature(config)))
            db.execute("""INSERT OR REPLACE INTO media_discovery_documents
                (query_signature,document_id,content_hash,scanned_at)
                SELECT ?,id,content_hash,'now' FROM documents""", (media_query_signature(config),))
            db.commit()

        def fetch(row, config, client):
            path = capture_path(self.root, cid, row['timestamp'], row['original_url'])
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(BODY)
            return {'capture_id':cid, 'path':path, 'bytes_saved':len(BODY),
                'content_hash':hashlib.sha256(BODY).hexdigest(), 'http_status':200,
                'final_url':'https://web.archive.org/web/20080101000000id_/http://example.org/user/a', 'response_content_type':'text/html',
                'deferred_scan':True}

        def media_download(config, db, *args, **kwargs):
            require_embedded_coverage(db, config, media_query_signature(config))
            acquired_document_counts.append(db.execute('SELECT COUNT(*) FROM documents').fetchone()[0])

        with patch('archive_scout.operations.index_media', side_effect=index_media), \
             patch('archive_scout.downloads.downloader.fetch_retain_streamed', side_effect=fetch), \
             patch('archive_scout.operations.download_media', side_effect=media_download):
            result = run_project(cfg, 'all')
        self.assertEqual(result.status, 'complete')
        self.assertEqual(indexed_document_counts, [0, 1])
        self.assertEqual(acquired_document_counts, [1])

    def test_acquisition_only_materializes_embeds_without_keyword_jobs(self):
        cid = self.seed()
        path = capture_path(self.root, cid, '20080101000000', 'http://example.org/user/a')
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(BODY)
        self.db.execute("UPDATE captures SET state='downloaded_unscanned' WHERE id=?", (cid,))
        self.db.commit()
        cfg = replace(self.config, keywords=[], keyword_sets=[],
                      media=MediaConfig(enabled=True, discover_embedded=True))
        counts = []
        def index_media(config, db, *args):
            counts.append(db.execute('SELECT COUNT(*) FROM documents').fetchone()[0])
            tid = get_or_create_media_target(db, config.targets[0])
            db.execute("""INSERT OR REPLACE INTO media_index_state(target_id,extension,year,
                query_signature,complete,updated_at) VALUES(?,'__all__',2008,?,1,'now')""",
                (tid, media_index_state_signature(config)))
            db.execute("""INSERT OR REPLACE INTO media_discovery_documents
                (query_signature,document_id,content_hash,scanned_at)
                SELECT ?,id,content_hash,'now' FROM documents""", (media_query_signature(config),))
            db.commit()
        with patch('archive_scout.operations.index_media', side_effect=index_media), \
             patch('archive_scout.operations.download_media'):
            result = run_project(cfg, 'download_only')
        self.assertEqual(result.status, 'complete')
        self.assertEqual(counts, [0, 1])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM scan_runs').fetchone()[0], 0)


if __name__ == '__main__':
    unittest.main()
