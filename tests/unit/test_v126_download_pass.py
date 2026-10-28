"""Normal-pass completion, retry fairness, and truthful UI/CLI explanations."""
from __future__ import annotations

import base64
import hashlib
import io
import json
import queue
import tempfile
import threading
import unittest
from collections import Counter
from contextlib import redirect_stdout
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import Mock, patch

from archive_scout.cdx.client import CDXRows, HttpClient, MalformedCDXResponse, TransientRequestError, PermanentRequestError
from archive_scout.cdx.indexer import index_archive, save_coverage_state
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.cdx.status import text_index_status, format_index_status
from archive_scout.config import ProjectConfig, NetworkConfig, MediaConfig, save_project_config
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, upsert_capture, record_error, upsert_media_capture
from archive_scout.downloads.outcomes import text_outcome_counts
from archive_scout.downloads.rate_limit import reset_shared_traffic_state_for_tests
from archive_scout.downloads.rate_limit import SharedFixedRateLimiter, SharedHostGate
from archive_scout.downloads.schedule import record_failure
from archive_scout.events import Stopped
from archive_scout.media.indexer import index_direct_media, media_query_signature, media_index_state_signature
from archive_scout.media.recovery import record_media_failure, recover_active_media
from archive_scout.operations import run_project, recover_transient_text_errors, _media_unfinished
from archive_scout.retry_budget import NoProgressBudget
from archive_scout.ui.dashboard import read_dashboard_counts, format_capture_accounting


class Fixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.cfg = ProjectConfig(self.root, targets=['example.org/*'], keywords=['needle'],
            from_date='2008', to_date='2008', cdx_collapses=[], workers=3,
            download_delay=0, cdx_delay=0, max_attempts=2,
            network=NetworkConfig(backend='httpx', trust_environment=False, index_strategy='resume',
                retry_base_seconds=1, retry_max_seconds=1)).normalized()
        self.db = open_database(self.root)
        self.addCleanup(self.db.close)
        self.tid = get_or_create_target(self.db, self.cfg.targets[0])
        save_coverage_state(self.db, self.tid, cdx_query_signature(self.cfg),
            self.cfg.from_date, self.cfg.to_date, None, True, 0, None)
        self.db.commit()

    def capture(self, url='http://example.org/a', *, target=None):
        upsert_capture(self.db, {'timestamp': '20080101000000', 'original': url,
            'mimetype': 'text/html', 'statuscode': '200', 'digest': '', 'length': '4'},
            self.tid if target is None else target, cdx_query_signature(self.cfg))
        cid = self.db.execute('SELECT id FROM captures WHERE original_url=?', (url,)).fetchone()[0]
        self.db.commit()
        return cid

    def failure(self, cid, category, retryable=False):
        with self.db:
            self.db.execute("UPDATE captures SET state='error' WHERE id=?", (cid,))
            record_error(self.db, 'download', category, 'fixture evidence', capture_id=cid,
                         http_status=404 if category == 'missing_capture' else None, retryable=retryable)
            record_failure(self.db, cid, category, None, retryable, 1, 1, max_bytes=self.cfg.max_file_bytes)


class PassOutcomes(Fixture):
    def test_unavailable_outcomes_finish_without_erasing_evidence_or_resume(self):
        for category in ('missing_capture', 'wayback_excluded', 'robots_blocked'):
            self.failure(self.capture('http://example.org/' + category), category)
        before = [tuple(row) for row in self.db.execute('SELECT * FROM errors ORDER BY id')]
        outcome = run_project(self.cfg, 'download_only')
        self.assertEqual(outcome.status, 'complete')
        self.assertIn('3 unavailable capture(s)', outcome.message)
        self.assertEqual(text_outcome_counts(self.db, self.cfg), {'unfinished': 0, 'unavailable': 3})
        self.assertEqual([tuple(row) for row in self.db.execute('SELECT * FROM errors ORDER BY id')], before)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM captures WHERE state='error'").fetchone()[0], 3)
        self.assertIsNone(self.db.execute("SELECT 1 FROM project_meta WHERE key='resume_request'").fetchone())
        counts = read_dashboard_counts(self.root / 'archive_scout.sqlite3')
        self.assertEqual((counts['failed_captures'], counts['unavailable_captures'], counts['accounted_captures']), (3, 3, 3))
        self.assertIn('3 confirmed unavailable', format_capture_accounting(counts))
        self.assertIn('3 confirmed unavailable', outcome['summary'].read_text())

    def test_full_scan_operation_finishes_with_only_known_unavailable(self):
        self.failure(self.capture(), 'missing_capture')
        outcome = run_project(self.cfg, 'all')
        self.assertEqual(outcome.status, 'complete')
        self.assertIn('1 unavailable text capture(s)', outcome.message)

    def test_report_snapshot_does_not_claim_all_captures_acquired_when_pass_finished(self):
        self.failure(self.capture(), 'missing_capture')
        result = run_project(self.cfg, 'download_only')
        self.assertEqual(result.status, 'complete')
        from archive_scout.reports.snapshot import export_project_snapshot
        paths = export_project_snapshot(self.cfg)
        manifest = json.loads(paths['manifest'].read_text())
        self.assertEqual(manifest['operation']['status'], 'complete')
        self.assertFalse(manifest['complete'])
        self.assertEqual(manifest['counts']['unavailable_text_captures'], 1)
        self.assertIn('finished download pass', manifest['note'])

    def test_unknown_size_policy_and_local_failures_remain_unfinished(self):
        for category in ('oversized_response', 'local_processing_error', 'live_redirect_blocked',
                         'invalid_wayback_replay', 'wayback_forbidden', 'http_client_error'):
            with self.subTest(category=category):
                cid = self.capture('http://example.org/' + category)
                self.failure(cid, category)
        # Keep the redirect policy unchanged: changing it deliberately reopens
        # legacy policy refusals, which is tested separately in the main suite.
        outcome = run_project(replace(self.cfg, download_external_redirects=True), 'download_only')
        self.assertEqual(outcome.status, 'incomplete')
        self.assertEqual(text_outcome_counts(self.db, self.cfg), {'unfinished': 6, 'unavailable': 0})

    def test_new_transient_decision_overrides_an_old_missing_response(self):
        cid = self.capture()
        self.failure(cid, 'missing_capture')
        self.failure(cid, 'incomplete_representation', True)
        self.assertEqual(text_outcome_counts(self.db, self.cfg), {'unfinished': 1, 'unavailable': 0})

    def test_unproven_routing_or_error_alone_cannot_settle_failure(self):
        cid = self.capture()
        with self.db:
            self.db.execute("UPDATE captures SET state='error' WHERE id=?", (cid,))
            record_error(self.db, 'download', 'missing_capture', 'legacy', capture_id=cid, retryable=False)
        self.assertEqual(text_outcome_counts(self.db, self.cfg)['unfinished'], 1)
        with self.db:
            self.db.execute('DELETE FROM errors')
            record_failure(self.db, cid, 'missing_capture', 404, False, 1, 1)
        self.assertEqual(text_outcome_counts(self.db, self.cfg)['unfinished'], 1)

    def test_outcomes_obey_current_target_and_date_scope(self):
        cid = self.capture()
        self.failure(cid, 'missing_capture')
        foreign = self.capture('http://outside.org/a', target=get_or_create_target(self.db, 'outside.org/*'))
        self.failure(foreign, 'connection', True)
        self.assertEqual(text_outcome_counts(self.db, self.cfg), {'unfinished': 0, 'unavailable': 1})
        later = replace(self.cfg, from_date='20090101000000', to_date='20091231235959')
        self.assertEqual(text_outcome_counts(self.db, later), {'unfinished': 0, 'unavailable': 0})

    def test_pending_state_is_never_hidden_by_old_missing_evidence(self):
        cid = self.capture()
        self.failure(cid, 'missing_capture')
        self.db.execute("UPDATE captures SET state='pending'")
        self.db.commit()
        self.assertEqual(text_outcome_counts(self.db, self.cfg), {'unfinished': 1, 'unavailable': 0})

    def test_media_replay_missing_is_settled_but_new_truncation_is_unfinished(self):
        cfg = replace(self.cfg, media=MediaConfig(enabled=True, discover_embedded=False))
        signature = media_query_signature(cfg)
        upsert_media_capture(self.db, {'timestamp': '20080101000000', 'original': 'http://example.org/a.jpg',
            'mimetype': 'image/jpeg', 'statuscode': '200', 'digest': '', 'length': '4'},
            None, signature, 'image', '.jpg')
        cid = self.db.execute('SELECT id FROM media_captures').fetchone()[0]
        with self.db:
            self.db.execute("UPDATE media_captures SET state='error'")
            record_error(self.db, 'media_download', 'missing_capture', 'HTTP 404', media_capture_id=cid, retryable=False)
            record_media_failure(self.db, cid, 'missing_capture', 404, False, cfg)
        with patch('archive_scout.operations.media_coverage_gaps', return_value=[]):
            self.assertFalse(_media_unfinished(self.db, cfg))
            with self.db:
                record_media_failure(self.db, cid, 'incomplete_representation', None, True, cfg)
            self.assertTrue(_media_unfinished(self.db, cfg))


class Fairness(Fixture):
    def test_paused_target_does_not_spend_another_targets_time(self):
        clock = [0.0]
        budget = NoProgressBudget(self.cfg, clock=lambda: clock[0])
        budget.attempted()
        clock[0] = 10
        budget.pause()
        clock[0] = 10000
        self.assertFalse(budget.exhausted)
        budget.resume()
        clock[0] += 19
        self.assertFalse(budget.exhausted)
        clock[0] += 2
        self.assertTrue(budget.exhausted)
        budget.progress()
        self.assertFalse(budget.exhausted)

    def test_forty_text_targets_each_receive_their_second_attempt(self):
        cfg = replace(self.cfg, targets=[f'site{n}.org/*' for n in range(40)])
        clock = [0.0]
        calls = Counter()
        def response(client, endpoints, params, **kwargs):
            target = dict(params)['url']
            calls[target] += 1
            clock[0] += 10
            if calls[target] == 1:
                raise TransientRequestError('HTTP 504', status=504)
            return CDXRows([])
        events = []
        with patch.object(HttpClient, 'get_cdx_any', response), \
             patch('archive_scout.cdx.indexer.time.monotonic', side_effect=lambda: clock[0]), \
             patch('archive_scout.cdx.indexer.transient_backoff', return_value=0):
            self.assertFalse(index_archive(cfg, self.db, threading.Event(), events.append))
        self.assertEqual(set(calls.values()), {2})
        self.assertEqual(len(calls), 40)
        self.assertEqual(events[-1].detail['unfinished_targets'], 0)
        self.assertEqual(events[-1].detail['complete_targets'], 40)

    def test_forty_media_targets_each_receive_their_second_attempt(self):
        cfg = replace(self.cfg, targets=[f'site{n}.org/*' for n in range(40)],
            media=MediaConfig(enabled=True, discover_embedded=False))
        clock = [0.0]
        calls = Counter()
        def response(scoped, client, target, window, extensions):
            calls[target] += 1
            clock[0] += 10
            if calls[target] == 1:
                raise TransientRequestError('HTTP 504', status=504)
            return [], True, None
        with patch('archive_scout.media.indexer._request_media_resume', side_effect=response), \
             patch('archive_scout.media.indexer.time.monotonic', side_effect=lambda: clock[0]), \
             patch('archive_scout.media.indexer._wait_seconds', return_value=0):
            self.assertFalse(index_direct_media(cfg, self.db, Mock(), threading.Event(), None,
                media_query_signature(cfg), media_index_state_signature(cfg)))
        self.assertEqual(set(calls.values()), {2})
        self.assertEqual(len(calls), 40)

    def test_text_inherited_long_cooldown_gets_an_automatic_attempt(self):
        cid = self.capture()
        self.failure(cid, 'connection', True)
        clock = [100.0]
        self.db.execute('UPDATE text_retry_schedule SET next_eligible_at=1000')
        self.db.commit()
        stop = threading.Event()
        waits = []
        def wait(seconds=None):
            waits.append(seconds)
            clock[0] += seconds or 0
            return False
        def download(*args, **kwargs):
            with self.db:
                self.db.execute("UPDATE captures SET state='skipped' WHERE id=?", (cid,))
        with patch.object(stop, 'wait', side_effect=wait), \
             patch('archive_scout.operations.time.time', side_effect=lambda: clock[0]), \
             patch('archive_scout.operations.download_archive', side_effect=download) as fetch:
            result = recover_transient_text_errors(self.cfg, self.db, 0, [], stop, None,
                continuous=True, acquire_only=True)
        self.assertEqual(result, 0)
        self.assertEqual(fetch.call_count, 1)
        self.assertGreater(sum(waits), 30)

    def test_long_failed_sweep_cannot_skip_the_next_sweep(self):
        cid = self.capture()
        self.failure(cid, 'connection', True)
        self.db.execute('UPDATE text_retry_schedule SET next_eligible_at=0')
        self.db.commit()
        clock = [0.0]
        calls = []
        def download(*args, **kwargs):
            calls.append(kwargs['capture_ids'])
            clock[0] += 3600
            if len(calls) == 2:
                self.db.execute("UPDATE captures SET state='skipped'")
                self.db.commit()
        with patch('archive_scout.operations.time.monotonic', side_effect=lambda: clock[0]), \
             patch('archive_scout.operations.download_archive', side_effect=download):
            self.assertEqual(recover_transient_text_errors(self.cfg, self.db, 0, [],
                threading.Event(), None, continuous=True, acquire_only=True), 0)
        self.assertEqual(calls, [[cid], [cid]])

    def test_media_inherited_long_cooldown_gets_an_automatic_attempt(self):
        cfg = replace(self.cfg, media=MediaConfig(enabled=True, discover_embedded=False))
        signature = media_query_signature(cfg)
        upsert_media_capture(self.db, {'timestamp': '20080101000000', 'original': 'http://example.org/a.jpg',
            'mimetype': 'image/jpeg', 'statuscode': '200', 'digest': '', 'length': '4'},
            None, signature, 'image', '.jpg')
        cid = self.db.execute('SELECT id FROM media_captures').fetchone()[0]
        with self.db:
            self.db.execute("UPDATE media_captures SET state='error'")
            record_error(self.db, 'media_download', 'connection', 'offline', media_capture_id=cid, retryable=True)
            record_media_failure(self.db, cid, 'connection', None, True, cfg)
            self.db.execute('UPDATE media_retry_schedule SET next_eligible_at=1000')
        clock = [100.0]
        stop = threading.Event()
        waits = []
        def wait(seconds=None):
            waits.append(seconds)
            clock[0] += seconds or 0
            return False
        def download(*args, **kwargs):
            self.db.execute("UPDATE media_captures SET state='downloaded'")
            self.db.commit()
        with patch.object(stop, 'wait', side_effect=wait), \
             patch('archive_scout.media.recovery.time.time', side_effect=lambda: clock[0]), \
             patch('archive_scout.media.downloader.download_media', side_effect=download) as fetch:
            recover_active_media(cfg, self.db, stop, None, allow_partial_inventory=True)
        self.assertEqual(fetch.call_count, 1)
        self.assertGreater(sum(waits), 30)

    def test_stop_interrupts_saved_cooldown(self):
        cid = self.capture()
        self.failure(cid, 'connection', True)
        self.db.execute('UPDATE text_retry_schedule SET next_eligible_at=99999999999')
        self.db.commit()
        stop = threading.Event()
        stop.set()
        with self.assertRaises(Stopped):
            recover_transient_text_errors(self.cfg, self.db, 0, [], stop, None,
                continuous=True, acquire_only=True)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM text_retry_schedule').fetchone()[0], 1)


class Explanations(Fixture):
    def test_unfinished_summary_counts_targets_and_preserves_reason(self):
        cfg = replace(self.cfg, targets=['example.org/*', 'broken.org/*', 'untouched.org/*'])
        tid = get_or_create_target(self.db, 'broken.org/*')
        error = record_error(self.db, 'index', 'invalid_index_response', 'foreign host rejected', retryable=True)
        save_coverage_state(self.db, tid, cdx_query_signature(cfg), cfg.from_date, cfg.to_date,
                            None, False, 0, error)
        self.db.commit()
        status = text_index_status(self.db, cfg)
        self.assertEqual((status['complete_targets'], status['unfinished_targets']), (1, 2))
        self.assertIn('invalid_index_response', format_index_status(status))
        self.assertIn('no index checkpoint', format_index_status(status))
        from archive_scout.reports.text import generate_index_reports
        paths = generate_index_reports(cfg, self.db)
        self.assertIn('foreign host rejected', paths['indexing_status'].read_text())
        self.assertIn('2 unfinished', paths['summary'].read_text())

    def test_unverified_page_is_still_incomplete_with_a_named_reason(self):
        cfg = replace(self.cfg, targets=['bad.org/*'])
        events = []
        with patch.object(HttpClient, 'get_cdx_any', side_effect=MalformedCDXResponse('foreign rows', splittable=False)), \
             patch('archive_scout.cdx.indexer.transient_backoff', return_value=0):
            outcome = run_project(cfg, 'index', callback=events.append)
        self.assertEqual(outcome.status, 'incomplete')
        self.assertIn('1 unfinished', outcome.message)
        self.assertIn('retry_budget_exhausted', outcome.message)
        self.assertIn('foreign rows', outcome['indexing_status'].read_text())
        status_event = next(event for event in events if event.stage == 'index_status')
        self.assertEqual(status_event.detail['unfinished_targets'], 1)

    def test_cli_complete_response_keeps_unavailable_explanation(self):
        self.failure(self.capture(), 'missing_capture')
        path = save_project_config(self.cfg)
        from archive_scout.cli import cli_main, EXIT_OK
        output = io.StringIO()
        with redirect_stdout(output):
            code = cli_main(['run', str(path), '--mode', 'download_only', '--format', 'json'])
        payload = json.loads(output.getvalue())
        self.assertEqual(code, EXIT_OK)
        self.assertEqual(payload['status'], 'complete')
        self.assertIn('1 unavailable', payload['message'])

    def test_gui_complete_and_incomplete_messages_keep_actual_reasons(self):
        from archive_scout.ui.main_window import ArchiveScoutApp
        from archive_scout.operations import OperationPaths
        for kind, payload in [('complete', OperationPaths({}, message='Pass complete; 22 unavailable captures')),
                              ('incomplete_result', {'paths': {}, 'message': '34 unfinished targets; HTTP 504'})]:
            with self.subTest(kind=kind):
                app = Mock()
                app.events = queue.Queue()
                app.events.put((kind, payload))
                with patch('archive_scout.ui.main_window.messagebox') as box:
                    ArchiveScoutApp.process_events(app)
                if kind == 'complete':
                    self.assertIn('22 unavailable', box.showinfo.call_args.args[1])
                else:
                    self.assertIn('34 unfinished targets; HTTP 504', box.showwarning.call_args.args[1])


class RealHTTP(Fixture):
    def test_streamed_rejections_use_archive_evidence_and_keep_plain_403_unfinished(self):
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(403)
                if self.path == '/excluded':
                    self.send_header('x-archive-wayback-runtime-error', 'Blocked Site Error')
                if self.path == '/robots':
                    self.send_header('x-archive-wayback-runtime-error', 'robots.txt blocked')
                self.send_header('Content-Length', '0')
                self.end_headers()
            def log_message(self, *args): pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        client = HttpClient(SharedFixedRateLimiter(0), 1, 2, 'test', threading.Event(),
            network_backend='httpx', trust_environment=False, host_gate=SharedHostGate())
        self.addCleanup(client.close)
        for path, category in [('/excluded', 'wayback_excluded'), ('/robots', 'robots_blocked'),
                               ('/forbidden', 'wayback_forbidden')]:
            with self.subTest(path=path), self.assertRaises(PermanentRequestError) as caught:
                client.download_to_path(f'http://127.0.0.1:{server.server_port}' + path,
                                        self.root / 'rejected', 1024, retain_partial=False)
            self.assertEqual(caught.exception.category, category)
            self.assertFalse((self.root / 'rejected').exists())

    def test_one_run_saves_functioning_urls_recovers_failure_and_finishes_with_404(self):
        body = b'<html><body>needle retained intact</body></html>'
        digest = base64.b32encode(hashlib.sha1(body).digest()).decode()
        calls = Counter()
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                calls[self.path] += 1
                status = 404 if self.path == '/missing' else 502 if self.path == '/transient' and calls[self.path] == 1 else 200
                payload = body if status == 200 else b'Unavailable'
                self.send_response(status)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.send_header('Content-Length', str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            def log_message(self, *args): pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        for name in ('good', 'transient', 'missing'):
            self.capture('http://example.org/' + name)
        with self.db:
            self.db.execute('UPDATE captures SET digest=?', (digest,))
        reset_shared_traffic_state_for_tests()
        base = f'http://127.0.0.1:{server.server_port}'
        events = []
        with patch('archive_scout.downloads.downloader.replay_url',
                   side_effect=lambda ts, url: base + '/' + url.rsplit('/', 1)[1]):
            outcome = run_project(self.cfg, 'all', callback=events.append)
        self.assertEqual(outcome.status, 'complete')
        self.assertEqual(calls, Counter({'/good': 1, '/transient': 2, '/missing': 1}))
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM captures WHERE state='downloaded'").fetchone()[0], 2)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM document_matches').fetchone()[0], 2)
        files = list((self.root / 'captures').rglob('*.txt'))
        self.assertEqual(len(files), 2)
        self.assertTrue(all(path.read_bytes() == body for path in files))
        self.assertIsNone(self.db.execute("SELECT 1 FROM project_meta WHERE key='resume_request'").fetchone())
        self.assertTrue(any(event.detail.get('unavailable_failures') == 1 for event in events))
        self.assertEqual(text_outcome_counts(self.db, self.cfg), {'unfinished': 0, 'unavailable': 1})


if __name__ == '__main__':
    unittest.main()
