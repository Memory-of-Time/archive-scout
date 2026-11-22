from __future__ import annotations

import errno
import json
import subprocess
import sys
import tempfile
import threading
import unittest
from contextlib import closing
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

import httpx

from archive_scout import operations
from archive_scout.cdx.indexer import save_coverage_state
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.config import NetworkConfig, ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, upsert_capture
from archive_scout.downloads import downloader
from archive_scout.downloads.diagnostics import AcquisitionDiagnostics
from archive_scout.downloads.rate_limit import reset_shared_traffic_state_for_tests
from archive_scout.error_runs import ErrorRun, active_error_run
from archive_scout.network.transports import HttpxBackend, ResilientTransport, TransportResponse, Urllib3Backend


STAMP = '20020101000000'
BODY = b'<html><body>needle in a complete archive capture</body></html>'
SECRET = 'fixture-session-sensitive-value'


class ErrorRunTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.config = ProjectConfig(self.root, ['*.example.test/*'], ['needle'], from_date='2002', to_date='2002',
            cdx_match_type='domain', download_delay=0, cdx_collapses=[], workers=2, auto_backup=False,
            retries=1, max_attempts=1,
            network=NetworkConfig(backend='httpx', trust_environment=False, persistent_retries=False)).normalized()
        reset_shared_traffic_state_for_tests()
        self.addCleanup(reset_shared_traffic_state_for_tests)

    def latest(self):
        parent = self.root / 'errors'
        return parent / json.loads((parent / 'latest-run.json').read_text())['directory']

    def journal(self, name='failures', directory=None):
        base = (directory or self.latest()) / (name + '.jsonl')
        paths = (base.with_name(base.name + '.2'), base.with_name(base.name + '.1'), base)
        return [json.loads(line) for path in paths if path.exists() for line in path.read_text().splitlines()]

    def test_late_capture_evidence_survives_history_rotation(self):
        limits = {'failures': 8192, 'timeline': 8192}
        with patch.object(ErrorRun, 'FILE_LIMITS', limits), ErrorRun(self.config, 'all', 'all') as run:
            diagnostic = AcquisitionDiagnostics(self.root, None)
            for number in range(100):
                diagnostic.failure({'id': number, 'timestamp': STAMP, 'original_url': 'http://www.example.test/'},
                                   'incomplete_representation', RuntimeError('read stopped: ' + 'X'*300))
            row = {'id': 777, 'timestamp': STAMP, 'original_url': f'http://bbs.example.test/search.php?sid={SECRET}'}
            diagnostic.failure(row, 'connection', ConnectionRefusedError(errno.ECONNREFUSED, 'late refusal'))
            diagnostic.close()
        rows = self.journal()
        evidence = next(item for item in rows if item.get('capture_id') == 777)
        self.assertEqual(evidence['exception_chain'][0]['errno'], errno.ECONNREFUSED)
        self.assertEqual(evidence['original_host'], 'bbs.example.test')
        self.assertNotIn(SECRET, json.dumps(rows))
        examples = json.loads((run.directory / 'failure-examples.json').read_text())
        same_group = examples['text_capture_outcome|incomplete_representation|www.example.test']
        self.assertEqual((same_group['first']['capture_id'], same_group['latest']['capture_id'], same_group['count']), (0,99,100))
        summary = json.loads((run.directory / 'overview.json').read_text())
        self.assertGreater(summary['journal_rotations']['failures'], 0)
        self.assertEqual(summary['queue_drops'], 0)
        self.assertTrue(summary['final'])
        for path in run.directory.glob('*.jsonl*'):
            self.assertLessEqual(path.stat().st_size, 8192)

    def test_backend_failure_is_kept_when_the_next_backend_succeeds(self):
        url = f'https://web.archive.org/web/{STAMP}id_/http://bbs.example.test/search.php?sid={SECRET}'
        cause = ConnectionRefusedError(errno.ECONNREFUSED, 'socket refused')
        error = httpx.ConnectError('retry the route')
        error.__cause__ = cause
        with ErrorRun(self.config, 'all', 'all'), closing(ResilientTransport(
                pool_size=2, connect_timeout=1, read_timeout=1, trust_env=False)) as transport:
            with patch.object(HttpxBackend, 'request', side_effect=error), patch.object(Urllib3Backend, 'request',
                    return_value=TransportResponse(200, {}, url, BODY, 'urllib3', .01)):
                self.assertEqual(transport.request(url, {}, 1024, threading.Event()).data, BODY)
        finding = next(row for row in self.journal() if row['event'] == 'backend_failure')
        self.assertEqual((finding['backend'], finding['phase']), ('httpx', 'connection_refused'))
        self.assertEqual(finding['network_endpoint'], {'host': 'web.archive.org', 'port': 443})
        self.assertEqual(finding['capture_timestamp'], STAMP)
        self.assertEqual(finding['replay_modifier'], 'id_')
        self.assertNotIn(SECRET, json.dumps(finding))

    def test_rotation_leaves_unrecognized_subdirectories_and_preserves_previous_failures(self):
        unrelated = self.root / 'errors' / 'run-20000101T000000000000-000000000000'
        unrelated.mkdir(parents=True)
        (unrelated / 'notes.txt').write_text('fixture')
        paths = []
        with patch.object(ErrorRun, 'KEEP_RUNS', 2):
            for number in range(3):
                with ErrorRun(self.config, 'resume', 'download_only') as run:
                    run.record('failures', 'fixture', {'number': number})
                    paths.append(run.directory)
        self.assertFalse(paths[0].exists())
        self.assertEqual(self.journal(directory=paths[1])[0]['number'], 1)
        self.assertTrue(self.latest().samefile(paths[2]))
        self.assertEqual((unrelated / 'notes.txt').read_text(), 'fixture')
        self.assertIsNone(active_error_run.get())

    def test_process_exit_keeps_published_evidence_and_resume_starts_another_run(self):
        code = """
import os, sys, time
from pathlib import Path
from archive_scout.config import ProjectConfig
from archive_scout.error_runs import ErrorRun
config = ProjectConfig(Path(sys.argv[1]), ['example.test/*'], []).normalized()
with ErrorRun(config, 'all', 'all') as run:
    run.operation_id = 41
    run.record('failures', 'fixture', {'capture_id': 123})
    journal = run.directory / 'failures.jsonl'
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        if journal.is_file() and journal.read_bytes().endswith(b'\\n'):
            os._exit(78)
        time.sleep(.05)
    raise RuntimeError('journal publication timed out')
"""
        process = subprocess.run([sys.executable, '-B', '-c', code, str(self.root)], capture_output=True, timeout=15)
        self.assertEqual(process.returncode, 78, process.stderr.decode(errors='replace'))
        interrupted = self.latest()
        with ErrorRun(self.config, 'resume', 'all') as run:
            self.assertNotEqual(run.directory, interrupted)
        saved, = self.journal(directory=interrupted)
        self.assertEqual((saved['operation_run_id'], saved['capture_id']), (41, 123))
        self.assertEqual(json.loads((self.latest() / 'run.json').read_text())['requested_mode'], 'resume')

    def test_database_open_failure_keeps_stack_location_and_status(self):
        with patch.object(operations, 'open_database', side_effect=OSError(errno.EACCES, 'opening fixture database')):
            with self.assertRaises(OSError):
                operations.run_project(self.config, 'download_only')
        row, = self.journal()
        self.assertEqual(row['stage'], 'opening project database')
        self.assertEqual(row['event'], 'operation_exception')
        self.assertTrue(any(frame['file'] == 'operations.py' for frame in row['frames']))
        self.assertEqual(json.loads((self.latest() / 'overview.json').read_text())['status'], 'failed')

    def test_path_obstruction_and_opt_out_keep_operation_running(self):
        with patch.object(ErrorRun, '_settings', side_effect=AssertionError('disabled diagnostics ran')):
            config = replace(self.config, network=replace(self.config.network, diagnostics=False))
            with ErrorRun(config, 'all', 'all') as run:
                self.assertIsNone(run.directory)
        self.assertFalse((self.root / 'errors').exists())
        (self.root / 'errors').write_text('unrelated fixture')
        with ErrorRun(self.config, 'all', 'all') as run:
            self.assertIsNone(run.directory)
            self.assertIsNone(active_error_run.get())

    def test_real_operation_records_redirect_rejection_retry_and_verified_save(self):
        class Replay(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_GET(self):
                if '/search.php' in self.path:
                    self.send_response(302)
                    self.send_header('Location', self.path.replace('search.php', 'login.php'))
                    self.end_headers()
                    return
                short = self.path.endswith('/partial')
                self.send_response(200)
                self.send_header('Content-Type', 'text/html')
                self.send_header('Set-Cookie', f'sid={SECRET}')
                self.send_header('Content-Length', str(len(BODY) * (2 if short else 1)))
                self.end_headers()
                self.wfile.write(BODY)
                self.close_connection = True

        server = ThreadingHTTPServer(('127.0.0.1', 0), Replay)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        def route(stamp, url, modifier='id_'):
            return f'http://127.0.0.1:{server.server_port}/web/{stamp}{modifier}/{url}'

        signature = cdx_query_signature(self.config)
        urls = ['http://news.example.test/valid', 'http://bbs.example.test/partial',
                f'http://bbs.example.test/search.php?sid={SECRET}']
        with closing(open_database(self.root)) as db, db:
            target = get_or_create_target(db, self.config.targets[0])
            for url in urls:
                upsert_capture(db, {'timestamp': STAMP, 'original': url, 'mimetype': 'text/html',
                                   'statuscode': '200', 'digest': '', 'length': len(BODY)}, target, signature)
            save_coverage_state(db, target, signature, self.config.from_date, self.config.to_date, None, True, len(urls), None)
        with patch.object(downloader, 'replay_url', side_effect=route):
            result = operations.run_project(self.config, 'download_only')
        self.assertEqual(result.status, 'incomplete')
        journal = self.journal()
        redirect = next(row for row in journal if row.get('phase') == 'redirect_policy')
        self.assertIn('/login.php', redirect['redirect_destination'])
        self.assertEqual(redirect['response_status'], 302)
        self.assertEqual(redirect['connected_peer'], ['127.0.0.1', server.server_port])
        representation = next(row['representation'] for row in journal if row.get('phase') == 'representation')
        self.assertEqual(representation['response_status'], 200)
        self.assertNotIn('set-cookie', representation.get('response_headers', {}))
        retry = next(row for row in journal if row['event'] == 'retry_scheduled')
        self.assertGreater(retry['next_eligible_epoch'], 0)
        self.assertEqual(retry['category'], 'incomplete_representation')
        overview = json.loads((self.latest() / 'overview.json').read_text())
        health = json.loads((self.latest() / 'text-acquisition/latest.json').read_text())
        self.assertEqual(overview['operation_run_id'], health['operation_run_id'])
        self.assertEqual(health['original_host_outcomes']['news.example.test']['saved'], 1)
        self.assertEqual(health['original_host_outcomes']['bbs.example.test']['redirect_skipped'], 1)
        self.assertNotIn('redirect_skipped', health['failure_categories'])
        self.assertNotIn(SECRET, json.dumps(journal))
        self.assertEqual([path.read_bytes() for path in (self.root / 'captures').rglob('*.txt')], [BODY])
