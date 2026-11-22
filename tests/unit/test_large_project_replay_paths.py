from __future__ import annotations

import base64
import hashlib
import json
import tempfile
import threading
import unittest
from contextlib import closing
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

from archive_scout.cdx.client import HttpClient
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.config import NetworkConfig, ProjectConfig
from archive_scout.content import decode_bytes
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, record_error, start_operation_run, upsert_capture
from archive_scout.downloads import downloader
from archive_scout.downloads.checkpoint import note_recovery_run, receipt_path, write_receipt
from archive_scout.downloads.rate_limit import FixedRateLimiter, reset_shared_traffic_state_for_tests
from archive_scout.downloads.schedule import record_failure
from archive_scout.downloads.validation import AlternateReplayUnverified
from archive_scout.scanning.pending import adopt_orphaned_bodies


BODY = b'<html><body>Recovered archival article containing a searchable word.</body></html>'
STAMP = '20020101000000'
CDX_DIGEST = base64.b32encode(hashlib.sha1(BODY).digest()).decode()
SAVED_HASH = hashlib.sha256(BODY).hexdigest()


class ReplayFixture(unittest.TestCase):
    def setUp(self):
        reset_shared_traffic_state_for_tests()
        self.addCleanup(reset_shared_traffic_state_for_tests)
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.db = self.enterContext(closing(open_database(self.root)))
        self.config = ProjectConfig(self.root, ['example.test/*'], [], from_date='2002', to_date='2002',
                                    download_delay=.04, workers=3, cdx_collapses=[],
                                    network=NetworkConfig(backend='httpx', trust_environment=False)).normalized()
        self.target = get_or_create_target(self.db, self.config.targets[0])
        self.signature = cdx_query_signature(self.config)

    def seed(self, name, digest=CDX_DIGEST):
        url = f'http://example.test/{name}'
        with self.db:
            upsert_capture(self.db, {'timestamp': STAMP, 'original': url, 'mimetype': 'text/html',
                                    'statuscode': '200', 'digest': digest, 'length': len(BODY)},
                           self.target, self.signature)
        return self.db.execute('SELECT * FROM captures WHERE original_url=?', (url,)).fetchone()

    def path(self, row):
        return downloader.capture_path(self.root, row['id'], row['timestamp'], row['original_url'])

    def proof(self, row):
        return {'capture_id': row['id'], 'original_url': row['original_url'], 'timestamp': row['timestamp'],
                'version': 1, 'content_hash': SAVED_HASH, 'bytes_saved': len(BODY), 'http_status': 200,
                'response_content_type': 'text/html', 'final_url': downloader.replay_url(STAMP, row['original_url'])}

    def recover(self):
        return adopt_orphaned_bodies(self.db, self.config, threading.Event(), incremental=True)


class LargeProjectResumeTests(ReplayFixture):
    def test_resume_checks_the_publication_directory_after_legacy_reconciliation(self):
        with self.db:
            self.db.executemany("""INSERT INTO captures(original_url,timestamp,target_id,query_signature,
                state,download_attempts,created_at,updated_at) VALUES(?,?,?,?,'error',2,'fixture','fixture')""",
                ((f'http://example.test/old/{number}', STAMP, self.target, self.signature) for number in range(1200)))
        new = self.seed('interrupted', digest='')
        self.assertEqual(self.recover(), 0)
        path = self.path(new)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.with_name(path.name + '.transfer').write_bytes(BODY)
        write_receipt(path, new, self.proof(new))
        self.assertEqual(receipt_path(path).parent.name, '.replay-recovery')
        with patch.object(downloader, 'capture_path', wraps=downloader.capture_path) as visit:
            self.assertEqual(self.recover(), 1)
            self.assertEqual([call.args[1] for call in visit.call_args_list], [new['id']])
        self.assertEqual(path.read_bytes(), BODY)
        self.assertEqual(self.db.execute('SELECT content_hash FROM capture_payload_evidence').fetchone()[0], SAVED_HASH)
        self.assertFalse(receipt_path(path).exists())
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM captures WHERE state='error'").fetchone()[0], 1200)

    def test_a_run_by_an_older_executable_reopens_legacy_receipt_checks(self):
        with self.db:
            note_recovery_run(self.db, start_operation_run(self.db, 'download_only', '1.3.1'))
        self.assertEqual(self.recover(), 0)
        old = self.seed('legacy-receipt', digest='')
        path = self.path(old)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(BODY)
        path.with_name(path.name + '.validated.json').write_text(json.dumps(self.proof(old)), encoding='utf-8')
        with self.db:
            start_operation_run(self.db, 'download_only', '1.3.1')
            self.db.execute("UPDATE captures SET download_attempts=1 WHERE id=?", (old['id'],))
            note_recovery_run(self.db, start_operation_run(self.db, 'resume', '1.3.1'))
        self.assertEqual(self.recover(), 1)
        self.assertEqual(path.read_bytes(), BODY)
        self.assertFalse(path.with_name(path.name + '.validated.json').exists())

    def test_raising_the_size_limit_rechecks_old_receipts(self):
        row = self.seed('previously-too-large', digest='')
        payload = BODY + b' ' * 110000
        path = self.path(row)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        proof = dict(self.proof(row), bytes_saved=len(payload), content_hash=hashlib.sha256(payload).hexdigest())
        path.with_name(path.name + '.validated.json').write_text(json.dumps(proof), encoding='utf-8')
        with self.db:
            self.db.execute("UPDATE captures SET download_attempts=1 WHERE id=?", (row['id'],))
        small_limit = replace(self.config, max_file_mb=.1)
        self.assertEqual(adopt_orphaned_bodies(self.db, small_limit, threading.Event(), incremental=True), 0)
        self.assertEqual(self.recover(), 1)
        self.assertEqual(path.read_bytes(), payload)


class AlternateReplayTests(ReplayFixture):
    def test_equal_bytes_from_another_capture_timestamp_stay_unverified(self):
        row = dict(self.seed('date-changed'), alternate_replay=1)
        client = HttpClient(FixedRateLimiter(0), 1, 3, 'fixture', threading.Event(),
                            network_backend='httpx', trust_environment=False)
        self.addCleanup(client.close)

        def transfer(url, path, *_args, **_kwargs):
            path.write_bytes(BODY)
            return {'final_url': url.replace(STAMP, '20020201000000'), 'status': 200,
                    'preview': BODY, 'bytes': len(BODY), 'content_hash': SAVED_HASH, 'headers': {}}

        with patch.object(client, 'download_to_path', side_effect=transfer):
            with self.assertRaises(AlternateReplayUnverified):
                downloader.fetch_retain_streamed(row, self.config, client)
        path = self.path(row)
        self.assertFalse(path.exists())
        self.assertFalse(receipt_path(path).exists())
        self.assertFalse(path.with_name(path.name + '.transfer').exists())

    def test_recovery_variants_require_the_indexed_body_and_never_follow_redirects(self):
        rows = [self.seed('valid', digest='sha1:' + CDX_DIGEST), self.seed('mismatch'),
                self.seed('redirect'), self.seed('unknown-digest', digest=''), self.seed('legacy-alternate')]
        with self.db:
            for row in rows:
                cid = row['id']
                self.db.execute("UPDATE captures SET state='error' WHERE id=?", (cid,))
                category = ('alternate_replay_unverified' if row['original_url'].endswith('/legacy-alternate')
                            else 'incomplete_representation')
                record_error(self.db, 'download', category, 'fixture', capture_id=cid, retryable=True)
                record_failure(self.db, cid, category, 200, True, 1, 1)
        requested = []

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                identity = self.headers.get('Accept-Encoding') == 'identity'
                requested.append((self.path, identity))
                if self.path.endswith('/redirect'):
                    self.send_response(302)
                    self.send_header('Location', '/web/20030101000000if_/changed')
                    self.send_header('Content-Length', '0')
                    self.end_headers()
                    return
                payload = BODY.replace(b'archival', b'replayed') if (
                    self.path.endswith('/mismatch') and (identity or 'if_/' in self.path)) else BODY
                self.send_response(200)
                self.send_header('Content-Type', 'text/html')
                self.send_header('Content-Length', str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *_args):
                pass

        server = self.enterContext(ThreadingHTTPServer(('127.0.0.1', 0), Handler))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.shutdown)

        def route(timestamp, original, modifier='id_'):
            name = original.rsplit('/', 1)[1]
            return f'http://127.0.0.1:{server.server_port}/web/{timestamp}{modifier}/{name}'

        self.config.download_external_redirects = True
        with patch.object(downloader, 'replay_url', route):
            downloader.download_archive(self.config, self.db, 0, threading.Event(), None,
                states=('error',), capture_ids=[row['id'] for row in rows], acquire_only=True,
                allow_partial_inventory=True)
            self.assertEqual(set(requested), {(f'/web/{STAMP}id_/valid', True),
                (f'/web/{STAMP}id_/mismatch', True), (f'/web/{STAMP}id_/redirect', True),
                (f'/web/{STAMP}id_/unknown-digest', False), (f'/web/{STAMP}id_/legacy-alternate', True)})
            self.assertEqual([r[0] for r in self.db.execute('SELECT capture_id FROM capture_payload_evidence ORDER BY 1')],
                             [rows[0]['id'], rows[3]['id'], rows[4]['id']])
            self.assertEqual([r[0] for r in self.db.execute('SELECT last_category FROM text_retry_schedule ORDER BY capture_id')],
                             ['identity_replay_unverified', 'identity_replay_unverified'])
            self.assertEqual(self.path(rows[0]).read_bytes(), BODY)
            self.assertFalse(self.path(rows[1]).exists())
            downloader.download_archive(self.config, self.db, 0, threading.Event(), None,
                states=('error',), capture_ids=[rows[1]['id']], acquire_only=True, allow_partial_inventory=True)
            self.assertEqual(requested[-1], (f'/web/{STAMP}if_/mismatch', False))
            self.assertFalse(self.path(rows[1]).exists())
            downloader.download_archive(self.config, self.db, 0, threading.Event(), None,
                states=('error',), capture_ids=[rows[1]['id']], acquire_only=True, allow_partial_inventory=True)
        self.assertEqual(requested[-1], (f'/web/{STAMP}id_/mismatch', False))
        self.assertEqual(self.path(rows[1]).read_bytes(), BODY)


class HistoricCharsetTests(unittest.TestCase):
    def test_undeclared_big5_gb18030_and_shift_jis_are_searchable(self):
        samples = {'big5': '鳳凰衛視報道紐約世界貿易中心的新聞。',
                   'gb18030': '凤凰卫视报道纽约世界贸易中心的新闻。',
                   'shift_jis': '日本語のニュースを見てください。'}
        for encoding, sample in samples.items():
            with self.subTest(encoding=encoding):
                page = f'<html><body>{sample}</body></html>'
                self.assertEqual(decode_bytes(page.encode(encoding), 'text/html'), page)

    def test_declared_encoding_and_utf8_do_not_invoke_detection(self):
        with patch('chardet.detect') as detect:
            self.assertEqual(decode_bytes('日本語'.encode(), 'text/html'), '日本語')
            self.assertEqual(decode_bytes('Résumé'.encode('cp1252'), 'text/html; charset=cp1252'), 'Résumé')
            decode_bytes(b'x\xf1o', 'text/html', detect_missing_charset=False)
            detect.assert_not_called()

    def test_long_ascii_preamble_does_not_hide_legacy_body_encoding(self):
        page = '<html><body>' + ' ' * 20000 + '鳳凰衛視報道紐約世界貿易中心的新聞。' + '</body></html>'
        self.assertEqual(decode_bytes(page.encode('big5'), 'text/html'), page)
