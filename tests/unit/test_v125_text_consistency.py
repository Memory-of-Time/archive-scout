"""v1.2.5 text-recovery regressions against real local HTTP responses."""
from __future__ import annotations

import gzip
import os
import re
import shutil
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from archive_scout.cdx.client import HttpClient, TransientRequestError
from archive_scout.downloads.rate_limit import FixedRateLimiter
from archive_scout.network.transports import (
    CurlBackend, HttpxBackend, IncompleteRepresentationError,
    ResilientTransport, TransportExhaustedError, Urllib3Backend,
)

BODY = b"<html>checksum marker " + b"z" * 1024 + b"</html>"


class Fixture(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        if self.path == '/partial':
            payload = BODY[:400]
            code = 206
            headers = {'Content-Range': f'bytes 0-399/{len(BODY)}'}
        elif self.path == '/gzip-truncated':
            payload = gzip.compress(BODY)[:-9]
            code = 200
            headers = {'Content-Encoding': 'gzip'}
        elif self.path == '/gzip-valid':
            payload = gzip.compress(BODY) + gzip.compress(b' second member')
            code = 200
            headers = {'Content-Encoding': 'gzip'}
        elif self.path == '/error-gzip':
            payload = b'not even compressed'
            code = 503
            headers = {'Content-Encoding': 'gzip', 'Retry-After': '1'}
        else:
            payload = BODY
            code = 200
            headers = {}
        self.send_response(code)
        self.send_header('Content-Length', str(len(payload)))
        for k, v in headers.items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):
        pass


class TransportIntegrity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.http = ThreadingHTTPServer(('127.0.0.1', 0), Fixture)
        cls.server_thread = threading.Thread(target=cls.http.serve_forever, daemon=True)
        cls.server_thread.start()
        cls.base = f'http://127.0.0.1:{cls.http.server_port}'

    @classmethod
    def tearDownClass(cls):
        cls.http.shutdown()
        cls.http.server_close()
        cls.server_thread.join(timeout=2)

    def _backend(self, name):
        if name == 'httpx':
            return HttpxBackend(2, 2, 2, trust_env=False)
        if name == 'urllib3':
            return Urllib3Backend(2, 2, 2, trust_env=False)
        return CurlBackend(2, 2, trust_env=False)

    def test_complete_wire_and_content_evidence_on_each_backend(self):
        names = ('httpx', 'urllib3') + (('curl',) if shutil.which('curl') else ())
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp)/'body'
            for name in names:
                backend = self._backend(name)
                try:
                    for path in ('/partial', '/gzip-truncated'):
                        with self.subTest(backend=name, path=path):
                            with self.assertRaises(IncompleteRepresentationError):
                                backend.download(self.base+path, {}, dest, 100000, threading.Event())
                            self.assertFalse(dest.is_file())
                    with self.subTest(backend=name, path='/gzip-valid'):
                        response = backend.download(self.base+'/gzip-valid', {}, dest, 100000, threading.Event())
                        self.assertEqual(dest.read_bytes(), BODY+b' second member')
                        self.assertEqual(response.bytes_written, len(BODY)+len(b' second member'))
                    with self.subTest(backend=name, path='/error-gzip'):
                        response = backend.download(self.base+'/error-gzip', {}, dest, 100000, threading.Event())
                        self.assertEqual(response.status, 503)
                finally:
                    backend.close()

    def test_bad_content_does_not_cool_other_requests(self):
        transport = ResilientTransport(pool_size=2, connect_timeout=2, read_timeout=2, mode='httpx', trust_env=False)
        try:
            with tempfile.TemporaryDirectory() as tmp:
                dest = Path(tmp)/'body'
                with self.assertRaises(TransportExhaustedError):
                    transport.download(self.base+'/gzip-truncated', {}, dest, 100000, threading.Event())
                self.assertLessEqual(transport.cooldown_until.get('httpx', 0.0), time.monotonic())
                response = transport.download(self.base+'/healthy', {}, dest, 100000, threading.Event())
                self.assertEqual(response.bytes_written, len(BODY))
        finally:
            transport.close()

    def test_pacing_after_cooldown_wait(self):
        transport = ResilientTransport(pool_size=2, connect_timeout=2, read_timeout=2, mode='httpx', trust_env=False)
        pacer = FixedRateLimiter(.05)
        transport.physical_pacer = pacer
        transport.cooldown_until['httpx'] = time.monotonic() + .12
        starts = []
        from archive_scout.network.transports import TransportFileResponse
        def tracked(url, headers, destination, max_bytes, stop_event, **kwargs):
            starts.append(time.monotonic())
            return TransportFileResponse(200, {}, url, destination, len(BODY),
                                         "fixture-digest", BODY[:100], "httpx", 0.0)
        transport.backends['httpx'].download = tracked
        with tempfile.TemporaryDirectory() as tmp:
            try:
                errors = []
                def worker(i):
                    try:
                        transport.download(self.base+'/healthy', {}, Path(tmp)/str(i), 100000,
                                           threading.Event())
                    except Exception as exc:
                        errors.append(exc)
                threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
                for t in threads: t.start()
                for t in threads: t.join(timeout=5)
                self.assertFalse(errors)
                self.assertEqual(len(starts), 4)
                intervals = [b-a for a,b in zip(sorted(starts), sorted(starts)[1:])]
                self.assertTrue(all(gap>=.035 for gap in intervals), intervals)
            finally:
                transport.close()

if __name__ == '__main__':
    unittest.main()


class FormatAndScopeRegressions(unittest.TestCase):
    def test_all_explicit_legacy_suffixes_survive_media_selection(self):
        from archive_scout.classification import SUFFIX_NON_TEXT_IMAGES, SUFFIX_NON_TEXT_MEDIA
        from archive_scout.config import MediaConfig
        from archive_scout.media.extensions import extension_from_url, selected_extensions
        selected = SUFFIX_NON_TEXT_IMAGES | SUFFIX_NON_TEXT_MEDIA
        cfg = MediaConfig(include_images=True, include_videos=True,
                          include_extensions=tuple(sorted(selected))).normalized()
        self.assertEqual(set(selected_extensions(cfg)), selected)
        self.assertEqual(extension_from_url('https://old.example.org/recording.DVR-MS?x=1'), '.dvr-ms')

    def test_wide_charset_and_bom_prefix_are_not_media(self):
        from archive_scout.classification import classify_payload_kind
        from archive_scout.content import decode_bytes
        for charset in ('utf-16le', 'utf-16be', 'utf-32le', 'utf-32be'):
            data = 'This is archived text with a needle'.encode(charset)
            with self.subTest(charset=charset):
                decision = classify_payload_kind(data, f'text/plain; charset={charset}', 'http://example.org/data.txt')
                self.assertEqual(decision.resource_class, 'text')
                self.assertIn('needle', decode_bytes(data, f'text/plain; charset={charset}'))
        # Trailing incomplete multibyte sequence at sample boundary is not an
        # invalid full document; the complete scanner handles final decoding.
        data = b'\xef\xbb\xbf' + (b'a' * (16384-3-1)) + '€'.encode('utf-8') + b' needle'
        self.assertEqual(classify_payload_kind(data, 'text/plain', 'http://example.org/file.txt').resource_class, 'text')
        self.assertIn('needle', decode_bytes(data, 'text/plain'))

    def test_overlapping_owner_does_not_hide_acquisition_gap_or_due_retry(self):
        from archive_scout.config import ProjectConfig
        from archive_scout.cdx.parameters import cdx_query_signature
        from archive_scout.database.connection import open_database
        from archive_scout.database.repositories import get_or_create_target, upsert_captures, record_error
        from archive_scout.downloads.schedule import record_failure, next_due
        from archive_scout.operations import remaining_acquisition_gaps
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = ProjectConfig(root, targets=['example.org/*'], keywords=['needle']).normalized()
            sig = cdx_query_signature(cfg)
            db = open_database(root)
            try:
                owner = get_or_create_target(db, 'example.org/page*')
                member = get_or_create_target(db, 'example.org/*')
                row = ('20090101120000', 'http://example.org/page1', 'text/html', '200', 'A', '18')
                with db:
                    upsert_captures(db, [row], member, sig)
                    upsert_captures(db, [row], owner, sig)
                    cid = int(db.execute('SELECT id FROM captures LIMIT 1').fetchone()[0])
                    db.execute("UPDATE captures SET state='error' WHERE id=?", (cid,))
                    record_error(db, 'download', 'connection', 'fixture retry', capture_id=cid, retryable=True)
                    record_failure(db, cid, 'connection', None, True, 0.1, 1.0)
                    db.execute('UPDATE text_retry_schedule SET next_eligible_at=0 WHERE capture_id=?', (cid,))
                self.assertEqual(remaining_acquisition_gaps(cfg, db), 1)
                self.assertEqual(next_due(db, cfg), 0.0)
            finally:
                db.close()

    def test_full_disk_nested_transport_exception_is_a_storage_blocker(self):
        import errno
        from archive_scout.downloads.downloader import _storage_full
        self.assertTrue(_storage_full(TransportExhaustedError('http://localhost',
                       [('httpx', OSError(errno.ENOSPC, 'disk full'))])))
