"""Completeness-first CDX representation and failure-only Range recovery tests."""
from __future__ import annotations

import base64
import gzip
import hashlib
import json
import tempfile
import threading
import unittest
from pathlib import Path

import httpx

from archive_scout.cdx.client import (
    MalformedCDXResponse, decode_cdx_encoded_body, parse_cdx_text_rows,
    parse_json_response, parse_cdx_rows_payload,
)
from archive_scout.cdx.indexer import PendingWindow, _reconcile_paged_completion
from archive_scout.network.transports import (
    HttpxBackend, _partial_metadata_path, _resume_partial_httpx,
)


class CDXEncodedResponses(unittest.TestCase):
    def setUp(self):
        self.header = ["urlkey", "timestamp", "original", "mimetype", "statuscode", "digest", "length"]
        self.data = [self.header, ["com,example)/", "20060102030405", "http://example.com/a.jpg", "image/jpeg", "200", "ABC", "100"]]

    def test_gzip_wrapped_native_json_is_decoded_before_parse(self):
        raw = gzip.compress(json.dumps(self.data).encode())
        parsed = parse_json_response(raw, 'test')
        self.assertEqual(parsed, self.data)
        rows = parse_cdx_text_rows(raw, 'test')
        self.assertEqual(len(rows.rows), 1)
        self.assertEqual(rows.rows[0][1], 'http://example.com/a.jpg')

    def test_gzip_wrapped_text_cdx_is_decoded_before_parse(self):
        raw = gzip.compress(b'com,example)/ 20060102030405 image/jpeg 200 ABC 100 http://example.com/a.jpg\n')
        parsed = parse_cdx_text_rows(raw, 'test', [('fl', ','.join(self.header))])
        self.assertEqual(len(parsed.rows), 1)
        self.assertEqual(parsed.rows[0][0], '20060102030405')

    def test_utf16_json_and_normal_buffer_are_accepted(self):
        utf16 = json.dumps(self.data).encode('utf-16')
        self.assertEqual(parse_json_response(utf16), self.data)
        plain = bytearray(b'normal text')
        self.assertIs(decode_cdx_encoded_body(plain), plain)

    def test_malformed_and_oversized_encoded_bodies_do_not_appear_empty(self):
        with self.assertRaises(MalformedCDXResponse):
            parse_cdx_text_rows(b'\x1f\x8bmissing payload')
        with self.assertRaises(MalformedCDXResponse):
            decode_cdx_encoded_body(gzip.compress(b'long output'), max_bytes=3)


class PagedEvidence(unittest.TestCase):
    def test_uncommitted_page_forbids_window_completion(self):
        window = PendingWindow('20010101000000', '20011231235959', page_count=4, page=4)
        self.assertFalse(_reconcile_paged_completion(window, {0, 1, 3}))
        self.assertEqual(window.retry_pages, [2])
        self.assertTrue(_reconcile_paged_completion(window, {0, 1, 2, 3}))

    def test_unrelated_or_stale_page_number_is_not_completion_evidence(self):
        window = PendingWindow('20010101000000', '20011231235959', page_count=3)
        self.assertFalse(_reconcile_paged_completion(window, {0, 1, 999}))
        self.assertEqual(window.retry_pages, [2])


class BrokenAfterPrefix(httpx.SyncByteStream):
    def __iter__(self):
        yield b'P' * (64 * 1024)
        raise httpx.RemoteProtocolError('peer closed connection during body')
    def close(self):
        pass


class PartialReplayTests(unittest.TestCase):
    def setUp(self):
        self.prefix = b'P' * (64 * 1024)
        self.payload = self.prefix + b' plus complete remainder'
        self.uri = 'https://web.archive.org/web/20060102030405if_/http://example.com/file.wmv'
        self.etag = '"original-unique-entity"'
        self.backend = HttpxBackend(1, 1, 1)
        self.backend.client.close()

    def tearDown(self):
        self.backend.close()

    def test_partial_response_recovered_with_aligned_206_and_matching_digest(self):
        requests = []
        def handle(request):
            requests.append(request)
            if len(requests) == 1:
                return httpx.Response(200, headers={'content-length':str(len(self.payload)), 'etag':self.etag}, stream=BrokenAfterPrefix())
            self.assertEqual(request.headers['range'], f'bytes={len(self.prefix)}-')
            self.assertEqual(request.headers['if-range'], self.etag)
            part = self.payload[len(self.prefix):]
            return httpx.Response(206, headers={
                'content-range':f'bytes {len(self.prefix)}-{len(self.payload)-1}/{len(self.payload)}',
                'content-length':str(len(part)), 'etag':self.etag,
            }, content=part)
        self.backend.client = httpx.Client(transport=httpx.MockTransport(handle))
        digest = base64.b32encode(hashlib.sha1(self.payload).digest()).decode('ascii').rstrip('=')
        with tempfile.TemporaryDirectory() as tmp:
            destination = Path(tmp) / 'file.wmv.part'
            with self.assertRaises(httpx.RemoteProtocolError):
                self.backend.download(self.uri, {}, destination, len(self.payload)+100, threading.Event(), retain_partial=True, expected_digest=digest)
            self.assertEqual(destination.read_bytes(), self.prefix)
            self.assertTrue(_partial_metadata_path(destination).is_file())
            result = self.backend.download(self.uri, {}, destination, len(self.payload)+100, threading.Event(), retain_partial=True, expected_digest=digest)
            self.assertEqual(result.bytes_written, len(self.payload))
            self.assertEqual(destination.read_bytes(), self.payload)
            self.assertFalse(_partial_metadata_path(destination).exists())
            self.assertEqual(len(requests), 2)

    def test_wrong_range_is_rejected_and_restarted_as_new_full_body(self):
        requests=[]
        def handle(request):
            requests.append(request)
            if len(requests)==1:
                return httpx.Response(206,headers={'content-range':'bytes 0-4/50'}, content=b'WRONG')
            return httpx.Response(200,content=self.payload)
        self.backend.client = httpx.Client(transport=httpx.MockTransport(handle))
        with tempfile.TemporaryDirectory() as tmp:
            dest=Path(tmp)/'file.part'
            prefix=b'example prefix'
            dest.write_bytes(prefix)
            _partial_metadata_path(dest).write_text(json.dumps({
                'url':self.uri, 'final_url':self.uri, 'size':len(prefix), 'total':len(self.payload),
                'etag':self.etag, 'sha256':hashlib.sha256(prefix).hexdigest()}))
            digest=base64.b32encode(hashlib.sha1(self.payload).digest()).decode('ascii').rstrip('=')
            result=self.backend.download(self.uri, {}, dest, len(self.payload)+100, threading.Event(), retain_partial=True,expected_digest=digest)
            self.assertEqual(result.bytes_written,len(self.payload))
            self.assertEqual(dest.read_bytes(),self.payload)
            self.assertEqual(len(requests),2)

    def test_changed_prefix_fails_validation_before_sending_range(self):
        calls=[]
        self.backend.client=httpx.Client(transport=httpx.MockTransport(lambda req: (calls.append(req),httpx.Response(200,content=self.payload))[1]))
        with tempfile.TemporaryDirectory() as tmp:
            dest=Path(tmp)/'file.part'
            dest.write_bytes(b'corrupt')
            _partial_metadata_path(dest).write_text(json.dumps({
                'url':self.uri,'final_url':self.uri,'size':7,'total':len(self.payload),
                'etag':self.etag,'sha256':'wrong-hash'}))
            result=self.backend.download(self.uri,{},dest,len(self.payload)+100,threading.Event(),retain_partial=True)
            self.assertEqual(result.bytes_written,len(self.payload))
            self.assertEqual(len(calls),1)
            self.assertNotIn('range',calls[0].headers)


if __name__ == '__main__':
    unittest.main()

class ExhaustedInventoryTests(unittest.TestCase):
    def test_explicit_text_retry_includes_lifetime_exhausted_pending_but_automatic_does_not(self):
        from archive_scout.config import ProjectConfig
        from archive_scout.database.connection import open_database
        from archive_scout.database.repositories import get_or_create_target, upsert_capture
        from archive_scout.cdx.parameters import cdx_query_signature
        from archive_scout.operations import _retryable_text_batches
        with tempfile.TemporaryDirectory() as tmp:
            cfg = ProjectConfig(output_dir=Path(tmp), targets=['example.com/*'],
                                keywords=[], max_attempts=3).normalized()
            db = open_database(cfg.output_dir)
            try:
                target=get_or_create_target(db,'example.com/*')
                upsert_capture(db, {'original':'http://example.com/missing.html',
                    'timestamp':'20010101000000','mimetype':'text/html','statuscode':'200',
                    'digest':'XYZ','length':'100'}, target, cdx_query_signature(cfg))
                db.execute("UPDATE captures SET state='pending',download_attempts=3")
                db.commit()
                self.assertEqual(list(_retryable_text_batches(cfg,db)),[])
                batches=list(_retryable_text_batches(cfg,db,include_exhausted_pending=True))
                self.assertEqual(len(batches),1)
                self.assertEqual(len(batches[0]),1)
            finally:db.close()

    def test_attempt_exhaustion_manifest_is_nonempty_and_no_state_is_changed(self):
        from archive_scout.config import ProjectConfig
        from archive_scout.database.connection import open_database
        from archive_scout.database.repositories import get_or_create_target, upsert_capture
        from archive_scout.cdx.parameters import cdx_query_signature
        from archive_scout.reports.text import generate_index_reports
        with tempfile.TemporaryDirectory() as tmp:
            cfg=ProjectConfig(output_dir=Path(tmp),targets=['example.com/*'],
                              keywords=[],max_attempts=3).normalized()
            db=open_database(cfg.output_dir)
            try:
                t=get_or_create_target(db,'example.com/*')
                upsert_capture(db,{'original':'http://example.com/over.html',
                    'timestamp':'20010101000000','mimetype':'text/html','statuscode':'200',
                    'digest':'X','length':'20'},t,cdx_query_signature(cfg))
                db.execute("UPDATE captures SET state='pending',download_attempts=3")
                db.commit()
                path=generate_index_reports(cfg,db)['unfinished_over_attempts']
                self.assertIn('over.html',path.read_text())
                self.assertEqual(db.execute('SELECT download_attempts FROM captures').fetchone()[0],3)
            finally:db.close()
