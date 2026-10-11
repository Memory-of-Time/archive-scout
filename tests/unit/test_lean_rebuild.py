"""Targeted evidence/correctness boundaries for the lean v1.0.5 engine."""
from __future__ import annotations

import hashlib
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from archive_scout.cdx.client import CDXRows, MalformedCDXResponse, TransientRequestError, parse_cdx_rows_payload
from archive_scout.cdx.indexer import index_archive
from archive_scout.cdx.parameters import parse_cdx
from archive_scout.config import NetworkConfig, ProjectConfig
from archive_scout.content import is_text_candidate, looks_textual_bytes
from archive_scout.database.connection import open_database
from archive_scout.downloads.downloader import fetch_parse_scan
from archive_scout.downloads.rate_limit import SharedHostGate
from archive_scout.scanning.jobs import ScanJob
from archive_scout.scanning.rescanner import _analyze_saved_document
from archive_scout.scanning.scoring import analyze_content, prepare_analysis_fields
from archive_scout.utils import atomic_write_bytes


class LeanRebuildTests(unittest.TestCase):
    def test_default_clocks_and_parallel_index_settings(self):
        config = ProjectConfig(Path('.'), text_collapse_scope="year", targets=['example.com/*'], keywords=['needle']).normalized()
        self.assertEqual(config.cdx_delay, 2.5)
        self.assertEqual(config.download_delay, 0.125)
        self.assertEqual(config.network.cdx_workers, 10)
        self.assertEqual(config.workers, 10)

    def test_failed_index_waits_do_not_escalate(self):
        from archive_scout.cdx.indexer import transient_backoff
        from archive_scout.media.indexer import _wait_seconds
        config = ProjectConfig(Path('.'), text_collapse_scope="year", targets=['example.com/*'], keywords=['needle']).normalized()
        self.assertEqual(transient_backoff(config, 1), transient_backoff(config, 10_000))
        self.assertEqual(_wait_seconds(config, 1), _wait_seconds(config, 10_000))

    def test_both_json_cdx_parsers_reject_missing_cells(self):
        broken = [['timestamp', 'original'], ['20010101000000']]
        with self.assertRaises(MalformedCDXResponse):
            parse_cdx_rows_payload(broken)
        with self.assertRaises(RuntimeError):
            parse_cdx(broken)

    def test_text_mime_overrides_media_extension_but_not_binary_signature(self):
        self.assertTrue(is_text_candidate('http://example.org/page.jpg', 'text/html'))
        self.assertFalse(looks_textual_bytes(b'\x89PNG\r\n\x1a\n' + b'\x00' * 80, 'text/plain'))
        self.assertTrue(looks_textual_bytes(b'BMW owners document', 'text/plain'))

    def test_full_source_match_past_historical_cutoff(self):
        raw = 'A' * 500_050 + ' rare_archive_term '
        job = ScanJob.create(1, 'Test', ['rare_archive_term'])
        fields, normalized = prepare_analysis_fields('http://a.invalid', '', '', raw, [])
        self.assertIn('rare_archive_term', fields['source'])
        output = analyze_content('http://a.invalid', '', '', raw, [], job.patterns, job.prefilter, fields, normalized)
        self.assertGreater(output['score'], 0)

    def test_binary_atomic_write_preserves_prior_body_after_publish_error(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'snapshot.txt'
            path.write_bytes(b'previous complete body')
            with patch('archive_scout.utils.os.replace', side_effect=OSError('disk failure')):
                with self.assertRaises(OSError):
                    atomic_write_bytes(path, b'new body')
            self.assertEqual(path.read_bytes(), b'previous complete body')
            self.assertEqual(list(path.parent.glob('*.tmp')), [])

    def test_replay_preserves_encoding_and_scan_hashes_exact_file_bytes(self):
        class Client:
            def get(self, *_args):
                return {'data':b'<html><title>Title</title>caf\xe9 needle</html>',
                        'headers':{'content-type':'text/html; charset=windows-1252'},
                        'status':200,'final_url':'https://web.archive.org/web/20010101000000id_/http://example.org/a'}
        with tempfile.TemporaryDirectory() as temp:
            config = ProjectConfig(Path(temp), text_collapse_scope="year", targets=['example.org/*'], keywords=['needle']).normalized()
            row = {'id':1,'timestamp':'20010101000000','original_url':'http://example.org/a','mimetype':'text/html'}
            job = ScanJob.create(1,'Test',['needle'])
            result = fetch_parse_scan(row,config,[job],Client())
            body = result['path'].read_bytes()
            self.assertIn(b'caf\xe9', body)
            self.assertEqual(result['content_hash'],hashlib.sha256(body).hexdigest())
            scanned = _analyze_saved_document({
                'id':1,'capture_id':1,'path':str(result['path']),
                'original_url':row['original_url'], 'content_hash':result['content_hash'],
                'title':result['title'],'body_text':result['visible'],
                'links_json':'[]', 'normalized_hash':result['normalized_hash'],
            }, [job])
            self.assertEqual(scanned['kind'],'success')
            self.assertFalse(scanned['document_changed'])
            self.assertEqual(scanned['content_hash'],hashlib.sha256(body).hexdigest())

    def test_retry_recovers_without_manual_resume_after_transient_index_errors(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            config=ProjectConfig(root, text_collapse_scope="year", targets=['example.org/*'],keywords=[],from_date='20010101',to_date='20010101',cdx_delay=0,
                network=NetworkConfig(index_strategy='resume',connection_failure_pause_threshold=2)).normalized()
            db=open_database(root)
            offline=TransientRequestError('temporary offline',connection_failed=True,splittable=False)
            responses=[offline,offline,offline,CDXRows([])]
            try:
                with patch('archive_scout.cdx.client.HttpClient.get_cdx_any', side_effect=responses) as requests, patch.object(threading.Event,'wait',return_value=False):
                    index_archive(config,db,threading.Event())
                self.assertEqual(requests.call_count,4)
                self.assertEqual(int(db.execute('select complete from index_state').fetchone()[0]),1)
            finally:
                db.close()

    def test_throttle_fallback_does_not_grow_over_repeated_incidents(self):
        gate=SharedHostGate(base_pause=5,max_pause=5)
        waits=[gate.pause_for_rate_limit(None,'HTTP 429') for _ in range(3)]
        self.assertTrue(all(4.9<=value<=5.1 for value in waits),waits)


if __name__=='__main__':
    unittest.main()
