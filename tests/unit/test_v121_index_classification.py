"""v1.2.1 capture-routing and durable scan-stage regression checks."""
from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.classification import classify_indexed_resource, classify_payload_kind
from archive_scout.config import MediaConfig, ProjectConfig
from archive_scout.database.classification import classify_indexed_captures, classification_counts
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, upsert_captures, get_or_create_keyword_set, start_scan_run
from archive_scout.downloads.downloader import capture_path, download_archive
from archive_scout.scanning.jobs import ScanJob


class RoutingRegressionTests(unittest.TestCase):
    def test_conflicts_remain_eligible_for_payload_validation(self):
        self.assertEqual(classify_indexed_resource('http://x.test/a.jpg', 'text/html').resource_class, 'unknown')
        self.assertEqual(classify_payload_kind(b'<html>needle</html>', 'image/jpeg', 'http://x.test/a.jpg').resource_class, 'text')
        self.assertEqual(classify_payload_kind(b'BMW owners', 'text/plain', 'http://x.test/a').resource_class, 'text')
        self.assertEqual(classify_payload_kind(b'\x89PNG\r\n\x1a\n'+b'\0'*48, 'text/plain', 'http://x.test/a.html').resource_class, 'image')
        self.assertEqual(classify_payload_kind(b'<svg xmlns="http://www.w3.org/2000/svg"/>', 'image/svg+xml', 'http://x.test/a.svg').resource_class, 'image')

    def _fixture(self, root: Path, mime: str='text/html', media: bool=False):
        config = ProjectConfig(root, targets=['example.net/*'], keywords=['needle'], cdx_delay=0, download_delay=0,
                               media=MediaConfig(enabled=media)).normalized()
        db = open_database(root)
        target = get_or_create_target(db, 'example.net/*')
        signature = cdx_query_signature(config)
        with db:
            upsert_captures(db, [('20010101000000','http://example.net/page.html',mime,'200','digest',120)], target, signature)
        keyword_id = get_or_create_keyword_set(db, 'Test', ['needle'])
        scan_id = start_scan_run(db, keyword_id, 'Test run', 1, 'download', {})
        db.commit()
        return config, db, scan_id

    def test_mislabeled_png_is_deferred_not_an_open_error_or_text_file(self):
        class FakeClient:
            def __init__(self,*args,**kwargs): pass
            def get(self,*args,**kwargs):
                return {'data': b'\x89PNG\r\n\x1a\n'+b'\0'*48,
                        'headers': {'content-type':'text/plain'},'status':200,
                        'final_url':'https://web.archive.org/web/20010101000000id_/http://example.net/page.html'}
            def close(self): pass
        with tempfile.TemporaryDirectory() as folder:
            config, db, scan_id=self._fixture(Path(folder),media=True)
            try:
                with patch('archive_scout.downloads.downloader.HttpClient',FakeClient):
                    download_archive(config, db, scan_id,threading.Event(),None,
                                     scan_jobs=[ScanJob.create(scan_id,'Test',['needle'])])
                row=db.execute('SELECT state FROM captures').fetchone()
                self.assertEqual(row['state'],'skipped')
                routing=db.execute('SELECT resource_class,routing FROM capture_routing').fetchone()
                self.assertEqual(tuple(routing),('image','deferred_to_media'))
                media=db.execute('SELECT source_type,media_kind,state FROM media_captures').fetchone()
                self.assertEqual(tuple(media),('text_replay_deferred','image','pending'))
                self.assertEqual(db.execute('SELECT COUNT(*) FROM errors WHERE resolved=0').fetchone()[0],0)
                self.assertFalse(list((Path(folder)/'captures').rglob('*.txt')))
            finally:
                db.close()

    def test_index_classification_is_idempotent_and_skip_has_reason(self):
        with tempfile.TemporaryDirectory() as folder:
            config, db, scan_id=self._fixture(Path(folder),mime='image/png')
            try:
                signature=cdx_query_signature(config)
                self.assertEqual(classify_indexed_captures(db,signature),1)
                self.assertEqual(classify_indexed_captures(db,signature),0)
                from archive_scout.downloads.downloader import prepare_download_rows
                count, _=prepare_download_rows(db,config,ScanJob.create(scan_id,'Test',['needle']).patterns)
                self.assertEqual(count,1)
                self.assertEqual(db.execute('SELECT state FROM captures').fetchone()[0],'pending') # conflicting text extension, needs payload proof
                counts=classification_counts(db,signature)
                self.assertEqual(counts['unknown'],1)
            finally:
                db.close()

    def test_index_report_preserves_classification_and_routing_fields(self):
        with tempfile.TemporaryDirectory() as folder:
            config, db, scan_id=self._fixture(Path(folder))
            try:
                from archive_scout.reports.text import generate_index_reports
                self.assertEqual(classify_indexed_captures(db,cdx_query_signature(config)),1)
                paths=generate_index_reports(config,db)
                inventory=paths['all_indexed_urls'].read_text(encoding='utf-8')
                self.assertIn('text', inventory)
                self.assertIn('mime+extension:', inventory)
                self.assertIn('pending',inventory)
                self.assertIn('http://example.net/page.html',inventory)
            finally:
                db.close()

    def test_previously_saved_unscanned_body_recovers_without_network(self):
        with tempfile.TemporaryDirectory() as folder:
            config, db, scan_id=self._fixture(Path(folder))
            try:
                id_,timestamp,original=db.execute('SELECT id,timestamp,original_url FROM captures').fetchone()
                path=capture_path(config.output_dir,id_,timestamp,original)
                path.parent.mkdir(parents=True,exist_ok=True)
                path.write_bytes(b'<html>archived needle source</html>')
                with db:
                    db.execute("UPDATE captures SET state='downloaded_unscanned' WHERE id=?",(id_,))
                # No eligible network work. Scan must still drain the durable backlog.
                with patch('archive_scout.downloads.downloader.HttpClient',side_effect=AssertionError('network must not open')):
                    download_archive(config,db,scan_id,threading.Event(),None,
                                     scan_jobs=[ScanJob.create(scan_id,'Test',['needle'])])
                self.assertEqual(db.execute('SELECT state FROM captures').fetchone()[0],'downloaded')
                self.assertEqual(db.execute('SELECT COUNT(*) FROM documents').fetchone()[0],1)
                self.assertGreater(db.execute('SELECT COUNT(*) FROM document_matches').fetchone()[0],0)
            finally:
                db.close()

if __name__=='__main__': unittest.main()
