"""v1.2.2: streamed scanning, saved-document backfill, CDX response modes."""
from __future__ import annotations
import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from archive_scout.config import ProjectConfig
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, upsert_capture
from archive_scout.downloads.rate_limit import FixedRateLimiter
from archive_scout.cdx.client import HttpClient
from archive_scout.network.transports import TransportResponse
from archive_scout.operations import run_project, prepare_scan_jobs
from archive_scout.scanning.rescanner import rescan_keyword_sets


class FixtureClient:
    def __init__(self, *args, **kwargs): self.gets = 0
    def get(self, url, *_args, **_kwargs):
        self.gets += 1
        return {'data': b'<html><title>Page</title><body>needle is here</body></html>',
                'status': 200, 'headers': {'content-type':'text/html'}, 'final_url':url}
    def close(self): pass


class InlineScanRestoration(unittest.TestCase):
    def project(self, root, keyword='needle'):
        return ProjectConfig(root, targets=['example.org/*'], keywords=[keyword],
                             from_date='20010101000000',to_date='20011231235959',
                             cdx_delay=0,download_delay=0,workers=2).normalized()

    def add_capture(self, root, config, suffix='page.html'):
        db=open_database(root)
        with db:
            target=get_or_create_target(db,'example.org/*')
            upsert_capture(db, {'original':'http://example.org/'+suffix,
                'timestamp':'20010101000000','mimetype':'text/html',
                'statuscode':'200','digest':'D1','length':'100'},target,cdx_query_signature(config))
        db.close()

    def test_download_and_scan_are_one_pass_and_report_has_match(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg=self.project(Path(tmp))
            self.add_capture(Path(tmp),cfg)
            with patch('archive_scout.downloads.downloader.HttpClient',FixtureClient):
                paths=run_project(cfg,'download')
            db=open_database(Path(tmp))
            try:
                self.assertEqual(db.execute("SELECT state FROM captures").fetchone()[0], 'downloaded')
                self.assertGreater(db.execute("SELECT COUNT(*) FROM document_matches").fetchone()[0],0)
                self.assertEqual(db.execute("SELECT COUNT(*) FROM captures WHERE state='downloaded_unscanned'").fetchone()[0],0)
                self.assertIn('needle', paths['matches_ranked'].read_text().lower())
                self.assertIn('Scan status:',paths['summary'].read_text())
            finally: db.close()

    def test_new_keywords_rescan_saved_documents_without_network(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);original=self.project(root,'notpresent')
            self.add_capture(root,original)
            with patch('archive_scout.downloads.downloader.HttpClient',FixtureClient):
                run_project(original,'download')
            revised=self.project(root,'needle')
            with patch('archive_scout.downloads.downloader.HttpClient',return_value=FixtureClient()) as client:
                paths=run_project(revised,'download')
                client.assert_not_called()  # Existing documents are scanned purely locally
            db=open_database(root)
            try:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM documents').fetchone()[0],1)
                self.assertGreater(db.execute('SELECT COUNT(*) FROM document_matches').fetchone()[0],0)
                self.assertIn('needle',paths['matches_ranked'].read_text().lower())
            finally: db.close()

    def test_compatible_reopen_reuses_scan_without_rescanning(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);cfg=self.project(root)
            self.add_capture(root,cfg)
            with patch('archive_scout.downloads.downloader.HttpClient',FixtureClient):
                run_project(cfg,'download')
            db=open_database(root)
            try:
                with patch('archive_scout.operations.rescan_keyword_sets',side_effect=AssertionError('unnecessary scan')):
                    jobs=prepare_scan_jobs(db,cfg,'download')
                self.assertEqual(jobs[0].scan_run_id, 1)
            finally:db.close()


class CDXUTF16Representation(unittest.TestCase):
    def test_utf16_json_through_text_endpoint_needs_no_second_request(self):
        payload=[['urlkey','timestamp','original','mimetype','statuscode','digest','length'],
                 ['org,example)/','20010101000000','http://example.org/','text/html','200','D','12']]
        class FakeTransport:
            def __init__(self):self.count=0
            def request(self,url,headers,max_bytes,stop):
                self.count+=1
                return TransportResponse(200,{'content-type':'application/json'},url,
                    json.dumps(payload).encode('utf-16'),'fake',0.01)
            def close(self):pass
        transport=FakeTransport()
        client=HttpClient(FixedRateLimiter(0),1,5,'test',threading.Event(),transport=transport)
        try:
            result=client.get_cdx_rows_any(('https://web.archive.org/cdx/search/cdx',),[('output','json')],prefer_text=True)
            self.assertEqual(len(result.rows),1)
            self.assertEqual(transport.count,1)
        finally:client.close()


if __name__=='__main__': unittest.main()

class IndexOutageRecovery(unittest.TestCase):
    def test_temporary_page_count_503_does_not_replace_paged_strategy(self):
        from archive_scout.config import NetworkConfig
        from archive_scout.cdx.indexer import index_archive
        from archive_scout.cdx.client import TransientRequestError
        with tempfile.TemporaryDirectory() as tmp:
            cfg=ProjectConfig(Path(tmp), targets=['example.org/*'], keywords=['needle'],
                from_date='20010101000000', to_date='20010101235959', cdx_delay=0,
                text_collapse_scope='year',
                network=NetworkConfig(index_strategy='paged', page_blocks=2)).normalized()
            db=open_database(Path(tmp))
            requested=[]
            def request(_self,_urls,params,max_bytes=64*1024*1024,prefer_text=False):
                args=dict(params);requested.append(args)
                if 'showNumPages' in args:
                    if sum('showNumPages' in x for x in requested)<=5:
                        raise TransientRequestError('HTTP 503', status=503, splittable=True)
                    return 1
                self.assertIn('page',args)
                return [['timestamp','original','mimetype','statuscode','digest','length'],
                        ['20010101000000','http://example.org/page.html','text/html','200','D','12']]
            try:
                with patch('archive_scout.cdx.client.HttpClient.get_cdx_any',new=request), \
                     patch('archive_scout.cdx.indexer.transient_backoff',return_value=0):
                    index_archive(cfg,db,threading.Event())
                self.assertEqual(sum('showNumPages' in x for x in requested),6)
                self.assertEqual(sum('page' in x for x in requested),1)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM captures').fetchone()[0],1)
            finally:db.close()

    def test_cdx_defers_persistent_503_per_request_without_disabling_auto_retries(self):
        from archive_scout.cdx.indexer import _client_for_config, transient_backoff
        with tempfile.TemporaryDirectory() as tmp:
            cfg=ProjectConfig(Path(tmp),targets=['example.org/*'], keywords=['needle'],cdx_delay=0).normalized()
            client=_client_for_config(cfg,threading.Event(),None)
            try:
                self.assertEqual(client.rate_limit_attempts,3)
                self.assertEqual(cfg.rate_limit_attempts,0)  # user config unchanged
                self.assertEqual(transient_backoff(cfg,3),transient_backoff(cfg,1))
                self.assertLessEqual(transient_backoff(cfg,100),60)
            finally:client.close()

class CDXHostWidePause(unittest.TestCase):
    def test_rate_limit_deferral_does_not_cycle_all_cdx_endpoints(self):
        from archive_scout.cdx.client import RateLimitDeferred
        client=HttpClient(FixedRateLimiter(0),1,5,'test',threading.Event())
        attempts=[]
        try:
            def refused(*args,**kwargs):
                attempts.append(args[0])
                raise RateLimitDeferred('HTTP 503',status=503,waited=5)
            with patch.object(client,'get',side_effect=refused):
                with self.assertRaises(RateLimitDeferred):
                    client.get_cdx_rows_any((
                        'https://web.archive.org/cdx/search/cdx',
                        'https://web.archive.org/web/timemap/cdx',
                        'https://web.archive.org/web/timemap/json',
                    ),[('output','json')],prefer_text=True)
            self.assertEqual(len(attempts),1)
        finally: client.close()
