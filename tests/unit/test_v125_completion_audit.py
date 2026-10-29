"""Adversarial report publication, CDX traversal and media inventory checks."""
from __future__ import annotations

import gzip
import json
import os
import sqlite3
import tempfile
import threading
import time
import unittest
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock, patch

from archive_scout.cdx.client import (CDXRows, HttpClient, MalformedCDXResponse,
    cdx_text_fallback_params, parse_cdx_text_response, parse_cdx_rows_payload)
from archive_scout.cdx.indexer import (_request_resume, validate_cdx_scope,
    save_coverage_state, save_state, PendingWindow, IndexPlan, encode_plan, PagedBatch)
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.cdx.parallel import PageFetchResult
from archive_scout.config import ProjectConfig, MediaConfig, NetworkConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import (get_or_create_target,
    get_or_create_media_target, upsert_captures, upsert_media_captures)
from archive_scout.media.indexer import (_request_media_resume, _reuse_completed_main_index,
    _adopt_compatible_media_state, _save_media_state, index_direct_media,
    media_query_signature, media_index_state_signature)
from archive_scout.media.downloader import iter_media_download_rows, download_media
from archive_scout.media.recovery import recover_active_media
from archive_scout.media.downloader import retry_media_errors
from archive_scout.media.reports import generate_media_reports
from archive_scout.media.embedded_pages import index_all_embedded_pages
from archive_scout.reports.snapshot import export_project_snapshot, ReportSnapshotPublishError
from archive_scout.events import Stopped
from archive_scout.downloads.downloader import prepare_download_rows, StorageFullError
from archive_scout.downloads.diagnostics import AcquisitionDiagnostics, build_fingerprint
from archive_scout.network.transports import PayloadDigestMismatch, TransportExhaustedError
from archive_scout.downloads.rate_limit import FixedRateLimiter, SharedHostGate


def config(root, **kwargs):
    return ProjectConfig(root, targets=['example.org/*'], keywords=['needle'],
        from_date='2008', to_date='2008', page_size=100,
        cdx_collapses=[], cdx_delay=0, download_delay=0,
        media=MediaConfig(enabled=True, discover_embedded=False, snapshot_strategy='all'),
        network=NetworkConfig(trust_environment=False), **kwargs).normalized()


def rows(count=100):
    return [('20080101000000', f'http://example.org/{i}.jpg', 'image/jpeg', '200', '', '12')
            for i in range(count)]


class ScopeAndContinuation(unittest.TestCase):
    def test_trailing_url_wildcard_and_www_canonical_spelling(self):
        row=('20080101000000','http://www.nothingtoxic.com:80/media/a','text/html','200','','10')
        for target in ('*.nothingtoxic.com*', '*.nothingtoxic.com/*', 'nothingtoxic.com/*'):
            validate_cdx_scope([row],target,'20080101000000','20081231235959',['statuscode:200'])
        root=tuple([row[0], 'http://nothingtoxic.com/media/a', *row[2:]])
        validate_cdx_scope([root],'*.nothingtoxic.com*','20080101000000','20081231235959',[])
        for bad in ('http://unrelated.org/media/a', 'http://evilnothingtoxic.com/media/a'):
            with self.assertRaises(MalformedCDXResponse):
                validate_cdx_scope([(row[0],bad,*row[2:])],'*.nothingtoxic.com*',
                                   '20080101000000','20081231235959',[])

    def test_unrelated_host_status_and_date_still_blocked(self):
        for row in [('20070101000000','http://example.org/a','text/html','200','','1'),
                    ('20080101000000','http://example.org/a','text/html','404','','1'),
                    ('20080101000000','http://example.org.evil/a','text/html','200','','1')]:
            with self.assertRaises(MalformedCDXResponse):
                validate_cdx_scope([row],'example.org/*','20080101000000','20081231235959',['statuscode:200'])

    def test_legacy_text_api_uses_requested_urlkey_and_preserves_resume_and_spaces(self):
        params=cdx_text_fallback_params([('output','json')])
        wire=b'org,example)/a 20080101000000 text/html 200 D 9 http://example.org/a b\n\nNEXT!\n'
        for data in (wire,gzip.compress(wire)):
            result=parse_cdx_rows_payload(parse_cdx_text_response(data,params=params))
            self.assertEqual(result.rows[0][1],'http://example.org/a b')
            self.assertEqual(result.resume_key,'NEXT!')

    def test_alternate_cursor_advances_text_and_media_without_discarding_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg=config(Path(tmp));client=HttpClient.__new__(HttpClient)
            for module, request in [('archive_scout.cdx.indexer',_request_resume),
                                    ('archive_scout.media.indexer',_request_media_resume)]:
                current=PendingWindow(cfg.from_date,cfg.to_date)
                confirmed=rows()+[('20080102000000','http://example.org/new.jpg','image/jpeg','200','','12')]
                with patch(module+'.request_cdx_rows',side_effect=[CDXRows(rows()),CDXRows(confirmed,'NEXT')]) as get:
                    actual=request(client,cfg,'example.org/*',current) if request is _request_resume else request(cfg,client,'example.org/*',current,['.jpg'])
                self.assertEqual(actual,(confirmed,False,'NEXT'));self.assertEqual(get.call_count,2)
                self.assertIsNone(current.resume_key)  # owner must commit cursor with rows

    def test_terminal_confirmation_compares_interior_membership(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg=config(Path(tmp));client=HttpClient.__new__(HttpClient)
            changed=rows();changed[50]=changed[51]
            with patch('archive_scout.cdx.indexer.request_cdx_rows',side_effect=[CDXRows(rows()),CDXRows(changed)]):
                with self.assertRaises(MalformedCDXResponse):
                    _request_resume(client,cfg,'example.org/*',PendingWindow(cfg.from_date,cfg.to_date))

    def test_full_terminal_page_is_confirmed_with_a_larger_limit(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg=config(Path(tmp));client=HttpClient.__new__(HttpClient)
            with patch('archive_scout.cdx.indexer.request_cdx_rows',side_effect=[CDXRows(rows()),CDXRows(rows())]) as get:
                actual=_request_resume(client,cfg,'example.org/*',PendingWindow(cfg.from_date,cfg.to_date))
            self.assertTrue(actual[1])
            self.assertEqual(dict(get.call_args_list[0].args[2])['limit'],'100')
            self.assertEqual(dict(get.call_args_list[1].args[2])['limit'],'101')

    def test_small_terminal_media_page_does_not_add_a_request(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg=config(Path(tmp));client=HttpClient.__new__(HttpClient)
            with patch('archive_scout.media.indexer.request_cdx_rows',return_value=CDXRows(rows(2))) as get:
                actual=_request_media_resume(cfg,client,'example.org/*',PendingWindow(cfg.from_date,cfg.to_date),['.jpg'])
            self.assertTrue(actual[1]);self.assertEqual(get.call_count,1)


class MediaInventory(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
        self.db=open_database(self.root);self.cfg=config(self.root)
        self.target=get_or_create_target(self.db,'example.org/*')
        self.media_target=get_or_create_media_target(self.db,'example.org/*')
        self.sig=media_query_signature(self.cfg);self.state_sig=media_index_state_signature(self.cfg)

    def tearDown(self):
        self.db.close();self.tmp.cleanup()

    def test_collapsed_text_index_cannot_prove_latest_media(self):
        cfg=replace(self.cfg,cdx_collapses=['digest'],media=replace(self.cfg.media,snapshot_strategy='latest'))
        sig=cdx_query_signature(cfg)
        with self.db:
            upsert_captures(self.db,rows(1),self.target,sig)
            save_state(self.db,self.target,2008,sig,None,True,1,None)
        result=_reuse_completed_main_index(cfg,self.db,'example.org/*',self.media_target,2008,
                                           media_query_signature(cfg),media_index_state_signature(cfg))
        self.assertEqual(result,(False,0,0))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM media_index_state').fetchone()[0],0)

    def test_uncollapsed_text_range_does_not_certify_standalone_media(self):
        sig=cdx_query_signature(self.cfg)
        with self.db:
            upsert_captures(self.db,rows(3),self.target,sig)
            save_coverage_state(self.db,self.target,sig,self.cfg.from_date,self.cfg.to_date,None,True,3,None)
        reused,seen,_=_reuse_completed_main_index(self.cfg,self.db,'example.org/*',self.media_target,0,self.sig,self.state_sig)
        self.assertFalse(reused);self.assertEqual(seen,0)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM media_captures').fetchone()[0],0)

    def test_missing_page_ledger_is_requeued_instead_of_looping_empty_batches(self):
        cfg=replace(self.cfg,network=replace(self.cfg.network,index_strategy='paged'))
        sig=media_query_signature(cfg);state_sig=media_index_state_signature(cfg)
        current=PendingWindow(cfg.from_date,cfg.to_date,strategy='paged',page=1,page_count=1)
        with self.db:
            _save_media_state(self.db,self.media_target,2008,state_sig,encode_plan(IndexPlan([current],0,1)),False,0,None)
        calls=[]
        def batch(_cfg,_client,_target,window,_ext,_stop,consume,ledger,**kwargs):
            calls.append(list(window.retry_pages))
            if len(calls)==1:return PagedBatch([],[],True)
            if len(calls)>2:raise AssertionError('media page ledger did not converge')
            self.assertEqual(window.retry_pages,[0])
            item=PageFetchResult(page=0,rows=rows(1),elapsed=0.01)
            consume(item);window.retry_pages=[]
            return PagedBatch([item],[0],True)
        with patch('archive_scout.media.indexer._request_media_paged_batch',side_effect=batch), \
             patch('archive_scout.media.indexer._request_media_resume',return_value=([],True,None)):
            index_direct_media(cfg,self.db,Mock(),threading.Event(),None,sig,state_sig)
        self.assertEqual(len(calls),2)
        self.assertEqual(self.db.execute('SELECT complete FROM media_index_state WHERE query_signature=?',(state_sig,)).fetchone()[0],1)

    def test_changed_media_filter_keeps_old_cursor_as_evidence_and_saved_files(self):
        old=media_index_state_signature(self.cfg,revision=3)
        cursor=encode_plan(IndexPlan([PendingWindow(self.cfg.from_date,self.cfg.to_date,resume_key='NEXT')],0,1))
        with self.db:
            _save_media_state(self.db,self.media_target,0,old,cursor,False,100,None)
            upsert_media_captures(self.db,[(rows(1)[0],'image','.jpg')],self.media_target,self.sig)
            self.db.execute("UPDATE media_captures SET state='downloaded',path='keep.jpg'")
            _adopt_compatible_media_state(self.db,self.media_target,0,self.cfg,self.sig,self.state_sig)
        state=self.db.execute('SELECT resume_key,complete FROM media_index_state WHERE query_signature=?',(self.state_sig,)).fetchone()
        self.assertIsNone(state)
        self.assertEqual(self.db.execute('SELECT resume_key FROM media_index_state WHERE query_signature=?',(old,)).fetchone()[0],cursor)
        self.assertEqual(tuple(self.db.execute('SELECT state,path FROM media_captures').fetchone()),('downloaded','keep.jpg'))

    def test_uncertain_old_full_terminal_page_requires_reverification(self):
        for size,seen in [(100,100),(1000,150)]:
            cfg=replace(self.cfg,page_size=size)
            old=media_index_state_signature(cfg,revision=3)
            current=media_index_state_signature(cfg)
            with self.db:
                _save_media_state(self.db,self.media_target,2008,old,None,True,seen,None)
                _adopt_compatible_media_state(self.db,self.media_target,2008,cfg,media_query_signature(cfg),current)
            self.assertIsNone(self.db.execute('SELECT 1 FROM media_index_state WHERE query_signature=?',(current,)).fetchone())
            self.assertIsNotNone(self.db.execute('SELECT 1 FROM media_index_state WHERE query_signature=?',(old,)).fetchone())

    def test_even_small_old_extension_filter_inventory_requires_new_discovery(self):
        old=media_index_state_signature(self.cfg,revision=3)
        with self.db:
            _save_media_state(self.db,self.media_target,2008,old,None,True,3,None)
            _adopt_compatible_media_state(self.db,self.media_target,2008,self.cfg,self.sig,self.state_sig)
        self.assertIsNone(self.db.execute('SELECT complete FROM media_index_state WHERE query_signature=?',(self.state_sig,)).fetchone())
        self.assertEqual(self.db.execute('SELECT complete FROM media_index_state WHERE query_signature=?',(old,)).fetchone()[0],1)

    def test_queue_has_stable_size_order_and_no_repeated_history_scan(self):
        items=[(tuple([*row[:5],str(length)]),'image','.jpg') for row,length in zip(rows(5),[0,7,2,7,-1])]
        with self.db:upsert_media_captures(self.db,items,self.media_target,self.sig)
        total,iterator=iter_media_download_rows(self.db,['query_signature=?',"state='pending'"],[self.sig],batch_size=2)
        vm=[0];self.db.set_progress_handler(lambda:vm.__setitem__(0,vm[0]+1),100)
        result=list(iterator);self.db.set_progress_handler(None,0)
        self.assertEqual(total,5);self.assertEqual([r['length'] for r in result],[2,7,7,0,0])
        self.assertEqual(len({r['id'] for r in result}),5)
        plan=' '.join(str(tuple(r)) for r in self.db.execute('''EXPLAIN QUERY PLAN SELECT mc.*
            FROM archive_scout_media_download_queue q CROSS JOIN media_captures mc ON mc.id=q.id
            WHERE (q.priority,q.length,q.id)>(0,0,0) ORDER BY q.priority,q.length,q.id LIMIT 2'''))
        self.assertNotIn('TEMP B-TREE',plan)

    def test_media_disk_full_is_a_hard_blocker(self):
        with self.db:
            _save_media_state(self.db,self.media_target,2008,self.state_sig,None,True,1,None)
            upsert_media_captures(self.db,[(rows(1)[0],'image','.jpg')],self.media_target,self.sig)
        with patch('archive_scout.media.downloader.HttpClient',return_value=Mock()), \
             patch('archive_scout.media.downloader.fetch_media',side_effect=OSError(28,'disk full')):
            with self.assertRaises(StorageFullError):download_media(self.cfg,self.db,threading.Event())
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM errors').fetchone()[0],0)

    def test_automatic_media_retries_respect_nonpersistent_setting(self):
        cfg=replace(self.cfg,network=replace(self.cfg.network,persistent_retries=False))
        with self.db:_save_media_state(self.db,self.media_target,2008,self.state_sig,None,True,0,None)
        with patch('archive_scout.media.downloader.download_media') as download:
            recover_active_media(cfg,self.db,threading.Event())
        download.assert_not_called()

    def test_explicit_media_404_recheck_does_not_reopen_policy_errors(self):
        from archive_scout.database.repositories import record_error
        with self.db:
            upsert_media_captures(self.db,[(r,'image','.jpg') for r in rows(2)],self.media_target,self.sig)
            self.db.execute("UPDATE media_captures SET state='error'")
            ids=[r[0] for r in self.db.execute('SELECT id FROM media_captures ORDER BY id')]
            record_error(self.db,'media_download','missing_capture','HTTP 404',media_capture_id=ids[0],retryable=False)
            record_error(self.db,'media_download','robots_blocked','blocked',media_capture_id=ids[1],retryable=False)
        with patch('archive_scout.media.downloader.download_media') as download:
            retry_media_errors(self.cfg,self.db,threading.Event(),media_capture_ids=ids)
        self.assertEqual(download.call_count,1)
        self.assertEqual(download.call_args.kwargs['media_capture_ids'],[ids[0]])

    def test_report_separates_current_completeness_from_historical_errors(self):
        with self.db:
            _save_media_state(self.db,self.media_target,2008,self.state_sig,None,True,0,None)
            upsert_media_captures(self.db,[(rows(1)[0],'image','.jpg')],self.media_target,'old-signature')
            self.db.execute("UPDATE media_captures SET state='error'")
        report=generate_media_reports(self.cfg,self.db)
        evidence=json.loads(report['media_completeness'].read_text())
        self.assertTrue(evidence['complete']);self.assertEqual(evidence['states'],{})
        with self.db:upsert_media_captures(self.db,[(rows(1)[0],'image','.jpg')],self.media_target,self.sig)
        report=generate_media_reports(self.cfg,self.db)
        self.assertFalse(json.loads(report['media_completeness'].read_text())['complete'])


class ReportPublication(unittest.TestCase):
    def export(self, replace_fn):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup)
        root=Path(tmp.name);cfg=config(root);db=open_database(root);db.close()
        with patch('archive_scout.reports.snapshot.os.replace',side_effect=replace_fn), \
             patch('archive_scout.reports.snapshot.time.sleep'):
            return cfg,export_project_snapshot(cfg)

    def test_transient_rename_failure_retries_existing_render_and_succeeds(self):
        real=os.replace;attempts=[]
        def rename(src,dest):
            if Path(src).name.startswith('.partial-'):
                attempts.append(src)
                if len(attempts)<3:raise PermissionError(13,'sharing violation')
            return real(src,dest)
        cfg,result=self.export(rename)
        self.assertEqual(len(attempts),3)
        self.assertTrue(result['manifest'].is_file())
        self.assertFalse(any((cfg.output_dir/'reports/manual').glob('.partial-*')))

    def test_permanent_rename_failure_preserves_complete_render(self):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup)
        cfg=config(Path(tmp.name));db=open_database(cfg.output_dir);db.close();real=os.replace
        def rename(src,dest):
            if Path(src).name.startswith('.partial-'):raise PermissionError(13,'denied')
            return real(src,dest)
        with patch('archive_scout.reports.snapshot.os.replace',side_effect=rename), \
             patch('archive_scout.reports.snapshot.time.sleep'):
            with self.assertRaises(ReportSnapshotPublishError) as caught:export_project_snapshot(cfg)
        preserved=caught.exception.staging_path
        self.assertTrue((preserved/'manifest.json').is_file())
        self.assertFalse(caught.exception.final_path.exists())
        self.assertIn(str(preserved),str(caught.exception))
        self.assertFalse(json.loads((preserved/'manifest.json').read_text())['complete'])
        self.assertTrue(export_project_snapshot(cfg)['manifest'].is_file())  # locks released


class DiagnosticsAndSelectedRetries(unittest.TestCase):
    def test_page_pool_cancellation_releases_gate_waiter_without_user_stop(self):
        client=HttpClient.__new__(HttpClient);user_stop=threading.Event();local_stop=threading.Event()
        client.stop_event=user_stop;gate=SharedHostGate();gate.pause_for_rate_limit(120)
        entered=threading.Event();outcomes=[]
        def worker():
            with client.cancellation_scope(local_stop):
                entered.set()
                try:gate.acquire_request(client.stop_event)
                except Stopped:outcomes.append('cancelled')
        thread=threading.Thread(target=worker);thread.start();entered.wait(1);local_stop.set();thread.join(2)
        self.assertFalse(thread.is_alive());self.assertEqual(outcomes,['cancelled'])
        self.assertFalse(user_stop.is_set());self.assertIs(client.stop_event,user_stop)

    def test_rejected_http_response_is_visible_without_fake_timing_samples(self):
        with tempfile.TemporaryDirectory() as tmp:
            diag=AcquisitionDiagnostics(tmp,None)
            diag.response({'status':200,'backend_elapsed_seconds':2.0})
            client=HttpClient.__new__(HttpClient);client.response_callback=diag.response
            client.host_gate=SharedHostGate();client.coordinated_connection_recovery=False
            error=PayloadDigestMismatch('bad candidate')
            error.transfer_diagnostic={'response_status':200}
            from archive_scout.cdx.client import TransientRequestError
            with self.assertRaises(TransientRequestError):
                client._finish_failed_transport_turn(TransportExhaustedError('url',[('httpx',error)]),
                    'url',client.host_gate.acquire_request(threading.Event()),splittable=False)
            result=diag.snapshot()
            self.assertEqual(result['response_counts'],{'200':2})
            self.assertEqual(result['validation_rejected_response_counts'],{'200':1})
            self.assertEqual(result['avg_backend_elapsed_seconds'],2.0)

    def test_frozen_build_fingerprint_uses_packaged_code_when_source_is_absent(self):
        build_fingerprint.cache_clear()
        try:
            with patch('archive_scout.downloads.diagnostics.Path.is_file',return_value=False):
                self.assertNotIn('unfingerprinted',build_fingerprint())
        finally:build_fingerprint.cache_clear()

    def test_selected_retry_does_not_change_unrelated_redirect_errors(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg=config(Path(tmp));db=open_database(cfg.output_dir)
            try:
                target=get_or_create_target(db,'example.org/*');sig=cdx_query_signature(cfg)
                with db:
                    upsert_captures(db,rows(2),target,sig)
                    db.execute("UPDATE captures SET mimetype='text/html',original_url=original_url||'.html',state='error'")
                    from archive_scout.database.repositories import record_error
                    ids=[r[0] for r in db.execute('SELECT id FROM captures ORDER BY id')]
                    record_error(db,'download','external_redirect_blocked','old',capture_id=ids[1],retryable=False)
                total,iterator=prepare_download_rows(db,cfg,[],states=('error',),capture_ids=[ids[0]])
                self.assertEqual(total,1);self.assertEqual([r['id'] for r in iterator],[ids[0]])
                self.assertEqual(db.execute('SELECT state FROM captures WHERE id=?',(ids[1],)).fetchone()[0],'error')
            finally:db.close()


class RealMediaRecovery(unittest.TestCase):
    def test_media_500_is_scheduled_once_then_recovers_after_database_reopen(self):
        body=b'\xff\xd8\xff' + b'archived image evidence' * 100
        requests={}
        class Handler(BaseHTTPRequestHandler):
            protocol_version='HTTP/1.1'
            def do_GET(self):
                requests[self.path]=requests.get(self.path,0)+1
                fail=self.path=='/0.jpg' and requests[self.path]==1
                self.send_response(500 if fail else 200)
                self.send_header('Content-Type','image/jpeg')
                self.send_header('Content-Length',str(0 if fail else len(body)));self.end_headers()
                if not fail:self.wfile.write(body)
            def log_message(self,*args):pass
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);cfg=config(root,workers=2);db=open_database(root)
                target=get_or_create_media_target(db,'example.org/*')
                sig=media_query_signature(cfg);state_sig=media_index_state_signature(cfg)
                with db:
                    _save_media_state(db,target,2008,state_sig,None,True,2,None)
                    upsert_media_captures(db,[(row,'image','.jpg') for row in rows(2)],target,sig)
                events=[];stop=threading.Event()
                def replay(row):return f'http://127.0.0.1:{server.server_port}/'+row['original_url'].rsplit('/',1)[1]
                with patch('archive_scout.media.downloader.media_replay_url',side_effect=replay):
                    download_media(cfg,db,stop,events.append)
                    self.assertFalse(stop.is_set())
                    self.assertEqual(requests['/0.jpg'],1)
                    self.assertEqual(db.execute("SELECT COUNT(*) FROM media_captures WHERE state='downloaded'").fetchone()[0],1)
                    self.assertEqual(db.execute('SELECT COUNT(*) FROM media_retry_schedule').fetchone()[0],1)
                    self.assertTrue(any(e.detail.get('new_saves')==1 and e.detail.get('errors')==1 for e in events))
                    # Persisted deadlines survive closing and reopening. Move
                    # this fixture's deadline forward without a real 5s sleep.
                    with db:db.execute('UPDATE media_retry_schedule SET next_eligible_at=0')
                    db.close();db=open_database(root)
                    recover_active_media(cfg,db,stop,events.append)
                self.assertEqual(requests,{'/0.jpg':2,'/1.jpg':1})
                self.assertEqual(db.execute("SELECT COUNT(*) FROM media_captures WHERE state='downloaded'").fetchone()[0],2)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM media_retry_schedule').fetchone()[0],0)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM errors WHERE resolved=0').fetchone()[0],0)
                for row in db.execute('SELECT path,content_hash,bytes_saved FROM media_captures'):
                    self.assertEqual(Path(row['path']).read_bytes(),body)
                    self.assertEqual(row['content_hash'],hashlib.sha256(body).hexdigest())
                    self.assertEqual(row['bytes_saved'],len(body))
                db.close()
        finally:
            server.shutdown();server.server_close();thread.join(2)


class EmbeddedPageCheckpoints(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
        self.cfg=config(self.root);self.db=open_database(self.root)
        self.cfg.media=replace(self.cfg.media,discover_embedded=True)
        self.sig=media_query_signature(self.cfg);self.url='http://example.org/clip.jpg'
        with self.db:self.db.execute('''INSERT INTO media_discovery_queue
            (query_signature,original_url,kind_hint,created_at,updated_at)
            VALUES(?,?,'image','now','now')''',(self.sig,self.url))

    def tearDown(self):self.db.close();self.tmp.cleanup()

    def page(self,timestamp):return [(timestamp,self.url,'image/jpeg','200','','10')]

    def test_interrupted_all_snapshots_lookup_resumes_committed_page_after_reopen(self):
        from archive_scout.cdx.client import TransientRequestError
        calls=[]
        def fetch(cfg,client,url,window,extensions,**kwargs):
            calls.append(window.resume_key)
            if window.resume_key is None:return self.page('20080101000000'),False,'NEXT'
            raise TransientRequestError('HTTP 500',status=500)
        with patch('archive_scout.media.indexer._request_media_resume',side_effect=fetch):
            index_all_embedded_pages(self.cfg,self.db,object(),threading.Event(),None,self.sig)
        self.assertEqual(calls,[None,'NEXT'])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM media_captures').fetchone()[0],1)
        self.assertEqual(tuple(self.db.execute('SELECT state,result_count FROM media_discovery_queue').fetchone()),('error',1))
        self.assertEqual(json.loads(self.db.execute("SELECT value FROM project_meta WHERE key LIKE 'media_embed_cursor_v1:%'").fetchone()[0])['resume_key'],'NEXT')
        self.db.close();self.db=open_database(self.root)
        def finish(cfg,client,url,window,extensions,**kwargs):
            self.assertEqual(window.resume_key,'NEXT')
            return self.page('20080102000000'),True,None
        with patch('archive_scout.media.indexer._request_media_resume',side_effect=finish):
            index_all_embedded_pages(self.cfg,self.db,object(),threading.Event(),None,self.sig)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM media_captures').fetchone()[0],2)
        self.assertEqual(tuple(self.db.execute('SELECT state,result_count FROM media_discovery_queue').fetchone()),('indexed',2))
        self.assertIsNone(self.db.execute("SELECT 1 FROM project_meta WHERE key LIKE 'media_embed_cursor_v1:%'").fetchone())

    def test_page_write_failure_cannot_advance_cursor_or_lookup_state(self):
        with patch('archive_scout.media.indexer._request_media_resume',
                   return_value=(self.page('20080101000000'),False,'NEXT')), \
             patch('archive_scout.media.embedded_pages.upsert_media_captures',
                   side_effect=sqlite3.OperationalError('injected page write failure')):
            with self.assertRaises(sqlite3.OperationalError):
                index_all_embedded_pages(self.cfg,self.db,object(),threading.Event(),None,self.sig)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM media_captures').fetchone()[0],0)
        self.assertEqual(self.db.execute('SELECT state FROM media_discovery_queue').fetchone()[0],'pending')
        self.assertIsNone(self.db.execute("SELECT 1 FROM project_meta WHERE key LIKE 'media_embed_cursor_v1:%'").fetchone())

    def test_old_all_snapshot_lookup_is_reverified_once_without_erasing_attempt_history(self):
        from archive_scout.coverage import require_embedded_coverage, IndexCoveragePending
        with self.db:self.db.execute("UPDATE media_discovery_queue SET state='indexed',result_count=1,lookup_attempts=50")
        with self.assertRaises(IndexCoveragePending):require_embedded_coverage(self.db,self.cfg,self.sig)
        with patch('archive_scout.media.indexer._request_media_resume',
                   return_value=(self.page('20080101000000'),True,None)) as request:
            index_all_embedded_pages(self.cfg,self.db,object(),threading.Event(),None,self.sig)
            self.assertEqual(request.call_count,1)
        self.assertEqual(tuple(self.db.execute('SELECT state,lookup_attempts FROM media_discovery_queue').fetchone()),('indexed',51))
        require_embedded_coverage(self.db,self.cfg,self.sig)
        with patch('archive_scout.media.indexer._request_media_resume') as request:
            index_all_embedded_pages(self.cfg,self.db,object(),threading.Event(),None,self.sig)
            request.assert_not_called()
