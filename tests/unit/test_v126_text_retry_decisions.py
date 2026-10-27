from __future__ import annotations

import base64
import hashlib
import tempfile
import threading
import unittest
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

from archive_scout.cdx.indexer import save_coverage_state
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.config import ProjectConfig, NetworkConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, record_error, upsert_capture
from archive_scout.downloads.downloader import prepare_download_rows, _populate_download_scopes
from archive_scout.downloads.rate_limit import reset_shared_traffic_state_for_tests
from archive_scout.downloads.schedule import (record_failure, next_due, prepare_retry_queue,
    select_due_retry, restore_increased_size_limit)
from archive_scout.operations import _retryable_text_batches, run_project
from archive_scout.ui.dashboard import read_dashboard_counts


class FastStop(threading.Event):
    def wait(self, timeout=None):return self.is_set()


class Fixture(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        self.cfg=ProjectConfig(self.root,targets=['example.org/*'],keywords=['needle'],
            from_date='2008',to_date='2008',cdx_collapses=[],max_file_mb=.1,
            download_delay=0,cdx_delay=0,workers=1,max_attempts=2,
            network=NetworkConfig(backend='httpx',trust_environment=False,
                retry_base_seconds=1,retry_max_seconds=1)).normalized()
        self.db=open_database(self.root)
        self.tid=get_or_create_target(self.db,self.cfg.targets[0])
        save_coverage_state(self.db,self.tid,cdx_query_signature(self.cfg),
            self.cfg.from_date,self.cfg.to_date,None,True,0,None)
        self.db.commit()

    def tearDown(self):
        self.db.close();self.temp.cleanup()

    def seed(self,url='http://example.org/a',target=None,timestamp='20080101000000'):
        upsert_capture(self.db,{'timestamp':timestamp,'original':url,
            'mimetype':'text/html','statuscode':'200','digest':'','length':'4'},
            self.tid if target is None else target,cdx_query_signature(self.cfg))
        cid=self.db.execute('SELECT id FROM captures WHERE original_url=?',(url,)).fetchone()[0]
        self.db.execute("UPDATE captures SET state='error' WHERE id=?",(cid,))
        self.db.execute("INSERT INTO capture_routing VALUES(?,'text','cdx:text/html',1,'failed','now')",(cid,))
        record_error(self.db,'download','connection','earlier connection error',capture_id=cid,retryable=True)
        record_failure(self.db,cid,'connection',None,True,1,1)
        self.db.execute('UPDATE text_retry_schedule SET next_eligible_at=0 WHERE capture_id=?',(cid,))
        self.db.commit();return cid

    def hold(self,cid,category='oversized_response',limit=None):
        with self.db:
            record_error(self.db,'download',category,'latest failure',capture_id=cid,retryable=False)
            record_failure(self.db,cid,category,None,False,1,1,
                max_bytes=self.cfg.max_file_bytes if limit is None else limit)


class LatestDecisionTests(Fixture):
    def test_old_retryable_history_cannot_revive_latest_size_refusal(self):
        cid=self.seed();self.hold(cid)
        self.assertEqual(list(_retryable_text_batches(self.cfg,self.db)),[])
        self.assertIsNone(next_due(self.db,self.cfg))
        total,items=prepare_download_rows(self.db,self.cfg,[],states=('pending','error'))
        self.assertEqual((total,list(items)),(0,[]))
        errors=[tuple(row) for row in self.db.execute('SELECT category,retryable,resolved,ignored FROM errors ORDER BY id')]
        self.assertEqual(errors,[('connection',1,0,0),('oversized_response',0,0,0)])
        counts=read_dashboard_counts(self.root/'archive_scout.sqlite3')
        self.assertEqual((counts['captures'],counts['failed_captures'],counts['accounted_captures']),(1,1,1))

    def test_nonretryable_decision_survives_reopening(self):
        cid=self.seed();self.hold(cid,'missing_capture')
        self.db.close();self.db=open_database(self.root)
        self.assertEqual(list(_retryable_text_batches(self.cfg,self.db)),[])
        self.assertEqual(self.db.execute('SELECT state FROM captures').fetchone()[0],'error')

    def test_stale_due_schedule_does_not_override_latest_decision(self):
        cid=self.seed();self.hold(cid)
        self.db.execute("INSERT INTO text_retry_schedule VALUES(?,0,99,'connection',NULL,'now')",(cid,))
        _populate_download_scopes(self.db,self.cfg)
        self.db.execute('CREATE TEMP TABLE archive_scout_download_queue(id INTEGER PRIMARY KEY)')
        prepare_retry_queue(self.db)
        self.assertIsNone(select_due_retry(self.db,1))
        self.assertIsNone(next_due(self.db,self.cfg))
        self.assertEqual(list(_retryable_text_batches(self.cfg,self.db)),[])

    def test_new_transient_response_can_replace_a_held_decision(self):
        cid=self.seed();self.hold(cid)
        with self.db:
            record_error(self.db,'download','incomplete_representation','new incomplete transfer',capture_id=cid,retryable=True)
            record_failure(self.db,cid,'incomplete_representation',None,True,1,1)
            self.db.execute('UPDATE text_retry_schedule SET next_eligible_at=0')
        self.assertEqual(list(_retryable_text_batches(self.cfg,self.db)),[[cid]])
        self.assertEqual(next_due(self.db,self.cfg),0)

    def test_retry_decision_preserves_resource_classification(self):
        cid=self.seed();before=tuple(self.db.execute('SELECT resource_class,evidence,confident FROM capture_routing').fetchone())
        self.hold(cid)
        self.assertEqual(tuple(self.db.execute('SELECT resource_class,evidence,confident FROM capture_routing').fetchone()),before)

    def test_decision_and_schedule_rollback_together(self):
        cid=self.seed()
        with self.assertRaises(RuntimeError):
            with self.db:
                record_failure(self.db,cid,'oversized_response',None,False,1,1,max_bytes=100)
                raise RuntimeError('simulated failed owner commit')
        self.assertEqual(self.db.execute('SELECT routing FROM capture_routing').fetchone()[0],'failed')
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM text_retry_schedule').fetchone()[0],1)

    def test_schema_and_existing_attempts_remain_intact(self):
        cid=self.seed();self.db.execute('UPDATE captures SET download_attempts=123');self.db.commit()
        self.hold(cid)
        self.assertEqual(self.db.execute('SELECT version FROM schema_info').fetchone()[0],11)
        self.assertEqual(self.db.execute('SELECT download_attempts FROM captures').fetchone()[0],123)


class SizePolicyTests(Fixture):
    def test_same_or_smaller_limit_does_not_requeue(self):
        cid=self.seed();self.hold(cid)
        restore_increased_size_limit(self.db,self.cfg)
        self.assertEqual(self.db.execute('SELECT state FROM captures').fetchone()[0],'error')
        self.hold(cid,limit=self.cfg.max_file_bytes*2)
        restore_increased_size_limit(self.db,self.cfg)
        self.assertEqual(self.db.execute('SELECT state FROM captures').fetchone()[0],'error')

    def test_increased_limit_requeues_only_selected_scope(self):
        cid=self.seed();self.hold(cid)
        other_tid=get_or_create_target(self.db,'other.org/*')
        other=self.seed('http://other.org/a',other_tid);self.hold(other)
        old=self.seed('http://example.org/old',timestamp='20070101000000');self.hold(old)
        bigger=replace(self.cfg,max_file_mb=.2)
        with self.db:restore_increased_size_limit(self.db,bigger)
        states=dict(self.db.execute('SELECT id,state FROM captures'))
        self.assertEqual(states,{cid:'pending',other:'error',old:'error'})
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM errors').fetchone()[0],6)

    def test_later_target_can_reactivate_its_own_old_size_limit(self):
        cid=self.seed();self.hold(cid)
        tid=get_or_create_target(self.db,'other.org/*')
        other=self.seed('http://other.org/a',tid);self.hold(other)
        with self.db:restore_increased_size_limit(self.db,replace(self.cfg,max_file_mb=.2))
        other_cfg=replace(self.cfg,targets=['other.org/*'],max_file_mb=.2)
        with self.db:restore_increased_size_limit(self.db,other_cfg)
        self.assertEqual(dict(self.db.execute('SELECT id,state FROM captures')),{cid:'pending',other:'pending'})

    def test_larger_limit_does_not_revive_a_newer_404(self):
        cid=self.seed();self.hold(cid)
        self.hold(cid,'missing_capture')
        with self.db:restore_increased_size_limit(self.db,replace(self.cfg,max_file_mb=.2))
        self.assertEqual(self.db.execute('SELECT state FROM captures').fetchone()[0],'error')


class ExplicitRetryTests(Fixture):
    def test_explicit_id_can_request_one_held_retry(self):
        cid=self.seed();self.hold(cid)
        cfg=replace(self.cfg,retry_capture_ids=[cid])
        selected=list(_retryable_text_batches(cfg,self.db,include_exhausted_pending=True,respect_selection=True))
        self.assertEqual(selected,[[cid]])
        self.assertEqual(list(_retryable_text_batches(cfg,self.db,respect_selection=True,automatic=True)),[])

    def test_explicit_category_is_scoped_and_does_not_enable_automatic_repetition(self):
        a=self.seed();self.hold(a,'oversized_response')
        b=self.seed('http://example.org/b');self.hold(b,'missing_capture')
        cfg=replace(self.cfg,retry_error_categories=['oversized_response'])
        self.assertEqual(list(_retryable_text_batches(cfg,self.db,include_exhausted_pending=True,respect_selection=True)),[[a]])
        self.assertEqual(list(_retryable_text_batches(cfg,self.db,respect_selection=True,automatic=True)),[])

    def test_manual_acquisition_retry_is_not_repeated_automatically(self):
        cid=self.seed();self.hold(cid)
        cfg=replace(self.cfg,retry_capture_ids=[cid])
        with patch('archive_scout.downloads.downloader.fetch_retain_streamed',side_effect=RuntimeError('response exceeds 104857 bytes')) as fetch:
            result=run_project(cfg,'retry_download_errors',stop_event=FastStop())
        self.assertEqual(fetch.call_count,1)
        self.assertEqual(result.status,'incomplete')
        self.assertEqual(self.db.execute('SELECT download_attempts FROM captures').fetchone()[0],1)


class LiveFixtureTests(Fixture):
    def serve(self,body,short=False):
        calls=[]
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                calls.append(self.path)
                self.send_response(200);self.send_header('Content-Type','text/html')
                self.send_header('Content-Length',str(len(body)));self.end_headers()
                try:self.wfile.write(body[:-5] if short else body)
                except (BrokenPipeError,ConnectionResetError):pass
            def log_message(self,*args):pass
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        self.addCleanup(server.server_close);self.addCleanup(server.shutdown)
        return f'http://127.0.0.1:{server.server_port}/data',calls

    def test_real_size_refusal_then_larger_limit_saves_the_complete_body(self):
        reset_shared_traffic_state_for_tests()
        body=b'<html><body>needle '+b'x'*140000+b'</body></html>'
        cid=self.seed()
        self.db.execute('UPDATE captures SET digest=?',(base64.b32encode(hashlib.sha1(body).digest()).decode(),));self.db.commit()
        url,calls=self.serve(body);events=[]
        with patch('archive_scout.downloads.downloader.replay_url',return_value=url):
            first=run_project(self.cfg,'download_only',stop_event=FastStop(),callback=events.append)
            second=run_project(self.cfg,'download_only',stop_event=FastStop())
            self.assertEqual(len(calls),1)
            self.assertEqual((first.status,second.status),('incomplete','incomplete'))
            self.assertFalse(list((self.root/'captures').rglob('*.txt')))
            result=run_project(replace(self.cfg,max_file_mb=.3),'download_only',stop_event=FastStop())
        self.assertEqual(result.status,'complete')
        self.assertEqual(len(calls),2)
        path=next((self.root/'captures').rglob('*.txt'))
        self.assertEqual(path.read_bytes(),body)
        self.assertTrue(any(e.detail.get('oversized_failures')==1 for e in events))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM errors WHERE resolved=0').fetchone()[0],0)

    def test_repeated_truncation_stays_retryable_but_the_run_is_bounded(self):
        reset_shared_traffic_state_for_tests()
        body=b'<html><body>needle'+b'x'*2000+b'</body></html>'
        cid=self.seed();url,calls=self.serve(body,short=True)
        def due(db,cid,category,status,retryable,base,maximum,**kwargs):
            record_failure(db,cid,category,status,retryable,base,maximum,**kwargs)
            db.execute('UPDATE text_retry_schedule SET next_eligible_at=0 WHERE capture_id=?',(cid,))
        with patch('archive_scout.downloads.downloader.replay_url',return_value=url), \
             patch('archive_scout.downloads.downloader.record_failure',side_effect=due):
            result=run_project(self.cfg,'download_only',stop_event=FastStop())
        self.assertEqual(result.status,'incomplete')
        self.assertEqual(len(calls),3)
        self.assertEqual(self.db.execute('SELECT last_category FROM text_retry_schedule').fetchone()[0],'incomplete_representation')
        self.assertFalse(list((self.root/'captures').rglob('*.txt')))
        self.assertTrue(list(_retryable_text_batches(self.cfg,self.db)))

class MediaExplicitSizeTests(Fixture):
    def test_selected_media_size_failure_can_be_rechecked_without_reopening_policy_errors(self):
        from archive_scout.database.repositories import upsert_media_capture
        from archive_scout.media.indexer import media_query_signature
        from archive_scout.media.downloader import retry_media_errors
        sig=media_query_signature(self.cfg)
        for n,category in enumerate(['oversized_response','robots_blocked']):
            upsert_media_capture(self.db,{'timestamp':'20080101000000',
                'original':f'http://example.org/{n}.jpg','mimetype':'image/jpeg',
                'statuscode':'200','digest':'','length':'5'},None,sig,'image','.jpg')
            cid=self.db.execute('SELECT MAX(id) FROM media_captures').fetchone()[0]
            self.db.execute("UPDATE media_captures SET state='error' WHERE id=?",(cid,))
            from archive_scout.media.recovery import record_media_failure
            record_error(self.db,'media_download','connection','old error',media_capture_id=cid,retryable=True)
            record_error(self.db,'media_download',category,'fixture',media_capture_id=cid,retryable=False)
            record_media_failure(self.db,cid,category,None,False,self.cfg)
        self.db.commit()
        with patch('archive_scout.media.downloader.download_media') as download:
            retry_media_errors(self.cfg,self.db,threading.Event(),media_capture_ids=[1,2])
        self.assertEqual(download.call_args.kwargs['media_capture_ids'],[1])


class DefaultRetrySelectionTests(Fixture):
    def test_unselected_retry_errors_does_not_revive_a_held_capture(self):
        from archive_scout.downloads.retry import retry_error_urls
        cid=self.seed();self.hold(cid)
        with patch('archive_scout.downloads.retry.download_archive') as download:
            retry_error_urls(self.cfg,self.db,0,threading.Event(),None)
        download.assert_not_called()

    def test_selecting_an_old_connection_category_does_not_override_the_new_size_outcome(self):
        from archive_scout.downloads.retry import retry_error_urls
        cid=self.seed();self.hold(cid)
        cfg=replace(self.cfg,retry_error_categories=['connection'])
        self.assertEqual(list(_retryable_text_batches(cfg,self.db,respect_selection=True)),[])
        with patch('archive_scout.downloads.retry.download_archive') as download:
            retry_error_urls(cfg,self.db,0,threading.Event(),None)
        download.assert_not_called()


class PolicyHoldTests(Fixture):
    def test_selected_old_connection_error_does_not_reopen_a_newer_policy_hold(self):
        from archive_scout.downloads.retry import retry_error_urls
        cid=self.seed();self.hold(cid,'robots_blocked')
        cfg=replace(self.cfg,retry_capture_ids=[cid])
        self.assertEqual(list(_retryable_text_batches(cfg,self.db,respect_selection=True)),[])
        with patch('archive_scout.downloads.retry.download_archive') as download:
            retry_error_urls(cfg,self.db,0,threading.Event(),None)
        download.assert_not_called()

    def test_default_media_retry_respects_a_newer_nonretryable_decision(self):
        from archive_scout.database.repositories import upsert_media_capture
        from archive_scout.media.indexer import media_query_signature
        from archive_scout.media.downloader import retry_media_errors
        from archive_scout.media.recovery import record_media_failure
        upsert_media_capture(self.db,{'timestamp':'20080101000000','original':'http://example.org/a.jpg',
            'mimetype':'image/jpeg','statuscode':'200','digest':'','length':'4'},
            None,media_query_signature(self.cfg),'image','.jpg')
        cid=self.db.execute('SELECT id FROM media_captures').fetchone()[0]
        self.db.execute("UPDATE media_captures SET state='error'")
        record_error(self.db,'media_download','connection','old',media_capture_id=cid,retryable=True)
        record_error(self.db,'media_download','invalid_media_representation','new',media_capture_id=cid,retryable=False)
        record_media_failure(self.db,cid,'invalid_media_representation',None,False,self.cfg);self.db.commit()
        with patch('archive_scout.media.downloader.download_media') as download:
            retry_media_errors(self.cfg,self.db,threading.Event())
        download.assert_not_called()
