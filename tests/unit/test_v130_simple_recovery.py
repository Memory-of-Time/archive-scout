from __future__ import annotations

import sqlite3
import tempfile
import threading
import unittest
from collections import Counter
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import httpx

from archive_scout.cdx.client import CDXRows, HttpClient, PermanentRequestError, TransientRequestError
from archive_scout.cdx.indexer import PendingWindow, _client_for_config, _recover_with_numbered_pages, decode_plan, index_archive
from archive_scout.config import MediaConfig, NetworkConfig, ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, upsert_capture, record_error
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.downloads.schedule import record_failure
from archive_scout.events import Stopped
from archive_scout.media.indexer import index_direct_media, media_query_signature, media_index_state_signature
from archive_scout.operations import recover_transient_text_errors


def config(root):
    return ProjectConfig(Path(root), targets=['*.example.org/*'], keywords=['needle'],
        from_date='2000', to_date='2006', cdx_delay=0, cdx_collapses=['urlkey'],
        cdx_filters=['statuscode:200', 'original:.*needle.*'], text_collapse_scope='range',
        max_attempts=4, network=NetworkConfig(index_strategy='auto', trust_environment=False,
            retry_base_seconds=1, retry_max_seconds=1),
        media=MediaConfig(enabled=True, include_extensions=['jpg'], discover_embedded=False)).normalized()


def row(page=0):
    return ('20050101000000', f'http://example.org/needle/{page}.jpg', 'image/jpeg', '200', '', '10')


class PlanningTests(unittest.TestCase):
    def test_stalled_first_page_uses_parallel_pages_and_retries_only_failed_page(self):
        with tempfile.TemporaryDirectory() as temp:
            cfg = config(temp); db = open_database(cfg.output_dir)
            calls = []; lock = threading.Lock(); barrier = threading.Barrier(3)
            page_calls = Counter()

            def fetch(_client, endpoints, params, **kwargs):
                p = dict(params)
                with lock:
                    calls.append(list(params))
                if 'showNumPages' in p:
                    return [['pages'], [3]]
                if 'page' not in p:
                    if int(p['limit']) == 5000:
                        return CDXRows([row(3)])
                    raise TransientRequestError('HTTP 504', status=504)
                page = int(p['page'])
                with lock:
                    page_calls[page] += 1
                    attempt = page_calls[page]
                if attempt == 1:
                    barrier.wait(timeout=5)
                if page == 1 and attempt == 1:
                    raise TransientRequestError('read timeout', read_timed_out=True)
                return CDXRows([row(page)])

            try:
                with patch.object(HttpClient, 'get_cdx_any', fetch):
                    self.assertFalse(index_archive(cfg, db, threading.Event()))
                self.assertEqual(page_calls, {0: 1, 1: 2, 2: 1})
                self.assertEqual(db.execute('SELECT COUNT(*) FROM captures').fetchone()[0], 4)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM index_pages').fetchone()[0], 3)
                self.assertEqual(db.execute('SELECT complete FROM index_coverage').fetchone()[0], 1)
                self.assertEqual(sum('showResumeKey' in dict(p) for p in calls), 2)
                for p in calls:
                    self.assertEqual([v for k,v in p if k == 'filter'], cfg.cdx_filters)
                    self.assertEqual([v for k,v in p if k == 'collapse'], cfg.cdx_collapses)
                    self.assertEqual((dict(p)['from'],dict(p)['to']), (cfg.from_date,cfg.to_date))
                with patch.object(HttpClient, 'get_cdx_any', side_effect=AssertionError('repeated completed coverage')):
                    self.assertFalse(index_archive(cfg, db, threading.Event()))
            finally:
                db.close()

    def test_committed_cursor_explicit_strategy_and_service_pauses_are_never_replanned(self):
        with tempfile.TemporaryDirectory() as temp:
            cfg = config(temp); db = open_database(cfg.output_dir)
            try:
                cases = [
                    (cfg, PendingWindow(cfg.from_date,cfg.to_date,'SAVED',strategy='resume'), TransientRequestError('HTTP 504',status=504)),
                    (replace(cfg,network=replace(cfg.network,index_strategy='resume')), PendingWindow(cfg.from_date,cfg.to_date,strategy='resume'), TransientRequestError('timeout',timed_out=True)),
                    (cfg, PendingWindow(cfg.from_date,cfg.to_date,strategy='resume',pagination_supported=False), TransientRequestError('HTTP 504',status=504)),
                    (cfg, PendingWindow(cfg.from_date,cfg.to_date,strategy='resume'), TransientRequestError('connection',connection_failed=True)),
                    (cfg, PendingWindow(cfg.from_date,cfg.to_date,strategy='resume'), TransientRequestError('HTTP 429',status=429)),
                    (cfg, PendingWindow(cfg.from_date,cfg.to_date,strategy='resume'), TransientRequestError('HTTP 503',status=503)),
                    (cfg, PendingWindow(cfg.from_date,cfg.to_date,strategy='resume'), TransientRequestError('HTTP 502',status=502)),
                    (cfg, PendingWindow(cfg.from_date,cfg.to_date,strategy='paged',page_count=3,page=1), TransientRequestError('timeout',timed_out=True)),
                ]
                for scoped,current,error in cases:
                    with self.subTest(current=current,error=error):
                        self.assertFalse(_recover_with_numbered_pages(scoped, db, current, error,
                            lambda: self.fail('unexpected checkpoint change')))
                for cause in (httpx.PoolTimeout('pool busy'), httpx.ConnectTimeout('connection failed')):
                    error=TransientRequestError('wrapped timeout',timed_out=True)
                    error.__cause__=cause
                    current=PendingWindow(cfg.from_date,cfg.to_date,strategy='resume')
                    self.assertFalse(_recover_with_numbered_pages(cfg,db,current,error,
                        lambda:self.fail('transport failure changed query plan')))
            finally:
                db.close()

    def test_replan_transaction_failure_restores_window(self):
        with tempfile.TemporaryDirectory() as temp:
            cfg=config(temp);db=open_database(cfg.output_dir)
            current=PendingWindow(cfg.from_date,cfg.to_date,strategy='resume',next_eligible_at=123)
            def fail():
                db.execute("INSERT INTO project_meta(key,value) VALUES('injected','x')")
                raise sqlite3.OperationalError('injected checkpoint failure')
            try:
                with self.assertRaisesRegex(sqlite3.OperationalError,'injected checkpoint'):
                    _recover_with_numbered_pages(cfg,db,current,TransientRequestError('HTTP 504',status=504),fail)
                self.assertEqual((current.strategy,current.page_blocks,current.next_eligible_at),('resume',0,123))
                self.assertIsNone(db.execute("SELECT value FROM project_meta WHERE key='injected'").fetchone())
            finally:db.close()

    def test_numbered_plan_survives_stop_reopen_without_retrying_initial_resume(self):
        with tempfile.TemporaryDirectory() as temp:
            cfg=config(temp);db=open_database(cfg.output_dir);stop=threading.Event()
            def callback(event):
                if 'saved a numbered-page plan' in event.message:stop.set()
            try:
                with patch.object(HttpClient,'get_cdx_any',side_effect=TransientRequestError('HTTP 504',status=504)):
                    with self.assertRaises(Stopped):index_archive(cfg,db,stop,callback)
                state=db.execute('SELECT plan_json,complete,seen FROM index_coverage').fetchone()
                self.assertEqual((state['complete'],state['seen']),(0,0))
                self.assertEqual(decode_plan(state['plan_json'],[]).pending[0].strategy,'paged')
                db.close();db=open_database(cfg.output_dir);calls=[]
                def finish(_client,endpoints,params,**kwargs):
                    p=dict(params);calls.append(p)
                    if 'showResumeKey' in p:
                        self.assertEqual(int(p['limit']), 5000)
                    return [['pages'],[1]] if 'showNumPages' in p else CDXRows([row()])
                with patch.object(HttpClient,'get_cdx_any',finish):
                    self.assertFalse(index_archive(cfg,db,threading.Event()))
                self.assertEqual(len(calls),3)
                self.assertIn('showNumPages', calls[0])
                self.assertIn('showResumeKey', calls[-1])
            finally:db.close()

    def test_partial_response_can_replan_but_scope_corruption_cannot(self):
        from archive_scout.cdx.client import MalformedCDXResponse
        with tempfile.TemporaryDirectory() as temp:
            cfg=config(temp);db=open_database(cfg.output_dir)
            try:
                for error,expected in [(MalformedCDXResponse('wrong host',splittable=False),False),
                                       (TransientRequestError('read timeout',read_timed_out=True),True)]:
                    current=PendingWindow(cfg.from_date,cfg.to_date,strategy='resume')
                    self.assertEqual(_recover_with_numbered_pages(cfg,db,current,error,lambda:None),expected)
                error=MalformedCDXResponse('representation failed',splittable=True)
                error.__cause__=httpx.RemoteProtocolError('incomplete chunked read')
                current=PendingWindow(cfg.from_date,cfg.to_date,strategy='resume')
                self.assertTrue(_recover_with_numbered_pages(cfg,db,current,error,lambda:None))
            finally:db.close()

    def test_unsupported_paging_falls_back_once_and_does_not_oscillate(self):
        with tempfile.TemporaryDirectory() as temp:
            cfg=config(temp);db=open_database(cfg.output_dir);calls=[]
            def fetch(_client,endpoints,params,**kwargs):
                p=dict(params);calls.append(p)
                if 'showNumPages' in p:
                    raise PermanentRequestError('HTTP 400: paging unavailable',status=400)
                if len(calls)<4:raise TransientRequestError('HTTP 504',status=504)
                return CDXRows([row()])
            try:
                with patch.object(HttpClient,'get_cdx_any',fetch),patch('archive_scout.cdx.indexer.transient_backoff',return_value=0):
                    self.assertFalse(index_archive(cfg,db,threading.Event()))
                self.assertEqual(sum('showNumPages' in p for p in calls),1)
                self.assertEqual(len(calls),4)
            finally:db.close()

    def test_direct_media_recovers_initial_timeout_without_changing_route(self):
        with tempfile.TemporaryDirectory() as temp:
            cfg=config(temp);db=open_database(cfg.output_dir);calls=[]
            stop=threading.Event();client=_client_for_config(cfg,stop,None)
            def fetch(_client,endpoints,params,**kwargs):
                p=dict(params);calls.append(p)
                if 'showNumPages' in p:return [['pages'],[1]]
                if 'page' in p:return CDXRows([row()])
                raise TransientRequestError('read timeout',read_timed_out=True)
            try:
                with patch.object(HttpClient,'get_cdx_any',fetch):
                    self.assertFalse(index_direct_media(cfg,db,client,stop,None,
                        media_query_signature(cfg),media_index_state_signature(cfg)))
                self.assertEqual(len(calls),3)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM captures').fetchone()[0],0)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM media_captures').fetchone()[0],1)
                self.assertEqual(db.execute('SELECT complete FROM media_index_state').fetchone()[0],1)
            finally:client.close();db.close()


class TailTests(unittest.TestCase):
    def test_elapsed_tail_limit_cannot_be_reset_by_network_category(self):
        from archive_scout.retry_budget import NoProgressBudget
        with tempfile.TemporaryDirectory() as temp:
            cfg=replace(config(temp),max_attempts=100);db=open_database(cfg.output_dir)
            target=get_or_create_target(db,cfg.targets[0])
            upsert_capture(db,dict(zip(('timestamp','original','mimetype','statuscode','digest','length'),row())),target,cdx_query_signature(cfg))
            cid=db.execute('SELECT id FROM captures').fetchone()[0]
            db.execute("UPDATE captures SET state='error'")
            record_error(db,'download','read_timeout','stalled',capture_id=cid,retryable=True)
            record_failure(db,cid,'read_timeout',None,True,1,1)
            db.execute('UPDATE text_retry_schedule SET next_eligible_at=0');db.commit()
            clock=[0.0];calls=[]
            def download(*args,**kwargs):calls.append(1);clock[0]+=40
            def budget(scoped):return NoProgressBudget(scoped,clock=lambda:clock[0])
            try:
                with patch('archive_scout.operations.NoProgressBudget',budget),patch('archive_scout.operations.download_archive',download):
                    self.assertEqual(recover_transient_text_errors(cfg,db,0,[],threading.Event(),None,
                        continuous=True,acquire_only=True),1)
                self.assertEqual(len(calls),2)
                self.assertEqual(db.execute('SELECT state FROM captures').fetchone()[0],'error')
                self.assertEqual(db.execute('SELECT COUNT(*) FROM text_retry_schedule').fetchone()[0],1)
            finally:db.close()

    def test_long_saved_cooldown_yields_to_reports_without_early_request(self):
        import time
        with tempfile.TemporaryDirectory() as temp:
            cfg=config(temp);db=open_database(cfg.output_dir)
            target=get_or_create_target(db,cfg.targets[0])
            upsert_capture(db,dict(zip(('timestamp','original','mimetype','statuscode','digest','length'),row())),target,cdx_query_signature(cfg))
            cid=db.execute('SELECT id FROM captures').fetchone()[0]
            db.execute("UPDATE captures SET state='error'")
            record_error(db,'download','read_timeout','stalled',capture_id=cid,retryable=True)
            record_failure(db,cid,'read_timeout',None,True,1,1)
            due=time.time()+3600
            db.execute('UPDATE text_retry_schedule SET next_eligible_at=?',(due,));db.commit()
            try:
                with patch('archive_scout.operations.download_archive') as download:
                    self.assertEqual(recover_transient_text_errors(cfg,db,0,[],threading.Event(),None,
                        continuous=True,acquire_only=True),1)
                download.assert_not_called()
                self.assertEqual(db.execute('SELECT next_eligible_at FROM text_retry_schedule').fetchone()[0],due)
                self.assertEqual(db.execute('SELECT state FROM captures').fetchone()[0],'error')
            finally:db.close()

    def test_temporary_failure_recovers_automatically_within_tail_budget(self):
        with tempfile.TemporaryDirectory() as temp:
            cfg=config(temp);db=open_database(cfg.output_dir)
            target=get_or_create_target(db,cfg.targets[0])
            upsert_capture(db,dict(zip(('timestamp','original','mimetype','statuscode','digest','length'),row())),target,cdx_query_signature(cfg))
            cid=db.execute('SELECT id FROM captures').fetchone()[0]
            db.execute("UPDATE captures SET state='error'")
            record_error(db,'download','read_timeout','stalled',capture_id=cid,retryable=True)
            record_failure(db,cid,'read_timeout',None,True,1,1)
            db.execute('UPDATE text_retry_schedule SET next_eligible_at=0');db.commit();calls=[]
            def download(*args,**kwargs):
                calls.append(kwargs['capture_ids'])
                if len(calls)==2:
                    db.execute("UPDATE captures SET state='downloaded'")
                    db.execute('DELETE FROM text_retry_schedule');db.commit()
            try:
                with patch('archive_scout.operations.download_archive',download):
                    self.assertEqual(recover_transient_text_errors(cfg,db,0,[],threading.Event(),None,
                        continuous=True,acquire_only=True),0)
                self.assertEqual(calls,[[cid],[cid]])
            finally:db.close()


if __name__=='__main__':unittest.main()
