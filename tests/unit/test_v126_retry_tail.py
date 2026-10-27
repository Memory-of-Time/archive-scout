from __future__ import annotations

import hashlib
import sqlite3
import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock, patch

from archive_scout.config import ProjectConfig, MediaConfig, NetworkConfig
from archive_scout.cdx.client import HttpClient, CDXRows, MalformedCDXResponse, TransientRequestError, RateLimitDeferred
from archive_scout.cdx.indexer import index_archive, decode_plan
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import (get_or_create_target, upsert_capture,
    upsert_media_capture, record_error)
from archive_scout.downloads.rate_limit import SharedHostGate
from archive_scout.events import Stopped
from archive_scout.media.indexer import (media_query_signature, media_index_state_signature,
    _complete_embedded_media, index_direct_media)
from archive_scout.media.downloader import download_media
from archive_scout.media.recovery import recover_active_media
from archive_scout.operations import run_project, recover_transient_text_errors
from archive_scout.retry_budget import NoProgressBudget
from archive_scout.ui.dashboard import read_dashboard_counts, format_capture_accounting


class FastStop(threading.Event):
    def wait(self, timeout=None):
        return self.is_set()


class Fixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.cfg = ProjectConfig(self.root, targets=['example.org/*'], keywords=['needle'],
            from_date='2008', to_date='2008', text_collapse_scope='range', cdx_collapses=[],
            cdx_delay=0, download_delay=0, workers=1, max_attempts=2,
            network=NetworkConfig(index_strategy='resume', trust_environment=False,
                                  retry_base_seconds=1, retry_max_seconds=1),
            media=MediaConfig(enabled=True, discover_embedded=True,
                              snapshot_strategy='earliest')).normalized()
        self.db = open_database(self.root)
        self.sig = media_query_signature(self.cfg)
        self.events = []

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()

    def queue(self, url, state='pending'):
        self.db.execute('''INSERT INTO media_discovery_queue(original_url,query_signature,
            state,created_at,updated_at) VALUES(?,?,?,'now','now')''', (url,self.sig,state))
        self.db.commit()

    def media(self, url='http://example.org/a.jpg', timestamp='20080101000000', signature=None):
        upsert_media_capture(self.db, {'timestamp':timestamp,'original':url,
            'mimetype':'image/jpeg','statuscode':'200','digest':'','length':'4'},
            None, signature or self.sig, 'image','.jpg')
        self.db.commit()

    def fetch(self, row, cfg, client, destination):
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b'test')
        return {'id':row['id'], 'path':destination, 'bytes':4,
                'hash':hashlib.sha256(b'test').hexdigest(), 'status':200,
                'final_url':row['original_url']}


class BudgetTests(Fixture):
    def test_idle_wait_does_not_consume_attempt(self):
        clock=[0.0]
        budget=NoProgressBudget(replace(self.cfg,max_attempts=1),clock=lambda:clock[0])
        budget.waiting()
        self.assertFalse(budget.exhausted)
        budget.attempted()
        self.assertTrue(budget.exhausted)
        budget.progress()
        self.assertFalse(budget.exhausted)
        clock[0]=100
        self.assertFalse(budget.exhausted)
        budget.waiting();clock[0]=131
        self.assertTrue(budget.exhausted)
        self.assertEqual(budget.wait_seconds(300),0)

    def test_stop_remains_a_stop(self):
        stop=threading.Event();stop.set()
        with self.assertRaises(Stopped):
            _complete_embedded_media(self.cfg,self.db,Mock(),stop,None,self.sig)

    def test_shared_cooldown_can_defer_without_early_probe(self):
        gate=SharedHostGate()
        gate.pause_for_rate_limit(3600)
        before=gate.blocked_until
        with self.assertRaises(RateLimitDeferred):
            gate.acquire_request(threading.Event(), deadline=0)
        self.assertEqual(gate.blocked_until,before)
        self.assertFalse(gate.probe_inflight)


class IndexTailTests(Fixture):
    def test_text_bad_response_is_bounded_and_resume_retains_cursor(self):
        valid=('20080101000000','http://example.org/a','text/html','200','','4')
        calls=[]
        def response(_client,_endpoints,params,**kwargs):
            calls.append(dict(params))
            if len(calls)==1:return CDXRows([valid],'SAVED')
            raise MalformedCDXResponse('bad terminal page',splittable=False)
        with patch.object(HttpClient,'get_cdx_any',response), \
             patch('archive_scout.cdx.indexer.transient_backoff',return_value=0):
            self.assertTrue(index_archive(self.cfg,self.db,FastStop(),self.events.append))
        self.assertEqual(len(calls),3)
        row=self.db.execute('SELECT plan_json,complete,seen FROM index_coverage').fetchone()
        self.assertEqual((row['complete'],row['seen']),(0,1))
        self.assertEqual(decode_plan(row['plan_json'],[]).pending[0].resume_key,'SAVED')
        with patch.object(HttpClient,'get_cdx_any',return_value=CDXRows([])) as resume:
            self.assertFalse(index_archive(self.cfg,self.db,FastStop()))
        self.assertEqual(dict(resume.call_args.args[1])['resumeKey'],'SAVED')
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM captures').fetchone()[0],1)
        self.assertEqual(self.db.execute('SELECT complete FROM index_coverage').fetchone()[0],1)

    def test_direct_media_bad_response_is_bounded(self):
        error=MalformedCDXResponse('unverified media page',splittable=False)
        with patch('archive_scout.media.indexer._request_media_resume',side_effect=error) as request, \
             patch('archive_scout.media.indexer._wait_seconds',return_value=0):
            result=index_direct_media(self.cfg,self.db,Mock(),FastStop(),self.events.append,
                                      self.sig,media_index_state_signature(self.cfg))
        self.assertTrue(result)
        self.assertEqual(request.call_count,2)
        self.assertEqual(self.db.execute('SELECT complete FROM media_index_state').fetchone()[0],0)

    def test_productive_pages_reset_budget(self):
        values=[]
        for n in range(6):
            values.extend([MalformedCDXResponse('temporary',splittable=False),
                CDXRows([('20080101000000',f'http://example.org/{n}','text/html','200','','4')],str(n))])
        values.append(CDXRows([]))
        with patch.object(HttpClient,'get_cdx_any',side_effect=values), \
             patch('archive_scout.cdx.indexer.transient_backoff',return_value=0):
            self.assertFalse(index_archive(self.cfg,self.db,FastStop()))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM captures').fetchone()[0],6)


class EmbeddedTailTests(Fixture):
    def test_ready_capture_downloads_before_failed_url_retry(self):
        self.queue('http://example.org/good.jpg')
        self.queue('http://example.org/bad.jpg')
        downloaded=[]; bad_calls=[]
        def lookup(cfg,client,row,*args):
            url=row['original_url']
            if 'bad.jpg' in url:
                bad_calls.append(len(downloaded))
                return row,[],TransientRequestError('HTTP 504',status=504)
            return row,[('20080101000000',url,'image/jpeg','200','','4')],None
        def fetch(*args):
            downloaded.append(args[0]['original_url'])
            return self.fetch(*args)
        with patch('archive_scout.media.indexer._embedded_lookup',side_effect=lookup), \
             patch('archive_scout.media.downloader.fetch_media',side_effect=fetch):
            _complete_embedded_media(self.cfg,self.db,Mock(),FastStop(),self.events.append,
                                     self.sig,acquire_ready=True)
        self.assertEqual(downloaded,['http://example.org/good.jpg'])
        self.assertEqual(bad_calls,[0,1,1])
        self.assertEqual(self.db.execute("SELECT state FROM media_discovery_queue WHERE original_url LIKE '%bad.jpg'").fetchone()[0],'error')
        self.assertTrue(any('retry budget reached' in e.message for e in self.events))

    def test_unproven_earliest_waits_but_unrelated_proven_url_is_admitted(self):
        self.queue('http://example.org/a.jpg')
        self.queue('http://example.org/b.jpg','indexed')
        for url in ['http://example.org/a.jpg','http://example.org/b.jpg']:
            self.media(url,'20080101000000');self.media(url,'20080201000000')
        with patch('archive_scout.media.downloader.fetch_media',side_effect=self.fetch) as fetch:
            download_media(self.cfg,self.db,FastStop(),None,allow_partial_inventory=True)
        self.assertEqual(fetch.call_count,1)
        self.assertEqual(fetch.call_args.args[0]['original_url'],'http://example.org/b.jpg')
        self.assertEqual(fetch.call_args.args[0]['timestamp'],'20080101000000')
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM media_captures WHERE state='pending'").fetchone()[0],3)

    def test_all_snapshots_can_download_committed_partial_page(self):
        cfg=replace(self.cfg,media=replace(self.cfg.media,snapshot_strategy='all'))
        sig=media_query_signature(cfg)
        self.media(signature=sig)
        with patch('archive_scout.media.downloader.fetch_media',side_effect=self.fetch) as fetch:
            download_media(cfg,self.db,FastStop(),None,allow_partial_inventory=True)
        self.assertEqual(fetch.call_count,1)

    def test_partial_admission_does_not_accept_arbitrary_signature(self):
        self.media(signature='unrelated')
        from archive_scout.coverage import IndexCoveragePending
        with self.assertRaises(IndexCoveragePending):
            download_media(self.cfg,self.db,FastStop(),None,
                           media_signature='unrelated',allow_partial_inventory=True)

    def test_index_only_does_not_acquire(self):
        self.queue('http://example.org/bad.jpg')
        with patch('archive_scout.media.indexer._embedded_lookup',
                   side_effect=lambda cfg,c,row,*args:(row,[],TransientRequestError('HTTP 504',status=504))), \
             patch('archive_scout.media.downloader.download_media') as download:
            _complete_embedded_media(self.cfg,self.db,Mock(),FastStop(),None,self.sig)
        download.assert_not_called()
        self.assertEqual(self.db.execute('SELECT lookup_attempts FROM media_discovery_queue').fetchone()[0],2)


class PayloadTailTests(Fixture):
    def test_media_recovery_is_fair_bounded_and_keeps_schedules(self):
        for n in range(260):
            self.media(f'http://example.org/{n}.jpg')
        self.db.execute("UPDATE media_captures SET state='error'")
        for row in self.db.execute('SELECT id FROM media_captures').fetchall():
            record_error(self.db,'media_download','connection','offline',media_capture_id=row[0],retryable=True)
            self.db.execute("INSERT INTO media_retry_schedule VALUES(?,0,1,'connection',NULL,'now')",(row[0],))
        self.db.commit();calls=[]
        def failure(*args,**kwargs):calls.extend(kwargs['media_capture_ids'])
        with patch('archive_scout.media.downloader.download_media',side_effect=failure):
            recover_active_media(self.cfg,self.db,FastStop(),None,allow_partial_inventory=True)
        from collections import Counter
        self.assertEqual(set(Counter(calls).values()),{2})
        self.assertEqual(len(set(calls)),260)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM media_retry_schedule').fetchone()[0],260)

    def test_text_recovery_counts_failed_sweeps_not_changed_attempt_totals(self):
        target=get_or_create_target(self.db,'example.org/*')
        from archive_scout.cdx.parameters import cdx_query_signature
        upsert_capture(self.db,{'timestamp':'20080101000000','original':'http://example.org/a',
            'mimetype':'text/html','statuscode':'200','length':'4'},target,cdx_query_signature(self.cfg))
        self.db.execute("UPDATE captures SET state='error'");self.db.commit()
        with patch('archive_scout.operations._retryable_text_batches',side_effect=lambda *a,**k:iter([[1]])), \
             patch('archive_scout.operations.download_archive') as download, \
             patch('archive_scout.downloads.schedule.next_due',return_value=None):
            result=recover_transient_text_errors(self.cfg,self.db,0,[],FastStop(),
                                                 self.events.append,continuous=True,acquire_only=True)
        self.assertEqual(result,1)
        self.assertEqual(download.call_count,2)
        self.assertEqual(self.db.execute('SELECT state FROM captures').fetchone()[0],'error')


class DashboardAccountingTests(Fixture):
    def test_every_state_and_skip_reason_has_one_bucket(self):
        states=[('pending','pending'),('downloading','pending'),('downloaded','downloaded_scanned'),
            ('downloaded_unscanned','downloaded_awaiting_scan'),('error','failed'),
            ('skipped','skipped_non_text'),('skipped','skipped_url_filter'),
            ('skipped','deferred_to_media'),('skipped','unsupported_non_text'),
            ('skipped','excluded_robots_txt'),('skipped',None),('future_state','pending')]
        target=get_or_create_target(self.db,'example.org/*')
        for n,(state,route) in enumerate(states):
            cid=upsert_capture(self.db,{'timestamp':'20080101000000','original':f'http://example.org/{n}',
                'mimetype':'text/html','statuscode':'200','length':'4'},target,'test')
            cid=self.db.execute('SELECT MAX(id) FROM captures').fetchone()[0]
            self.db.execute('UPDATE captures SET state=? WHERE id=?',(state,cid))
            if route is not None:
                self.db.execute("INSERT OR REPLACE INTO capture_routing VALUES(?,'text','fixture',1,?,'now')",(cid,route))
        self.db.commit()
        counts=read_dashboard_counts(self.root/'archive_scout.sqlite3')
        self.assertEqual(counts['captures'],12)
        self.assertEqual(counts['accounted_captures'],12)
        self.assertEqual(counts['documents'],2)
        self.assertEqual(counts['skipped_other'],2)
        self.assertEqual(counts['downloading'],1)
        self.assertEqual(counts['other_states'],1)
        self.assertEqual(counts['skipped'],6)
        label=format_capture_accounting(counts)
        self.assertIn('Indexed 12 = pending 1 + in flight 1 + saved 2',label)
        self.assertIn('Accounted for: 12',label)

    def test_stale_route_does_not_double_count_pending_capture(self):
        tid=get_or_create_target(self.db,'example.org/*')
        cid=upsert_capture(self.db,{'timestamp':'20080101000000','original':'http://example.org/a',
             'mimetype':'text/html','statuscode':'200','length':'4'},tid,'test')
        self.db.execute("INSERT OR REPLACE INTO capture_routing VALUES(?,'text','fixture',1,'skipped_non_text','now')",(cid,))
        self.db.commit()
        counts=read_dashboard_counts(self.root/'archive_scout.sqlite3')
        self.assertEqual((counts['pending'],counts['skipped_non_text'],counts['accounted_captures']),(1,0,1))

class OperationCoverageTests(Fixture):
    def test_all_text_network_modes_return_incomplete_and_write_reports(self):
        cfg=replace(self.cfg, media=replace(self.cfg.media,enabled=False))
        for mode in ('index','all','download_only','download','resume','retry_errors',
                     'retry_download_errors','external_media_after_scan'):
            with self.subTest(mode=mode), \
                 patch.object(HttpClient,'get_cdx_any',side_effect=MalformedCDXResponse('bad page',splittable=False)) as request, \
                 patch('archive_scout.cdx.indexer.transient_backoff',return_value=0):
                outcome=run_project(cfg,mode,stop_event=FastStop())
                self.assertEqual(outcome.status,'incomplete')
                self.assertTrue(any(path.parent.name=='reports' and path.exists() for path in outcome.values()))
                self.assertEqual(request.call_count,2)
                self.assertEqual(self.db.execute('SELECT complete FROM index_coverage').fetchone()[0],0)

    def test_all_media_network_modes_return_incomplete_and_write_reports(self):
        for mode in ('media_all','media_index','media_download','media_retry'):
            with self.subTest(mode=mode), \
                 patch('archive_scout.media.indexer._request_media_resume',side_effect=MalformedCDXResponse('bad page',splittable=False)) as request, \
                 patch('archive_scout.media.indexer._wait_seconds',return_value=0):
                outcome=run_project(self.cfg,mode,stop_event=FastStop())
                self.assertEqual(outcome.status,'incomplete')
                self.assertTrue(any(path.parent.name=='reports' and path.exists() for path in outcome.values()))
                self.assertEqual(request.call_count,2)
                self.assertEqual(self.db.execute('SELECT complete FROM media_index_state').fetchone()[0],0)

    def test_rescan_and_reports_do_not_enter_network_retry(self):
        for mode in ('rescan','report'):
            with self.subTest(mode=mode), patch.object(HttpClient,'get_cdx_any') as request:
                run_project(self.cfg,mode,stop_event=FastStop())
                request.assert_not_called()

class CursorLoopTests(Fixture):
    def test_text_cursor_cycle_cannot_keep_resetting_progress(self):
        row=('20080101000000','http://example.org/a','text/html','200','','4')
        calls=[]
        def response(client,endpoints,params,**kwargs):
            calls.append(dict(params))
            current=dict(params).get('resumeKey')
            return CDXRows([row], 'B' if current=='A' else 'A')
        with patch.object(HttpClient,'get_cdx_any',response) as request, \
             patch('archive_scout.cdx.indexer.transient_backoff',return_value=0):
            self.assertTrue(index_archive(self.cfg,self.db,FastStop()))
        self.assertEqual(len(calls),4)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM captures').fetchone()[0],1)
        state=self.db.execute('SELECT complete,plan_json FROM index_coverage').fetchone()
        self.assertEqual(state['complete'],0)
        self.assertEqual(decode_plan(state['plan_json'],[]).pending[0].resume_key,'B')

    def test_all_embedded_cursor_cycle_is_bounded_across_sweeps(self):
        cfg=replace(self.cfg,media=replace(self.cfg.media,snapshot_strategy='all'))
        sig=media_query_signature(cfg)
        self.db.execute('''INSERT INTO media_discovery_queue(original_url,query_signature,state,created_at,updated_at)
            VALUES('http://example.org/a.jpg',?,'pending','now','now')''',(sig,));self.db.commit()
        row=('20080101000000','http://example.org/a.jpg','image/jpeg','200','','4')
        def response(cfg,client,url,window,*args,**kwargs):
            return [row],False,'B' if window.resume_key=='A' else 'A'
        with patch('archive_scout.media.indexer._request_media_resume',side_effect=response) as request:
            _complete_embedded_media(cfg,self.db,Mock(),FastStop(),self.events.append,sig)
        self.assertLessEqual(request.call_count,9)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM media_captures').fetchone()[0],1)
        self.assertEqual(self.db.execute('SELECT state FROM media_discovery_queue').fetchone()[0],'error')

    def test_all_embedded_oversize_shrinks_same_cursor(self):
        from archive_scout.media.embedded_pages import index_all_embedded_pages
        import json
        cfg=replace(self.cfg,page_size=1000,media=replace(self.cfg.media,snapshot_strategy='all'))
        sig=media_query_signature(cfg)
        self.db.execute('''INSERT INTO media_discovery_queue(original_url,query_signature,state,created_at,updated_at)
            VALUES('http://example.org/a.jpg',?,'pending','now','now')''',(sig,))
        qid=self.db.execute('SELECT id FROM media_discovery_queue').fetchone()[0]
        key=f'media_embed_cursor_v1:{sig}:{qid}'
        self.db.execute('INSERT INTO project_meta VALUES(?,?)',(key,json.dumps({'resume_key':'SAVED','page_size':1000})))
        self.db.commit()
        with patch('archive_scout.media.indexer._request_media_resume',side_effect=TransientRequestError('response exceeds 100 bytes')):
            index_all_embedded_pages(cfg,self.db,Mock(),FastStop(),None,sig)
        value=json.loads(self.db.execute('SELECT value FROM project_meta WHERE key=?',(key,)).fetchone()[0])
        self.assertEqual(value,{'resume_key':'SAVED','page_size':500})
        self.assertEqual(self.db.execute('SELECT state FROM media_discovery_queue').fetchone()[0],'error')

class SnapshotAccountingTests(Fixture):
    def test_refresh_uses_one_snapshot_during_a_concurrent_commit(self):
        tid=get_or_create_target(self.db,'example.org/*')
        upsert_capture(self.db,{'timestamp':'20080101000000','original':'http://example.org/a',
            'mimetype':'text/html','statuscode':'200','length':'4'},tid,'fixture')
        self.db.commit()
        original_connect=sqlite3.connect
        writer=self.db
        class SnapshotConnection(sqlite3.Connection):
            fired=False
            def execute(connection, sql, parameters=()):
                cursor=super(SnapshotConnection,connection).execute(sql,parameters)
                if '(SELECT COUNT(*) FROM captures)' in sql and not connection.fired:
                    connection.fired=True
                    upsert_capture(writer,{'timestamp':'20080101000000','original':'http://example.org/b',
                        'mimetype':'text/html','statuscode':'200','length':'4'},tid,'fixture')
                    writer.commit()
                return cursor
        def connect(*args,**kwargs):return original_connect(*args,**kwargs,factory=SnapshotConnection)
        with patch('archive_scout.ui.dashboard.sqlite3.connect',side_effect=connect):
            counts=read_dashboard_counts(self.root/'archive_scout.sqlite3')
        self.assertEqual(counts['captures'],1)
        self.assertEqual(counts['accounted_captures'],1)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM captures').fetchone()[0],2)

class CanonicalMediaTests(Fixture):
    def test_completed_exact_lookup_can_return_an_archived_scheme_variant(self):
        self.queue('https://example.org/a.jpg','indexed')
        upsert_media_capture(self.db,{'timestamp':'20080101000000','original':'http://example.org/a.jpg',
            'mimetype':'image/jpeg','statuscode':'200','digest':'','length':'4'},
            None,self.sig,'image','.jpg',source_type='external_embedded')
        self.db.commit()
        with patch('archive_scout.media.downloader.fetch_media',side_effect=self.fetch) as fetch:
            download_media(self.cfg,self.db,FastStop(),None,allow_partial_inventory=True)
        self.assertEqual(fetch.call_count,1)
        self.assertEqual(fetch.call_args.args[0]['original_url'],'http://example.org/a.jpg')
