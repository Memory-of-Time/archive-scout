"""Cursor retention, accurate recovery counters and bounded diagnostic notices."""
from __future__ import annotations

import json
from dataclasses import replace
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from archive_scout.cdx.client import CDXRows, HttpClient, TransientRequestError, summarize_cdx_error
from archive_scout.cdx.indexer import index_archive, decode_plan, save_coverage_state
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.config import NetworkConfig, ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, upsert_captures
from archive_scout.downloads.diagnostics import AcquisitionDiagnostics
from archive_scout.downloads.downloader import download_archive
from archive_scout.events import ProgressEvent, Stopped
from archive_scout.ui.event_queue import CoalescingEventQueue


def config(root, **kwargs):
    return ProjectConfig(root, targets=['*.example.com/*'], keywords=['needle'],
        from_date='2005', to_date='2011', page_size=1000, cdx_collapses=[],
        cdx_delay=0, download_delay=0,
        network=NetworkConfig(trust_environment=False, index_strategy='resume'), **kwargs).normalized()


def rows(count=1, host='www.example.com'):
    return [('20080101000000', f'http://{host}/{i}', 'text/html', '200', '', '100') for i in range(count)]


class RecoveryTests(unittest.TestCase):
    def test_committed_cursor_survives_timeout_stop_and_reopen(self):
        with tempfile.TemporaryDirectory() as temp:
            cfg=config(Path(temp)); db=open_database(cfg.output_dir); calls=[]
            def respond(_client, endpoints, params, **kwargs):
                calls.append(dict(params))
                if len(calls)==1:
                    return CDXRows(rows(), 'COMMITTED+CURSOR%2F')
                if len(calls)==2:
                    raise TransientRequestError('read timeout', read_timed_out=True, splittable=True)
                raise Stopped
            with patch.object(HttpClient, 'get_cdx_any', respond), patch('archive_scout.cdx.indexer.transient_backoff', return_value=0):
                with self.assertRaises(Stopped): index_archive(cfg, db, threading.Event())
            self.assertEqual([p.get('resumeKey') for p in calls], [None, 'COMMITTED+CURSOR%2F', 'COMMITTED+CURSOR%2F'])
            self.assertTrue(all((p['from'],p['to'])==(cfg.from_date,cfg.to_date) for p in calls))
            state=db.execute('SELECT plan_json,complete,seen FROM index_coverage').fetchone()
            self.assertEqual((state['complete'],state['seen']), (0,1))
            self.assertEqual(len(decode_plan(state['plan_json'],[]).pending),1)
            db.close(); db=open_database(cfg.output_dir)
            resumed=[]
            def finish(_client,endpoints,params,**kwargs):resumed.append(dict(params)); return CDXRows([])
            with patch.object(HttpClient,'get_cdx_any',finish):index_archive(cfg,db,threading.Event())
            self.assertEqual(resumed[0]['resumeKey'],'COMMITTED+CURSOR%2F')
            self.assertEqual(db.execute('SELECT COUNT(*) FROM captures').fetchone()[0],1)
            self.assertEqual(db.execute('SELECT complete FROM index_coverage').fetchone()[0],1)
            db.close()

    def test_evidenced_oversize_reduces_limit_at_same_cursor(self):
        with tempfile.TemporaryDirectory() as temp:
            cfg=config(Path(temp)); db=open_database(cfg.output_dir); calls=[]
            def respond(_client,endpoints,params,**kwargs):
                calls.append(dict(params))
                if len(calls)==1:return CDXRows(rows(),'SAVED')
                if len(calls)==2:raise TransientRequestError('response exceeds 100 bytes',splittable=True)
                raise Stopped
            with patch.object(HttpClient,'get_cdx_any',respond), patch('archive_scout.cdx.indexer.transient_backoff',return_value=0):
                with self.assertRaises(Stopped):index_archive(cfg,db,threading.Event())
            self.assertEqual((calls[2]['resumeKey'],calls[2]['limit']),('SAVED','500'))
            self.assertEqual((calls[2]['from'],calls[2]['to']),(cfg.from_date,cfg.to_date))
            self.assertEqual(db.execute('SELECT complete FROM index_coverage').fetchone()[0],0)
            db.close()

    def test_each_rejected_attempt_counts_once_and_remains_incomplete(self):
        class VirtualWait(threading.Event):
            def __init__(self):super().__init__();self.waits=[]
            def wait(self,timeout=None):
                self.waits.append(timeout)
                if len(self.waits)==8:self.set()
                return self.is_set()
        with tempfile.TemporaryDirectory() as temp:
            cfg=config(Path(temp)); db=open_database(cfg.output_dir); stop=VirtualWait(); events=[]
            with patch.object(HttpClient,'get_cdx_any',return_value=CDXRows(rows(host='unrelated.invalid'))) as request:
                self.assertTrue(index_archive(cfg,db,stop,events.append))
            self.assertEqual(request.call_count,4)
            self.assertEqual(stop.waits,[5,10,20])
            state=db.execute('SELECT plan_json,complete FROM index_coverage').fetchone()
            self.assertEqual(decode_plan(state['plan_json'],[]).pending[0].failures,4)
            self.assertEqual(state['complete'],0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM captures').fetchone()[0],0)
            notices=[e for e in events if 'remains unverified' in e.message]
            self.assertEqual(len(notices),3)
            self.assertTrue(all(e.detail['retain_notice'] and 'unrelated host' in e.message for e in notices))
            db.close()

    def test_repeated_cursor_timeouts_retain_last_resort_recovery(self):
        with tempfile.TemporaryDirectory() as temp:
            cfg=replace(config(Path(temp)),max_attempts=8);db=open_database(cfg.output_dir);calls=[];events=[]
            def respond(_client,endpoints,params,**kwargs):
                calls.append(dict(params))
                if len(calls)==1:return CDXRows(rows(),'SAVED')
                if len(calls)<=5:raise TransientRequestError('read timeout',read_timed_out=True,splittable=True)
                raise Stopped
            with patch.object(HttpClient,'get_cdx_any',respond),patch('archive_scout.cdx.indexer.transient_backoff',return_value=0):
                with self.assertRaises(Stopped):index_archive(cfg,db,threading.Event(),events.append)
            self.assertEqual([p.get('resumeKey') for p in calls[1:5]],['SAVED']*4)
            self.assertNotIn('resumeKey',calls[5])
            self.assertTrue(any('last-resort restart' in e.message and e.detail['retain_notice'] for e in events))
            self.assertEqual(db.execute('SELECT COUNT(*) FROM captures').fetchone()[0],1)
            self.assertEqual(db.execute('SELECT complete FROM index_coverage').fetchone()[0],0)
            db.close()

    def test_committed_page_timing_survives_following_phase(self):
        with tempfile.TemporaryDirectory() as temp:
            cfg=config(Path(temp));db=open_database(cfg.output_dir);events=[]
            with patch.object(HttpClient,'get_cdx_any',side_effect=[CDXRows(rows(),'NEXT'),Stopped()]):
                with self.assertRaises(Stopped):index_archive(cfg,db,threading.Event(),events.append)
            page=next(e for e in events if e.detail and 'database_seconds' in e.detail)
            self.assertEqual((page.detail['received'],page.detail['stored']),(1,1))
            self.assertTrue(page.detail['retain_notice'])
            self.assertGreaterEqual(page.detail['request_parse_scope_seconds'],0)
            db.close()


class DiagnosticTests(unittest.TestCase):
    def test_leaf_error_survives_long_query_and_urls_are_removed(self):
        leaf=RuntimeError('peer closed response before body completed')
        wrapper=TransientRequestError('representation failure for https://host.test/?secret='+'x'*20000)
        wrapper.__cause__=leaf
        message=summarize_cdx_error(wrapper)
        self.assertIn('peer closed response',message)
        self.assertNotIn('secret',message)
        self.assertLessEqual(len(message),260)
        self.assertNotIn('password',summarize_cdx_error(RuntimeError('failure at https://user:password@host.test/?secret=value then stopped')))

    def test_endpoint_notice_keeps_reason_without_query(self):
        client=HttpClient.__new__(HttpClient);client.retry_callback=Mock()
        error=TransientRequestError('representation failure for https://host.test/?token='+'x'*10000)
        error.__cause__=RuntimeError('incomplete chunked read')
        client._report_cdx_endpoint_failure('https://host.test/cdx',error,None,63.5)
        message=client.retry_callback.call_args.args[2]
        self.assertIn('incomplete chunked read',message)
        self.assertNotIn('token=',message)
        self.assertIn('63.5s',message)

    def test_gui_routes_explicit_notices_outside_coalescing_slot(self):
        from archive_scout.ui.main_window import ArchiveScoutApp
        queue=CoalescingEventQueue(); owner=SimpleNamespace(events=queue)
        failure=ProgressEvent('index','unverified: exact reason',detail={'retain_notice':True})
        ArchiveScoutApp.on_engine_event(owner,failure)
        ArchiveScoutApp.on_engine_event(owner,ProgressEvent('index','next phase'))
        self.assertEqual(queue.get_nowait(),('notice',failure))
        self.assertEqual(queue.get_nowait()[1].message,'next phase')

    def test_batch_metrics_only_sampled_on_health_interval_and_reset(self):
        with tempfile.TemporaryDirectory() as temp:
            diag=AcquisitionDiagnostics(Path(temp),None);diag.begin_batch(None)
            sampler=Mock(return_value={'configured_workers':10,'avg_owner_commit_seconds':0.01})
            diag.batch_metrics=sampler
            diag.summary(1);sampler.assert_not_called()
            diag.summary(1,force=True);sampler.assert_called_once()
            payload=json.loads((Path(temp)/'diagnostics/text-acquisition/latest.json').read_text())
            self.assertEqual(payload['acquisition']['configured_workers'],10)
            diag.begin_batch(None);self.assertIsNone(diag.batch_metrics)
            diag.summary(2,force=True)
            payload=json.loads((Path(temp)/'diagnostics/text-acquisition/latest.json').read_text())
            self.assertNotIn('acquisition',payload)
            self.assertEqual(payload['new_saves'],3)

    def test_disabled_diagnostics_never_sample_or_write(self):
        with tempfile.TemporaryDirectory() as temp:
            diag=AcquisitionDiagnostics(Path(temp),None,enabled=False)
            diag.batch_metrics=Mock(side_effect=AssertionError('disabled sampler'))
            diag.summary(1,force=True);diag.batch_metrics.assert_not_called()
            self.assertFalse((Path(temp)/'diagnostics').exists())

    def test_sampler_failure_does_not_interrupt_summary(self):
        with tempfile.TemporaryDirectory() as temp:
            callback=Mock();diag=AcquisitionDiagnostics(Path(temp),callback)
            diag.batch_metrics=Mock(side_effect=RuntimeError('diagnostic sample unavailable'))
            diag.summary(1,force=True)
            self.assertEqual(callback.call_count,1)
            payload=json.loads((Path(temp)/'diagnostics/text-acquisition/latest.json').read_text())
            self.assertNotIn('acquisition',payload)
            self.assertEqual(payload['new_saves'],1)

    def test_download_persists_stage_metrics_and_keeps_payload(self):
        body=b'<html><body>needle</body></html>'
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);cfg=config(root);db=open_database(root)
            sig=cdx_query_signature(cfg)
            with db:
                tid=get_or_create_target(db,cfg.targets[0]);upsert_captures(db,rows(),tid,sig)
                save_coverage_state(db,tid,sig,cfg.from_date,cfg.to_date,None,True,1,None)
            with patch.object(HttpClient,'get',return_value={'status':200,'data':body,'headers':{'content-type':'text/html'},'final_url':'http://www.example.com/0'}):
                download_archive(cfg,db,0,threading.Event(),None,acquire_only=True)
            data=json.loads((root/'diagnostics/text-acquisition/latest.json').read_text())
            self.assertEqual(data['new_saves'],1)
            self.assertEqual(data['acquisition']['committed_samples'],1)
            self.assertEqual(data['acquisition']['configured_workers'],cfg.workers)
            self.assertEqual(data['acquisition']['queued_running_tasks'],0)
            self.assertGreaterEqual(data['acquisition']['avg_owner_commit_seconds'],0)
            self.assertIsNone(data['acquisition']['avg_payload_publication_seconds'])
            self.assertIsNone(data['acquisition']['avg_acquisition_seconds'])
            self.assertEqual(next((root/'captures').rglob('*.txt')).read_bytes(),body)
            db.close()


if __name__=='__main__':unittest.main()
