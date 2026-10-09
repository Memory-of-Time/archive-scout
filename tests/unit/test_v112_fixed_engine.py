from __future__ import annotations
import contextlib, hashlib, json, tempfile, threading, time, unittest
from pathlib import Path
from unittest import mock
import httpx
from archive_scout.cdx.client import HttpClient, RateLimitDeferred, parse_retry_after
from archive_scout.cli import build_parser
from archive_scout.config import ProjectConfig, load_project_config, save_project_config, ResearchConfig
from archive_scout.downloads import rate_limit as rate
from archive_scout.downloads.downloader import make_replay_redirect_validator, _acquire_archive
from archive_scout.network.transports import ResilientTransport, TransportResponse, TransportExhaustedError, RedirectPolicyError
from archive_scout.operations import run_project
from archive_scout.events import ConnectivityPaused
from archive_scout.database.connection import open_database
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.utils import utc_now

class FixedEngineTests(unittest.TestCase):
    def setUp(self):rate.reset_shared_traffic_state_for_tests()
    def tearDown(self):rate.reset_shared_traffic_state_for_tests()
    def test_throttles_never_change_shared_spacing(self):
        a=rate.SharedFixedRateLimiter(.125,rate.WAYBACK_REPLAY_RATE_KEY)
        for i in range(1000):a.note_rate_limit(i);a.note_healthy_response()
        self.assertEqual(a.effective_delay,.125);self.assertNotIn('adaptive_delay',vars(a._state));a.close()
    def test_slower_active_user_floor_is_released_on_close(self):
        a=rate.SharedFixedRateLimiter(.125,rate.WAYBACK_REPLAY_RATE_KEY);b=rate.SharedFixedRateLimiter(2,rate.WAYBACK_REPLAY_RATE_KEY)
        self.assertEqual(a.effective_delay,2);b.close();self.assertEqual(a.effective_delay,.125);a.close()
    def test_headerless_incidents_do_not_escalate_or_require_probe(self):
        now=[100.]
        with mock.patch.object(rate.time,'monotonic',lambda:now[0]),mock.patch.object(rate.time,'time',lambda:now[0]+1000):
            g=rate.SharedHostGate(60,600)
            for i in range(20):
                self.assertEqual(g.pause_for_rate_limit(),5);now[0]+=5
                self.assertFalse(g.acquire_request(threading.Event()).probe)
                self.assertFalse(g.acquire_request(threading.Event()).probe)
    def test_retry_after_zero_and_long_server_deadlines_are_preserved(self):
        with mock.patch.object(rate.time,'monotonic',return_value=100),mock.patch.object(rate.time,'time',return_value=1000):
            g=rate.SharedHostGate();self.assertEqual(g.pause_for_rate_limit(0),0)
            self.assertEqual(g.pause_for_rate_limit(3600),3600)
            self.assertEqual(g.pause_for_rate_limit(None),3600)
            self.assertEqual(g.snapshot()['server_eligible_at_epoch'],4600)
    def test_known_application_wait_expires_but_server_and_unknown_waits_remain(self):
        d={'wait_source':'fallback','eligible_at_epoch':5000,'rate_limit_signal_at_epoch':1000}
        self.assertEqual(rate.saved_service_eligibility(d),1005)
        self.assertEqual(rate.saved_service_eligibility({**d,'server_eligible_at_epoch':1500}),1500)
        self.assertEqual(rate.saved_service_eligibility({'eligible_at_epoch':5000}),5000)
    def test_old_adaptive_setting_is_ignored_and_no_control_is_exposed(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);p=save_project_config(ProjectConfig(root,['example.com/*'],[]))
            payload=json.loads(p.read_text());payload['adaptive_rate_limiting']=True;p.write_text(json.dumps(payload))
            cfg=load_project_config(p);self.assertNotIn('adaptive_rate_limiting',cfg.to_payload())
            help_text=build_parser()._subparsers._group_actions[0].choices['run'].format_help()
            self.assertNotIn('adaptive',help_text)
    def test_paused_admissions_do_not_set_user_stop_or_interrupt_healthy_body(self):
        entered=threading.Event();release=threading.Event();stop=threading.Event()
        class Transport:
            def request(self,url,headers,max_bytes,event):
                entered.set();release.wait(2);assert not event.is_set()
                return TransportResponse(200,{},url,b'healthy','fixture',0)
            def close(self):pass
        client=HttpClient(rate.FixedRateLimiter(0),1,1,'test',stop,transport=Transport())
        result=[];thread=threading.Thread(target=lambda:result.append(client.get('http://example.com',100)))
        thread.start();self.assertTrue(entered.wait(2));pause=RateLimitDeferred('fixture');client.pause_admissions(pause)
        with self.assertRaises(RateLimitDeferred):client.get('http://example.com/new',100)
        release.set();thread.join(2);self.assertEqual(result[0]['data'],b'healthy');self.assertFalse(stop.is_set());client.close()
    def test_connection_failure_retries_locally_without_shared_cooldown(self):
        class Transport:
            calls=0
            def request(self,url,*args):
                self.calls+=1
                if self.calls==1:raise TransportExhaustedError(url,[('fixture',httpx.ConnectError('offline'))])
                return TransportResponse(200,{},url,b'ok','fixture',0)
            def close(self):pass
        t=Transport();g=rate.SharedHostGate();c=HttpClient(rate.FixedRateLimiter(0),1,1,'test',threading.Event(),host_gate=g,transport=t,persistent_retries=True)
        with mock.patch.object(c,'retry_wait') as wait:self.assertEqual(c.get('http://example.com',10)['data'],b'ok')
        wait.assert_called_once();self.assertEqual(g.remaining(),0);self.assertEqual(t.calls,2);c.close()
    def test_redirect_policy_blocks_live_and_unapproved_external_evidence(self):
        cfg=ProjectConfig(Path('.'),['example.com/*'],[]).normalized();v=make_replay_redirect_validator(cfg,'http://example.com/a')
        source='https://web.archive.org/web/20010101000000id_/http://example.com/a'
        v(source,'https://web.archive.org/web/20010101000000id_/http://example.com/b')
        with self.assertRaises(RedirectPolicyError):v(source,'https://outside.example/a')
        with self.assertRaises(RedirectPolicyError):v(source,'https://web.archive.org/web/20010101000000id_/http://outside.example/a')
    def test_automatic_resume_uses_same_operation_and_preserves_pending_capture(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);cfg=ProjectConfig(root,['example.com/*'],[],research=ResearchConfig(enabled=False,auto_build=False)).normalized();ids=[]
            def download(c,db,*args,**kwargs):
                ids.append(db.execute('SELECT id FROM operation_runs ORDER BY id DESC LIMIT 1').fetchone()[0])
                if len(ids)==1:raise ConnectivityPaused('temporary outage')
                return {'downloaded':0,'skipped':0,'errors':0,'queued':0}
            with mock.patch('archive_scout.operations.index_archive'),mock.patch('archive_scout.operations.download_archive_only',side_effect=download):run_project(cfg,'download_only')
            self.assertEqual(len(ids),2);self.assertEqual(ids[0],ids[1])
    def test_stop_preserves_server_deadline_before_periodic_progress_write(self):
        from archive_scout.events import Stopped
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);cfg=ProjectConfig(root,['example.com/*'],[]).normalized();stop=threading.Event()
            def interrupted(*args,**kwargs):
                rate.shared_host_gate().pause_for_rate_limit(60,'HTTP 503');stop.set();raise Stopped
            with mock.patch('archive_scout.operations.index_archive',side_effect=interrupted),self.assertRaises(Stopped):run_project(cfg,'index',stop)
            db=open_database(root)
            try:
                row=db.execute('SELECT status,progress_json FROM operation_runs ORDER BY id DESC LIMIT 1').fetchone()
                detail=json.loads(row['progress_json'])['detail'];self.assertEqual(row['status'],'interrupted')
                self.assertEqual(detail['wait_source'],'server');self.assertGreater(detail['server_eligible_at_epoch'],time.time()+58)
                rate.reset_shared_traffic_state_for_tests();rate.shared_host_gate().restore_service_wait(detail);self.assertGreater(rate.shared_host_gate().remaining(),58)
            finally:db.close()
    def test_local_scan_finishes_while_second_network_request_waits(self):
        from archive_scout.database.repositories import get_or_create_keyword_set,start_scan_run
        from archive_scout.scanning.jobs import ScanJob
        from archive_scout.downloads import downloader
        blocked=threading.Event();release=threading.Event();scanned=threading.Event();overlapped=[]
        body=b'<html><body>needle</body></html>'
        class Client:
            calls=0
            def __init__(self,*a,**kw):pass
            def close(self):pass
            def download_to_path(self,url,path,*a,**kw):
                self.calls+=1
                if self.calls==2:blocked.set();release.wait(3)
                Path(path).write_bytes(body);return {'headers':{'content-type':'text/html'},'preview':body,'bytes':len(body),'content_hash':'','status':200,'final_url':url}
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);cfg=ProjectConfig(root,['example.com/*'],['needle'],from_date='2001',to_date='2001',workers=1,scan_workers=1,scan_backend='thread').normalized()
            db=open_database(root);now=utc_now();sig=cdx_query_signature(cfg)
            with db:
                db.executemany("INSERT INTO captures(original_url,timestamp,query_signature,mimetype,statuscode,length,state,created_at,updated_at) VALUES(?,'20010101000000',?,'text/html','200',100,'pending',?,?)",[(f'http://example.com/{i}',sig,now,now) for i in range(2)])
            k=get_or_create_keyword_set(db,'rules',['needle']);run=start_scan_run(db,k,'rules',1,'download');db.commit();job=ScanJob.create(run,'rules',['needle'])
            original=downloader._scan_saved_capture
            def scan(*args):result=original(*args);scanned.set();return result
            def unblock():
                blocked.wait(3);overlapped.append(scanned.wait(2));release.set()
            thread=threading.Thread(target=unblock);thread.start()
            with mock.patch.object(downloader,'HttpClient',Client),mock.patch.object(downloader,'_scan_saved_capture',side_effect=scan):
                downloader.download_archive(cfg,db,run,threading.Event(),None,scan_jobs=[job])
            thread.join(3);self.assertEqual(overlapped,[True]);self.assertEqual(db.execute('SELECT COUNT(*) FROM documents').fetchone()[0],2);db.close()

if __name__=='__main__':unittest.main()
