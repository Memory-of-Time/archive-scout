"""Actual broken-wire responses plus reset/probe and persistent-owner regressions."""
from __future__ import annotations

import base64
import errno
import hashlib
import http.client
import shutil
import tempfile
import threading
import time
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import Mock, patch

import httpx
import urllib3

from archive_scout.cdx.client import HttpClient, TransientRequestError
from archive_scout.cdx.indexer import save_coverage_state
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.config import ProjectConfig, NetworkConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, upsert_capture
from archive_scout.downloads.downloader import cumulative_download_progress, download_archive
from archive_scout.downloads.rate_limit import FixedRateLimiter, SharedHostGate, reset_shared_traffic_state_for_tests
from archive_scout.downloads.validation import classify_exception
from archive_scout.events import Stopped
from archive_scout.network.transports import (
    CurlBackend, CurlTransportError, IncompleteRepresentationError, RedirectPolicyError,
    ResilientTransport, TransportExhaustedError, TransportFileResponse,
    exception_chain, is_transport_connection_failure, normalize_transport_error,
)
from archive_scout.operations import run_project
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BODY = b'<html><title>Preserved</title><body>needle, complete original profile</body></html>'


class WireFixture(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    calls = Counter()

    def do_GET(self):
        type(self).calls[self.path] += 1
        short = self.path == '/short' or (self.path == '/flaky' and self.calls[self.path] == 1)
        self.send_response(200)
        self.send_header('Content-Type', 'text/html')
        if self.path == '/chunked':
            self.send_header('Transfer-Encoding', 'chunked')
            self.end_headers()
            self.wfile.write(b'5\r\nabc')  # no complete chunk, no terminating chunk
        else:
            self.send_header('Content-Length', str(len(BODY)))
            self.end_headers()
            self.wfile.write(BODY[:-2] if short else BODY)
        self.close_connection = True

    def log_message(self, *args):
        pass


class ExceptionNormalization(unittest.TestCase):
    def test_all_logged_reset_forms_are_connection_failures(self):
        errors = [CurlTransportError(35, 'Recv failure: Connection was reset'),
            OSError('curl: (35) Recv failure: Connection was reset'),
            httpx.ConnectError('[WinError 10054] reset'),
            urllib3.exceptions.ProtocolError('Connection aborted.', ConnectionResetError(10054, 'reset'))]
        for error in errors:
            with self.subTest(error=repr(error)):
                self.assertTrue(is_transport_connection_failure(error))
                self.assertEqual(classify_exception(error), ('connection', None, True))

    def test_body_failures_are_normalized_but_pool_and_storage_failures_are_not(self):
        errors = [httpx.RemoteProtocolError('peer closed connection without sending complete message body (received 4741 bytes, expected 4743)'),
            urllib3.exceptions.ProtocolError('Response ended prematurely'),
            urllib3.exceptions.ProtocolError('Connection broken', http.client.IncompleteRead(b'abc',2)),
            CurlTransportError(18, 'transfer closed with 2 bytes remaining')]
        for error in errors:
            result = normalize_transport_error(error)
            self.assertIsInstance(result, IncompleteRepresentationError)
            self.assertEqual(classify_exception(result), ('incomplete_representation',None,True))
        for error in (httpx.PoolTimeout('pool'), httpx.ReadTimeout('read'),
                      OSError(errno.ENOSPC, 'full'), httpx.RemoteProtocolError('illegal header')):
            self.assertIs(normalize_transport_error(error), error)
        self.assertEqual(classify_exception(httpx.PoolTimeout('pool'))[0], 'connection_pool_timeout')

    def test_exception_cycles_are_bounded(self):
        a, b = RuntimeError('a'), RuntimeError('b')
        a.__cause__ = b
        b.__cause__ = a
        b.args = (a,)
        self.assertEqual(len(list(exception_chain(a))), 2)

    def test_curl_wrapper_keeps_its_exit_code_for_request_and_download(self):
        if not shutil.which('curl'):
            self.skipTest('curl unavailable')
        backend = CurlBackend(1,1,trust_env=False)
        for method in ('request','download'):
            proc=Mock(returncode=35)
            proc.poll.return_value=35
            proc.communicate.return_value=('', 'curl: (35) Recv failure: Connection was reset')
            with tempfile.TemporaryDirectory() as tmp, patch('archive_scout.network.transports.subprocess.Popen',return_value=proc):
                with self.assertRaises(CurlTransportError) as caught:
                    if method=='request':backend.request('https://archive.test/',{},10000,threading.Event())
                    else:backend.download('https://archive.test/',{},Path(tmp)/'body',10000,threading.Event())
                self.assertEqual(caught.exception.curl_code,35)


class RecoveryAdmission(unittest.TestCase):
    def client(self, transport, gate=None, coordinated=True):
        return HttpClient(FixedRateLimiter(0),4,1,'test',threading.Event(),transport=transport,
                          host_gate=gate, coordinated_connection_recovery=coordinated)

    def test_failed_connectivity_turn_is_not_repeated_four_times(self):
        transport=Mock()
        error=TransportExhaustedError('https://archive.test/', [('curl',CurlTransportError(35,'reset'))])
        transport.request.side_effect=error
        transport.download.side_effect=error
        for method in ('get','download_to_path'):
            gate=SharedHostGate();gate.last_reachable=0
            client=self.client(transport,gate)
            client.retry_wait=Mock()
            with tempfile.TemporaryDirectory() as tmp:
                with self.assertRaises(TransientRequestError) as caught:
                    if method=='get':client.get('https://archive.test/',10000)
                    else:client.download_to_path('https://archive.test/',Path(tmp)/'body',10000,retain_partial=False)
            self.assertTrue(caught.exception.connection_failed)
            client.retry_wait.assert_not_called()
        self.assertEqual(transport.request.call_count,1)
        self.assertEqual(transport.download.call_count,1)

    def test_real_transport_cools_failed_curl_and_keeps_healthy_alternate(self):
        transport=ResilientTransport(pool_size=1,connect_timeout=1,read_timeout=1,mode='httpx',trust_env=False)
        self.addCleanup(transport.close)
        first=Mock();first.download.side_effect=CurlTransportError(35,'reset')
        second=Mock();second.download.side_effect=lambda url,headers,path,*args,**kwargs: TransportFileResponse(200,{},url,path,0,'',b'','httpx',0)
        transport.backends['curl']=first
        original=transport.backends['httpx'];original.close();transport.backends['httpx']=second
        transport.order=['curl','httpx']
        with tempfile.TemporaryDirectory() as tmp:
            result=transport.download('https://archive.test/',{},Path(tmp)/'body',10000,threading.Event())
        self.assertEqual(result.backend,'httpx')
        self.assertGreater(transport.cooldown_until['curl'],time.monotonic())
        self.assertEqual(second.download.call_count,1)

    def test_stream_success_and_redirect_refusal_resolve_host_recovery_probe(self):
        for outcome in ('success','redirect'):
            gate=SharedHostGate();gate.probe_required=True;gate.last_reachable=0
            transport=Mock()
            if outcome=='redirect':transport.download.side_effect=RedirectPolicyError('a','b','redirect_skipped')
            else:transport.download.side_effect=lambda url,headers,path,*a,**kw:TransportFileResponse(200,{},url,path,0,'',b'','fixture',0)
            client=self.client(transport,gate)
            with tempfile.TemporaryDirectory() as tmp:
                if outcome=='redirect':
                    with self.assertRaises(RedirectPolicyError):client.download_to_path('https://archive.test/',Path(tmp)/'body',10000)
                else:client.download_to_path('https://archive.test/',Path(tmp)/'body',10000)
            self.assertFalse(gate.probe_required)
            self.assertFalse(gate.probe_inflight)
            self.assertGreater(gate.last_reachable,0)

    def test_bad_representation_releases_connectivity_probe_without_accepting_body(self):
        gate=SharedHostGate();gate.probe_required=True
        transport=Mock()
        transport.download.side_effect=TransportExhaustedError('u',[('httpx',httpx.RemoteProtocolError('incomplete chunked read'))])
        client=self.client(transport,gate)
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(TransientRequestError):client.download_to_path('https://archive.test/',Path(tmp)/'body',10000)
            self.assertFalse((Path(tmp)/'body').exists())
        self.assertFalse(gate.probe_required)

    def test_backend_cooldown_wait_can_be_stopped(self):
        transport=ResilientTransport(pool_size=1,connect_timeout=1,read_timeout=1,mode='httpx',trust_env=False)
        self.addCleanup(transport.close)
        transport.cooldown_until['httpx']=time.monotonic()+30
        stop=threading.Event();stop.set()
        with self.assertRaises(Stopped):transport._await_backend('httpx',stop)
        self.assertEqual(transport.recovery_probes,set())


class BrokenWireAndOwners(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server=ThreadingHTTPServer(('127.0.0.1',0),WireFixture)
        cls.thread=threading.Thread(target=cls.server.serve_forever,daemon=True);cls.thread.start()
        cls.url=f'http://127.0.0.1:{cls.server.server_port}'

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown();cls.server.server_close();cls.thread.join(2)

    def test_actual_short_bodies_and_chunks_take_one_turn_on_each_backend(self):
        names=['httpx','urllib3']+(['curl'] if shutil.which('curl') else [])
        for name in names:
            for route in ('/short','/chunked'):
                for method in ('get','download_to_path'):
                    with self.subTest(backend=name,route=route,method=method), tempfile.TemporaryDirectory() as tmp:
                        client=HttpClient(FixedRateLimiter(0),4,2,'test',threading.Event(),network_backend=name,trust_environment=False)
                        before=WireFixture.calls[route]
                        try:
                            with self.assertRaises(TransientRequestError) as caught:
                                if method=='get':client.get(self.url+route,10000)
                                else:client.download_to_path(self.url+route,Path(tmp)/'body',10000,retain_partial=False)
                            self.assertEqual(classify_exception(caught.exception)[0],'incomplete_representation')
                            self.assertEqual(WireFixture.calls[route]-before,1)
                            self.assertFalse((Path(tmp)/'body').exists())
                            self.assertNotIn(name,client.transport.cooldown_until)
                        finally:client.close()

    def seed(self, root, state='pending'):
        cfg=ProjectConfig(root,targets=['example.org/*'],keywords=[],from_date='2008',to_date='2008',
            workers=1,download_delay=0, max_attempts=1,
            network=NetworkConfig(backend='httpx',trust_environment=False,persistent_retries=True,
                                  retry_base_seconds=1,retry_max_seconds=1)).normalized()
        db=open_database(root)
        tid=get_or_create_target(db,cfg.targets[0]);sig=cdx_query_signature(cfg)
        upsert_capture(db,{'original':'http://example.org/user/a','timestamp':'20080101000000',
            'mimetype':'text/html','statuscode':'200','length':'10',
            'digest':base64.b32encode(hashlib.sha1(BODY).digest()).decode()},tid,sig)
        db.execute('UPDATE captures SET state=?',(state,))
        save_coverage_state(db,tid,sig,cfg.from_date,cfg.to_date,None,True,1,None)
        db.commit()
        return cfg,db

    def test_persistent_owner_recovers_short_transfer_without_manual_retry(self):
        reset_shared_traffic_state_for_tests();WireFixture.calls['/flaky']=0
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);cfg,db=self.seed(root);events=[]
            try:
                with patch('archive_scout.downloads.downloader.replay_url',return_value=self.url+'/flaky'):
                    outcome=run_project(cfg,'download_only',callback=events.append)
                self.assertEqual(outcome.status,'complete')
                self.assertEqual(WireFixture.calls['/flaky'],2)
                self.assertEqual(db.execute('SELECT download_attempts FROM captures').fetchone()[0],2)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM errors WHERE resolved=0').fetchone()[0],0)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM text_retry_schedule').fetchone()[0],0)
                progress=[e for e in events if 'attempt_outcomes' in e.detail]
                self.assertEqual([e.current for e in progress],[0,1])
                self.assertEqual([e.detail['new_saves'] for e in progress],[0,1])
                self.assertTrue(all(e.total==1 for e in progress))
                self.assertTrue(any(e.stage=='download_prepare' for e in events))
                self.assertGreaterEqual(progress[-1].detail['invocation_save_rate'],progress[-1].detail['operation_save_rate'])
            finally:db.close()

    def test_errors_outside_due_queue_never_count_as_completed_captures(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg,db=self.seed(Path(tmp),state='error')
            try:
                self.assertEqual(cumulative_download_progress(db,cfg,queued_total=0),(0,1))
                db.execute("UPDATE captures SET state='skipped'")
                self.assertEqual(cumulative_download_progress(db,cfg,queued_total=0),(1,1))
            finally:db.close()

    def test_explicit_retry_progress_does_not_sweep_historical_captures(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg,db=self.seed(Path(tmp),state='error')
            try:
                with db:
                    db.execute("""WITH RECURSIVE nums(i) AS(VALUES(1) UNION ALL SELECT i+1 FROM nums WHERE i<10000)
                        INSERT INTO captures(original_url,timestamp,target_id,query_signature,state,created_at,updated_at)
                        SELECT 'http://example.org/history/'||i,'20080101000000',1,?,'downloaded','now','now' FROM nums""",
                        (cdx_query_signature(cfg),))
                steps=[]
                def bounded_work():
                    steps.append(1)
                    return len(steps)>10  # fail a history sweep, independent of machine speed
                db.set_progress_handler(bounded_work,1000)
                self.assertEqual(cumulative_download_progress(db,cfg,queued_total=1,capture_ids=[1]),(0,1))
                self.assertLessEqual(len(steps),10)
            finally:
                db.set_progress_handler(None,0);db.close()

    def test_stop_after_failed_turn_then_resume_keeps_retry_and_saved_identity(self):
        reset_shared_traffic_state_for_tests();WireFixture.calls['/flaky']=0
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);cfg,db=self.seed(root);stop=threading.Event()
            def interrupt(event):
                if event.detail.get('attempt_errors')==1:stop.set()
            try:
                with patch('archive_scout.downloads.downloader.replay_url',return_value=self.url+'/flaky'):
                    with self.assertRaises(Stopped):run_project(cfg,'download_only',stop,interrupt)
                    self.assertEqual(db.execute('SELECT COUNT(*) FROM text_retry_schedule').fetchone()[0],1)
                    outcome=run_project(cfg,'resume')
                self.assertEqual(outcome.status,'complete')
                self.assertEqual(db.execute('SELECT COUNT(*) FROM captures').fetchone()[0],1)
                self.assertEqual(db.execute('SELECT download_attempts FROM captures').fetchone()[0],2)
                self.assertEqual(WireFixture.calls['/flaky'],2)
            finally:db.close()

    def test_local_scanning_completes_while_transport_is_cooling(self):
        from archive_scout.downloads.downloader import capture_path
        from archive_scout.database.repositories import get_or_create_keyword_set, start_scan_run
        from archive_scout.scanning.jobs import ScanJob
        from archive_scout.scanning.pending import scan_pending_saved
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);cfg,db=self.seed(root,state='downloaded_unscanned')
            transport=ResilientTransport(pool_size=1,connect_timeout=1,read_timeout=1,mode='httpx',trust_env=False)
            stop=threading.Event();waiting=threading.Event()
            def wait_network():
                waiting.set()
                try:transport._await_backend('httpx',stop)
                except Stopped:pass
            transport.cooldown_until['httpx']=time.monotonic()+30
            thread=threading.Thread(target=wait_network)
            try:
                path=capture_path(root,1,'20080101000000','http://example.org/user/a')
                path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(BODY)
                key=get_or_create_keyword_set(db,'test',['needle'])
                scan=start_scan_run(db,key,'scan',1,'all',{})
                job=ScanJob.create(scan,'test',['needle']);db.commit()
                thread.start();self.assertTrue(waiting.wait(2))
                self.assertEqual(scan_pending_saved(db,cfg,[job],threading.Event(),workers=1),1)
                self.assertTrue(thread.is_alive())
                self.assertEqual(db.execute('SELECT COUNT(*) FROM document_matches').fetchone()[0],1)
            finally:
                stop.set()
                if thread.ident:thread.join(2)
                transport.close();db.close()


if __name__=='__main__':
    unittest.main()
