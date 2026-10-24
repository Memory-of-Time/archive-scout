"""Real HTTP, malformed representations, scoped scheduling and explicit rechecks."""
from __future__ import annotations
import base64
import gzip
import hashlib
import json
import shutil
import socket
import sqlite3
import tempfile
import threading
import time
import unittest
import zlib
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch, Mock

import httpx
import urllib3
from archive_scout.cdx.client import HttpClient, RateLimitDeferred, is_timeout_error
from archive_scout.config import ProjectConfig, NetworkConfig
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.cdx.indexer import save_coverage_state
from archive_scout.database.connection import open_database, DATABASE_NAME
from archive_scout.database.repositories import get_or_create_target, upsert_capture, record_error, get_or_create_keyword_set, start_scan_run
from archive_scout.downloads.downloader import download_archive, _populate_download_scopes
from archive_scout.downloads.rate_limit import FixedRateLimiter, SharedHostGate, reset_shared_traffic_state_for_tests
from archive_scout.downloads.schedule import prepare_retry_queue, select_due_retry, record_failure, next_due
from archive_scout.downloads.retry import retry_error_urls
from archive_scout.downloads.validation import classify_exception
from archive_scout.downloads.diagnostics import AcquisitionDiagnostics, _safe_url
from archive_scout.network.transports import (_decoded_verified_chunks, PayloadDigestMismatch,
    IncompleteRepresentationError, TransportExhaustedError, is_transport_timeout,
    HttpxBackend, Urllib3Backend, CurlBackend, ResilientTransport)
from archive_scout.operations import _retryable_text_batches
from archive_scout.scanning.jobs import ScanJob

BODY = b'<html><title>Profile</title><body>needle complete archived evidence</body></html>'
DIGEST = base64.b32encode(hashlib.sha1(BODY).digest()).decode()


class Decoder(unittest.TestCase):
    def decode(self, wire, encoding, digest=DIGEST, length=None):
        headers={'content-encoding':encoding,'content-length':str(len(wire) if length is None else length)}
        chunks=(wire[i:i+1] for i in range(len(wire)))
        return b''.join(_decoded_verified_chunks(chunks,headers,threading.Event(),expected_digest=digest))
    def test_all_layers_are_removed_in_reverse_order(self):
        for encoding,wire in [('gzip, gzip',gzip.compress(gzip.compress(BODY))),
                              ('gzip, deflate',zlib.compress(gzip.compress(BODY))),
                              ('deflate, gzip',gzip.compress(zlib.compress(BODY))),
                              ('identity, gzip',gzip.compress(BODY))]:
            with self.subTest(encoding=encoding):self.assertEqual(self.decode(wire,encoding),BODY)
    def test_unknown_inner_encoding_cannot_become_terminal_binary(self):
        with self.assertRaises(IncompleteRepresentationError):self.decode(gzip.compress(BODY),'br, gzip')
    def test_truncated_inner_member_is_rejected(self):
        with self.assertRaises(IncompleteRepresentationError):self.decode(gzip.compress(gzip.compress(BODY)[:-3]),'gzip, gzip')
    def test_chain_limit_and_wire_length_are_checked(self):
        with self.assertRaises(IncompleteRepresentationError):self.decode(BODY,','.join(['identity']*5))
        with self.assertRaises(IncompleteRepresentationError):self.decode(gzip.compress(gzip.compress(BODY)),'gzip, gzip',length=9999)
    def test_header_repair_still_requires_exact_digest_and_keeps_diagnostic(self):
        self.assertEqual(self.decode(BODY,'gzip'),BODY)
        with self.assertRaises(PayloadDigestMismatch) as caught:self.decode(BODY,'gzip','A'*32)
        detail=caught.exception.representation_diagnostic
        self.assertEqual(detail['expected_cdx_digest'],'A'*32)
        self.assertEqual(detail['actual_candidate_digest'],DIGEST)
        self.assertEqual(base64.b64decode(detail['raw_prefix_base64']),BODY)
    def test_error_category_remains_connection_for_refusal_and_dns(self):
        errors=[urllib3.exceptions.NewConnectionError(None,'actively refused'),
                urllib3.exceptions.NameResolutionError('archive.test',None,socket.gaierror('DNS unavailable'))]
        for error in errors:
            wrapped=TransportExhaustedError('u',[('urllib3',error)])
            for value in (error,wrapped):
                self.assertEqual(classify_exception(value),('connection',None,True))
                self.assertFalse(is_transport_timeout(value));self.assertFalse(is_timeout_error(value))
        self.assertEqual(classify_exception(urllib3.exceptions.ConnectTimeoutError('actual timeout'))[0],'connect_timeout')


class Fixture(BaseHTTPRequestHandler):
    protocol_version='HTTP/1.1'
    def do_GET(self):
        if self.path=='/503':
            self.send_response(503);self.send_header('Retry-After','60');self.send_header('Content-Length','99999999');self.end_headers();self.close_connection=True;return
        wire=BODY;encoding='gzip'
        if self.path=='/stacked':wire=gzip.compress(gzip.compress(BODY));encoding='gzip, gzip'
        if self.path=='/wrong':wire=BODY.replace(b'needle',b'xxxxxx')
        self.send_response(200);self.send_header('Content-Type','text/html');self.send_header('Content-Encoding',encoding)
        self.send_header('Content-Length',str(len(wire)));self.send_header('Set-Cookie','secret=never-log');self.end_headers();self.wfile.write(wire)
    def log_message(self,*args):pass


class RealHTTP(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server=ThreadingHTTPServer(('127.0.0.1',0),Fixture)
        cls.thread=threading.Thread(target=cls.server.serve_forever,daemon=True);cls.thread.start()
    @classmethod
    def tearDownClass(cls):cls.server.shutdown();cls.server.server_close();cls.thread.join(3)
    def url(self,route):return f'http://127.0.0.1:{self.server.server_port}{route}'
    def test_real_backends_decode_stack_and_reject_mismatch(self):
        backends=[HttpxBackend(2,2,2,trust_env=False),Urllib3Backend(2,2,2,trust_env=False)]
        if shutil.which('curl'):backends.append(CurlBackend(2,2,trust_env=False))
        with tempfile.TemporaryDirectory() as tmp:
            for backend in backends:
                try:
                    dest=Path(tmp)/backend.name
                    response=backend.download(self.url('/stacked'),{},dest,10000,threading.Event(),expected_digest=DIGEST)
                    self.assertEqual(dest.read_bytes(),BODY)
                    with self.assertRaises(PayloadDigestMismatch) as caught:
                        backend.download(self.url('/wrong'),{},dest,10000,threading.Event(),expected_digest=DIGEST)
                    self.assertFalse(dest.exists())
                    detail=caught.exception.transfer_diagnostic
                    self.assertEqual(detail['response_status'],200)
                    self.assertNotIn('set-cookie',detail['response_headers'])
                    self.assertEqual(base64.b64decode(detail['decoded_prefix_base64']),BODY.replace(b'needle',b'xxxxxx'))
                finally:backend.close()
    def test_retry_after_observed_without_reading_error_body_or_changing_policy(self):
        samples=[];client=HttpClient(FixedRateLimiter(0),1,2,'test',threading.Event(),
            network_backend='httpx',trust_environment=False,host_gate=SharedHostGate(),
            rate_limit_attempts=1,response_callback=samples.append)
        try:
            with tempfile.TemporaryDirectory() as tmp:
                with self.assertRaises(RateLimitDeferred):client.download_to_path(self.url('/503'),Path(tmp)/'body',10000,retain_partial=False)
            self.assertEqual(samples[0]['status'],503);self.assertEqual(samples[0]['retry_after'],'60')
            self.assertGreater(samples[0]['pause_seconds'],59)
        finally:client.close()
    def test_control_status_precedes_announced_size_on_each_backend(self):
        backends=[HttpxBackend(2,2,2,trust_env=False),Urllib3Backend(2,2,2,trust_env=False)]
        if shutil.which('curl'):backends.append(CurlBackend(2,2,trust_env=False))
        with tempfile.TemporaryDirectory() as tmp:
            for backend in backends:
                try:
                    result=backend.download(self.url('/503'),{},Path(tmp)/backend.name,10000,threading.Event())
                    self.assertEqual(result.status,503);self.assertEqual(result.bytes_written,0)
                finally:backend.close()
    def test_end_to_end_scans_stacked_text_but_keeps_bad_capture_unfinished(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);db=open_database(root)
            cfg=ProjectConfig(root,targets=['example.org/*'],keywords=['needle'],from_date='2008',to_date='2008',workers=2,download_delay=0,
                network=NetworkConfig(backend='httpx',trust_environment=False)).normalized()
            target=get_or_create_target(db,cfg.targets[0]);sig=cdx_query_signature(cfg)
            with db:
                for name in ['stacked','wrong']:upsert_capture(db,{'timestamp':'20080101000000','original':'http://example.org/'+name,'mimetype':'text/html','statuscode':'200','digest':DIGEST,'length':len(BODY)},target,sig)
                save_coverage_state(db,target,sig,cfg.from_date,cfg.to_date,None,True,2,None)
                kid=get_or_create_keyword_set(db,'test',['needle']);run=start_scan_run(db,kid,'test',1,'all',{})
            reset_shared_traffic_state_for_tests()
            with patch('archive_scout.downloads.downloader.replay_url',side_effect=lambda timestamp,url:self.url('/'+url.rsplit('/',1)[-1])):
                download_archive(cfg,db,run,threading.Event(),lambda event:None,scan_jobs=[ScanJob.create(run,'test',['needle'])])
            states={r['original_url'].rsplit('/',1)[-1]:r['state'] for r in db.execute('SELECT original_url,state FROM captures')}
            self.assertEqual(states,{'stacked':'downloaded','wrong':'error'})
            self.assertEqual(db.execute('SELECT COUNT(*) FROM document_matches WHERE score>0').fetchone()[0],1)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM text_retry_schedule').fetchone()[0],1)
            samples=list((root/'diagnostics/text-acquisition').glob('sample-*.json'));self.assertEqual(len(samples),1)
            value=json.loads(samples[0].read_text());self.assertNotEqual(value['evidence']['actual_candidate_digest'],DIGEST)
            self.assertEqual(db.execute("SELECT category FROM errors WHERE resolved=0").fetchone()[0],'payload_digest_mismatch')
            db.close()


class RetryQueue(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name);self.db=open_database(self.root)
        self.cfg=ProjectConfig(self.root,targets=['active.test/*'],keywords=['needle']).normalized()
        self.target=get_or_create_target(self.db,'active.test/*');self.other=get_or_create_target(self.db,'other.test/*');self.sig=cdx_query_signature(self.cfg)
        _populate_download_scopes(self.db,self.cfg)
        self.db.execute('CREATE TEMP TABLE archive_scout_download_queue(id INTEGER PRIMARY KEY) WITHOUT ROWID')
    def tearDown(self):self.db.close();self.tmp.cleanup()
    def add(self,*,target=None,category='connection',retryable=True,due=1,state='error'):
        with self.db:
            upsert_capture(self.db,{'timestamp':'20080101000000','original':f'http://active.test/{self.db.total_changes}','mimetype':'text/html','statuscode':'200','length':10},target or self.target,self.sig)
            cid=int(self.db.execute('SELECT MAX(id) FROM captures').fetchone()[0])
            self.db.execute('UPDATE captures SET state=? WHERE id=?',(state,cid))
            record_error(self.db,'download',category,'fixture',capture_id=cid,retryable=retryable)
            if retryable:self.db.execute("INSERT INTO text_retry_schedule(capture_id,next_eligible_at,failure_count,last_category,updated_at) VALUES(?,?,1,?,'now')",(cid,due,category))
        return cid
    def test_scope_queue_deadline_error_filters_and_incremental_failure(self):
        outside=self.add(target=self.other);queued=self.add();future=self.add(due=time.time()+1000);valid=self.add()
        self.db.execute('INSERT INTO archive_scout_download_queue VALUES(?)',(queued,));self.db.commit()
        prepare_retry_queue(self.db)
        self.assertEqual(select_due_retry(self.db,100)['id'],valid);self.assertIsNone(select_due_retry(self.db,100))
        record_failure(self.db,valid,'connection',None,True,1,1)
        self.assertEqual(select_due_retry(self.db,time.time()+10)['id'],valid)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM text_retry_schedule').fetchone()[0],4)
    def test_stale_wal_success_cannot_be_readmitted(self):
        cid=self.add();prepare_retry_queue(self.db);self.db.commit()
        other=sqlite3.connect(self.root/DATABASE_NAME)
        try:
            with other:other.execute("UPDATE captures SET state='downloaded' WHERE id=?",(cid,));other.execute('DELETE FROM text_retry_schedule WHERE capture_id=?',(cid,))
            self.assertIsNone(select_due_retry(self.db,100))
        finally:other.close()
    def test_changed_external_deadline_is_preserved(self):
        cid=self.add();prepare_retry_queue(self.db);self.db.commit()
        other=sqlite3.connect(self.root/DATABASE_NAME)
        try:
            with other:other.execute('UPDATE text_retry_schedule SET next_eligible_at=500 WHERE capture_id=?',(cid,))
            self.assertIsNone(select_due_retry(self.db,100));self.assertEqual(select_due_retry(self.db,600)['id'],cid)
        finally:other.close()
    def test_queue_rebuild_after_reopen_preserves_persistent_work(self):
        cid=self.add();prepare_retry_queue(self.db);self.db.commit();self.db.close();self.db=open_database(self.root)
        _populate_download_scopes(self.db,self.cfg);self.db.execute('CREATE TEMP TABLE archive_scout_download_queue(id INTEGER PRIMARY KEY) WITHOUT ROWID')
        prepare_retry_queue(self.db);self.assertEqual(select_due_retry(self.db,100)['id'],cid)
    def test_selected_404_is_rechecked_once_without_reopening_other_permanent_errors(self):
        missing=self.add(category='missing_capture',retryable=False);blocked=self.add(category='robots_blocked',retryable=False)
        self.assertEqual(list(_retryable_text_batches(self.cfg,self.db)),[])
        chosen=replace(self.cfg,retry_capture_ids=[missing,blocked])
        self.assertEqual(list(_retryable_text_batches(chosen,self.db,respect_selection=True)),[[missing]])
        with patch('archive_scout.downloads.retry.download_archive') as download:
            retry_error_urls(chosen,self.db,1,threading.Event(),None)
            self.assertEqual(download.call_count,1);self.assertEqual(download.call_args.kwargs['capture_ids'],[missing])
        self.assertEqual(self.db.execute('SELECT retryable FROM errors WHERE capture_id=?',(missing,)).fetchone()[0],0)
        category=replace(self.cfg,retry_error_categories=['missing_capture'])
        self.assertEqual(list(_retryable_text_batches(category,self.db,respect_selection=True)),[[missing]])
    def test_queued_history_is_not_rescanned_on_each_probe(self):
        with self.db:
            self.db.execute('''WITH RECURSIVE n(i) AS(VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<10000)
                INSERT INTO captures(original_url,timestamp,target_id,query_signature,mimetype,statuscode,state,created_at,updated_at)
                SELECT 'http://active.test/history/'||i,'20080101000000',?,?,'text/html','200','error','now','now' FROM n''',(self.target,self.sig))
            self.db.execute("INSERT INTO text_retry_schedule SELECT id,1,1,'connection',NULL,'now' FROM captures")
            self.db.execute("""INSERT INTO errors(capture_id,operation,category,message,retryable,first_seen,last_seen)
                SELECT id,'download','connection','fixture',1,'now','now' FROM captures""")
            self.db.execute('INSERT INTO archive_scout_download_queue SELECT id FROM captures')
        prepare_retry_queue(self.db);instructions=[0]
        def progress():instructions[0]+=1;return int(instructions[0]>500)
        self.db.set_progress_handler(progress,1)
        try:
            for _ in range(5):self.assertIsNone(select_due_retry(self.db,100))
        finally:self.db.set_progress_handler(None,0)


class Diagnostics(unittest.TestCase):
    def test_samples_and_headers_are_bounded_and_do_not_change_acceptance(self):
        with tempfile.TemporaryDirectory() as tmp:
            diag=AcquisitionDiagnostics(Path(tmp),None)
            error=IncompleteRepresentationError('fixture')
            error.representation_diagnostic={'raw_prefix_base64':base64.b64encode(b'x'*4096).decode()}
            for cid in range(1000):diag.failure({'id':cid,'timestamp':'20080101000000','original_url':'http://user:password@example.test/?api_key=secret','digest':DIGEST},'incomplete_representation',error)
            paths=list((Path(tmp)/'diagnostics/text-acquisition').glob('sample-*.json'))
            self.assertLessEqual(len(paths),16);self.assertTrue(paths)
            for p in paths:
                self.assertLessEqual(p.stat().st_size,diag.MAX_SAMPLE_BYTES);self.assertNotIn('password',p.read_text());self.assertNotIn('=secret',p.read_text())
            self.assertEqual(diag.failures['incomplete_representation'],1000)
    def test_disabled_diagnostics_creates_no_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            diag=AcquisitionDiagnostics(Path(tmp),None,enabled=False)
            diag.failure({'id':1},'connection',OSError('refused'));diag.summary(0,force=True)
            self.assertEqual(list(Path(tmp).iterdir()),[])
    def test_http_control_history_is_bounded_and_retains_server_retry_after(self):
        with tempfile.TemporaryDirectory() as tmp:
            diag=AcquisitionDiagnostics(Path(tmp),None)
            for _ in range(100):diag.response({'status':503,'retry_after':'60','pause_seconds':60,'url':'http://example.test/'})
            diag.summary(0,force=True);value=json.loads((Path(tmp)/'diagnostics/text-acquisition/latest.json').read_text())
            self.assertEqual(value['response_counts'],{'503':100});self.assertEqual(len(value['recent_control_responses']),8)
            self.assertEqual(value['recent_control_responses'][-1]['retry_after'],'60')
    def test_sample_write_failure_does_not_abort_acquisition(self):
        with tempfile.TemporaryDirectory() as tmp,patch.object(Path,'mkdir',side_effect=OSError('full')):
            AcquisitionDiagnostics(Path(tmp),None).failure({'id':1,'timestamp':'20080101000000','original_url':'http://example.test/'},'connection',OSError('refused'))
    def test_expired_backend_cooldown_never_displays_negative(self):
        t=ResilientTransport(pool_size=1,connect_timeout=1,read_timeout=1,mode='httpx',trust_env=False)
        try:t.cooldown_until['httpx']=time.monotonic()-500;self.assertEqual(t.snapshot()['cooldown_seconds'],0)
        finally:t.close()


if __name__=='__main__':unittest.main()
