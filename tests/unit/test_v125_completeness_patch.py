"""Publication-crash, proof, retry-query and GUI sampling regressions."""
from __future__ import annotations

import base64
import hashlib
import json
import os
import queue
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import Mock, patch

from archive_scout.config import ProjectConfig
from archive_scout.cdx.indexer import save_coverage_state
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, upsert_capture, record_error
from archive_scout.downloads.checkpoint import write_receipt, receipt_path, prove_file
from archive_scout.downloads.downloader import capture_path
from archive_scout.downloads.rate_limit import reset_shared_traffic_state_for_tests
from archive_scout.events import ProgressEvent, Stopped
from archive_scout.scanning.pending import adopt_orphaned_bodies, _scan_one, PayloadIntegrityError, scan_pending_saved
from archive_scout.operations import run_project
from archive_scout.ui.event_queue import CoalescingEventQueue
from archive_scout.ui.main_window import ArchiveScoutApp

BODY=b'<html><title>Preserved</title><body>needle '+b'complete evidence '*200+b'</body></html>'
CDX=base64.b32encode(hashlib.sha1(BODY).digest()).decode()


class ProofRecovery(unittest.TestCase):
    def setUp(self):
        reset_shared_traffic_state_for_tests()
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
        self.cfg=ProjectConfig(self.root,targets=['example.org/*'],keywords=['needle'],download_delay=0).normalized()
        self.db=open_database(self.root);tid=get_or_create_target(self.db,self.cfg.targets[0]);sig=cdx_query_signature(self.cfg)
        upsert_capture(self.db,{'original':'http://example.org/a','timestamp':'20080101000000',
            'mimetype':'text/html','statuscode':'200','digest':CDX,'length':len(BODY)},tid,sig)
        self.db.execute("UPDATE captures SET download_attempts=1")
        save_coverage_state(self.db,tid,sig,self.cfg.from_date,self.cfg.to_date,None,True,1,None)
        self.db.commit();self.row=self.db.execute('SELECT * FROM captures').fetchone()
        self.path=capture_path(self.root,1,self.row['timestamp'],self.row['original_url']);self.path.parent.mkdir(parents=True,exist_ok=True)
    def tearDown(self):self.db.close();self.tmp.cleanup()
    def receipt(self):
        write_receipt(self.path,self.row,{'content_hash':hashlib.sha256(BODY).hexdigest(),
            'bytes_saved':len(BODY),'http_status':200,'response_content_type':'text/html','final_url':'https://web.archive.org/web/20080101000000id_/http://example.org/a'})
    def test_truncated_orphan_is_preserved_and_remains_unfinished(self):
        self.path.write_bytes(BODY[:-200])
        self.assertEqual(adopt_orphaned_bodies(self.db,self.cfg,threading.Event()),0)
        self.assertEqual(self.db.execute('SELECT state FROM captures').fetchone()[0],'error')
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM capture_payload_evidence').fetchone()[0],0)
        self.assertFalse(self.path.exists())
        self.assertEqual(len(list(self.path.parent.glob('*.unverified-*'))),1)
        self.assertEqual(tuple(self.db.execute('SELECT operation,category,retryable FROM errors').fetchone()),
                         ('download','local_integrity_unproven',1))
    def test_matching_cdx_recovers_legacy_bytes_and_persists_sha256(self):
        self.path.write_bytes(BODY)
        self.assertEqual(adopt_orphaned_bodies(self.db,self.cfg,threading.Event()),1)
        self.assertEqual(self.db.execute('SELECT content_hash FROM capture_payload_evidence').fetchone()[0],hashlib.sha256(BODY).hexdigest())
        self.assertEqual(adopt_orphaned_bodies(self.db,self.cfg,threading.Event()),0)
    def test_receipt_recovers_without_cdx_digest_and_is_cleaned_after_commit(self):
        self.db.execute("UPDATE captures SET digest=''");self.db.commit()
        self.row=self.db.execute('SELECT * FROM captures').fetchone();self.path.write_bytes(BODY);self.receipt()
        self.assertEqual(adopt_orphaned_bodies(self.db,self.cfg,threading.Event()),1)
        self.assertFalse(receipt_path(self.path).exists())
    def test_receipt_for_other_capture_and_invalid_json_cannot_prove_file(self):
        self.path.write_bytes(BODY);row=dict(self.row);row['digest']=''
        self.receipt();v=json.loads(receipt_path(self.path).read_text());v['original_url']='http://example.org/other'
        receipt_path(self.path).write_text(json.dumps(v))
        self.assertIsNone(prove_file(self.path,row))
        receipt_path(self.path).write_text('{broken')
        self.assertIsNone(prove_file(self.path,row))
    def test_existing_sha256_mismatch_cannot_fall_back_to_cdx(self):
        self.path.write_bytes(BODY)
        self.assertIsNone(prove_file(self.path,self.row,expected_hash='0'*64,expected_size=len(BODY)))
    def test_scanner_rejects_unproven_body_even_if_text_is_readable(self):
        self.path.write_bytes(BODY[:-200]);row=dict(self.row);row['path']=str(self.path)
        with self.assertRaises(PayloadIntegrityError):_scan_one(row,[])
    def test_unproven_unscanned_body_is_a_scan_gap(self):
        self.path.write_bytes(BODY[:-200]);self.db.execute("UPDATE captures SET state='downloaded_unscanned'");self.db.commit()
        adopt_orphaned_bodies(self.db,self.cfg,threading.Event())
        self.assertEqual(tuple(self.db.execute('SELECT operation,retryable FROM errors').fetchone()),('scan',1))
        self.assertEqual(self.db.execute('SELECT state FROM captures').fetchone()[0],'error')
    def crash(self,boundary):
        # Real process exit: neither finally blocks nor DB cleanup can run.
        script=r'''
import hashlib,os,sys
from pathlib import Path
from unittest.mock import patch
from archive_scout.database.connection import open_database
from archive_scout.config import ProjectConfig
from archive_scout.downloads.downloader import fetch_retain_streamed,capture_path,save_success
root=Path(sys.argv[1]);phase=sys.argv[2];db=open_database(root)
cfg=ProjectConfig(root,targets=['example.org/*'],keywords=['needle']).normalized()
row=db.execute('SELECT * FROM captures').fetchone();body=bytes.fromhex(sys.argv[3])
# Match the normalized destination used by fetch_retain_streamed. macOS /var
# aliases and Windows short paths can otherwise make the exit hook miss it.
path=capture_path(cfg.output_dir,1,row['timestamp'],row['original_url']);original=os.replace
def replace(source,destination):
 if Path(destination)==path and phase=='before_publish':os._exit(71)
 original(source,destination)
 if Path(destination)==path and phase=='after_publish':os._exit(72)
class Client:
 def download_to_path(self,url,destination,*a,**kw):
  Path(destination).write_bytes(body)
  return {'bytes':len(body),'content_hash':hashlib.sha256(body).hexdigest(),'preview':body[:65536],
    'headers':{'Content-Type':'text/html'},'status':200,'final_url':url}
with patch('archive_scout.downloads.downloader.os.replace',side_effect=replace):result=fetch_retain_streamed(row,cfg,Client())
if phase=='after_sql':
 with patch('archive_scout.downloads.downloader.discard_receipt',side_effect=lambda path:os._exit(73)):save_success(db,result)
'''
        # Exercise an alternate spelling on every OS, including Linux where
        # temporary-directory paths otherwise often happen to be canonical.
        alias_parent = self.root / 'crash-path-alias'
        alias_parent.mkdir(exist_ok=True)
        project_arg = alias_parent / '..'
        result=subprocess.run([sys.executable,'-c',script,str(project_arg),boundary,BODY.hex()],capture_output=True,text=True,timeout=15)
        self.assertEqual(result.returncode,{'before_publish':71,'after_publish':72,'after_sql':73}[boundary],result.stderr)
        if boundary=='after_sql':
            self.assertEqual(adopt_orphaned_bodies(self.db,self.cfg,threading.Event()),0)
            self.assertEqual(scan_pending_saved(self.db,self.cfg,[],threading.Event(),workers=1),1)
        else:
            self.assertEqual(adopt_orphaned_bodies(self.db,self.cfg,threading.Event()),1)
        self.assertEqual(self.path.read_bytes(),BODY)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM capture_payload_evidence').fetchone()[0],1)
        self.assertFalse(receipt_path(self.path).exists())
    def test_process_exit_after_receipt_before_publication_recovers_staging(self):self.crash('before_publish')
    def test_process_exit_after_publication_before_sql_recovers_body(self):self.crash('after_publish')
    def test_process_exit_after_sql_before_cleanup_preserves_evidence(self):self.crash('after_sql')
    def test_unreadable_unproven_saved_body_remains_a_scan_gap(self):
        self.path.write_bytes(BODY)
        self.db.execute("UPDATE captures SET state='downloaded_unscanned'");self.db.commit()
        with patch('archive_scout.downloads.checkpoint.prove_file',side_effect=PermissionError('denied')):
            adopt_orphaned_bodies(self.db,self.cfg,threading.Event())
        self.assertEqual(self.db.execute('SELECT state FROM captures').fetchone()[0],'error')
        self.assertEqual(self.db.execute('SELECT operation FROM errors').fetchone()[0],'scan')
    def test_receipt_disk_full_does_not_publish_success(self):
        from archive_scout.downloads.downloader import fetch_retain_streamed
        import errno
        class Client:
            def download_to_path(_,url,path,*args,**kwargs):
                Path(path).write_bytes(BODY)
                return {'bytes':len(BODY),'content_hash':hashlib.sha256(BODY).hexdigest(),'preview':BODY,
                        'headers':{'Content-Type':'text/html'},'status':200,'final_url':url}
        with patch('archive_scout.downloads.checkpoint.atomic_write_bytes',side_effect=OSError(errno.ENOSPC,'full')):
            with self.assertRaises(OSError):fetch_retain_streamed(self.row,self.cfg,Client())
        self.assertFalse(self.path.exists())
    def test_bad_orphan_reacquires_automatically_and_only_full_bytes_complete(self):
        self.path.write_bytes(BODY[:-200]);self.receipt()
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200);self.send_header('Content-Type','text/html')
                self.send_header('Content-Length',str(len(BODY)));self.end_headers();self.wfile.write(BODY)
            def log_message(self,*args):pass
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        cfg=replace(self.cfg,network=replace(self.cfg.network,backend='httpx',trust_environment=False))
        try:
            with patch('archive_scout.downloads.downloader.replay_url',return_value=f'http://127.0.0.1:{server.server_port}/body'):
                result=run_project(cfg,'download')
            self.assertEqual(result.status,'complete')
            self.assertEqual(self.path.read_bytes(),BODY)
            self.assertEqual(self.db.execute('SELECT COUNT(*) FROM captures').fetchone()[0],1)
            self.assertEqual(self.db.execute('SELECT COUNT(*) FROM document_matches').fetchone()[0],1)
            self.assertEqual(self.db.execute('SELECT COUNT(*) FROM errors WHERE resolved=0').fetchone()[0],0)
            self.assertFalse(receipt_path(self.path).exists())
        finally:server.shutdown();server.server_close();thread.join(3)
    def test_failed_owner_commit_recovers_locally_without_scanner_error_race(self):
        cfg=replace(self.cfg,network=replace(self.cfg.network,backend='httpx',trust_environment=False))
        def fetch(row,config,client):
            self.path.write_bytes(BODY);self.receipt()
            return {'capture_id':1,'path':self.path,'bytes_saved':len(BODY),
                'content_hash':hashlib.sha256(BODY).hexdigest(),'http_status':200,
                'final_url':'https://web.archive.org/web/20080101000000id_/http://example.org/a','response_content_type':'text/html','deferred_scan':True}
        stop=threading.Event();timer=threading.Timer(10,stop.set);timer.start()
        try:
            with patch('archive_scout.downloads.downloader.fetch_retain_streamed',side_effect=fetch) as network, \
                 patch('archive_scout.downloads.downloader.save_success',side_effect=sqlite3.OperationalError('injected commit failure')):
                result=run_project(cfg,'download',stop)
        finally:timer.cancel()
        self.assertEqual(result.status,'complete');self.assertEqual(network.call_count,1)
        self.assertEqual(self.db.execute('SELECT state FROM captures').fetchone()[0],'downloaded')
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM errors WHERE resolved=0').fetchone()[0],0)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM text_retry_schedule').fetchone()[0],0)
        self.assertFalse(receipt_path(self.path).exists())
    def test_rescan_resolves_error_recorded_before_document_existed(self):
        self.path.write_bytes(BODY)
        adopt_orphaned_bodies(self.db,self.cfg,threading.Event())
        scan_pending_saved(self.db,self.cfg,[],threading.Event(),workers=1)
        record_error(self.db,'scan','scan_commit_failure','injected previous failure',capture_id=1,retryable=True)
        self.db.commit()
        self.assertEqual(run_project(self.cfg,'rescan').status,'complete')
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM errors WHERE resolved=0').fetchone()[0],0)


class DisplayQueue(unittest.TestCase):
    def test_cadence_is_display_only_and_flushes_before_stop(self):
        for interval in (0,.5,1):
            q=CoalescingEventQueue(max_events=8);q.set_acquisition_interval(interval)
            with patch('archive_scout.ui.event_queue.time.monotonic',return_value=0):
                q.put_acquisition(1);self.assertEqual(q.get_nowait(),('progress',1))
                for n in range(2,1000):q.put_acquisition(n)
                if interval:
                    with self.assertRaises(queue.Empty):q.get_nowait()
                else:
                    self.assertEqual(q.get_nowait(),('progress',999))
                q.put(('stopped',None))
                if interval:
                    self.assertEqual(q.get_nowait(),('progress',999))
                self.assertEqual(q.get_nowait(),('stopped',None))
    def test_idle_timer_delivers_latest_at_selected_interval(self):
        q=CoalescingEventQueue();q.set_acquisition_interval(.5)
        with patch('archive_scout.ui.event_queue.time.monotonic',return_value=0):
            q.put_acquisition(1);q.get_nowait();q.put_acquisition(2)
        with patch('archive_scout.ui.event_queue.time.monotonic',return_value=.5):
            self.assertEqual(q.get_nowait(),('progress',2))
    def test_notice_cannot_be_replaced_by_routine_progress(self):
        q=CoalescingEventQueue();q.put_notice('diagnostic');q.put_acquisition('latest')
        self.assertEqual(q.get_nowait(),('notice','diagnostic'))
        self.assertEqual(q.get_nowait(),('progress','latest'))
    def test_overflow_is_bounded_and_explicit_without_blocking_producer(self):
        q=CoalescingEventQueue(max_events=8)
        for n in range(10000):q.put_notice(n);q.put_acquisition(n)
        self.assertEqual(q.qsize(),8)
        for n in range(8):self.assertEqual(q.get_nowait(),('notice',n))
        self.assertEqual(q.get_nowait(),('notices_dropped',9992))
        self.assertEqual(q.get_nowait(),('progress',9999))
    def test_gui_callback_routes_notice_separately_without_tk(self):
        app=ArchiveScoutApp.__new__(ArchiveScoutApp);app.events=CoalescingEventQueue()
        event=ProgressEvent('download','count',detail={'attempt_outcomes':1})
        app.on_engine_event(event);app.on_engine_event(ProgressEvent('site_issue','failure'))
        self.assertEqual(app.events.get_nowait()[0],'notice')
        self.assertEqual(app.events.get_nowait()[1],event)
        self.assertEqual(event.detail['attempt_outcomes'],1)
    def test_invalid_cadence_is_rejected_and_project_reset_drops_old_snapshot(self):
        q=CoalescingEventQueue()
        with self.assertRaises(ValueError):q.set_acquisition_interval(-1)
        q.set_acquisition_interval(0)
        with patch('archive_scout.ui.event_queue.time.monotonic',return_value=0):
            q.put_acquisition(20);self.assertEqual(q.get_nowait(),('progress',20))
            q.put_acquisition(21);self.assertEqual(q.get_nowait(),('progress',21))
        q.put_acquisition(10);q.reset_acquisition()
        with self.assertRaises(queue.Empty):q.get_nowait()


if __name__ == "__main__":
    unittest.main()
