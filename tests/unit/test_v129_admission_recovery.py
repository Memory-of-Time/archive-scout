from __future__ import annotations

import hashlib
import os
import queue
import sqlite3
import tempfile
import threading
import time
import unittest
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from archive_scout.cdx.indexer import save_coverage_state
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.config import NetworkConfig, ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import (get_or_create_target, upsert_captures,
    get_or_create_keyword_set, start_scan_run, record_error)
from archive_scout.downloads.downloader import download_archive
from archive_scout.downloads.rate_limit import reset_shared_traffic_state_for_tests
from archive_scout.downloads.schedule import record_failure
from archive_scout.events import ProgressEvent, Stopped
from archive_scout.network.transports import ResilientTransport
from archive_scout.scanning.jobs import ScanJob
from archive_scout.ui.event_queue import CoalescingEventQueue
from archive_scout.ui.main_window import ArchiveScoutApp

BODY = b'<html><body>needle intact evidence</body></html>'


class AdmissionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        reset_shared_traffic_state_for_tests()
        self.addCleanup(reset_shared_traffic_state_for_tests)
        self.requests = []
        owner = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'
            def do_GET(self):
                owner.requests.append(self.path)
                self.send_response(200)
                self.send_header('Content-Type', 'text/html')
                self.send_header('Content-Length', str(len(BODY)))
                self.end_headers()
                self.wfile.write(BODY)
            def log_message(self, *_):
                pass

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.cfg = ProjectConfig(self.root, ['example.test/*'], ['needle'],
            from_date='2001', to_date='2001', workers=4, download_delay=.002,
            network=NetworkConfig(backend='httpx', trust_environment=False)).normalized()
        self.db = open_database(self.root)
        self.addCleanup(self.db.close)
        signature = cdx_query_signature(self.cfg)
        with self.db:
            target = get_or_create_target(self.db, self.cfg.targets[0])
            upsert_captures(self.db, (('20010101000000', f'http://example.test/{n}',
                'text/html','200','',len(BODY)) for n in range(67)), target, signature)
            save_coverage_state(self.db,target,signature,self.cfg.from_date,self.cfg.to_date,None,True,67,None)
            kid = get_or_create_keyword_set(self.db,'fixture',['needle'])
            self.run_id = start_scan_run(self.db,kid,'fixture',1,'all',{})
        self.jobs = [ScanJob.create(self.run_id,'fixture',['needle'])]

    def run_download(self, stop=None, callback=None):
        with patch('archive_scout.downloads.downloader.replay_url',
            side_effect=lambda stamp,url:f'http://127.0.0.1:{self.server.server_port}/'+url.rsplit('/',1)[1]):
            download_archive(self.cfg,self.db,self.run_id,stop or threading.Event(),callback,
                             scan_jobs=self.jobs)

    def test_original_temp_write_read_promotion_reproduces_snapshot_busy(self):
        with closing(sqlite3.connect(self.root/'archive_scout.sqlite3')) as other:
            self.db.execute('CREATE TEMP TABLE probe(id INTEGER)')
            self.db.execute('INSERT INTO probe VALUES(1)')
            self.db.execute('SELECT state FROM captures WHERE id=1').fetchall()
            other.execute("UPDATE captures SET updated_at='external' WHERE id=1")
            other.commit()
            with self.assertRaises(sqlite3.OperationalError) as caught:
                self.db.execute("UPDATE captures SET state='downloading' WHERE id=1")
            self.assertEqual(caught.exception.sqlite_errorcode, sqlite3.SQLITE_BUSY_SNAPSHOT)
            self.db.rollback()

    def test_busy_after_selection_requeues_fresh_and_due_rows_without_extra_attempts(self):
        with self.db:
            for cid in (65,66,67):
                self.db.execute("UPDATE captures SET state='error' WHERE id=?",(cid,))
                record_error(self.db,'download','connection','temporary',capture_id=cid,retryable=True)
                record_failure(self.db,cid,'connection',None,True,1,1)
            self.db.execute('UPDATE text_retry_schedule SET next_eligible_at=0')
        db_path = self.root/'archive_scout.sqlite3'
        self.db.close()
        class BusyConnection(sqlite3.Connection):
            injected = False
            def executemany(connection, sql, args):
                if not connection.injected and sql.startswith("UPDATE captures SET state='downloading'"):
                    connection.injected = True
                    error = sqlite3.OperationalError('database is locked')
                    error.sqlite_errorcode = sqlite3.SQLITE_BUSY_SNAPSHOT
                    raise error
                return super().executemany(sql,args)
        self.db = sqlite3.connect(db_path,factory=BusyConnection)
        self.db.row_factory = sqlite3.Row
        self.addCleanup(self.db.close)
        notices = []
        self.run_download(callback=notices.append)
        self.assertTrue(self.db.injected)
        self.assertTrue(any(e.stage=='database_wait' for e in notices))
        self.assertEqual(len(self.requests),67)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM captures WHERE state='downloaded' AND download_attempts=1").fetchone()[0],67)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM document_matches WHERE scan_run_id=?',(self.run_id,)).fetchone()[0],67)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM text_retry_schedule').fetchone()[0],0)
        self.assertEqual(len(set(self.requests)),67)
        for row in self.db.execute('SELECT path,content_hash FROM documents'):
            self.assertEqual(hashlib.sha256(Path(row['path']).read_bytes()).hexdigest(),row['content_hash'])

    def external_writer_case(self, stop_on_notice):
        locked = threading.Event()
        release = threading.Event()
        holder = None
        original = ResilientTransport.dispatch_slots
        def dispatch(transport, slots):
            nonlocal holder
            if holder is None:
                def hold():
                    with closing(sqlite3.connect(self.root/'archive_scout.sqlite3')) as other:
                        other.execute('BEGIN IMMEDIATE')
                        locked.set()
                        release.wait(5 if stop_on_notice else .6)
                        other.rollback()
                holder = threading.Thread(target=hold)
                holder.start()
                self.assertTrue(locked.wait(2))
            return original(transport,slots)
        stop = threading.Event()
        def notice(event):
            if event.stage=='database_wait' and stop_on_notice:
                stop.set()
        try:
            with patch.object(ResilientTransport,'dispatch_slots',dispatch):
                if stop_on_notice:
                    with self.assertRaises(Stopped):
                        self.run_download(stop,notice)
                else:
                    self.run_download(stop,notice)
            self.assertEqual(self.db.execute('SELECT SUM(download_attempts) FROM captures').fetchone()[0],0 if stop_on_notice else 67)
            self.assertEqual(len(self.requests),0 if stop_on_notice else 67)
            self.assertEqual(self.db.execute('PRAGMA busy_timeout').fetchone()[0],60000)
        finally:
            release.set()
            if holder:
                holder.join(3)

    def test_external_writer_wait_recovers_all_captures(self):
        self.external_writer_case(False)

    def test_stop_during_external_writer_wait_preserves_attempts(self):
        self.external_writer_case(True)


class RecoveryPolicyTests(unittest.TestCase):
    def transport(self):
        value = ResilientTransport(pool_size=2,connect_timeout=1,read_timeout=2,mode='httpx',trust_env=False)
        self.addCleanup(value.close)
        return value

    def test_client_uses_configured_fixed_connection_recovery_interval(self):
        from archive_scout.cdx.client import HttpClient
        from archive_scout.downloads.rate_limit import FixedRateLimiter
        client = HttpClient(FixedRateLimiter(0),1,2,'test',threading.Event(),
            network_backend='httpx',trust_environment=False,connection_retry_seconds=7)
        self.addCleanup(client.close)
        self.assertEqual(client.transport.snapshot()['recovery_interval_seconds'],7)

    def test_isolated_failures_do_not_cool_backend_and_success_resets_streak(self):
        t = self.transport()
        for _ in range(20):
            failed = t._await_backend('httpx',threading.Event())
            t._backend_result(failed,False)
            self.assertEqual(t.dispatch_slots(10),10)
            good = t._await_backend('httpx',threading.Event())
            t._backend_result(good,True)
        self.assertEqual(t.cooldown_until,{})
        self.assertEqual(t.connection_failure_streaks,{})

    def test_timeout_before_http_headers_does_not_count_as_a_success(self):
        import httpx
        from archive_scout.network.transports import TransportExhaustedError
        t = self.transport()
        for _ in range(2):
            turn = t._await_backend('httpx',threading.Event())
            t._backend_result(turn,False)
        with patch.object(t.backends['httpx'],'request',side_effect=httpx.ReadTimeout('first byte timeout')):
            with self.assertRaises(TransportExhaustedError):
                t.request('http://example.test/',{},100,threading.Event())
        self.assertEqual(t.connection_failure_streaks['httpx'],2)
        self.assertIsNone(t.last_success)
        self.assertEqual(t.cooldown_until,{})

    def test_repeated_refusals_close_circuit_but_older_live_response_reopens_it(self):
        t = self.transport()
        already_running = t._await_backend('httpx',threading.Event())
        for _ in range(3):
            failed = t._await_backend('httpx',threading.Event())
            t._backend_result(failed,False)
        self.assertEqual(t.dispatch_slots(10),0)
        t._backend_result(already_running,True)
        self.assertEqual(t.dispatch_slots(10),10)
        t._backend_result(failed,False)
        self.assertEqual(t.cooldown_until,{})


class ActivityTests(unittest.TestCase):
    def test_default_on_and_live_disable_drops_notices_before_queueing(self):
        q = CoalescingEventQueue()
        owner = SimpleNamespace(events=q)
        event = ProgressEvent('site_issue','connection refused')
        ArchiveScoutApp.on_engine_event(owner,event)
        self.assertEqual(q.get_nowait(),('notice',event))
        q.show_error_updates = False
        for _ in range(10000):
            ArchiveScoutApp.on_engine_event(owner,event)
            ArchiveScoutApp.on_engine_event(owner,ProgressEvent('network','Network backend httpx failed during connection setup'))
        with self.assertRaises(queue.Empty):
            q.get_nowait()
        critical = ProgressEvent('download_incomplete','Requested work remains incomplete',detail={'retain_notice':True})
        ArchiveScoutApp.on_engine_event(owner,critical)
        self.assertEqual(q.get_nowait(),('notice',critical))
        q.show_error_updates = True
        ArchiveScoutApp.on_engine_event(owner,event)
        self.assertEqual(q.get_nowait(),('notice',event))

class ActivityPersistenceTests(unittest.TestCase):
    def test_setting_defaults_on_and_persists_off_in_gui(self):
        import subprocess
        import sys
        if os.environ.get('SCOUT_ACTIVITY_TEST_CHILD') != '1':
            result = subprocess.run([sys.executable,'-m','unittest',('tests.'+__name__ if __name__.startswith('unit.') else __name__)+'.ActivityPersistenceTests','-v'],
                env=dict(os.environ,SCOUT_ACTIVITY_TEST_CHILD='1'),text=True,capture_output=True,timeout=60)
            self.assertEqual(result.returncode,0,result.stdout+result.stderr)
            if 'skipped=1' in result.stderr:
                self.skipTest('Tk display unavailable')
            return
        import tkinter as tk
        with tempfile.TemporaryDirectory() as temp, \
             patch('archive_scout.ui.main_window.app_support_dir',return_value=Path(temp)), \
             patch.object(ArchiveScoutApp,'show_welcome'):
            try:
                app = ArchiveScoutApp()
            except tk.TclError as exc:
                if 'display' in str(exc).casefold():
                    self.skipTest(str(exc))
                raise
            try:
                self.assertTrue(app.show_error_activity_var.get())
                def widgets(parent):
                    for child in parent.winfo_children():
                        yield child
                        yield from widgets(child)
                controls = [w for w in widgets(app) if w.winfo_class()=='TCheckbutton'
                            and w.cget('text')=='Show error updates in Activity']
                self.assertEqual(len(controls),1)
                controls[0].invoke()
                self.assertFalse(app.events.show_error_updates)
                app.show_error_activity_var.set(True)
                app.load_app_state()
                self.assertFalse(app.show_error_activity_var.get())
                self.assertFalse(app.events.show_error_updates)
            finally:
                app.destroy()
