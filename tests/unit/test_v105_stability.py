from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import httpx

from archive_scout.cdx.client import HttpClient
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.classification import capture_body_coverage, capture_routing_decision
from archive_scout.config import ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.downloads import downloader
from archive_scout.downloads.rate_limit import FixedRateLimiter, SharedHostGate, reset_shared_traffic_state_for_tests
from archive_scout.events import ConnectivityPaused, Stopped
from archive_scout.network.transports import ResilientTransport
from archive_scout.scanning.hitlist import _resume_or_create_run, search_with_hitlist
from archive_scout.ui.dashboard import read_dashboard_counts
from archive_scout.utils import utc_now


class V105StabilityTests(unittest.TestCase):
    def setUp(self):
        reset_shared_traffic_state_for_tests()
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.config = ProjectConfig(self.root, ['example.com/*'], [], workers=2).normalized()
        self.db = open_database(self.root)

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()
        reset_shared_traffic_state_for_tests()

    def capture(self, name):
        now = utc_now()
        cursor = self.db.execute("""INSERT INTO captures(original_url,timestamp,query_signature,mimetype,
            statuscode,length,state,resource_class,created_at,updated_at)
            VALUES(?,'20010101000000',?,'text/plain','200',32,'pending','text',?,?)""",
            (f'http://example.com/{name}',cdx_query_signature(self.config),now,now))
        self.db.commit()
        return int(cursor.lastrowid)

    def body(self, capture_id, text):
        path = self.root / 'captures' / f'{capture_id}.txt'
        path.parent.mkdir(exist_ok=True)
        path.write_text(text, encoding='utf-8')
        self.db.execute("UPDATE captures SET local_path=?,payload_availability='retained_unscanned',state='downloaded_unscanned',bytes_saved=? WHERE id=?",(str(path),path.stat().st_size,capture_id))
        self.db.commit()
        return path

    def interrupt_first_batch(self):
        stop = threading.Event()
        with self.assertRaises(Stopped):
            search_with_hitlist(self.root,self.db,['needle'],stop,lambda e: stop.set(),batch_size=1)

    def test_expired_incident_can_admit_one_real_healthy_probe_after_renewal(self):
        gate = SharedHostGate()
        gate.signal_rate_limit(1,'HTTP 429')
        gate.incident_started = max(1.0,time.monotonic())
        gate.recovery_cycle_started = gate.incident_started
        gate.blocked_until = time.monotonic()-1
        gate.blocked_until_wall = time.time()-1
        # Advance the monotonic clock, preserving its positive origin.
        original = time.monotonic
        calls = []
        transport = SimpleNamespace(close=lambda: None,
            request=lambda *args: calls.append(1) or SimpleNamespace(status=200,headers={},data=b'ok',final_url=args[0],backend='offline',elapsed=0))
        with mock.patch('time.monotonic',side_effect=lambda: original()+10000):
            self.assertLess(gate.recovery_deadline(900),time.monotonic())
            gate.renew_recovery_cycle(gate.incident_id)
            events = []
            client = HttpClient(FixedRateLimiter(0),1,1,'offline',threading.Event(),host_gate=gate,
                                rate_limit_max_wait=900,transport=transport,network_callback=events.append)
            try:
                self.assertEqual(client.get('https://web.archive.org/web/offline',64)['data'],b'ok')
            finally:
                client.close()
        self.assertEqual(len(calls),1)
        self.assertEqual(events,['Internet Archive recovery probe admitted'])
        self.assertFalse(gate.probe_required)

    def test_renewal_preserves_server_deadline_and_pause_is_cancellable(self):
        gate = SharedHostGate()
        gate.signal_rate_limit(120,'HTTP 429')
        deadline = gate.blocked_until
        gate.renew_recovery_cycle(gate.incident_id)
        self.assertEqual(gate.blocked_until,deadline)
        stop = threading.Event(); stop.set()
        with self.assertRaises(Stopped): gate.acquire_request(stop,deadline=gate.recovery_deadline(900))

    def test_only_one_shared_probe_can_enter(self):
        gate = SharedHostGate()
        gate.signal_rate_limit(1,'HTTP 429')
        gate.blocked_until = time.monotonic()-1
        first = gate.acquire_request(threading.Event())
        stop = threading.Event(); admitted=[]
        def second():
            try: admitted.append(gate.acquire_request(stop))
            except Stopped: pass
        thread = threading.Thread(target=second); thread.start()
        stop.set()
        with gate.condition: gate.condition.notify_all()
        thread.join(1)
        self.assertFalse(thread.is_alive()); self.assertEqual(admitted,[])
        gate.finish_request(first,True)

    def test_replay_recovery_drains_old_worker_and_reuses_one_client(self):
        first = self.capture('first'); self.capture('second')
        started = threading.Event(); release = threading.Event()
        attempts = {}; clients=[]; old_finished=threading.Event()
        class Client:
            def __init__(self,*args,**kwargs): clients.append(self)
            def close(self): self.closed_after_worker = old_finished.is_set()
        def worker(row,path,*args,**kwargs):
            cid=int(row['id']); attempts[cid]=attempts.get(cid,0)+1
            if cid==first:
                started.set()
                if not release.wait(3): raise AssertionError('worker was not drained')
                old_finished.set()
            elif attempts[cid]==1:
                if not started.wait(3): raise AssertionError('first worker did not start')
                raise ConnectivityPaused('offline outage')
            path.parent.mkdir(parents=True,exist_ok=True); path.write_text('plain')
            return dict(kind='downloaded',path=path,bytes_saved=5,content_hash='',http_status=200,final_url='offline')
        def event(e):
            if e.stage=='network_waiting': release.set()
        try:
            with mock.patch.object(downloader,'HttpClient',Client),mock.patch.object(downloader,'_download_capture',side_effect=worker):
                result=downloader.download_archive_only(self.config,self.db,threading.Event(),event)
        finally: release.set()
        self.assertEqual(result['downloaded'],2)
        self.assertEqual(len(clients),1)
        self.assertTrue(clients[0].closed_after_worker)
        self.assertEqual(attempts[first],1)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM captures WHERE state='downloaded_unscanned'").fetchone()[0],2)

    def test_text_validation_can_fall_back_to_healthy_curl(self):
        calls=[]
        class Backend:
            def __init__(self,name): self.name=name
            def download(self,*args,**kwargs):
                calls.append(self.name)
                if self.name!='curl': raise httpx.ConnectError('offline')
                self.validator=kwargs['preview_validator']
                return SimpleNamespace(status=200)
        transport=ResilientTransport.__new__(ResilientTransport)
        transport.lock=threading.Lock(); transport.cooldown_until={}; transport.last_success=None
        transport.callback=None; transport.attempt_context_factory=None
        transport.order=['httpx','urllib3','curl']; transport.backends={n:Backend(n) for n in transport.order}
        validator=lambda headers,prefix: None
        transport.download('https://web.archive.org/web/offline',{},self.root/'unused.part',4096,threading.Event(),preview_validator=validator)
        self.assertEqual(calls,['httpx','urllib3','curl'])
        self.assertIs(transport.backends['curl'].validator,validator)

    def test_dashboard_default_skips_operation_accounting_and_reports_outcomes(self):
        cid=self.capture('image')
        self.db.execute("UPDATE captures SET resource_class='image',state='skipped',skip_reason='deferred_to_media' WHERE id=?",(cid,)); self.db.commit()
        with mock.patch('archive_scout.ui.dashboard._capture_aggregate',side_effect=AssertionError('operation audit ran')), mock.patch('archive_scout.ui.dashboard._latest_operation_scope',side_effect=AssertionError('operation scope ran')):
            result=read_dashboard_counts(self.root/'archive_scout.sqlite3')
        self.assertEqual(result['deferred_to_media'],1)
        self.assertEqual(result['skipped_other'],0)
        self.assertEqual(capture_routing_decision('image','skipped','deferred_to_media','not_acquired'),'deferred_to_media')
        self.assertEqual(capture_routing_decision('image','skipped','classified_media','not_acquired'),'skipped_non_text')
        self.assertEqual(capture_body_coverage('media_descriptor','downloaded_unscanned','retained_unscanned'),'body_available')

    def test_hitlist_resume_revisits_late_older_body_and_reconciles_coverage(self):
        first=self.capture('first'); self.capture('second')
        self.interrupt_first_batch()
        stamp=self.db.execute('SELECT updated_at FROM quick_search_runs').fetchone()[0]
        self.body(first,'needle')
        self.db.execute('UPDATE captures SET updated_at=? WHERE id=?',(stamp,first)); self.db.commit()
        result=search_with_hitlist(self.root,self.db,['needle'],threading.Event(),batch_size=1)
        self.assertEqual(result['matches'],1); self.assertEqual(result['indexed_checked'],2)
        self.assertEqual(result['local_checked'],1); self.assertEqual(result['unavailable'],1)

    def test_hitlist_metadata_update_preserves_cursor_and_hits(self):
        first=self.capture('first'); self.capture('second'); self.body(first,'needle')
        self.interrupt_first_batch()
        self.db.execute("UPDATE captures SET updated_at='2099-01-01',download_attempts=99 WHERE id=?",(first,)); self.db.commit()
        _run,last,matched=_resume_or_create_run(self.db,['needle'])
        self.assertEqual((last,matched),(first,1))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM quick_search_hits').fetchone()[0],1)

    def test_hitlist_catches_body_changed_before_later_checkpoint(self):
        first=self.capture('first'); self.capture('second')
        def event(e):
            if e.current==2: self.body(first,'needle')
        result=search_with_hitlist(self.root,self.db,['needle'],threading.Event(),event,batch_size=1)
        self.assertEqual(result['matches'],1); self.assertEqual(result['indexed_checked'],2)
        self.assertEqual(result['local_checked'],1)

    def test_project_lease_blocks_other_operation_and_releases(self):
        from archive_scout.database.lease import project_lease
        with project_lease(self.root):
            with self.assertRaises(RuntimeError):
                with project_lease(self.root): pass
        with project_lease(self.root): pass

    def test_gui_restore_drains_reads_and_releases_maintenance_on_failure(self):
        from archive_scout.ui import main_window
        queued = []
        app = SimpleNamespace(
            worker_thread=None,
            output_var=SimpleNamespace(get=lambda: str(self.root)),
            ui_query_lock=threading.Lock(),
            ui_query_pending={"errors_view": ("stale",)},
            ui_query_inflight={"results_view": True},
            dashboard_refresh=SimpleNamespace(inflight=True),
            project_identity=lambda root=None: str(Path(root or self.root).resolve()),
            _invalidate_project_views=mock.Mock(),
            _queue_ui_query=lambda *args: queued.append(args),
        )
        with mock.patch.object(main_window, "live_project_writer_pids", return_value=[]), \
                mock.patch.object(main_window.filedialog, "askopenfilename", return_value=str(self.root / "backup.sqlite3")), \
                mock.patch.object(main_window.messagebox, "askyesno", return_value=True):
            main_window.ArchiveScoutApp.restore_backup_ui(app)
        self.assertEqual(app.project_restore_identity, str(self.root.resolve()))
        self.assertEqual(app.ui_query_pending, {})
        self.assertEqual(len(queued), 1)
        entered = threading.Event()
        finished = threading.Event()
        failures = []

        def restore(*args):
            entered.set()
            raise RuntimeError("offline restore failure")

        def worker():
            try:
                queued[0][1]()
            except RuntimeError as exc:
                failures.append(str(exc))
            finally:
                finished.set()

        with mock.patch.object(main_window, "restore_project_backup", side_effect=restore):
            thread = threading.Thread(target=worker)
            thread.start()
            try:
                self.assertFalse(entered.wait(0.05))
                with app.ui_query_lock:
                    app.ui_query_inflight["results_view"] = False
                app.dashboard_refresh.inflight = False
                self.assertTrue(finished.wait(2))
            finally:
                with app.ui_query_lock:
                    app.ui_query_inflight.clear()
                app.dashboard_refresh.inflight = False
                thread.join(2)
        self.assertEqual(failures, ["offline restore failure"])
        self.assertIsNone(app.project_restore_identity)

    def test_v11_migration_preserves_capture_and_note(self):
        cid=self.capture('keep'); path=self.body(cid,'retained evidence')
        self.db.execute('INSERT INTO notes(capture_id,text,created_at,updated_at) VALUES(?,?,?,?)',(cid,'human evidence',utc_now(),utc_now()))
        self.db.execute('DROP TRIGGER captures_body_revision_update')
        self.db.execute('DROP TABLE quick_search_coverage')
        self.db.execute('ALTER TABLE captures DROP COLUMN body_revision')
        for column in ('coverage_version','capture_limit'): self.db.execute(f'ALTER TABLE quick_search_runs DROP COLUMN {column}')
        self.db.execute('UPDATE schema_info SET version=11'); self.db.commit(); self.db.close()
        self.db=open_database(self.root,migrate=True)
        self.assertEqual(self.db.execute('SELECT version FROM schema_info').fetchone()[0],13)
        self.assertEqual(self.db.execute('SELECT text FROM notes WHERE capture_id=?',(cid,)).fetchone()[0],'human evidence')
        self.assertEqual(path.read_text(),'retained evidence')
        self.assertEqual(self.db.execute('SELECT local_path FROM captures WHERE id=?',(cid,)).fetchone()[0],str(path))


if __name__=='__main__': unittest.main()
