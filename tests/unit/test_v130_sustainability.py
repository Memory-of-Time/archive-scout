from __future__ import annotations

import random
import sqlite3
import tempfile
import threading
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from archive_scout.database.connection import open_database
from archive_scout.database.dashboard import initialize_dashboard
from archive_scout.ui.dashboard import read_dashboard_counts


class DashboardSummaryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root/'archive_scout.sqlite3'
        self.db = open_database(self.root)
        self.addCleanup(lambda:self.db.close())
        self.db.execute("INSERT INTO targets(id,pattern,created_at) VALUES(1,'example.test/*','t')")
        self.db.execute("INSERT INTO keyword_sets(id,name,fingerprint,keywords_json,created_at,updated_at) VALUES(1,'test','test','[]','t','t')")
        for rid in (1,2):
            self.db.execute("INSERT INTO scan_runs(id,keyword_set_id,name,status,started_at,source_operation) VALUES(?,1,'test','complete','t','test')",(rid,))
        for cid in range(1,31):
            self.db.execute("INSERT INTO captures(id,original_url,timestamp,target_id,query_signature,created_at,updated_at) VALUES(?,?,'20010101000000',1,'sig','t','t')",(cid,f'http://example.test/{cid}'))
            self.db.execute("INSERT INTO documents(id,capture_id,path,created_at,updated_at) VALUES(?,?,'fixture','t','t')",(cid,cid))
        self.db.commit()

    def compare(self):
        self.db.commit()
        actual=read_dashboard_counts(self.path,max_query_seconds=.1)
        self.assertTrue(actual['_summary'])
        with patch('archive_scout.database.dashboard.read_summary',return_value=None):
            expected=read_dashboard_counts(self.path,max_query_seconds=30)
        self.assertEqual({k:v for k,v in actual.items() if not k.startswith('_')},expected)

    def test_random_committed_state_route_match_and_error_changes(self):
        rng=random.Random(130)
        for n in range(150):
            cid=rng.randrange(1,31)
            self.db.execute('UPDATE captures SET state=? WHERE id=?',(rng.choice(['pending','downloading','downloaded','downloaded_unscanned','error','skipped','custom']),cid))
            self.db.execute("INSERT INTO capture_routing VALUES(?,?,?,1,?,'t') ON CONFLICT(capture_id) DO UPDATE SET resource_class=excluded.resource_class,routing=excluded.routing",(cid,rng.choice(['text','image','video','unknown']),'fixture',rng.choice(['pending','skipped_non_text','deferred_to_media','failed_nonretryable:missing_capture'])))
            self.db.execute("INSERT INTO document_matches(scan_run_id,document_id,score,excluded,required_missing,created_at,updated_at) VALUES(?,?,?,?,?,'t','t') ON CONFLICT(scan_run_id,document_id) DO UPDATE SET score=excluded.score,excluded=excluded.excluded,required_missing=excluded.required_missing",(rng.randrange(1,3),cid,rng.randrange(0,3),rng.randrange(0,2),rng.randrange(0,2)))
            self.db.execute("INSERT INTO errors(capture_id,operation,category,message,retryable,first_seen,last_seen) VALUES(?,'download','missing_capture','fixture',0,'t','t')",(cid,))
            self.db.execute('UPDATE errors SET ignored=?,resolved=? WHERE id=?',(rng.randrange(2),rng.randrange(2),rng.randrange(1,n+2)))
            if n%10==0:self.compare()
        self.compare()

    def test_duplicate_positive_matches_updates_and_cascaded_deletes(self):
        for rid in (1,2):
            self.db.execute("INSERT INTO document_matches(scan_run_id,document_id,score,created_at,updated_at) VALUES(?,1,4,'t','t')",(rid,))
        self.compare()
        self.db.execute('UPDATE document_matches SET score=0 WHERE scan_run_id=1');self.compare()
        self.db.execute('DELETE FROM captures WHERE id=1');self.compare()
        self.db.execute('DELETE FROM captures');self.db.execute('DELETE FROM targets WHERE id=1');self.compare()

    def test_media_and_network_rollover(self):
        self.db.execute("INSERT INTO media_targets(id,pattern,created_at) VALUES(1,'example.test/*','t')")
        self.db.execute("INSERT INTO media_captures(id,original_url,timestamp,query_signature,target_id,media_kind,created_at,updated_at) VALUES(1,'http://example.test/a.jpg','2001','sig',1,'image','t','t')")
        self.db.execute("INSERT INTO network_events(stage,message,created_at) VALUES('network_recovery','fixture','t')")
        self.compare()
        self.db.execute("UPDATE media_captures SET media_kind='video',state='downloaded'")
        self.db.execute("UPDATE network_events SET stage='ordinary'");self.compare()
        self.db.execute('DELETE FROM media_captures');self.db.execute('DELETE FROM media_targets');self.db.execute('DELETE FROM network_events');self.compare()

    def test_rollback_savepoint_and_wal_observer(self):
        before=read_dashboard_counts(self.path)
        self.db.execute("UPDATE captures SET state='downloaded' WHERE id=1")
        self.assertEqual(read_dashboard_counts(self.path),before)
        self.db.execute('SAVEPOINT state_change')
        self.db.execute("UPDATE captures SET state='error' WHERE id=1")
        self.db.execute('ROLLBACK TO state_change')
        self.db.execute('RELEASE state_change')
        self.db.rollback()
        self.assertEqual(read_dashboard_counts(self.path),before)
        self.compare()

    def test_rebuild_repairs_summary_without_changing_sources(self):
        self.db.execute("UPDATE dashboard_capture_counts SET n=999")
        self.db.commit()
        initialize_dashboard(self.db,rebuild=True)
        self.compare()
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM captures').fetchone()[0],30)

    def test_replace_and_ignore_routing_preserve_counts(self):
        for kind in ('text','image','video'):
            self.db.execute("INSERT OR REPLACE INTO capture_routing VALUES(1,?,'fixture',1,'pending','t')",(kind,))
            self.compare()
        self.db.execute("INSERT OR IGNORE INTO capture_routing VALUES(1,'text','fixture',1,'pending','t')")
        self.compare()

    def test_schema_eleven_upgrade_is_backed_up_and_keeps_inventory(self):
        for name, in self.db.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND name GLOB 'dashboard_*'").fetchall():
            self.db.execute('DROP TRIGGER '+name)
        for name in ('dashboard_capture_counts','dashboard_media_counts','dashboard_scalar_counts','dashboard_unavailable_captures'):
            self.db.execute('DROP TABLE '+name)
        self.db.execute("DELETE FROM project_meta WHERE key='dashboard_summary_v1'")
        self.db.execute('UPDATE schema_info SET version=11');self.db.commit();self.db.close()
        self.db=open_database(self.root)
        self.compare()
        self.assertEqual(self.db.execute('SELECT version FROM schema_info').fetchone()[0],12)
        backups=list((self.root/'backups').glob('*.sqlite3'))
        self.assertTrue(backups)
        # SQLite's transaction context manager does not close its file handle.
        with closing(sqlite3.connect(backups[0])) as original:
            self.assertEqual(original.execute('SELECT version FROM schema_info').fetchone()[0],11)
            self.assertEqual(original.execute('SELECT COUNT(*) FROM captures').fetchone()[0],30)

    def test_observer_uses_summary_not_capture_history(self):
        seen=[]
        from archive_scout.ui import dashboard
        real=sqlite3.connect
        def connect(*args,**kwargs):
            connection=real(*args,**kwargs);connection.set_trace_callback(seen.append);return connection
        with patch.object(dashboard.sqlite3,'connect',side_effect=connect):
            counts=read_dashboard_counts(self.path,max_query_seconds=.1)
        self.assertEqual(counts['captures'],30)
        self.assertFalse(any('FROM captures' in sql or 'FROM document_matches' in sql for sql in seen))

    def test_concurrent_initialization_rechecks_inside_writer_transaction(self):
        for name, in self.db.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND name GLOB 'dashboard_*'").fetchall():
            self.db.execute('DROP TRIGGER '+name)
        for name in ('dashboard_capture_counts','dashboard_media_counts','dashboard_scalar_counts','dashboard_unavailable_captures'):
            self.db.execute('DROP TABLE '+name)
        self.db.execute("DELETE FROM project_meta WHERE key='dashboard_summary_v1'");self.db.commit()
        barrier=threading.Barrier(2);failures=[]
        def initialize():
            connection=sqlite3.connect(self.path,timeout=5)
            try:
                barrier.wait();initialize_dashboard(connection)
            except BaseException as exc:failures.append(exc)
            finally:connection.close()
        threads=[threading.Thread(target=initialize) for _ in range(2)]
        for thread in threads:thread.start()
        for thread in threads:thread.join(timeout=10)
        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual(failures,[]);self.compare()

    def test_scan_cursor_handles_nonmonotonic_dates_and_equal_timestamps(self):
        from archive_scout.config import ProjectConfig
        from archive_scout.cdx.parameters import cdx_query_signature
        from archive_scout.scanning.pending import _pending_rows
        cfg=ProjectConfig(self.root,['example.test/*'],['needle'],from_date='2001',to_date='2001').normalized()
        self.db.executemany("INSERT INTO captures(id,original_url,timestamp,target_id,query_signature,created_at,updated_at) VALUES(?,?,'20010101000000',1,'sig','t','t')",
                            ((cid,f'http://example.test/{cid}') for cid in range(31,1231)))
        self.db.execute("UPDATE captures SET query_signature=?,state='downloaded_unscanned'",(cdx_query_signature(cfg),))
        self.db.execute("UPDATE captures SET timestamp='20011201000000' WHERE id%2=0")
        self.db.execute("UPDATE captures SET timestamp='20001201000000' WHERE id=3")
        self.db.commit()
        rows=list(_pending_rows(self.db,cfg))
        self.assertEqual({row['id'] for row in rows},set(range(1,1231))-{3})
        self.assertEqual(len(rows),1229)
        self.assertEqual([(r['timestamp'],r['id']) for r in rows],sorted((r['timestamp'],r['id']) for r in rows))


class ScannerFailureTests(unittest.TestCase):
    def test_fatal_local_scanner_failure_stops_acquisition_and_retains_work(self):
        from tests.unit.test_v129_admission_recovery import AdmissionTests
        fixture=AdmissionTests('test_original_temp_write_read_promotion_reproduces_snapshot_busy')
        try:
            fixture.setUp()
            def fail(*args,**kwargs):raise RuntimeError('injected scanner supervisor failure')
            with patch('archive_scout.scanning.pending.scan_pending_saved',side_effect=fail):
                with self.assertRaisesRegex(RuntimeError,'background scanner failed'):
                    fixture.run_download()
            self.assertLess(len(fixture.requests),67)
            self.assertEqual(fixture.db.execute('SELECT COUNT(*) FROM captures').fetchone()[0],67)
        finally:fixture.doCleanups()


class HistoryTests(unittest.TestCase):
    def test_health_history_is_throttled_and_rotated(self):
        from archive_scout.downloads.diagnostics import AcquisitionDiagnostics
        with tempfile.TemporaryDirectory() as temp:
            d=AcquisitionDiagnostics(Path(temp),None)
            d.summary_path.mkdir(parents=True,exist_ok=True)
            d.HISTORY_MAX_BYTES=300
            with patch('archive_scout.downloads.diagnostics.time.monotonic',return_value=0):
                d._append_history({'value':'x'*60})
                d._append_history({'value':'should not append'})
            self.assertEqual(len((d.summary_path/'history.jsonl').read_text().splitlines()),1)
            for n in range(1,20):
                with patch('archive_scout.downloads.diagnostics.time.monotonic',return_value=n*300):
                    d._append_history({'value':'x'*60})
            files=list(d.summary_path.glob('history.jsonl*'))
            self.assertEqual(len(files),3)
            self.assertTrue(all(file.stat().st_size<=300 for file in files))
