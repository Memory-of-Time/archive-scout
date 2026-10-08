from __future__ import annotations

import hashlib
import os
import sqlite3
import tempfile
import threading
import time
import unittest
from dataclasses import replace
from pathlib import Path
from unittest import mock

from archive_scout.config import ProjectConfig, save_project_config, load_project_config
from archive_scout.content import decode_bytes_with_encoding, looks_textual_bytes
from archive_scout.text_encoding import TextDecodingError, decode_prefix
from archive_scout.database.connection import open_database, open_database_readonly
from archive_scout.database.repositories import get_or_create_keyword_set, start_scan_run, record_error, upsert_document
from archive_scout.database.repositories import start_operation_run, update_operation_run, finish_operation_run
from archive_scout.downloads import downloader
from archive_scout.downloads.retry import retry_error_urls, retry_error_downloads
from archive_scout.downloads.metrics import CommittedThroughput
from archive_scout.downloads.rate_limit import reset_shared_traffic_state_for_tests, shared_host_gate
from archive_scout.projects.importers import import_text_folder
from archive_scout.projects.backups import create_project_backup, list_project_backups, restore_project_backup
from archive_scout.projects.integrity import check_project_integrity
from archive_scout.scanning.jobs import ScanJob
from archive_scout.scanning.workers import scanner_workers
from archive_scout.events import Stopped, ProgressEvent
from archive_scout.ui.eta import OperationEtaTracker


class EncodingTests(unittest.TestCase):
    def test_html_fallback_keeps_head_out_of_body_and_preserves_links(self):
        from archive_scout import content
        raw = '<html><head><title>Needle title</title><style>hidden</style><a href="/head">head link</a></head><body>needle body <a href="/body">body link</a><script>hidden</script></body></html>'
        # Links in a malformed head can be moved into the body by HTML5 repair;
        # compare valid markup, preserving attribute links in both paths.
        raw = '<html><head><title>Needle title</title><link href="/head"><style>hidden</style></head><body>needle body <a href="/body">body link</a><script>hidden</script></body></html>'
        expected = content.parse_page(raw, 'http://example.com/')
        with mock.patch.object(content, 'LexborHTMLParser', None):
            actual = content.parse_page(raw, 'http://example.com/')
        self.assertEqual(actual, expected)
        self.assertNotIn('Needle title', actual[1])

    def test_bad_wide_header_keeps_ascii_keywords(self):
        body = b'<html><body>needle archive words</body></html>'
        for declared in ('utf-16', 'utf16', 'utf-16le', 'utf-16be', 'utf-32'):
            with self.subTest(declared=declared):
                self.assertTrue(looks_textual_bytes(body, 'text/html; charset=' + declared))
                decoded, encoding = decode_bytes_with_encoding(body, 'text/html; charset=' + declared)
                self.assertIn('needle', decoded)
                self.assertEqual(encoding, 'utf-8')

    def test_unicode_wide_encodings_and_both_endianness(self):
        text = 'needle café 東京 Ελληνικά 😀'
        for encoding in ('utf-8-sig','utf-16','utf-32','utf-16-le','utf-16-be','utf-32-le','utf-32-be'):
            with self.subTest(encoding=encoding):
                data = text.encode(encoding)
                decoded, actual = decode_bytes_with_encoding(data, 'text/plain; charset=' + encoding)
                self.assertEqual(decoded.lstrip('\ufeff'), text)
                self.assertTrue(looks_textual_bytes(data, 'text/plain; charset=' + encoding))

    def test_prefix_can_end_inside_multibyte_character(self):
        for encoding in ('utf-8','utf-16','utf-32'):
            data = 'needle 😀'.encode(encoding)
            self.assertIn('needle', decode_prefix(data[:-1], 'text/plain; charset='+encoding)[0])

    def test_generic_wide_ambiguity_is_visible(self):
        data = '東京'.encode('utf-16-be')
        with self.assertRaises(TextDecodingError):
            decode_bytes_with_encoding(data, 'text/plain; charset=utf-16')
        self.assertEqual(decode_bytes_with_encoding(data, 'text/plain; charset=utf-16-be')[0], '東京')

    def test_malformed_bom_wide_body_is_not_a_successful_replacement_scan(self):
        with self.assertRaises(TextDecodingError):
            decode_bytes_with_encoding(b'\xff\xfeA', 'text/plain')

    def test_legacy_cyrillic_declaration_is_respected(self):
        text = 'needle архив'
        self.assertEqual(decode_bytes_with_encoding(text.encode('cp1251'), 'text/plain; charset=windows-1251')[0], text)


class RestoreOwnershipTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        db = open_database(self.root)
        db.execute("INSERT INTO project_meta(key,value) VALUES('restore_probe','backup')")
        db.commit(); db.close()
        self.backup = create_project_backup(self.root)
        db = open_database(self.root)
        db.execute("UPDATE project_meta SET value='current' WHERE key='restore_probe'")
        db.commit(); db.close()

    def tearDown(self):
        self.temp.cleanup()

    def value(self, path=None):
        db = sqlite3.connect(path or self.root / 'archive_scout.sqlite3')
        try:
            self.assertEqual(db.execute('PRAGMA integrity_check').fetchall(), [('ok',)])
            return db.execute("SELECT value FROM project_meta WHERE key='restore_probe'").fetchone()[0]
        finally:
            db.close()

    def test_existing_reader_keeps_snapshot_then_sees_restore_without_inode_replacement(self):
        path = self.root / 'archive_scout.sqlite3'
        reader = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)
        try:
            reader.execute('BEGIN')
            self.assertEqual(reader.execute("SELECT value FROM project_meta WHERE key='restore_probe'").fetchone()[0], 'current')
            inode = path.stat().st_ino
            safety = restore_project_backup(self.root, self.backup)
            self.assertEqual(path.stat().st_ino, inode)
            self.assertEqual(reader.execute("SELECT value FROM project_meta WHERE key='restore_probe'").fetchone()[0], 'current')
            reader.commit()
            self.assertEqual(reader.execute("SELECT value FROM project_meta WHERE key='restore_probe'").fetchone()[0], 'backup')
            self.assertEqual(self.value(safety), 'current')
            self.assertEqual(self.value(), 'backup')
        finally:
            reader.close()

    def test_uncompressed_selection_includes_committed_source_wal(self):
        source_root = self.root / 'source'
        source = open_database(source_root)
        try:
            source.execute("INSERT INTO project_meta(key,value) VALUES('restore_probe','wal source')")
            source.commit()
            self.assertGreater((source_root / 'archive_scout.sqlite3-wal').stat().st_size, 0)
            safety = restore_project_backup(self.root, source_root / 'archive_scout.sqlite3')
            self.assertEqual(self.value(), 'wal source')
            self.assertEqual(self.value(safety), 'current')
            self.assertEqual(source.execute("SELECT value FROM project_meta WHERE key='restore_probe'").fetchone()[0], 'wal source')
        finally:
            source.close()

    def test_invalid_compressed_input_does_not_create_safety_or_leave_temporary_files(self):
        bad = self.root / 'broken.sqlite3.gz'
        bad.write_bytes(b'not a gzip database')
        backups = set(list_project_backups(self.root))
        with self.assertRaises(OSError):
            restore_project_backup(self.root, bad)
        self.assertEqual(self.value(), 'current')
        self.assertEqual(set(list_project_backups(self.root)), backups)
        self.assertEqual(list(self.root.glob('archive-scout-restore-*')), [])

    def test_newer_schema_input_preserves_current_state_and_backups(self):
        path = self.root / 'newer.sqlite3'
        db = sqlite3.connect(path)
        try:
            db.execute('CREATE TABLE schema_info(version INTEGER)')
            db.execute('INSERT INTO schema_info VALUES(999)'); db.commit()
        finally:
            db.close()
        backups = set(list_project_backups(self.root))
        with self.assertRaisesRegex(RuntimeError, 'newer'):
            restore_project_backup(self.root, path)
        self.assertEqual(self.value(), 'current')
        self.assertEqual(set(list_project_backups(self.root)), backups)
        self.assertEqual(list(self.root.glob('archive-scout-restore-*')), [])

    def test_interrupted_destination_copy_rolls_back_and_retains_safety(self):
        from archive_scout.projects import backups
        original = backups._copy_database
        target = self.root / 'archive_scout.sqlite3'
        def interrupt(source, destination):
            path = Path(destination.execute('PRAGMA database_list').fetchone()[2])
            if path.resolve() != target.resolve():
                return original(source, destination)
            def progress(status, remaining, total):
                if remaining:
                    raise OSError('injected restore write interruption')
            source.backup(destination, pages=1, progress=progress)
        with mock.patch.object(backups, '_copy_database', side_effect=interrupt):
            with self.assertRaisesRegex(OSError, 'injected'):
                restore_project_backup(self.root, self.backup)
        self.assertEqual(self.value(), 'current')
        safety = list(self.root.glob('backups/*before_restore.sqlite3'))
        self.assertEqual(len(safety), 1)
        self.assertEqual(self.value(safety[0]), 'current')
        self.assertEqual(list(self.root.glob('archive-scout-restore-*')), [])

    def test_different_page_size_restores_through_sqlite(self):
        from archive_scout.projects import backups
        source = self.root / 'larger-pages.sqlite3'
        backups._materialize_backup(self.backup, source)
        db = sqlite3.connect(source)
        try:
            db.execute('PRAGMA journal_mode=DELETE')
            db.execute('PRAGMA page_size=8192')
            db.execute('VACUUM')
            self.assertEqual(db.execute('PRAGMA page_size').fetchone()[0], 8192)
        finally:
            db.close()
        restore_project_backup(self.root, source)
        self.assertEqual(self.value(), 'backup')
        db = sqlite3.connect(self.root / 'archive_scout.sqlite3')
        try:
            self.assertEqual(db.execute('PRAGMA page_size').fetchone()[0], 8192)
            self.assertEqual(db.execute('PRAGMA journal_mode').fetchone()[0], 'wal')
        finally:
            db.close()

    def test_writer_lock_times_out_without_discarding_uncommitted_or_current_state(self):
        from archive_scout.projects import backups
        writer = sqlite3.connect(self.root / 'archive_scout.sqlite3')
        try:
            writer.execute('BEGIN IMMEDIATE')
            writer.execute("UPDATE project_meta SET value='uncommitted' WHERE key='restore_probe'")
            with mock.patch.object(backups.time, 'monotonic', side_effect=[0.0, 11.0]):
                with self.assertRaisesRegex(RuntimeError, 'busy'):
                    restore_project_backup(self.root, self.backup)
            self.assertEqual(writer.execute("SELECT value FROM project_meta WHERE key='restore_probe'").fetchone()[0], 'uncommitted')
            writer.rollback()
            self.assertEqual(self.value(), 'current')
            safety = list(self.root.glob('backups/*before_restore.sqlite3'))
            self.assertEqual(len(safety), 1)
            self.assertEqual(self.value(safety[0]), 'current')
            self.assertEqual(list(self.root.glob('archive-scout-restore-*')), [])
        finally:
            writer.close()

    def test_different_page_size_with_live_reader_preserves_original_on_lock_failure(self):
        from archive_scout.projects import backups
        source = self.root / 'larger-locked-pages.sqlite3'
        backups._materialize_backup(self.backup, source)
        db = sqlite3.connect(source)
        try:
            db.execute('PRAGMA journal_mode=DELETE'); db.execute('PRAGMA page_size=8192'); db.execute('VACUUM')
        finally:
            db.close()
        reader = sqlite3.connect((self.root / 'archive_scout.sqlite3').as_uri() + '?mode=ro', uri=True)
        try:
            reader.execute('BEGIN')
            self.assertEqual(reader.execute("SELECT value FROM project_meta WHERE key='restore_probe'").fetchone()[0], 'current')
            with self.assertRaises(sqlite3.OperationalError):
                restore_project_backup(self.root, source)
            self.assertEqual(reader.execute("SELECT value FROM project_meta WHERE key='restore_probe'").fetchone()[0], 'current')
            reader.commit()
            self.assertEqual(self.value(), 'current')
            self.assertEqual(list(self.root.glob('archive-scout-restore-*')), [])
        finally:
            reader.close()

    def test_restore_into_empty_project(self):
        empty = self.root / 'empty'
        result = restore_project_backup(empty, self.backup)
        self.assertEqual(result, (empty / 'archive_scout.sqlite3').resolve())
        self.assertEqual(self.value(result), 'backup')
        self.assertEqual(list(empty.glob('backups/*before_restore.sqlite3')), [])

    def test_live_wal_selection_pins_a_complete_source_view(self):
        from archive_scout.projects import backups
        source_root = self.root / 'live-source'
        writer = open_database(source_root)
        try:
            writer.execute("INSERT INTO project_meta(key,value) VALUES('restore_probe','pinned')")
            writer.commit()
            original = backups._copy_database
            def change_after_snapshot(source, destination):
                writer.execute("UPDATE project_meta SET value='newer commit' WHERE key='restore_probe'")
                writer.commit()
                return original(source, destination)
            snapshot = self.root / 'pinned.sqlite3'
            with mock.patch.object(backups, '_copy_database', side_effect=change_after_snapshot):
                backups._materialize_backup(source_root / 'archive_scout.sqlite3', snapshot)
            self.assertEqual(self.value(snapshot), 'pinned')
            self.assertEqual(writer.execute("SELECT value FROM project_meta WHERE key='restore_probe'").fetchone()[0], 'newer commit')
        finally:
            writer.close()


class ReliabilityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = open_database(self.root)
        self.cfg = ProjectConfig(self.root, ['example.com/*'], ['needle'], scan_workers=2).normalized()
        kid = get_or_create_keyword_set(self.db, 'Rules', ['needle'])
        run = start_scan_run(self.db, kid, 'Rules', 1, 'download')
        self.job = ScanJob.create(run, 'Rules', ['needle'])
        self.db.commit()

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()

    def capture(self, index=1):
        now = '2026-01-01'
        cur = self.db.execute("INSERT INTO captures(original_url,timestamp,query_signature,mimetype,state,created_at,updated_at) VALUES(?,'20050101000000','fixture','text/plain','error',?,?)", ('http://example.com/'+str(index), now, now))
        return cur.lastrowid

    def test_mapping_off_and_readonly_preserved(self):
        self.assertEqual(self.db.execute('PRAGMA mmap_size').fetchone()[0], 0)
        ro = open_database_readonly(self.root)
        try:
            with self.assertRaises(sqlite3.OperationalError):
                ro.execute("DELETE FROM captures")
        finally:
            ro.close()

    def test_operation_local_retry_runs_during_saved_service_cooldown(self):
        from archive_scout.operations import run_project
        reset_shared_traffic_state_for_tests()
        cid = self.capture()
        path = self.root / 'saved.txt'; path.write_text('needle complete saved evidence')
        self.db.execute("UPDATE captures SET local_path=?,bytes_saved=?,payload_availability='retained_unscanned' WHERE id=?", (str(path),path.stat().st_size,cid))
        record_error(self.db, 'scan', 'scan_failure', 'injected previous scan failure', capture_id=cid, retryable=True)
        operation = start_operation_run(self.db, 'download', '1.0.9')
        update_operation_run(self.db, operation, stage='rate_limit_waiting', detail={'reason_code':'service_rate_limit','eligible_at_epoch':time.time()+3600,'http_status':429})
        finish_operation_run(self.db, operation, 'paused')
        self.db.commit(); self.db.close()
        config = replace(self.cfg, network=replace(self.cfg.network,persistent_retries=False),
                         research=replace(self.cfg.research,enabled=False,auto_build=False))
        started = time.monotonic()
        try:
            with mock.patch('archive_scout.downloads.downloader.HttpClient') as client:
                run_project(config, 'retry_errors')
                client.assert_not_called()
            self.assertLess(time.monotonic()-started, 4)
            self.db = open_database(self.root)
            self.assertEqual(self.db.execute('SELECT state FROM captures WHERE id=?',(cid,)).fetchone()[0], 'downloaded')
            self.assertGreater(self.db.execute('SELECT score FROM document_matches WHERE document_id=(SELECT document_id FROM captures WHERE id=?) ORDER BY id DESC',(cid,)).fetchone()[0], 0)
            self.assertGreater(shared_host_gate(config.rate_limit_base_pause,config.rate_limit_max_pause).snapshot()['eligible_at_epoch'], time.time()+3500)
        finally:
            reset_shared_traffic_state_for_tests()
            # Keep tearDown valid even if the operation failed.
            try:
                self.db.execute('SELECT 1')
            except sqlite3.ProgrammingError:
                self.db = open_database(self.root)

    def test_import_changed_file_and_same_mtime_preserve_history(self):
        folder = self.root / 'input'; folder.mkdir()
        path = folder / 'capture.txt'; path.write_bytes(b'needle first version')
        when = 1104537600
        os.utime(path, (when, when))
        self.assertEqual(import_text_folder(self.root, folder, self.db, threading.Event()), 1)
        self.db.commit()
        first = dict(self.db.execute('SELECT * FROM captures').fetchone())
        document_id = first['document_id']
        self.db.execute("INSERT INTO document_matches(scan_run_id,document_id,score,hits_json,fields_json,snippets_json,excluded,required_missing,created_at,updated_at) VALUES(?,?,1,'{}','{}','[]',0,0,'2026','2026')", (self.job.scan_run_id, document_id))
        match_id = self.db.execute('SELECT id FROM document_matches WHERE document_id=?',(document_id,)).fetchone()[0]
        self.db.execute("INSERT INTO reviews(match_id,status) VALUES(?,'confirmed')", (match_id,))
        path.write_bytes(b'needle other version')
        os.utime(path, (when, when))
        self.assertEqual(import_text_folder(self.root, folder, self.db, threading.Event()), 1)
        self.assertEqual(import_text_folder(self.root, folder, self.db, threading.Event()), 0)
        rows = self.db.execute('SELECT * FROM captures ORDER BY id').fetchall()
        self.assertEqual(len(rows), 2)
        self.assertNotEqual(rows[0]['local_path'], rows[1]['local_path'])
        self.assertEqual(Path(rows[0]['local_path']).read_bytes(), b'needle first version')
        self.assertEqual(hashlib.sha256(Path(rows[0]['local_path']).read_bytes()).hexdigest(), first['content_hash'])
        self.assertEqual(self.db.execute('SELECT status FROM reviews WHERE match_id=?', (match_id,)).fetchone()[0], 'confirmed')
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM documents_fts WHERE documents_fts MATCH 'first'").fetchone()[0], 1)

    def test_import_changed_mtime_keeps_same_immutable_content(self):
        folder = self.root / 'input'; folder.mkdir()
        path = folder / 'a.txt'; path.write_text('needle first', encoding='utf-8')
        os.utime(path,(1104537600,1104537600))
        import_text_folder(self.root,folder,self.db,threading.Event())
        os.utime(path,(1136073600,1136073600))
        import_text_folder(self.root,folder,self.db,threading.Event())
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM captures').fetchone()[0],2)
        self.assertEqual(self.db.execute('SELECT COUNT(DISTINCT local_path) FROM captures').fetchone()[0],1)

    def test_backup_compression_failure_never_publishes_partial(self):
        self.db.commit()
        previous = create_project_backup(self.root)
        def fail(raw, destination, callback, report):
            destination.write_bytes(b'partial gzip')
            raise OSError('injected disk failure')
        with mock.patch('archive_scout.projects.backups._compress_backup', side_effect=fail):
            with self.assertRaises(OSError):
                create_project_backup(self.root)
        self.assertEqual(list_project_backups(self.root), [previous])
        self.assertFalse(list((self.root/'backups').glob('*.tmp')))

    def test_cancelled_integrity_keeps_previous_complete_report(self):
        destination = self.root/'reports'/'integrity.txt'
        destination.parent.mkdir(); destination.write_text('previous complete report')
        stop = threading.Event(); stop.set()
        with self.assertRaises(Stopped):
            check_project_integrity(self.root,self.db,stop_event=stop)
        self.assertEqual(destination.read_text(), 'previous complete report')
        self.assertFalse(list(destination.parent.glob('integrity-issues-*.tmp')))

    def test_integrity_reports_sqlite_and_fts_checks(self):
        text = check_project_integrity(self.root,self.db).read_text()
        self.assertIn('SQLite and FTS checks: passed', text)
        self.assertIn('Issues found: 0', text)

    def test_retry_uses_scanner_control_and_spillable_selection(self):
        capture = self.capture()
        path = self.root/'local.txt'; path.write_text('needle')
        document = upsert_document(self.db,capture,path,'','needle',[],'hash','normalized',6)
        record_error(self.db,'scan','scan_failure','injected',capture_id=capture,document_id=document)
        cfg = replace(self.cfg, workers=11, scan_workers=2)
        with mock.patch('archive_scout.downloads.retry.rescan_documents') as rescan, mock.patch('archive_scout.downloads.retry.download_archive') as network:
            retry_error_urls(cfg,self.db,self.job.scan_run_id,threading.Event(),None,[self.job])
            self.assertEqual(rescan.call_args.kwargs['workers'],2)
            self.assertEqual(list(rescan.call_args.args[5]),[document])
            network.assert_not_called()

    def test_manual_recheck_unavailable_does_not_change_automatic_eligibility(self):
        capture = self.capture()
        record_error(self.db,'download','missing_capture','HTTP 404',capture_id=capture,retryable=False)
        self.db.commit()
        with mock.patch('archive_scout.downloads.retry.download_archive_only', return_value={'downloaded':0}) as acquire:
            result = retry_error_downloads(self.cfg,self.db,threading.Event(),None)
            self.assertEqual(result['queued'],0)
            acquire.assert_not_called()
            retry_error_downloads(replace(self.cfg,retry_include_unavailable=True),self.db,threading.Event(),None)
            self.assertEqual(list(acquire.call_args.kwargs['capture_ids']),[capture])
        self.assertEqual(self.db.execute('SELECT retryable FROM errors').fetchone()[0],0)

    def test_new_scanner_settings_roundtrip(self):
        cfg = replace(self.cfg, scan_backend='process', scan_memory_mb=64, retry_include_unavailable=True)
        save_project_config(cfg)
        restored = load_project_config(self.root/"project.json")
        self.assertEqual((restored.scan_backend,restored.scan_memory_mb,restored.retry_include_unavailable),('process',64,True))
        self.assertEqual(scanner_workers(7),7)


class MetricsEtaTests(unittest.TestCase):
    def test_committed_metrics_separate_adoption_and_expire_without_progress(self):
        now = [100.0]
        tracker = CommittedThroughput(lambda:now[0])
        self.assertEqual(tracker.snapshot()['fresh_committed'],0)
        now[0] = 105
        tracker.commit([100,200,300],[False,True,False])
        result = tracker.snapshot()
        self.assertEqual((result['fresh_committed'],result['adopted_existing'],result['committed_bytes']),(2,1,400))
        self.assertEqual(result['fresh_rate_10s'],.4)
        now[0]=117
        self.assertEqual(tracker.snapshot()['fresh_rate_10s'],0)
        now[0]=500
        self.assertEqual(tracker.snapshot()['fresh_rate_300s'],0)
        self.assertEqual(tracker.snapshot()['fresh_committed'],2)

    def test_metric_history_is_bounded_over_long_runs(self):
        now=[0]
        tracker=CommittedThroughput(lambda:now[0])
        for second in range(10000):
            now[0]=second
            tracker.commit([1]*8,[False]*8)
        self.assertLessEqual(len(tracker.buckets),301)
        self.assertEqual(tracker.snapshot()['fresh_rate_60s'],8)

    def test_item_retry_does_not_reset_eta(self):
        tracker=OperationEtaTracker(True)
        tracker.observe(ProgressEvent('download','',0,100),now=0,epoch=0)
        tracker.observe(ProgressEvent('download','',40,100),now=5,epoch=5)
        before=tracker.seconds_remaining(now=5)
        tracker.observe(ProgressEvent('download_retry','item retry'),now=5,epoch=5)
        self.assertEqual(tracker.seconds_remaining(now=5),before)
        self.assertEqual(len(tracker.samples),2)
