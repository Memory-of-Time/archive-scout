"""Offline regression tests for operation-mode correctness and coverage truth."""
from __future__ import annotations

import hashlib
import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.config import ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import (
    get_or_create_target, get_or_create_keyword_set, save_match,
    start_scan_run, upsert_capture, upsert_document,
)
from archive_scout.downloads.downloader import prepare_download_rows
from archive_scout.events import Stopped
from archive_scout.operations import IncompleteCoverage, run_project
from archive_scout.scanning.keywords import compile_keywords
from archive_scout.utils import hash_text


def add_capture(db, target_id, sig, url, state='pending'):
    upsert_capture(db, {
        'original': url, 'timestamp': '20080101000000', 'mimetype': 'text/html',
        'statuscode': '200', 'digest': hashlib.md5(url.encode()).hexdigest(), 'length': '55',
    }, target_id, sig)
    capture_id = int(db.execute('SELECT id FROM captures WHERE original_url=?', (url,)).fetchone()[0])
    db.execute('UPDATE captures SET state=? WHERE id=?', (state, capture_id))
    return capture_id


class OperationAuditRegressions(unittest.TestCase):
    def test_changed_keyword_scope_revives_only_url_filter_skips(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = ProjectConfig(output_dir=Path(tmp), targets=['example.org/*'],
                                keywords=['nomatch'], download_scope='keyword_urls').normalized()
            db = open_database(cfg.output_dir)
            try:
                target = get_or_create_target(db, cfg.targets[0])
                sig = cdx_query_signature(cfg.for_target(cfg.targets[0]))
                cid = add_capture(db, target, sig, 'http://example.org/findme.html')
                other = add_capture(db, target, sig, 'http://example.org/never.png')
                with db:
                    db.execute("UPDATE captures SET state='skipped' WHERE id=?", (other,))
                    db.execute("""INSERT INTO capture_routing(capture_id,resource_class,evidence,confident,routing,updated_at)
                                  VALUES(?,'image','extension',1,'skipped_non_text','now')""", (other,))
                total, rows = prepare_download_rows(db, cfg, compile_keywords(['nomatch']))
                self.assertEqual((total,list(rows)), (0, []))
                self.assertEqual(db.execute('SELECT state FROM captures WHERE id=?', (cid,)).fetchone()[0], 'skipped')
                with db:
                    # Simulates changing the keyword box between two runs.
                    pass
                cfg = ProjectConfig(output_dir=cfg.output_dir, targets=cfg.targets,
                                    keywords=['findme'], download_scope='keyword_urls').normalized()
                total, rows = prepare_download_rows(db, cfg, compile_keywords(['findme']))
                self.assertEqual(total, 1)
                self.assertEqual([int(x['id']) for x in rows], [cid])
                self.assertEqual(db.execute('SELECT state FROM captures WHERE id=?', (other,)).fetchone()[0], 'skipped')
                # Once the fingerprint is persisted, do not repeatedly requeue.
                total, rows = prepare_download_rows(db, cfg, compile_keywords(['findme']))
                self.assertEqual(total, 1)
                self.assertEqual([int(x['id']) for x in rows], [cid])
            finally:
                db.close()

    def test_empty_background_scan_message_does_not_imply_operation_finished(self):
        from archive_scout.operations import scan_prior_documents_once
        from archive_scout.scanning.jobs import ScanJob
        with tempfile.TemporaryDirectory() as tmp:
            cfg = ProjectConfig(output_dir=Path(tmp), targets=['example.org/*'],
                                keywords=['hello']).normalized()
            db = open_database(cfg.output_dir)
            try:
                events = []
                scan_prior_documents_once(db, [ScanJob.create(0, 'words', ['hello'])], cfg,
                                          threading.Event(), events.append)
                self.assertTrue(events)
                self.assertIn('continuing with this operation', events[-1].message)
                self.assertNotIn('No saved documents to rescan', events[-1].message)
            finally:
                db.close()

    def test_index_classifies_per_target_overrides(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = ProjectConfig(output_dir=Path(tmp), targets=['example.org/*','other.org/*'],
                                keywords=[], target_settings={
                                    'other.org/*': {'cdx_filters': ['mimetype:text/html']},
                                }).normalized()
            def fixture_index(config, db, stop, callback):
                for target in config.targets:
                    tid = get_or_create_target(db, target)
                    add_capture(db, tid, cdx_query_signature(config.for_target(target)),
                                f'http://{target[:-2]}/hello.html')
                db.commit()
            with patch('archive_scout.operations.index_archive', side_effect=fixture_index):
                run_project(cfg, 'index')
            db = open_database(cfg.output_dir)
            try:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM capture_routing').fetchone()[0], 2)
                self.assertEqual(db.execute("SELECT COUNT(*) FROM capture_routing WHERE resource_class='text'").fetchone()[0], 2)
            finally:
                db.close()

    def test_download_only_uses_selected_url_filter_without_scan_runs(self):
        class FakeClient:
            contacted = []
            def __init__(self, *args, **kwargs): pass
            def close(self): pass
            def get(self, url, max_bytes, **kwargs):
                self.contacted.append(url)
                return {'data': b'<html><body>hello</body></html>', 'status': 200,
                        'headers': {'content-type': 'text/html'}, 'final_url':url}
        with tempfile.TemporaryDirectory() as tmp:
            cfg = ProjectConfig(output_dir=Path(tmp),targets=['example.org/*'],keywords=['include'],
                                download_scope='keyword_urls',download_delay=0.0).normalized()
            def fixture_index(config, db, stop, callback):
                tid=get_or_create_target(db,config.targets[0]);sig=cdx_query_signature(config)
                add_capture(db,tid,sig,'http://example.org/include.html')
                add_capture(db,tid,sig,'http://example.org/skip.html')
                db.commit()
            with patch('archive_scout.operations.index_archive',side_effect=fixture_index), \
                 patch('archive_scout.downloads.downloader.HttpClient',FakeClient):
                paths=run_project(cfg,'download_only')
            db=open_database(cfg.output_dir)
            try:
                self.assertEqual(len(FakeClient.contacted),1)
                self.assertIn('include.html',FakeClient.contacted[0])
                self.assertEqual(db.execute('SELECT COUNT(*) FROM scan_runs').fetchone()[0],0)
                self.assertEqual(db.execute("SELECT COUNT(*) FROM captures WHERE state='downloaded_unscanned'").fetchone()[0],1)
                self.assertIn('Download text only',paths['summary'].read_text())
            finally:
                db.close()

    def test_report_respects_stop_and_does_not_mislabel_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg=ProjectConfig(output_dir=Path(tmp),targets=['example.org/*'],keywords=['hello']).normalized()
            db=open_database(cfg.output_dir)
            try:
                tid=get_or_create_target(db,cfg.targets[0])
                cid=add_capture(db,tid,cdx_query_signature(cfg),'http://example.org/a',state='downloaded')
                data=b'<html><body>hello</body></html>'
                path=cfg.output_dir/'captures'/'1.html';path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(data)
                did=upsert_document(db,cid,path,'hi','hello',[],hashlib.sha256(data).hexdigest(),hash_text('hello'),len(data))
                kid=get_or_create_keyword_set(db,'words',['hello'])
                scan=start_scan_run(db,kid,'test',1,'rescan')
                save_match(db,scan,did,{'score':1,'hits':{'hello':1},'hit_fields':{'hello':['body']},
                                        'snippets':['hello'],'interesting_links':[],'proximity':{}})
                db.commit()
            finally: db.close()
            stop=threading.Event();stop.set()
            with self.assertRaises(Stopped):
                run_project(cfg,'report',stop)
            db=open_database(cfg.output_dir)
            try:
                self.assertEqual(db.execute('SELECT status FROM operation_runs ORDER BY id DESC LIMIT 1').fetchone()[0],'interrupted')
            finally:db.close()

    def test_incomplete_scan_is_never_reported_as_complete(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg=ProjectConfig(output_dir=Path(tmp),targets=['example.org/*'],keywords=['hello']).normalized()
            db=open_database(cfg.output_dir)
            try:
                tid=get_or_create_target(db,cfg.targets[0]);
                add_capture(db,tid,cdx_query_signature(cfg),'http://example.org/missing',state='downloaded_unscanned');db.commit()
            finally: db.close()
            with self.assertRaises(IncompleteCoverage) as observed:
                run_project(cfg,'rescan')
            self.assertTrue(observed.exception.paths['summary'].is_file())
            db=open_database(cfg.output_dir)
            try:
                self.assertEqual(db.execute('SELECT status FROM operation_runs ORDER BY id DESC LIMIT 1').fetchone()[0],'incomplete')
                self.assertEqual(db.execute('SELECT status FROM scan_runs ORDER BY id DESC LIMIT 1').fetchone()[0],'interrupted')
                self.assertEqual(db.execute("SELECT COUNT(*) FROM errors WHERE operation='scan' AND resolved=0").fetchone()[0],1)
            finally: db.close()


if __name__ == '__main__':
    unittest.main()

class RepairResumeSafety(unittest.TestCase):
    def test_repair_preserves_metadata_backed_media_partials(self):
        from archive_scout.projects.repair import repair_project
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            tracked=root/'media'/'videos'/'capture.mp4.part'
            tracked.parent.mkdir(parents=True,exist_ok=True)
            tracked.write_bytes(b'verifiable prefix')
            tracked.with_name(tracked.name+'.resume.json').write_text('{"size":17}', encoding='utf-8')
            orphan=root/'media'/'videos'/'orphan.mp4.part'
            orphan.write_bytes(b'orphan')
            db=open_database(root)
            try:
                report=repair_project(root,db)
                self.assertTrue(tracked.read_bytes() == b'verifiable prefix')
                self.assertTrue(tracked.with_name(tracked.name+'.resume.json').exists())
                self.assertFalse(orphan.exists())
                self.assertIn('resumable media parts preserved: 1',report.read_text())
            finally:
                db.close()

class RepairHistoricalEvidence(unittest.TestCase):
    def test_missing_source_does_not_delete_prior_match_evidence(self):
        from archive_scout.projects.repair import repair_project
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);db=open_database(root)
            try:
                tid=get_or_create_target(db,'example.org/*')
                cid=add_capture(db,tid,'sig','http://example.org/missing',state='downloaded')
                missing=root/'captures'/'missing-source.txt'
                did=upsert_document(db,cid,missing,'title','historical needle',[],
                                    'old-content-hash',hash_text('historical needle'),25)
                kid=get_or_create_keyword_set(db,'words',['needle'])
                rid=start_scan_run(db,kid,'original',1,'rescan')
                save_match(db,rid,did,{'score':5,'hits':{'needle':1},
                                       'hit_fields':{'needle':['body']},'snippets':['historical needle'],
                                       'interesting_links':[],'proximity':{}})
                db.commit()
                repair_project(root,db)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM document_matches').fetchone()[0],1)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM documents').fetchone()[0],1)
                self.assertEqual(db.execute('SELECT state FROM captures WHERE id=?',(cid,)).fetchone()[0],'pending')
                self.assertEqual(db.execute("SELECT COUNT(*) FROM errors WHERE operation='scan' AND category='missing_local_file' AND resolved=0").fetchone()[0],1)
            finally:db.close()

class BackupUniqueness(unittest.TestCase):
    def test_back_to_back_manual_backups_preserve_both_snapshots(self):
        import sqlite3
        from contextlib import closing
        from archive_scout.projects.backups import create_project_backup
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);db=open_database(root)
            try:
                db.execute("INSERT INTO project_meta(key,value) VALUES('proof','old')")
                db.commit()
                first=create_project_backup(root,'manual',keep=10)
                db.execute("UPDATE project_meta SET value='new' WHERE key='proof'")
                db.commit()
                second=create_project_backup(root,'manual',keep=10)
                self.assertNotEqual(first,second)
                for path,expected in ((first,'old'),(second,'new')):
                    # SQLite's connection context manager commits/rolls back but does
                    # not close the file handle. Explicit closure is essential on Windows.
                    with closing(sqlite3.connect(path)) as snapshot:
                        self.assertEqual(snapshot.execute("SELECT value FROM project_meta WHERE key='proof'").fetchone()[0], expected)
            finally:db.close()
