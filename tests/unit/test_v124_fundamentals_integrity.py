"""v1.2.4 fundamental regressions: retained evidence, charset, and hot-path bounds."""
from __future__ import annotations

import hashlib
import tempfile
import threading
import unittest
from pathlib import Path

from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.config import ProjectConfig
from archive_scout.content import classify_replay_content
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import (
    get_or_create_keyword_set, get_or_create_target, start_scan_run, upsert_capture, upsert_document,
)
from archive_scout.scanning.jobs import ScanJob
from archive_scout.scanning.rescanner import rescan_keyword_sets
from archive_scout.scanning.hitlist import search_with_hitlist
from archive_scout.scanning.pending import scan_pending_saved
from archive_scout.utils import hash_text, utc_now


class FundamentalsIntegrityTests(unittest.TestCase):
    def fixture(self, tmp, *, keyword='привет', initial=True):
        root = Path(tmp)
        cfg = ProjectConfig(output_dir=root, targets=['example.org/*'], keywords=[keyword]).normalized()
        db = open_database(root)
        target_id = get_or_create_target(db, 'example.org/*')
        url = 'http://example.org/article.html'
        upsert_capture(db, {'original': url, 'timestamp': '20060101000000',
            'mimetype': 'text/html', 'statuscode': '200', 'digest': '', 'length': '200'},
            target_id, cdx_query_signature(cfg))
        capture_id = int(db.execute('SELECT id FROM captures').fetchone()[0])
        from archive_scout.downloads.downloader import capture_path
        path = capture_path(root,capture_id,'20060101000000',url)
        path.parent.mkdir(parents=True,exist_ok=True)
        raw = b'<html><body><!-- ' + 'привет'.encode('windows-1251') + b' --></body></html>'
        path.write_bytes(raw)
        digest = hashlib.sha256(raw).hexdigest()
        db.execute('UPDATE captures SET state=? WHERE id=?', ('downloaded' if initial else 'downloaded_unscanned', capture_id))
        db.execute('''INSERT INTO capture_payload_evidence(capture_id,content_hash,bytes_saved,
            response_content_type,final_url,response_status,updated_at) VALUES(?,?,?,?,?,?,?)''',
            (capture_id,digest,len(raw),'text/html; charset=windows-1251','',200,utc_now()))
        did = upsert_document(db,capture_id,path,'','',[],digest,hash_text(''),len(raw))
        kid = get_or_create_keyword_set(db,'Words',[keyword])
        rid = start_scan_run(db,kid,'Words',1,'rescan')
        db.commit()
        return db,cfg,path,capture_id,did,ScanJob.create(rid,'Words',[keyword])

    def test_rescan_and_hitlist_use_saved_response_charset(self):
        with tempfile.TemporaryDirectory() as tmp:
            db,cfg,path,capture_id,did,job = self.fixture(tmp)
            try:
                rescan_keyword_sets(db,[job],threading.Event(),workers=1)
                self.assertIn('привет',db.execute('SELECT hits_json FROM document_matches').fetchone()[0])
                search_with_hitlist(cfg.output_dir, db, ['привет'], threading.Event())
                self.assertEqual(db.execute('SELECT COUNT(*) FROM quick_search_hits').fetchone()[0],1)
            finally:
                db.close()

    def test_changed_project_owned_body_does_not_overwrite_scan_or_hitlist(self):
        with tempfile.TemporaryDirectory() as tmp:
            db,cfg,path,capture_id,did,job = self.fixture(tmp,keyword='altered')
            try:
                path.write_bytes(b'<html><body>altered evidence</body></html>')
                initial_hash=db.execute('SELECT content_hash FROM documents WHERE id=?',(did,)).fetchone()[0]
                rescan_keyword_sets(db,[job],threading.Event(),workers=1)
                self.assertEqual(db.execute('SELECT content_hash FROM documents WHERE id=?',(did,)).fetchone()[0],initial_hash)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM document_matches').fetchone()[0],0)
                self.assertEqual(db.execute("SELECT COUNT(*) FROM errors WHERE category='local_integrity_failure'").fetchone()[0],1)
                search_with_hitlist(cfg.output_dir,db,['altered'],threading.Event())
                self.assertEqual(db.execute('SELECT COUNT(*) FROM quick_search_hits').fetchone()[0],0)
                self.assertGreater(db.execute('SELECT incomplete_count FROM quick_search_runs').fetchone()[0],0)
            finally: db.close()

    def test_quoted_wayback_phrase_is_not_a_false_missing_capture(self):
        self.assertIsNone(classify_replay_content(
            '<html><title>Historical article</title><body>The photograph is not in archive records.</body></html>',
            'https://web.archive.org/web/20060101id_/http://example.org/a'))
        self.assertEqual(classify_replay_content(
            '<html><title>Blocked Site Error</title><body>Denied</body></html>',
            'https://web.archive.org/web/20060101id_/http://example.org/a'),'wayback_excluded')
        self.assertEqual(classify_replay_content(
            '<html><title>Wayback Machine</title><p>This URL has been excluded from the Wayback Machine.</p></html>',
            'https://web.archive.org/web/20060101id_/http://example.org/a'),'wayback_excluded')

    def test_failed_index_scope_exponential_cooldown_is_durable(self):
        from archive_scout.cdx.client import TransientRequestError
        from archive_scout.cdx.indexer import (
            IndexPlan,PendingWindow,_defer_transient_window,encode_plan,decode_plan,transient_backoff,
        )
        with tempfile.TemporaryDirectory() as tmp:
            cfg=ProjectConfig(Path(tmp),targets=['example.org/*'],keywords=['needle']).normalized()
            db=open_database(cfg.output_dir)
            try:
                win=PendingWindow('20060101000000','20061231235959',strategy='resume')
                plan=IndexPlan([win],0,1)
                persisted=[]
                failure=TransientRequestError('HTTP 504',status=504)
                for _ in range(4):
                    _defer_transient_window(cfg,db,plan,win,
                        lambda encoded,*other: persisted.append(encoded),0,None,
                        failure,None,0,1,threading.Event(),yield_to_other_tasks=True)
                self.assertEqual(win.failures,4)
                self.assertGreater(transient_backoff(cfg,4),transient_backoff(cfg,1))
                recovered=decode_plan(persisted[-1],[(win.start,win.end)])
                self.assertEqual(recovered.pending[0].failures,4)
                self.assertGreater(recovered.pending[0].next_eligible_at,0)
                self.assertEqual(recovered.pending[0].resume_key,win.resume_key)
            finally:db.close()

    def test_finite_rate_limit_deferral_is_not_a_user_stop(self):
        from unittest.mock import patch
        from archive_scout.cdx.client import RateLimitDeferred
        from archive_scout.downloads.downloader import download_archive
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            cfg=ProjectConfig(output_dir=root,targets=['example.org/*'],keywords=['needle'],
                workers=1,download_delay=0).normalized()
            db=open_database(root)
            try:
                tid=get_or_create_target(db,'example.org/*')
                upsert_capture(db,{'original':'http://example.org/a.html',
                    'timestamp':'20060101000000','mimetype':'text/html',
                    'statuscode':'200','digest':'','length':'100'},tid,cdx_query_signature(cfg))
                from archive_scout.cdx.indexer import save_coverage_state
                save_coverage_state(db, tid, cdx_query_signature(cfg),cfg.from_date,
                                    cfg.to_date,None,True,1,None)
                db.commit()
                user_stop=threading.Event()
                failure=RateLimitDeferred('HTTP 429 finite wait budget',status=429,waited=0.5)
                with patch('archive_scout.downloads.downloader.fetch_retain_streamed',side_effect=failure):
                    download_archive(cfg,db,0,user_stop,None,acquire_only=True)
                self.assertFalse(user_stop.is_set())
                self.assertEqual(db.execute('SELECT state FROM captures').fetchone()[0],'error')
                self.assertEqual(db.execute('SELECT last_category FROM text_retry_schedule').fetchone()[0],'rate_limit')
            finally:db.close()

    def test_legacy_gap_reconciliation_is_once_per_scanner_connection(self):
        with tempfile.TemporaryDirectory() as tmp:
            db,cfg,path,capture_id,did,job=self.fixture(tmp,keyword='needle')
            try:
                # An old download was marked complete before a document commit.
                db.execute('DELETE FROM documents WHERE id=?',(did,))
                db.commit()
                traced=[]
                db.set_trace_callback(traced.append)
                scan_pending_saved(db,cfg,[job],threading.Event(),workers=1)
                scan_pending_saved(db,cfg,[job],threading.Event(),workers=1)
                db.set_trace_callback(None)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM documents').fetchone()[0],1)
                self.assertEqual(sum('NOT EXISTS (SELECT 1 FROM documents' in statement for statement in traced),1)
            finally: db.close()

if __name__=='__main__':
    unittest.main()
