"""v1.2.5 adversarial coverage, media and bounded-scanning regressions."""
from __future__ import annotations

import hashlib
import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from archive_scout.config import MediaConfig, ProjectConfig
from archive_scout.cdx.indexer import save_coverage_state
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.coverage import (IndexCoveragePending, require_text_coverage,
                                    require_media_coverage)
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, get_or_create_media_target, record_error
from archive_scout.media.indexer import (media_index_state_signature, media_query_signature,
                                        build_media_params)
from archive_scout.media.downloader import fetch_media, validate_media_representation
from archive_scout.media.recovery import record_media_failure, clear_media_retry, recover_active_media
from archive_scout.scanning.jobs import ScanJob
from archive_scout.scanning.rescanner import rescan_keyword_sets


class StrictIndexCoverage(unittest.TestCase):
    def test_absent_index_is_not_validated_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = ProjectConfig(Path(tmp), targets=['example.org/*'], keywords=[]).normalized()
            db = open_database(Path(tmp))
            try:
                with self.assertRaises(IndexCoveragePending):
                    require_text_coverage(db, cfg)
                t = get_or_create_target(db, cfg.targets[0])
                save_coverage_state(db, t, cdx_query_signature(cfg), cfg.from_date,
                                    cfg.to_date, None, True, 0, None)
                db.commit()
                require_text_coverage(db, cfg)  # proven zero-result inventory
                wider = ProjectConfig(Path(tmp), targets=['example.org/*','other.org/*'],keywords=[]).normalized()
                with self.assertRaises(IndexCoveragePending):
                    require_text_coverage(db, wider)
            finally:
                db.close()

    def test_media_absent_is_not_completed_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg=ProjectConfig(Path(tmp), targets=['example.org/*'], keywords=[],
                              from_date='2008',to_date='2008',media=MediaConfig(enabled=True)).normalized()
            db=open_database(Path(tmp))
            try:
                with self.assertRaises(IndexCoveragePending):
                    require_media_coverage(db,cfg)
                tid=get_or_create_media_target(db,cfg.targets[0])
                db.execute('''INSERT INTO media_index_state(target_id,extension,year,query_signature,complete,updated_at)
                              VALUES (?,?,?,?,1,'now')''',(tid,'__all__',2008,media_index_state_signature(cfg)))
                db.commit()
                require_media_coverage(db,cfg)
            finally: db.close()

    def test_target_override_changes_media_semantic_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            base=ProjectConfig(Path(tmp),targets=['example.org/*'],keywords=[],
                               media=MediaConfig(enabled=True)).normalized()
            revised=ProjectConfig(Path(tmp),targets=['example.org/*'],keywords=[],
                                  target_settings={'example.org/*':{'from_date':'2003','cdx_match_type':'domain'}},
                                  media=MediaConfig(enabled=True)).normalized()
            self.assertNotEqual(media_query_signature(base),media_query_signature(revised))


class BoundedRescanScopes(unittest.TestCase):
    def test_1200_scope_pairs_do_not_exceed_sql_expression_depth(self):
        with tempfile.TemporaryDirectory() as tmp:
            db=open_database(Path(tmp))
            try:
                jobs=[ScanJob.create(1,'words',['test'])]
                scopes=[(f'site{i}.example/*',f'sig{i}') for i in range(1200)]
                events=[]
                rescan_keyword_sets(db,jobs,threading.Event(),events.append,
                                    scope_pairs=scopes,workers=1)
                self.assertEqual(len(events),1)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM archive_scout_rescan_scope').fetchone()[0],1200)
            finally: db.close()


class MediaRepresentation(unittest.TestCase):
    @staticmethod
    def response(body: bytes, status=200, headers=None):
        return {'bytes':len(body), 'status':status,'headers': headers or {},
                'preview':body[:8192], 'content_hash':hashlib.sha256(body).hexdigest(),
                'final_url':'http://archive.test/clip.mp4'}

    def test_binary_phrase_cannot_override_positive_mp4_magic(self):
        mp4=b'\x00\x00\x00\x18ftypisom00000000' + b'not archived in sample metadata'
        response=self.response(mp4,headers={'content-type':'text/html','content-length':str(len(mp4))})
        validate_media_representation(response)

    def test_html_page_must_not_be_saved_as_media(self):
        body=b'<html><body>Some generic HTML error or player page</body></html>'
        with self.assertRaisesRegex(RuntimeError,'invalid_wayback_replay'):
            validate_media_representation(self.response(body,headers={'content-type':'video/mp4'}))

    def test_misaligned_206_range_is_incomplete_even_with_nonzero_bytes(self):
        body=b'\x00\x00\x00\x18ftypisom' + b'x'*52
        with self.assertRaisesRegex(RuntimeError,'incomplete_media_range'):
            validate_media_representation(self.response(body,206,
                {'content-range':'bytes 0-63/1726','content-type':'video/mp4'}))

    def test_incomplete_content_length_is_not_success(self):
        body=b'FLV'+b'\0'*20
        with self.assertRaisesRegex(RuntimeError,'incomplete_media_length'):
            validate_media_representation(self.response(body,200,
                {'content-length':str(len(body)+300),'content-type':'video/x-flv'}))

    def test_actual_media_fetch_with_binary_mislabeled_html(self):
        body=b'\x00\x00\x00\x18ftypisom'+b'0'*12+b'not archived'
        class Fixture:
            def download_to_path(self,url,path,max_bytes,**_kwargs):
                Path(path).write_bytes(body)
                return MediaRepresentation.response(body,headers={'content-type':'text/html'})
        with tempfile.TemporaryDirectory() as tmp:
            cfg=ProjectConfig(Path(tmp), targets=['example.org/*'],keywords=[]).normalized()
            row={'id':1,'original_url':'http://example.org/clip.mp4','timestamp':'20080101000000','digest':'','extension':'.mp4'}
            path=Path(tmp)/'clip.mp4'
            result=fetch_media(row,cfg,Fixture(),path)
            self.assertEqual(path.read_bytes(),body)
            self.assertEqual(result['bytes'],len(body))

    def test_http_transport_rejects_partial_206_representation(self):
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        from archive_scout.cdx.client import HttpClient
        from archive_scout.downloads.rate_limit import FixedRateLimiter
        body=b"\x00\x00\x00\x18ftypisom" + b"z"*52
        class Handler(BaseHTTPRequestHandler):
            protocol_version="HTTP/1.1"
            def do_GET(self):
                self.send_response(206)
                self.send_header("Content-Type","video/mp4")
                self.send_header("Content-Range","bytes 0-63/1726")
                self.send_header("Content-Length",str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            def log_message(self,*args): pass
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        worker=threading.Thread(target=server.serve_forever,daemon=True)
        worker.start()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                cfg=ProjectConfig(Path(tmp), targets=['example.org/*'],keywords=[]).normalized()
                client=HttpClient(FixedRateLimiter(0),1,5,'test',threading.Event(),
                                  network_backend='httpx',trust_environment=False)
                row={'id':1,'original_url':'http://example.org/clip.mp4',
                     'timestamp':'20080101000000','digest':'','extension':'.mp4'}
                dest=Path(tmp)/'clip.mp4'
                try:
                    with patch('archive_scout.media.downloader.media_replay_url',
                               return_value=f'http://127.0.0.1:{server.server_port}/clip'):
                        with self.assertRaisesRegex(RuntimeError,'incomplete_media_range'):
                            fetch_media(row,cfg,client,dest)
                    self.assertFalse(dest.exists())
                finally:
                    client.close()
        finally:
            server.shutdown();server.server_close();worker.join(timeout=3)

    def test_direct_media_latest_requires_uncollapsed_observations(self):
        with tempfile.TemporaryDirectory() as tmp:
            media=MediaConfig(enabled=True,snapshot_strategy='latest')
            cfg=ProjectConfig(Path(tmp), targets=['example.org/*'],keywords=[],
                              media=media, cdx_collapses=['urlkey']).normalized()
            params=build_media_params(cfg, cfg.targets[0],cfg.from_date,cfg.to_date,
                                      extensions=['.mp4'])
            self.assertNotIn('collapse',[key for key,_value in params])
            all_cfg=ProjectConfig(Path(tmp), targets=['example.org/*'],keywords=[],
                                  media=MediaConfig(enabled=True,snapshot_strategy='all'),
                                  cdx_collapses=['urlkey']).normalized()
            all_params=build_media_params(all_cfg,all_cfg.targets[0],all_cfg.from_date,all_cfg.to_date,
                                          extensions=['.mp4'])
            self.assertIn(('collapse','urlkey'),all_params)


class DurableMediaRecovery(unittest.TestCase):
    def test_due_media_failure_is_retried_from_database_and_cleared(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            cfg=ProjectConfig(root,targets=['example.org/*'],keywords=[],
                              from_date='2008',to_date='2008',
                              media=MediaConfig(enabled=True)).normalized()
            db=open_database(root)
            try:
                tid=get_or_create_media_target(db,cfg.targets[0])
                db.execute('''INSERT INTO media_index_state(target_id,extension,year,query_signature,complete,updated_at)
                              VALUES(?,?,?,?,1,'now')''',(tid,'__all__',2008,media_index_state_signature(cfg)))
                db.execute('''INSERT INTO media_captures(original_url,timestamp,target_id,query_signature,
                             media_kind,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)''',
                             ('http://example.org/a.mp4','20080101000000',tid,media_query_signature(cfg),
                              'video','error','now','now'))
                cid=int(db.execute('SELECT id FROM media_captures').fetchone()[0])
                record_error(db,'media_download','http_server_error','temporary 504',
                             media_capture_id=cid,retryable=True,http_status=504)
                record_media_failure(db,cid,'http_server_error',504,True,cfg)
                db.execute('UPDATE media_retry_schedule SET next_eligible_at=? WHERE media_capture_id=?',
                           (time.time()-10,cid))
                db.commit()
                called=[]
                def recovery_download(config,database,stop,callback,**kwargs):
                    called.extend(kwargs['media_capture_ids'])
                    with database:
                        database.execute("UPDATE media_captures SET state='downloaded' WHERE id=?",(cid,))
                        database.execute('UPDATE errors SET resolved=1 WHERE media_capture_id=?',(cid,))
                        clear_media_retry(database,cid)
                with patch('archive_scout.media.downloader.download_media',side_effect=recovery_download):
                    recover_active_media(cfg,db,threading.Event())
                self.assertEqual(called,[cid])
                self.assertEqual(db.execute('SELECT COUNT(*) FROM media_retry_schedule').fetchone()[0],0)
            finally: db.close()

    def test_embedded_retry_keeps_its_own_inventory_signature(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            cfg=ProjectConfig(root,targets=['example.org/*'],keywords=[],
                              media=MediaConfig(enabled=True)).normalized()
            db=open_database(root)
            try:
                embedded_signature='specific-embedded-inventory-signature'
                tid=get_or_create_media_target(db,cfg.targets[0])
                db.execute("""INSERT INTO media_captures(original_url,timestamp,target_id,query_signature,
                             media_kind,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)""",
                             ('http://elsewhere.org/clip.mp4','20080101000000',tid,embedded_signature,
                              'video','error','now','now'))
                cid=int(db.execute('SELECT id FROM media_captures').fetchone()[0])
                record_error(db,'media_download','http_server_error','temporary 504',
                             media_capture_id=cid,retryable=True,http_status=504)
                record_media_failure(db,cid,'http_server_error',504,True,cfg)
                db.execute('UPDATE media_retry_schedule SET next_eligible_at=? WHERE media_capture_id=?',
                           (time.time()-10,cid));db.commit()
                def download(config,database,stop,callback,**kwargs):
                    self.assertEqual(kwargs['media_signature'],embedded_signature)
                    self.assertEqual(kwargs['media_capture_ids'],[cid])
                    with database:
                        database.execute("UPDATE media_captures SET state='downloaded' WHERE id=?",(cid,))
                        database.execute('UPDATE errors SET resolved=1 WHERE media_capture_id=?',(cid,))
                        clear_media_retry(database,cid)
                with patch('archive_scout.media.downloader.download_media',side_effect=download) as mocked:
                    recover_active_media(cfg,db,threading.Event(),additional_signature=embedded_signature)
                self.assertEqual(mocked.call_count,1)
            finally: db.close()

    def test_legacy_media_retry_adopts_more_than_one_bounded_batch(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            cfg=ProjectConfig(root,targets=['example.org/*'],keywords=[],
                              from_date='2008',to_date='2008',media=MediaConfig(enabled=True)).normalized()
            db=open_database(root)
            try:
                sig=media_query_signature(cfg)
                tid=get_or_create_media_target(db,cfg.targets[0])
                db.execute("""INSERT INTO media_index_state(target_id,extension,year,query_signature,complete,updated_at)
                              VALUES(?,?,?,?,1,'now')""",(tid,'__all__',2008,media_index_state_signature(cfg)))
                with db:
                    for i in range(300):
                        db.execute("""INSERT INTO media_captures(original_url,timestamp,target_id,query_signature,
                                     media_kind,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)""",
                                   (f'http://example.org/{i}.jpg','20080101000000',tid,sig,'image','error','now','now'))
                        cid=int(db.execute('SELECT last_insert_rowid()').fetchone()[0])
                        record_error(db,'media_download','timeout','timeout',media_capture_id=cid,retryable=True)
                stop=threading.Event()
                # Do not wait through the backoff: verify all 300 errors are
                # scheduled even though each adoption batch is bounded to 256.
                with patch('archive_scout.media.recovery.time.time',return_value=0), \
                     patch.object(stop,'wait',side_effect=lambda seconds: stop.set()):
                    with self.assertRaises(Exception) as caught:
                        recover_active_media(cfg,db,stop)
                self.assertIn(type(caught.exception).__name__,{'Stopped'})
                self.assertEqual(db.execute('SELECT COUNT(*) FROM media_retry_schedule').fetchone()[0],300)
            finally: db.close()

    def test_schema9_upgrade_requires_safety_backup(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            db=open_database(root)
            db.execute('UPDATE schema_info SET version=9')
            db.commit();db.close()
            with patch('archive_scout.projects.backups.create_project_backup',side_effect=PermissionError('backup denied')):
                with self.assertRaises(PermissionError):
                    open_database(root)
            with sqlite3.connect(root/'archive_scout.sqlite3') as raw:
                self.assertEqual(raw.execute('SELECT version FROM schema_info').fetchone()[0],9)
            db=open_database(root)
            try:
                self.assertEqual(db.execute('SELECT version FROM schema_info').fetchone()[0],10)
                self.assertIsNotNone(db.execute("SELECT name FROM sqlite_master WHERE name='media_retry_schedule'").fetchone())
            finally: db.close()
