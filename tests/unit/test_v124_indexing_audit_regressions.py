"""Release-gate fault injection for Archive Scout v1.2.4 indexing corrections."""
from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from archive_scout.cdx.client import CDXRows, MalformedCDXResponse, TransientRequestError, HttpClient, parse_cdx_rows_payload
from archive_scout.cdx.indexer import index_archive, validate_cdx_scope
from archive_scout.cdx.parallel import PageFetchResult, memory_limited_page_workers
from archive_scout.config import ProjectConfig, NetworkConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, upsert_captures
from archive_scout.downloads.downloader import excluded_robots_filename, prepare_download_rows
from archive_scout.events import Stopped


def project(root, targets):
    return ProjectConfig(
        output_dir=root, targets=targets, keywords=['needle'],
        from_date='20010101000000', to_date='20010101235959',
        cdx_delay=0, network=NetworkConfig(index_strategy='resume'),
    ).normalized()


class IndexAuditV124(unittest.TestCase):
    def test_bad_target_does_not_starve_a_healthy_target(self):
        with tempfile.TemporaryDirectory() as temp:
            config = project(Path(temp), ['bad.example/*', 'good.example/*'])
            db = open_database(config.output_dir)
            stop = threading.Event()
            calls = []
            def response(_client, urls, params, **kwargs):
                target = dict(params)['url']
                calls.append(target)
                if target.startswith('bad.'):
                    raise TransientRequestError('upstream 504', status=504)
                stop.set()  # stop after the first healthy response is committed
                return CDXRows([('20010101010101', 'http://good.example/a', 'text/html', '200', 'ABC', '14')])
            try:
                with patch.object(HttpClient, 'get_cdx_any', response):
                    with self.assertRaises(Stopped):
                        index_archive(config, db, stop)
                self.assertGreaterEqual(len(calls), 2)
                self.assertTrue(calls[0].startswith('bad.'))
                self.assertTrue(any(c.startswith('good.') for c in calls))
                self.assertEqual(db.execute('SELECT COUNT(*) FROM captures').fetchone()[0], 1)
                self.assertGreater(db.execute('SELECT COUNT(*) FROM index_coverage WHERE complete=0').fetchone()[0], 0)
            finally:
                db.close()

    def test_valid_zero_rows_but_never_scalar_json(self):
        self.assertEqual(parse_cdx_rows_payload([]).rows, [])
        for wrong in (None, False, 5, 'service unavailable'):
            with self.subTest(wrong=wrong):
                with self.assertRaises(MalformedCDXResponse):
                    parse_cdx_rows_payload(wrong)
        rows = CDXRows([('20010101010101', 'http://good.example/a', 'text/html', '200', 'A', '1')])
        self.assertIs(parse_cdx_rows_payload(rows), rows)  # legacy fixture adapter

    def test_bad_date_and_status_never_commit(self):
        for item in [
            ('20100101010101', 'http://example.com/a', 'text/html', '200', 'A', '1'),
            ('20010101010101', 'http://example.com/a', 'text/html', '404', 'A', '1'),
            ('20010101010101', 'http://unrelated.invalid/a', 'text/html', '200', 'A', '1'),
        ]:
            with self.subTest(item=item):
                with self.assertRaises(MalformedCDXResponse):
                    validate_cdx_scope([item], 'example.com/*', '20010101000000', '20010101235959', ['statuscode:200'])

    def test_time_budget_validated_and_opt_in(self):
        with tempfile.TemporaryDirectory() as temp:
            c = project(Path(temp), ['example.com/*'])
            self.assertFalse(c.skip_slow_index_targets)
            self.assertEqual(c.index_target_minutes, 10)
            c.index_target_minutes = 0
            with self.assertRaises(ValueError):
                c.normalized()

    def test_budget_deferral_keeps_exact_cursor_and_incomplete(self):
        with tempfile.TemporaryDirectory() as temp:
            cfg = project(Path(temp), ['example.com/*'])
            cfg.skip_slow_index_targets = True
            cfg.index_target_minutes = 0.00005
            cfg = cfg.normalized()
            db = open_database(cfg.output_dir)
            calls = []
            def response(_client, urls, params, **kwargs):
                import time
                calls.append(dict(params).get('resumeKey'))
                time.sleep(0.01)
                return CDXRows([('20010101010101', 'http://example.com/a', 'text/html', '200', 'A', '1')], 'cursor1')
            try:
                with patch.object(HttpClient, 'get_cdx_any', response):
                    deferred = index_archive(cfg, db, threading.Event())
                self.assertTrue(deferred)
                self.assertEqual(len(calls), 1)
                state = db.execute('SELECT complete,plan_json FROM index_coverage').fetchone()
                self.assertEqual(int(state['complete']), 0)
                self.assertIn('cursor1', state['plan_json'])
                self.assertEqual(db.execute('SELECT COUNT(*) FROM captures').fetchone()[0], 1)
            finally:
                db.close()

    def test_robots_txt_path_only_exclusion(self):
        urls = [
            'http://example.com/robots.txt',
            'http://example.com/ROBOTS.TXT?x=1',
            'http://example.com/%72obots%2Etxt',
            'http://example.com/sub/robots.txt',
            'http://example.com/article.html?file=robots.txt',
        ]
        self.assertEqual([excluded_robots_filename(u) for u in urls], [True]*4+[False])
        with tempfile.TemporaryDirectory() as temp:
            cfg=project(Path(temp),['example.com/*'])
            db=open_database(cfg.output_dir)
            from archive_scout.cdx.parameters import cdx_query_signature
            try:
                with db:
                    tid=get_or_create_target(db, 'example.com/*')
                    sig=cdx_query_signature(cfg)
                    upsert_captures(db, [('20010101010101',u,'text/plain','200','ABC','3') for u in urls],tid,sig)
                count, rows=prepare_download_rows(db,cfg,[])
                self.assertEqual(count,1)
                self.assertEqual([r['original_url'] for r in rows],[urls[-1]])
                self.assertEqual(db.execute("SELECT COUNT(*) FROM capture_routing WHERE routing='excluded_robots_txt'").fetchone()[0],4)
            finally: db.close()

    def test_overlap_preserves_both_scopes_without_second_body(self):
        with tempfile.TemporaryDirectory() as temp:
            db=open_database(Path(temp))
            try:
                first=get_or_create_target(db,'*.example.com/*')
                second=get_or_create_target(db,'foo.example.com/a/*')
                row=('20010101010101','http://foo.example.com/a/x','text/html','200','A','3')
                with db:
                    upsert_captures(db,[row],first,'identical-semantic-signature')
                    upsert_captures(db,[row],second,'identical-semantic-signature')
                self.assertEqual(db.execute('SELECT COUNT(*) FROM captures').fetchone()[0],1)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM capture_target_membership').fetchone()[0],2)
            finally: db.close()

    def test_page_memory_ceiling_is_bounded(self):
        self.assertEqual(memory_limited_page_workers(10, 108*1024*1024, 512), 4)
        self.assertEqual(memory_limited_page_workers(10, 192*1024*1024, 512), 2)
        self.assertEqual(memory_limited_page_workers(1, 200*1024*1024, 128), 1)
        self.assertEqual(PageFetchResult(0,[],0.1,resume_key='more').resume_key,'more')


if __name__ == '__main__':
    unittest.main()

class MediaAuditV124(unittest.TestCase):
    @staticmethod
    def config(root,targets):
        from archive_scout.config import MediaConfig
        return ProjectConfig(
            output_dir=root,targets=targets,keywords=[],
            from_date='20010101000000',to_date='20010101235959',cdx_delay=0,
            network=NetworkConfig(index_strategy='resume'),
            media=MediaConfig(enabled=True, include_extensions=['jpg'],include_images=True,
                              include_videos=False, discover_embedded=False),
        ).normalized()

    def test_media_403_is_incomplete_and_keeps_pending_plan(self):
        from archive_scout.cdx.client import PermanentRequestError
        from archive_scout.media.indexer import index_media
        with tempfile.TemporaryDirectory() as temp:
            cfg=self.config(Path(temp),['example.com/*'])
            db=open_database(cfg.output_dir)
            try:
                def blocked(*args,**kwargs):
                    raise PermanentRequestError('403',status=403,category='wayback_forbidden')
                with patch.object(HttpClient,'get_cdx_any',blocked):
                    with self.assertRaises(PermanentRequestError):
                        index_media(cfg,db,threading.Event())
                state=db.execute('SELECT complete,resume_key FROM media_index_state').fetchone()
                self.assertEqual(state['complete'],0)
                self.assertIsNotNone(state['resume_key'])
            finally: db.close()

    def test_media_504_yields_to_healthy_target(self):
        from archive_scout.media.indexer import index_media
        with tempfile.TemporaryDirectory() as temp:
            cfg=self.config(Path(temp),['bad.example/*','good.example/*'])
            db=open_database(cfg.output_dir)
            stop=threading.Event()
            calls=[]
            def mocked(_client,urls,params,**kwargs):
                target=dict(params)['url'];calls.append(target)
                if target.startswith('bad.'):
                    raise TransientRequestError('gateway 504',status=504)
                stop.set()
                return CDXRows([('20010101010101','http://good.example/a.jpg','image/jpeg','200','A','4')])
            try:
                with patch.object(HttpClient,'get_cdx_any',mocked):
                    with self.assertRaises(Stopped):
                        index_media(cfg,db,stop)
                self.assertTrue(calls[0].startswith('bad.'))
                self.assertTrue(any(v.startswith('good.') for v in calls))
                self.assertEqual(db.execute('SELECT COUNT(*) FROM media_captures').fetchone()[0],1)
                self.assertEqual(db.execute('SELECT complete FROM media_index_state ORDER BY rowid LIMIT 1').fetchone()[0],0)
            finally: db.close()

    def test_media_deferral_preserves_page_grouping_and_cursor(self):
        from archive_scout.cdx.indexer import PendingWindow,IndexPlan
        from archive_scout.media.indexer import _defer_media_window
        from archive_scout.database.repositories import get_or_create_media_target
        with tempfile.TemporaryDirectory() as temp:
            cfg=self.config(Path(temp),['example.com/*'])
            db=open_database(cfg.output_dir)
            win=PendingWindow('20010101000000','20010101235959',strategy='paged',page_blocks=8,
                              page_count=100,page=20,resume_key='cursor',failures=1)
            plan=IndexPlan([win,PendingWindow('20010201000000','20010228235959')],0,2)
            try:
                tid=get_or_create_media_target(db,'example.com/*')
                with db:
                    _defer_media_window(cfg,db,plan,win,tid,2001,'sig',0,None,'example.com/*','2001',
                        TransientRequestError('503',status=503),0,2,threading.Event(),None,True)
                self.assertEqual((win.page_blocks,win.page_count,win.page,win.resume_key),(8,100,20,'cursor'))
            finally: db.close()
