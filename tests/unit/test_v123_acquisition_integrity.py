"""v1.2.3 fault injection: routing, collapse, storage, transport and retained bodies."""
from __future__ import annotations

import errno
import os
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from archive_scout.classification import (SUFFIX_NON_TEXT, SUFFIX_NON_TEXT_IMAGES,
                                          SUFFIX_NON_TEXT_MEDIA, classify_indexed_resource,
                                          classify_payload_kind)
from archive_scout.cdx.client import TransientRequestError
from archive_scout.cdx.indexer import (IndexPlan, PendingWindow,
    _maybe_subdivide_persistent_gateway_504, coalesce_unstarted_micro_windows)
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.config import ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, upsert_captures
from archive_scout.downloads.downloader import (_storage_full, StorageFullError,
    capture_path, download_archive, prepare_download_rows)
from archive_scout.network.transports import ResilientTransport
from archive_scout.scanning.pending import adopt_orphaned_bodies, scan_pending_saved
from archive_scout.scanning.jobs import ScanJob
from archive_scout.events import Stopped


class V123AcquisitionIntegrity(unittest.TestCase):
    def setup_one(self, folder, original='http://example.net/a.html'):
        cfg = ProjectConfig(output_dir=Path(folder), targets=['example.net/*'], keywords=['needle'],
                            download_delay=0, cdx_delay=0, retries=1).normalized()
        db = open_database(cfg.output_dir)
        target = get_or_create_target(db, cfg.targets[0])
        with db:
            upsert_captures(db, [('20010101000000', original, 'text/html', '200', 'digest', '123')],
                            target, cdx_query_signature(cfg))
            from archive_scout.cdx.indexer import save_coverage_state
            save_coverage_state(db, target, cdx_query_signature(cfg), cfg.from_date,
                                cfg.to_date, None, True, 1, None)
        return cfg, db

    def test_all_76_requested_suffixes_skip_text_regardless_of_mime(self):
        self.assertEqual(len(SUFFIX_NON_TEXT), 76)
        for suffix in SUFFIX_NON_TEXT:
            with self.subTest(suffix=suffix):
                original = f'http://example.net/path/test{suffix.upper()}'
                decision = classify_indexed_resource(original, 'text/html')
                self.assertNotEqual(decision.resource_class, 'text')
                self.assertTrue(decision.confident)
                self.assertTrue(decision.reason.startswith('suffix_policy:'))
                payload = classify_payload_kind(b'<html>needle</html>', 'text/html', original)
                self.assertEqual(payload.resource_class, decision.resource_class)
        self.assertEqual(SUFFIX_NON_TEXT, SUFFIX_NON_TEXT_IMAGES | SUFFIX_NON_TEXT_MEDIA)

    def test_query_value_does_not_change_suffix_and_descriptor_preserved(self):
        self.assertEqual(classify_indexed_resource('http://x/a.html?f=file.wmv', 'text/html').resource_class, 'text')
        self.assertEqual(classify_indexed_resource('http://x/a.WMV;token', 'text/html').resource_class, 'video')
        self.assertEqual(classify_indexed_resource('http://x/a%2EWMV&key=1', 'text/html').resource_class, 'video')
        self.assertEqual(classify_indexed_resource('http://x/a.wax', 'text/html').resource_class, 'media_descriptor')
        self.assertEqual(classify_indexed_resource('http://x/a.m4a', 'text/html').resource_class, 'audio')

    def test_mislabeled_media_never_enters_text_queue(self):
        with tempfile.TemporaryDirectory() as root:
            cfg, db = self.setup_one(root, 'http://example.net/a.mp4')
            try:
                total, rows = prepare_download_rows(db, cfg, [])
                self.assertEqual(total, 0)
                self.assertEqual(list(rows), [])
                self.assertEqual(db.execute('SELECT state FROM captures').fetchone()[0], 'skipped')
                routing = db.execute('SELECT resource_class,evidence FROM capture_routing').fetchone()
                self.assertEqual(routing[0], 'video')
                self.assertIn('suffix_policy', routing[1])
            finally:
                db.close()

    def test_collapsed_504_never_destroys_range_semantics(self):
        with tempfile.TemporaryDirectory() as root:
            db = open_database(Path(root))
            try:
                window = PendingWindow('20000101000000', '20051231235959')
                plan = IndexPlan([window], 0, 1)
                error = TransientRequestError('HTTP 504', status=504, splittable=True)
                for _ in range(4):
                    self.assertFalse(_maybe_subdivide_persistent_gateway_504(
                        db, plan, window, error, lambda *args: None, 0, None, None, 0, 1,
                        collapse_enabled=True))
                self.assertEqual(len(plan.pending), 1)
                self.assertEqual(plan.pending[0].start, '20000101000000')
                self.assertEqual(plan.gateway_splits_without_response, 0)
            finally:
                db.close()

    def test_legacy_tiny_windows_repaired_only_without_collapsed_progress(self):
        from datetime import datetime, timedelta
        begin = datetime(2001, 1, 1)
        pieces = [(begin + timedelta(seconds=i)).strftime('%Y%m%d%H%M%S') for i in range(30)]
        make_plan = lambda: IndexPlan([PendingWindow(value, value, strategy='resume') for value in pieces], 0, 30)
        plan = make_plan()
        self.assertEqual(coalesce_unstarted_micro_windows(plan, seen=0, collapse_enabled=False), 29)
        self.assertEqual((plan.pending[0].start, plan.pending[0].end), (pieces[0], pieces[-1]))
        self.assertEqual(len(plan.pending), 1)
        plan = make_plan()
        self.assertEqual(coalesce_unstarted_micro_windows(plan, seen=120, collapse_enabled=True), 0)
        self.assertEqual(len(plan.pending), 30)
        plan = make_plan()
        plan.pending[8].resume_key = 'committed-cursor'
        coalesce_unstarted_micro_windows(plan, seen=0, collapse_enabled=False)
        self.assertEqual([p.resume_key for p in plan.pending if p.resume_key], ['committed-cursor'])

    def test_disk_full_is_never_a_transient_download_error(self):
        self.assertTrue(_storage_full(OSError(errno.ENOSPC, 'disk full')))
        self.assertFalse(_storage_full(OSError(errno.EACCES, 'denied')))
        class NoSpaceClient:
            def __init__(self, *args, **kwargs): pass
            def get(self, *_args, **_kwargs): raise OSError(errno.ENOSPC, 'disk full')
            def close(self): pass
            @property
            def host_gate(self):
                class Gate:
                    @staticmethod
                    def snapshot(): return {}
                return Gate()
        with tempfile.TemporaryDirectory() as root:
            cfg, db = self.setup_one(root)
            try:
                with patch('archive_scout.downloads.downloader.HttpClient', NoSpaceClient):
                    with self.assertRaises(StorageFullError):
                        download_archive(cfg, db, 1, threading.Event(), None,
                                         scan_jobs=[ScanJob.create(1, 'test', ['needle'])])
                self.assertEqual(db.execute("SELECT COUNT(*) FROM errors WHERE category='unknown'").fetchone()[0], 0)
            finally:
                db.close()

    def test_atomic_saved_body_is_adopted_without_network(self):
        with tempfile.TemporaryDirectory() as root:
            cfg, db = self.setup_one(root)
            try:
                row = db.execute('SELECT id,timestamp,original_url FROM captures').fetchone()
                path = capture_path(cfg.output_dir, int(row['id']),row['timestamp'],row['original_url'])
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b'<html><title>needle</title><body>needle</body></html>')
                import base64, hashlib
                with db:
                    db.execute("UPDATE captures SET state='error',download_attempts=1,digest=?",
                        (base64.b32encode(hashlib.sha1(path.read_bytes()).digest()).decode(),))
                self.assertEqual(adopt_orphaned_bodies(db, cfg, threading.Event()), 1)
                self.assertEqual(db.execute('SELECT state FROM captures').fetchone()[0], 'downloaded_unscanned')
                self.assertEqual(adopt_orphaned_bodies(db, cfg, threading.Event()), 0)
            finally:
                db.close()

    def test_older_pending_routing_is_refreshed_without_deleting_scans(self):
        from archive_scout.database.classification import classify_indexed_captures
        with tempfile.TemporaryDirectory() as root:
            cfg, db = self.setup_one(root, 'http://example.net/a.mjpeg')
            try:
                sig = cdx_query_signature(cfg)
                with db:
                    db.execute("""INSERT OR REPLACE INTO capture_routing
                        (capture_id,resource_class,evidence,confident,routing,updated_at)
                        VALUES(1,'text','mime:text/html',1,'pending','old')""")
                self.assertGreaterEqual(classify_indexed_captures(db,sig), 1)
                row = db.execute('SELECT resource_class,evidence FROM capture_routing').fetchone()
                self.assertEqual(row[0], 'video')
                self.assertEqual(row[1], 'suffix_policy:.mjpeg')
                self.assertEqual(classify_indexed_captures(db,sig), 0)
            finally:
                db.close()

    def test_temp_queue_file_backed(self):
        with tempfile.TemporaryDirectory() as root:
            db = open_database(Path(root))
            try:
                self.assertEqual(db.execute('PRAGMA temp_store').fetchone()[0], 1)
            finally:
                db.close()

    def test_explicit_urllib3_does_not_instantiate_unused_httpx(self):
        class Dummy:
            def __init__(self, *a, **kw): pass
            def close(self): pass
        with patch('archive_scout.network.transports.HttpxBackend', side_effect=ValueError('missing SOCKS module')) as httpx, \
             patch('archive_scout.network.transports.Urllib3Backend', Dummy):
            transport = ResilientTransport(pool_size=2,connect_timeout=1,read_timeout=1,mode='urllib3')
            try:
                self.assertEqual(transport.backend_names, ('urllib3',))
                httpx.assert_not_called()
            finally:
                transport.close()


if __name__ == '__main__':
    unittest.main()
