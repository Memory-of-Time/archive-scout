"""Scout 1.2.1 restoration contracts: no external network needed."""
from __future__ import annotations

import hashlib
from contextlib import closing
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.config import MediaConfig, ProjectConfig, save_project_config, load_project_config
from archive_scout.database.connection import open_database
from archive_scout.database.lease import project_lease
from archive_scout.database.repositories import get_or_create_target, upsert_capture, upsert_document, record_error
from archive_scout.downloads.rate_limit import SharedHostGate
from archive_scout.events import ProgressEvent
from archive_scout.media.discovery import discover_media
from archive_scout.operations import recover_transient_text_errors, run_project
from archive_scout.projects.backups import create_project_backup, restore_project_backup
from archive_scout.projects.paths import _owned_project_suffix
from archive_scout.utils import hash_text


class V121RestorationTests(unittest.TestCase):
    def _cfg(self, root: Path, **kwargs) -> ProjectConfig:
        return ProjectConfig(
            output_dir=root, targets=['example.net/*'], keywords=[],
            from_date='20010101', to_date='20011231', cdx_delay=0,
            **kwargs,
        ).normalized()

    def test_index_and_download_only_requires_no_keywords_or_scan_jobs(self):
        class FakeClient:
            def __init__(self, *args, **kwargs):
                pass
            def get(self, url, max_bytes, **kwargs):
                return {'data': b'<html><body>saved text</body></html>', 'status': 200,
                        'headers': {'content-type': 'text/html'}, 'final_url': url}
            def close(self):
                pass
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = self._cfg(root)
            def fake_index(c, db, stop, callback):
                t = get_or_create_target(db, 'example.net/*')
                upsert_capture(db, {'original': 'http://example.net/a.html',
                    'timestamp': '20010101000000', 'mimetype': 'text/html',
                    'statuscode': '200', 'digest': 'D1', 'length': '50'},
                    t, cdx_query_signature(c))
                db.commit()
            with patch('archive_scout.operations.index_archive', side_effect=fake_index), \
                 patch('archive_scout.downloads.downloader.HttpClient', FakeClient):
                result = run_project(cfg, 'download_only')
            self.assertIn('project', result)
            db = open_database(root)
            try:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM scan_runs').fetchone()[0], 0)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM documents').fetchone()[0], 0)
                self.assertEqual(db.execute("SELECT COUNT(*) FROM captures WHERE state='downloaded_unscanned'").fetchone()[0], 1)
                self.assertEqual(len(list((root/'captures').rglob('*.txt'))), 1)
            finally:
                db.close()

    def test_full_operation_with_index_only_scope_skips_scan_and_download(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = self._cfg(Path(tmp), download_scope='index_only')
            with patch('archive_scout.operations.index_archive') as idx, \
                 patch('archive_scout.operations.download_archive') as acquire:
                result = run_project(cfg, 'all')
            idx.assert_called_once()
            acquire.assert_not_called()
            db = open_database(Path(tmp))
            try:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM scan_runs').fetchone()[0], 0)
            finally:
                db.close()
            self.assertIn('project', result)

    def test_retry_sweep_visits_a_still_failing_capture_only_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = self._cfg(Path(tmp))
            db = open_database(Path(tmp))
            try:
                t = get_or_create_target(db, 'example.net/*')
                upsert_capture(db, {'original': 'http://example.net/failed',
                    'timestamp': '20010101000000', 'mimetype': 'text/html',
                    'statuscode': '200', 'digest': 'D1', 'length': '5'}, t, cdx_query_signature(cfg))
                db.execute("UPDATE captures SET state='error'")
                capture_id = db.execute('SELECT id FROM captures').fetchone()[0]
                record_error(db, 'download', 'network', 'fail', capture_id=capture_id, retryable=True)
                db.commit()
                with patch('archive_scout.operations.download_archive') as downloader:
                    recover_transient_text_errors(cfg, db, 0, [], threading.Event(), None)
                self.assertEqual(downloader.call_count, 1)
            finally:
                db.close()

    def test_project_lease_rejects_second_process_and_releases(self):
        with tempfile.TemporaryDirectory() as tmp:
            snippet = (
                'from archive_scout.database.lease import project_lease\n'
                'from pathlib import Path\n'
                'with project_lease(Path(__import__("sys").argv[1])): pass\n'
            )
            with project_lease(Path(tmp)):
                p = subprocess.run([sys.executable, '-c', snippet, tmp], capture_output=True, text=True)
                self.assertNotEqual(p.returncode, 0)
                self.assertIn('active operation', p.stderr)
            p = subprocess.run([sys.executable, '-c', snippet, tmp], capture_output=True, text=True)
            self.assertEqual(p.returncode, 0, p.stderr)

    def test_move_rebases_existing_document_paths_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            original = Path(tmp) / 'original'
            moved = Path(tmp) / 'moved'
            db = open_database(original)
            capture_file = original/'captures'/'2001'/'01'/'page.txt'
            capture_file.parent.mkdir(parents=True)
            capture_file.write_bytes(b'portable text')
            t = get_or_create_target(db, 'example.net/*')
            upsert_capture(db, {'original': 'http://example.net/a', 'timestamp': '20010101000000',
                'mimetype': 'text/html', 'statuscode': '200', 'digest': 'D', 'length': '13'}, t, 'fixture')
            capture_id = db.execute('SELECT id FROM captures').fetchone()[0]
            upsert_document(db, capture_id, capture_file, 'title', 'portable text', [], hashlib.sha256(b'portable text').hexdigest(), hash_text('portable text'), 13)
            db.commit();db.close()
            save_project_config(self._cfg(original))
            shutil.move(str(original),str(moved))
            restored_config = load_project_config(moved/'project.json')
            self.assertTrue(os.path.samefile(restored_config.output_dir, moved))
            db = open_database(moved)
            try:
                path = Path(db.execute('SELECT path FROM documents').fetchone()[0])
                self.assertTrue(path.is_file())
                self.assertTrue(os.path.samefile(path, moved/'captures'/'2001'/'01'/'page.txt'))
            finally:db.close()

    def test_windows_short_and_long_root_spelling_is_project_owned(self):
        previous = r'C:\Users\runneradmin\AppData\Local\Temp\tmp123\original'
        short = r'C:\Users\RUNNER~1\AppData\Local\Temp\tmp123\original\captures\2001\page.txt'
        external = r'C:\Users\RUNNER~1\AppData\Local\Temp\other\original\captures\2001\page.txt'
        self.assertEqual(_owned_project_suffix(short, previous), 'captures/2001/page.txt')
        self.assertIsNone(_owned_project_suffix(external, previous))

    def test_rebase_handles_old_path_alias_after_relocation(self):
        # Mac temporary paths can be spelled /var or /private/var. On Windows
        # the same old root may use an 8.3 path or a long spelling. Rebase must
        # resolve aliases rather than comparing stored path strings.
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            alias = base.parent / (base.name + '_alias')
            try:
                alias.symlink_to(base, target_is_directory=True)
            except (OSError, NotImplementedError):
                self.skipTest('Directory symlink unavailable on this runner')
            try:
                original = base / 'original'
                moved = base / 'moved'
                db = open_database(original)
                canonical_file = original / 'captures' / '2001' / 'page.txt'
                canonical_file.parent.mkdir(parents=True)
                canonical_file.write_bytes(b'from alias')
                t = get_or_create_target(db, 'example.net/*')
                upsert_capture(db, {'original': 'http://example.net/alias',
                    'timestamp': '20010101000000', 'mimetype': 'text/plain',
                    'statuscode': '200', 'digest': 'X', 'length': '10'}, t, 'fixture')
                capture_id = db.execute('SELECT id FROM captures').fetchone()[0]
                aliased_file = alias / 'original' / 'captures' / '2001' / 'page.txt'
                upsert_document(db, capture_id, aliased_file, 'title', 'from alias', [],
                    hashlib.sha256(b'from alias').hexdigest(), hash_text('from alias'), 10)
                db.commit()
                db.close()
                shutil.move(str(original), str(moved))
                db = open_database(moved)
                try:
                    path = Path(db.execute('SELECT path FROM documents').fetchone()[0])
                    self.assertTrue(path.is_file())
                    self.assertTrue(os.path.samefile(path, moved / 'captures' / '2001' / 'page.txt'))
                finally:
                    db.close()
            finally:
                alias.unlink(missing_ok=True)

    def test_restore_safety_backup_contains_committed_wal_changes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db = open_database(root)
            db.execute("INSERT INTO project_meta(key,value) VALUES('before','one')")
            db.commit()
            db.close()
            baseline = create_project_backup(root, keep=99)
            writer = sqlite3.connect(root / 'archive_scout.sqlite3')
            try:
                writer.execute("INSERT INTO project_meta(key,value) VALUES('after','two')")
                writer.commit()
                # Deliberately keep this WAL connection open across restoration.
                # Old restore code failed on Windows by unlinking the live -wal.
                safety = restore_project_backup(root, baseline)
                with closing(sqlite3.connect(safety)) as copy:
                    self.assertEqual(copy.execute(
                        "SELECT value FROM project_meta WHERE key='after'"
                    ).fetchone()[0], 'two')
                with closing(sqlite3.connect(root / 'archive_scout.sqlite3')) as restored:
                    self.assertIsNone(restored.execute(
                        "SELECT value FROM project_meta WHERE key='after'"
                    ).fetchone())
                # The previously-open connection must still refer to the live
                # database after restoration, not an unlinked, stale file.
                writer.execute("INSERT INTO project_meta(key,value) VALUES('continued','yes')")
                writer.commit()
                with closing(sqlite3.connect(root / 'archive_scout.sqlite3')) as other:
                    self.assertEqual(other.execute(
                        "SELECT value FROM project_meta WHERE key='continued'"
                    ).fetchone()[0], 'yes')
            finally:
                writer.close()

    def test_restore_rejects_corrupt_backup_without_mutating_live_database(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db = open_database(root)
            db.execute("INSERT INTO project_meta(key,value) VALUES('keep','intact')")
            db.commit()
            db.close()
            bad = root / 'bad backup #1.sqlite3'
            bad.write_bytes(b'not a sqlite database')
            with self.assertRaises(sqlite3.DatabaseError):
                restore_project_backup(root, bad)
            with closing(sqlite3.connect(root / 'archive_scout.sqlite3')) as live:
                self.assertEqual(live.execute(
                    "SELECT value FROM project_meta WHERE key='keep'"
                ).fetchone()[0], 'intact')

    def test_media_include_extensions_are_respected_by_embed_parser(self):
        media=MediaConfig(include_images=True, include_videos=False,
                          include_extensions=['.jpg'], exclude_extensions=[]).normalized()
        found=discover_media('<img src="a.gif"><img src="b.jpg"><img src="noextension">',
                             'https://example.net/page.html',media)
        paths={x.url for x in found}
        self.assertNotIn('https://example.net/a.gif',paths)
        self.assertIn('https://example.net/b.jpg',paths)
        self.assertIn('https://example.net/noextension',paths)

    def test_connection_recovery_defaults_off_and_triggers_only_distinct_outages(self):
        cfg=self._cfg(Path('.'))
        self.assertFalse(cfg.network.coordinated_connection_recovery)
        gate=SharedHostGate(1,1)
        gate.last_reachable=time.monotonic()-20
        self.assertFalse(gate.connection_failed('https://example.net/a',3,1))
        self.assertFalse(gate.connection_failed('https://example.net/a',3,1))
        self.assertFalse(gate.connection_failed('https://example.net/b',3,1))
        self.assertTrue(gate.connection_failed('https://example.net/c',3,1))
        self.assertGreater(gate.remaining(),0)
        time.sleep(1.02)
        permit=gate.acquire_request(threading.Event())
        self.assertTrue(permit.probe)
        gate.finish_request(permit,True)
        self.assertEqual(gate.remaining(),0)
        self.assertFalse(gate.snapshot()['probe_required'])


if __name__=='__main__': unittest.main()
