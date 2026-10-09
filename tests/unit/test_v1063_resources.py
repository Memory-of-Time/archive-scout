from __future__ import annotations

import hashlib
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.config import ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.downloads import downloader as download_mod
from archive_scout.downloads.downloader import download_archive_only, prepare_download_only_rows
from archive_scout.network.transports import _write_limited
from archive_scout.projects.compaction import compact_project_storage
from archive_scout.scanning.jobs import ScanJob
from archive_scout.ui.main_window import ArchiveScoutApp
from archive_scout.utils import utc_now


class _LeanDownloadClient:
    calls = 0
    hash_flags: list[bool] = []

    def __init__(self, *args, **kwargs):
        pass

    def close(self):
        return None

    def download_to_path(self, url, destination, max_bytes, *, compute_hash=True):
        del max_bytes
        type(self).calls += 1
        type(self).hash_flags.append(bool(compute_hash))
        payload = b"<html><body>resource efficient acquisition</body></html>"
        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(payload)
        return {
            "headers": {"content-type": "text/html; charset=utf-8"},
            "preview": payload,
            "bytes": len(payload),
            "content_hash": hashlib.sha256(payload).hexdigest() if compute_hash else "",
            "status": 200,
            "final_url": url,
        }


class _Notebook:
    def select(self):
        return "dashboard"

    def tab(self, _selected, _field):
        return "Dashboard"


class _AliveWorker:
    def is_alive(self):
        return True


class V1063ResourceEfficiencyTests(unittest.TestCase):
    def make_config(self, root: Path, *, workers: int = 4) -> ProjectConfig:
        return ProjectConfig(
            output_dir=root,
            targets=["example.com/*"],
            keywords=[],
            workers=workers,
            download_delay=0.0,
            download_scope="all_text",
        ).normalized()

    def insert_captures(self, database, config: ProjectConfig, count: int) -> None:
        now = utc_now()
        signature = cdx_query_signature(config)
        database.executemany(
            """INSERT INTO captures(
                   original_url,timestamp,query_signature,mimetype,statuscode,length,state,created_at,updated_at
               ) VALUES(?,?,?,?,?,?,?,?,?)""",
            [
                (
                    f"http://example.com/page-{index}.html",
                    f"200101{(index % 28) + 1:02d}{index % 24:02d}{index % 60:02d}{index % 60:02d}",
                    signature,
                    "text/html",
                    "200",
                    64 + index,
                    "pending",
                    now,
                    now,
                )
                for index in range(count)
            ],
        )
        database.commit()

    def test_download_only_streams_candidates_without_project_sized_temp_queue(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = self.make_config(root)
            database = open_database(root)
            self.insert_captures(database, config, 40)
            total, rows, stats = prepare_download_only_rows(database, config)
            self.assertEqual(total, 40)
            self.assertEqual(len(list(rows)), 40)
            self.assertEqual(stats["metadata_skipped"], 0)
            names = {
                row[0]
                for row in database.execute(
                    "SELECT name FROM sqlite_temp_master WHERE type='table'"
                )
            }
            self.assertNotIn("archive_scout_download_queue", names)
            database.close()

    def test_download_only_disables_content_hashing_and_batches_database_commits(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            # One worker deliberately forces slot-by-slot completion. The old
            # v1.0.6.3 submission loop committed once for nearly every freed
            # slot under this scheduling pattern (which Windows exposed in CI).
            config = self.make_config(root, workers=1)
            database = open_database(root)
            commits = 0
            try:
                self.insert_captures(database, config, 80)

                def trace(sql: str) -> None:
                    nonlocal commits
                    if sql.strip().upper() == "COMMIT":
                        commits += 1

                database.set_trace_callback(trace)
                _LeanDownloadClient.calls = 0
                _LeanDownloadClient.hash_flags = []
                with mock.patch.object(download_mod, "HttpClient", _LeanDownloadClient):
                    stats = download_archive_only(config, database, threading.Event(), None)
                database.set_trace_callback(None)
                empty_hashes = database.execute(
                    "SELECT COUNT(*) FROM captures WHERE COALESCE(content_hash,'')=''"
                ).fetchone()[0]
                attempts = database.execute(
                    "SELECT MIN(download_attempts),MAX(download_attempts) FROM captures"
                ).fetchone()
            finally:
                # Windows keeps an open SQLite handle locked. Always close the
                # database before TemporaryDirectory tries to remove the project,
                # even when an assertion or mocked download unexpectedly fails.
                database.set_trace_callback(None)
                database.close()
            self.assertEqual(stats["downloaded"], 80)
            self.assertEqual(_LeanDownloadClient.calls, 80)
            self.assertTrue(_LeanDownloadClient.hash_flags)
            self.assertFalse(any(_LeanDownloadClient.hash_flags))
            # Destination paths are staged in bounded groups and completion
            # results are coalesced. The commit count must not depend on how
            # quickly Windows or macOS wakes individual completed futures.
            self.assertLess(commits, 10)
            self.assertEqual(empty_hashes, 80)
            self.assertEqual(tuple(attempts), (1, 1))


    def test_compaction_backfills_deferred_download_only_hashes(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = self.make_config(root, workers=2)
            database = open_database(root)
            self.insert_captures(database, config, 2)
            _LeanDownloadClient.calls = 0
            _LeanDownloadClient.hash_flags = []
            with mock.patch.object(download_mod, "HttpClient", _LeanDownloadClient):
                stats = download_archive_only(config, database, threading.Event(), None)
            self.assertEqual(stats["downloaded"], 2)
            self.assertEqual(
                database.execute(
                    "SELECT COUNT(*) FROM captures WHERE COALESCE(content_hash,'')=''"
                ).fetchone()[0],
                2,
            )
            result = compact_project_storage(root, database, threading.Event(), None)
            self.assertEqual(result["capture_hashes_backfilled"], 2)
            hashes = [row[0] for row in database.execute(
                "SELECT content_hash FROM captures ORDER BY id"
            )]
            self.assertEqual(len(set(hashes)), 1)
            self.assertTrue(hashes[0])
            database.close()

    def test_download_only_adopts_durable_final_file_without_network(self):
        """A crash after atomic rename must not force another Wayback GET."""
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = self.make_config(root, workers=1)
            database = open_database(root)
            try:
                self.insert_captures(database, config, 1)
                row = database.execute("SELECT id FROM captures").fetchone()
                capture_id = int(row[0])
                final_path = root / "captures" / "2001" / "01" / "already-final.html"
                final_path.parent.mkdir(parents=True, exist_ok=True)
                final_path.write_bytes(b"<html><body>already durable</body></html>")
                database.execute(
                    "UPDATE captures SET local_path=? WHERE id=?",
                    (str(final_path), capture_id),
                )
                database.commit()
                _LeanDownloadClient.calls = 0
                _LeanDownloadClient.hash_flags = []
                with mock.patch.object(download_mod, "HttpClient", _LeanDownloadClient):
                    stats = download_archive_only(config, database, threading.Event(), None)
                state, local_path = database.execute(
                    "SELECT state,local_path FROM captures WHERE id=?", (capture_id,)
                ).fetchone()
            finally:
                database.close()
            self.assertEqual(stats["downloaded"], 1)
            self.assertEqual(_LeanDownloadClient.calls, 0)
            self.assertEqual(state, "downloaded_unscanned")
            self.assertEqual(Path(local_path), final_path)

    def test_no_hash_streaming_writes_same_bytes_without_digest_work(self):
        with tempfile.TemporaryDirectory() as temp:
            destination = Path(temp) / "capture.part"
            payload = b"abc" * 10000
            size, digest, preview = _write_limited(
                [payload[:12000], payload[12000:]],
                destination,
                len(payload) + 1,
                threading.Event(),
                compute_hash=False,
            )
            self.assertEqual(size, len(payload))
            self.assertEqual(digest, "")
            self.assertEqual(destination.read_bytes(), payload)
            self.assertEqual(preview, payload[:20000])

    def test_scan_hashes_bytes_already_read_instead_of_rereading_file(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path = root / "capture.html"
            payload = b"<html><title>x</title><body>needle</body></html>"
            path.write_bytes(payload)
            config = ProjectConfig(output_dir=root, targets=[], keywords=["needle"]).normalized()
            job = ScanJob.create(1, "needle", ["needle"])
            row = {
                "id": 1,
                "original_url": "http://example.com/",
                "timestamp": "20010101000000",
                "mimetype": "text/html",
                "content_hash": "",
                "final_url": "",
            }
            with mock.patch.object(download_mod, "sha256_file", side_effect=AssertionError("second disk read")):
                result = download_mod._scan_saved_capture(row, path, config, [job])
            self.assertEqual(result["content_hash"], hashlib.sha256(payload).hexdigest())

    def test_active_dashboard_uses_progress_events_instead_of_sql_recounts(self):
        fake = object.__new__(ArchiveScoutApp)
        fake.notebook = _Notebook()
        fake.worker_thread = _AliveWorker()
        fake.dashboard_refresh_job = None
        fake.refresh_dashboard = mock.Mock()
        fake.after = mock.Mock(return_value="job")
        ArchiveScoutApp.dashboard_refresh_loop(fake)
        fake.refresh_dashboard.assert_not_called()
        fake.after.assert_called_once()
        self.assertEqual(fake.after.call_args.args[0], 1000)


if __name__ == "__main__":
    unittest.main()
