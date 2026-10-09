from __future__ import annotations

import hashlib
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.config import ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_keyword_set, start_scan_run
from archive_scout.downloads import downloader as download_mod
from archive_scout.downloads.downloader import download_archive
from archive_scout.utils import utc_now


class _ImmediateClient:
    lock = threading.Lock()
    call_times: list[float] = []

    def __init__(self, *args, **kwargs):
        pass

    def close(self):
        return None

    def download_to_path(self, url, destination, max_bytes):
        del max_bytes
        payload = b"<html><body>needle</body></html>"
        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(payload)
        with self.lock:
            self.call_times.append(time.monotonic())
        return {
            "headers": {"content-type": "text/html; charset=utf-8"},
            "preview": payload,
            "bytes": len(payload),
            "content_hash": hashlib.sha256(payload).hexdigest(),
            "status": 200,
            "final_url": url,
        }


class V1061PerformanceHotfixTests(unittest.TestCase):
    def test_database_uses_fast_wal_profile_and_local_path_index(self):
        with tempfile.TemporaryDirectory() as temp:
            db = open_database(Path(temp))
            self.assertEqual(int(db.execute("PRAGMA synchronous").fetchone()[0]), 1)  # NORMAL
            self.assertEqual(int(db.execute("PRAGMA wal_autocheckpoint").fetchone()[0]), 10000)
            indexes = {str(row[1]) for row in db.execute("PRAGMA index_list('captures')")}
            self.assertIn("captures_local_path_idx", indexes)
            db.close()

    def test_slow_scanner_does_not_throttle_replay_submission(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(
                output_dir=root,
                targets=["example.com/*"],
                keywords=["needle"],
                workers=10,
                scan_workers=1,
                download_delay=0.125,
            ).normalized()
            signature = cdx_query_signature(config)
            db = open_database(root)
            now = utc_now()
            rows = [
                (
                    f"http://example.com/page-{index}.html",
                    f"20010101{index:06d}"[:14],
                    signature,
                    "text/html",
                    "200",
                    35,
                    "pending",
                    now,
                    now,
                )
                for index in range(40)
            ]
            db.executemany(
                """INSERT INTO captures(
                       original_url,timestamp,query_signature,mimetype,statuscode,length,state,created_at,updated_at
                   ) VALUES(?,?,?,?,?,?,?,?,?)""",
                rows,
            )
            keyword_set_id = get_or_create_keyword_set(db, "hotfix", ["needle"])
            scan_run_id = start_scan_run(db, keyword_set_id, "hotfix", 1, "test")
            db.commit()
            db.close()

            release_scan = threading.Event()
            worker_error: list[BaseException] = []
            original_scan = download_mod._scan_saved_capture

            def blocked_scan(*args, **kwargs):
                release_scan.wait(timeout=3)
                return original_scan(*args, **kwargs)

            _ImmediateClient.call_times = []

            def run() -> None:
                connection = open_database(root)
                try:
                    download_archive(
                        config,
                        connection,
                        scan_run_id,
                        threading.Event(),
                        None,
                    )
                except BaseException as exc:  # surfaced in main test thread
                    worker_error.append(exc)
                finally:
                    connection.close()

            with mock.patch.object(download_mod, "HttpClient", _ImmediateClient), mock.patch.object(
                download_mod, "_scan_saved_capture", blocked_scan
            ):
                thread = threading.Thread(target=run, daemon=True)
                thread.start()
                deadline = time.monotonic() + 1.5
                while time.monotonic() < deadline:
                    with _ImmediateClient.lock:
                        count = len(_ImmediateClient.call_times)
                    if count >= 30:
                        break
                    time.sleep(0.01)
                with _ImmediateClient.lock:
                    count_before_scan_release = len(_ImmediateClient.call_times)
                release_scan.set()
                thread.join(timeout=10)

            self.assertFalse(thread.is_alive())
            if worker_error:
                raise worker_error[0]
            # With the v1.0.6 backpressure gate a blocked one-worker scanner
            # stalls acquisition after the small scan backlog fills. v1.0.6.1
            # must continue feeding replay independently.
            self.assertGreaterEqual(count_before_scan_release, 30)
            db = open_database(root)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM captures WHERE state='downloaded'").fetchone()[0], 40)
            db.close()


if __name__ == "__main__":
    unittest.main()
