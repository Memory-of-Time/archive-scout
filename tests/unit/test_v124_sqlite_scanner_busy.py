"""SQLite WAL writer races must not lose saved scan outcomes."""
from __future__ import annotations

import hashlib
import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.config import ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import (
    get_or_create_keyword_set, get_or_create_target, start_scan_run, upsert_capture,
)
from archive_scout.downloads.downloader import capture_path, save_success
from archive_scout.scanning.jobs import ScanJob
from archive_scout.scanning.pending import scan_pending_saved, _sqlite_write_busy
from archive_scout.scanning import pending as pending_module


class BusyScannerRecoveryTests(unittest.TestCase):
    def prepared(self, root: Path):
        config = ProjectConfig(output_dir=root, targets=["example.com/*"],
            keywords=["needle"], from_date="2000", to_date="2001").normalized()
        db = open_database(root)
        target = get_or_create_target(db, config.targets[0])
        upsert_capture(db, {"original": "http://example.com/test", "timestamp": "20010101000000",
            "mimetype": "text/html", "statuscode": "200", "digest": "", "length": "50"},
            target, cdx_query_signature(config))
        scan_id = start_scan_run(db, get_or_create_keyword_set(db, "sample", ["needle"]),
            "test", 1, "all")
        db.commit()
        row = db.execute("SELECT * FROM captures LIMIT 1").fetchone()
        payload = b"<html><body>needle and durable evidence</body></html>"
        path = capture_path(root, row["id"], row["timestamp"], row["original_url"])
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        save_success(db, {"capture_id": row["id"], "path": path,
            "bytes_saved": len(payload), "content_hash": hashlib.sha256(payload).hexdigest(),
            "http_status": 200, "final_url": row["original_url"],
            "response_content_type": "text/html", "deferred_scan": True})
        return config, db, [ScanJob.create(scan_id, "sample", ["needle"])]

    def test_busy_upsert_retries_whole_batch_and_keeps_single_score(self):
        with tempfile.TemporaryDirectory() as temp:
            config, db, jobs = self.prepared(Path(temp))
            try:
                original = pending_module.upsert_document
                attempts = [0]
                def conflict_once(*args, **kwargs):
                    attempts[0] += 1
                    if attempts[0] == 1:
                        raise sqlite3.OperationalError("database is locked")
                    return original(*args, **kwargs)
                with patch.object(pending_module, "upsert_document", side_effect=conflict_once):
                    self.assertEqual(scan_pending_saved(db, config, jobs, threading.Event(), workers=1), 1)
                self.assertEqual(attempts[0], 2)
                self.assertEqual(db.execute("SELECT COUNT(*) FROM document_matches").fetchone()[0], 1)
                self.assertEqual(db.execute("SELECT COUNT(*) FROM documents").fetchone()[0], 1)
                self.assertEqual(db.execute("SELECT COUNT(*) FROM errors WHERE operation='scan'").fetchone()[0], 0)
            finally:
                db.close()

    def test_external_writer_lock_retries_without_losing_local_scan(self):
        with tempfile.TemporaryDirectory() as temp:
            config, owner, jobs = self.prepared(Path(temp))
            scanner = sqlite3.connect(Path(temp) / 'archive_scout.sqlite3', timeout=0.05,
                                      check_same_thread=False)
            scanner.row_factory = sqlite3.Row
            scanner.execute("PRAGMA busy_timeout=50")
            results = {}
            stop = threading.Event()
            entered = threading.Event()
            def worker():
                entered.set()
                try:
                    results["count"] = scan_pending_saved(scanner, config, jobs, stop, workers=1)
                except BaseException as exc:
                    results["error"] = exc
            thread = None
            try:
                owner.execute("BEGIN IMMEDIATE")
                owner.execute("UPDATE captures SET updated_at=datetime('now')")
                thread = threading.Thread(target=worker, daemon=True)
                thread.start()
                self.assertTrue(entered.wait(2))
                time.sleep(0.25)
                self.assertTrue(thread.is_alive(), "scanner should retry a writer lock, not fail")
                owner.commit()
                thread.join(timeout=6)
                self.assertFalse(thread.is_alive(), "scanner did not recover after writer released")
                self.assertNotIn("error", results)
                self.assertEqual(results.get("count"), 1)
                self.assertEqual(owner.execute("SELECT COUNT(*) FROM document_matches").fetchone()[0], 1)
            finally:
                stop.set()
                owner.rollback()
                if thread and thread.is_alive():
                    thread.join(timeout=3)
                scanner.close()
                owner.close()

    def test_extended_busy_codes_are_retryable(self):
        self.assertTrue(_sqlite_write_busy(sqlite3.OperationalError('database is locked')))
        self.assertFalse(_sqlite_write_busy(sqlite3.OperationalError('database disk image is malformed')))


if __name__ == "__main__":
    unittest.main()
