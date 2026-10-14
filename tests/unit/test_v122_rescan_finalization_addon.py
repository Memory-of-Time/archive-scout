"""v1.2.2: large-report status and interrupted report-export recovery."""
from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.config import ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, upsert_capture, upsert_document
from archive_scout.events import Stopped
from archive_scout.operations import run_project
from archive_scout.reports.text import _write_selected, _copy_latest
from archive_scout.utils import hash_text, normalize_search


class RescanFinalizationTests(unittest.TestCase):
    def prepare(self, root: Path) -> ProjectConfig:
        config = ProjectConfig(
            output_dir=root, targets=["example.org/*"], keywords=["needle"],
            from_date="20000101000000", to_date="20091231235959",
            workers=2,
        ).normalized()
        db = open_database(root)
        try:
            tid = get_or_create_target(db, "example.org/*")
            path = root / "document.txt"
            path.write_text("needle", encoding="utf-8")
            upsert_capture(db, {
                "original": "http://example.org/document",
                "timestamp": "20010101000000", "mimetype": "text/plain",
                "statuscode": "200", "digest": "digest", "length": "6",
            }, tid, cdx_query_signature(config))
            cid = int(db.execute("SELECT id FROM captures").fetchone()[0])
            upsert_document(
                db, cid, path, "", "needle", [],
                hash_text("needle"), hash_text(normalize_search("needle")), 6,
            )
            db.commit()
        finally:
            db.close()
        return config

    def test_export_failure_retains_scan_evidence_and_can_retry_same_run(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            config = self.prepare(root)
            with patch("archive_scout.operations.generate_job_reports", side_effect=OSError("report device temporarily unavailable")):
                with self.assertRaisesRegex(OSError, "report device"):
                    run_project(config, "rescan")
            db = open_database(root)
            try:
                run_id = int(db.execute("SELECT id FROM scan_runs").fetchone()[0])
                self.assertEqual(db.execute("SELECT status FROM scan_runs").fetchone()[0], "interrupted")
                self.assertEqual(db.execute("SELECT COUNT(*) FROM document_matches WHERE scan_run_id=?", (run_id,)).fetchone()[0], 1)
                self.assertEqual(db.execute("SELECT status FROM operation_runs ORDER BY id DESC LIMIT 1").fetchone()[0], "failed")
            finally:
                db.close()
            from archive_scout.scanning import rescanner
            with patch.object(rescanner, "_analyze_saved_document", side_effect=AssertionError("already-scanned body was scanned again")):
                events = []
                result = run_project(config, "rescan", callback=events.append)
            self.assertTrue(result["summary"].is_file())
            self.assertIn("needle", result["matches_ranked"].read_text(encoding="utf-8"))
            self.assertTrue(any(e.stage == "scan_finalize" for e in events))
            self.assertTrue(any(e.stage == "report" and "Generating" in e.message for e in events))
            self.assertTrue(any(e.stage == "report" and "Wrote" in e.message for e in events))
            db = open_database(root)
            try:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM scan_runs").fetchone()[0], 1)
                self.assertEqual(db.execute("SELECT status FROM scan_runs").fetchone()[0], "complete")
                self.assertEqual(db.execute("SELECT COUNT(*) FROM document_matches").fetchone()[0], 1)
            finally:
                db.close()

    def test_long_export_emits_progress_and_interruption_keeps_previous_report(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "matches_ranked.txt").write_text("previous report\n", encoding="utf-8")
            events = []
            stopped = threading.Event()

            def progress(name, count, done):
                events.append((name, count, done))
                if count >= 10000 and not done:
                    stopped.set()

            with self.assertRaises(Stopped):
                _write_selected(root, None, "matches_ranked", (str(i) for i in range(12000)), {},
                                progress=progress, stop_event=stopped)
            self.assertIn(("matches_ranked", 10000, False), events)
            self.assertEqual((root / "matches_ranked.txt").read_text(encoding="utf-8"), "previous report\n")
            self.assertFalse(list(root.glob("matches_ranked.txt.*.tmp")))

    def test_failed_latest_report_promotion_keeps_previous_report(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            previous = root / "matches_ranked.txt"
            new = root / "new-report.txt"
            previous.write_text("old verified report\n", encoding="utf-8")
            new.write_text("new verified report\n", encoding="utf-8")
            with patch("archive_scout.reports.text.os.replace", side_effect=OSError("filesystem failure")):
                with self.assertRaisesRegex(OSError, "filesystem failure"):
                    _copy_latest(new, previous)
            self.assertEqual(previous.read_text(encoding="utf-8"), "old verified report\n")
            self.assertFalse(list(root.glob("matches_ranked.txt.*.tmp")))

    def test_successful_rescan_completes_and_reports(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            config = self.prepare(root)
            events = []
            result = run_project(config, "rescan", callback=events.append)
            self.assertTrue(result["summary"].is_file())
            db = open_database(root)
            try:
                self.assertEqual(db.execute("SELECT status FROM scan_runs").fetchone()[0], "complete")
                self.assertEqual(db.execute("SELECT status FROM operation_runs").fetchone()[0], "complete")
            finally:
                db.close()
            self.assertEqual(events[-1].stage, "report")


if __name__ == "__main__":
    unittest.main()
