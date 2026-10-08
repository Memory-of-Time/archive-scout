from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest import mock

from archive_scout.cdx.client import RateLimitDeferred
from archive_scout.config import MediaConfig, NetworkConfig, ProjectConfig, ReportConfig, REPORT_FIELD_NAMES, REPORT_OUTPUT_NAMES
from archive_scout.constants import VERSION
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, upsert_capture, get_or_create_keyword_set, start_scan_run, finish_scan_run
from archive_scout.downloads.rate_limit import reset_shared_traffic_state_for_tests
from archive_scout.events import ConnectivityPaused, Stopped
from archive_scout.operations import run_project
from archive_scout.ui.main_window import ArchiveScoutApp


class IndexReportTests(unittest.TestCase):
    def setUp(self):
        reset_shared_traffic_state_for_tests()
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.config = ProjectConfig(self.root, ["example.com/*"], [], from_date="2001", to_date="2001").normalized()

    def tearDown(self):
        self.temp.cleanup()
        reset_shared_traffic_state_for_tests()

    def index_one(self, config, database, stop_event, callback=None):
        target = get_or_create_target(database, config.targets[0])
        upsert_capture(database, {
            "original": "http://example.com/indexed.txt", "timestamp": "20010101000000",
            "mimetype": "text/plain", "statuscode": "200", "length": "10", "digest": "ABC",
        }, target, "fixture")
        database.commit()

    def counts(self):
        database = open_database(self.root)
        try:
            return {
                "scans": database.execute("SELECT COUNT(*) FROM scan_runs").fetchone()[0],
                "documents": database.execute("SELECT COUNT(*) FROM documents").fetchone()[0],
                "operations": [(row[0], row[1]) for row in database.execute("SELECT mode,status FROM operation_runs ORDER BY id")],
            }
        finally:
            database.close()

    def test_completed_index_generates_inventory_and_reports_completion(self):
        events = []
        with mock.patch("archive_scout.operations.index_archive", side_effect=self.index_one):
            paths = run_project(self.config, "index", threading.Event(), events.append)
        self.assertEqual(set(paths), {"project", "all_indexed_urls", "summary", "errors", "site_issues"})
        self.assertIn("http://example.com/indexed.txt", paths["all_indexed_urls"].read_text(encoding="utf-8"))
        self.assertIn("Bodies searched: 0", paths["summary"].read_text(encoding="utf-8"))
        reports = [event for event in events if event.stage == "report"]
        self.assertEqual(len(reports), 1)
        self.assertTrue(reports[0].detail["index_complete"])
        self.assertEqual(len(reports[0].detail["report_files"]), 4)
        self.assertEqual(self.counts()["scans"], 0)
        self.assertEqual(VERSION, "1.0.9")

    def test_zero_capture_index_still_writes_a_summary(self):
        with mock.patch("archive_scout.operations.index_archive"):
            paths = run_project(self.config, "index")
        self.assertIn("Indexed captures: 0", paths["summary"].read_text(encoding="utf-8"))
        self.assertTrue(paths["all_indexed_urls"].is_file())

    def test_pause_writes_partial_inventory_and_resume_restores_index_contract(self):
        def stopped(config, database, stop, callback=None):
            self.index_one(config, database, stop, callback)
            stop.set()
            raise Stopped
        with mock.patch("archive_scout.operations.index_archive", side_effect=stopped):
            with self.assertRaises(Stopped):
                run_project(self.config, "index", threading.Event())
        summary = (self.root / "reports" / "summary.txt").read_text(encoding="utf-8")
        self.assertIn("partial", summary)
        self.assertIn("http://example.com/indexed.txt", (self.root / "reports" / "all_indexed_urls.txt").read_text(encoding="utf-8"))
        self.assertEqual(self.counts()["operations"], [("index", "interrupted")])
        with mock.patch("archive_scout.operations.index_archive", side_effect=self.index_one) as indexing, \
             mock.patch("archive_scout.operations.download_archive", side_effect=AssertionError("index resume downloaded")), \
             mock.patch("archive_scout.operations.prepare_scan_jobs", side_effect=AssertionError("index resume created scans")):
            paths = run_project(replace(self.config, targets=["different.example/*"]), "resume", threading.Event())
        self.assertEqual(indexing.call_args.args[0].targets, ["example.com/*"])
        self.assertNotIn("partial", paths["summary"].read_text(encoding="utf-8"))
        self.assertEqual(self.counts()["operations"], [("index", "interrupted"), ("index", "complete")])
        self.assertEqual(self.counts()["scans"], 0)

    def test_one_shot_service_pause_exports_partial_without_losing_deadline(self):
        config = replace(self.config, network=NetworkConfig(persistent_retries=False))
        pause = RateLimitDeferred("quota pause", eligible_at_epoch=1234567890, incident_id=4, status=429)
        def paused(config, database, stop, callback=None):
            self.index_one(config, database, stop, callback)
            raise pause
        with mock.patch("archive_scout.operations.index_archive", side_effect=paused):
            with self.assertRaises(RateLimitDeferred) as raised:
                run_project(config, "index")
        self.assertIs(raised.exception, pause)
        database = open_database(self.root)
        try:
            import json
            row = database.execute("SELECT status,progress_json FROM operation_runs ORDER BY id DESC LIMIT 1").fetchone()
            detail = json.loads(row["progress_json"])["detail"]
            self.assertEqual(row["status"], "paused")
            self.assertEqual(detail["eligible_at_epoch"], 1234567890)
            self.assertEqual(detail["reason_code"], "service_rate_limit")
        finally:
            database.close()
        self.assertIn("partial", (self.root / "reports" / "summary.txt").read_text(encoding="utf-8"))

    def test_one_shot_connectivity_pause_exports_partial(self):
        config = replace(self.config, network=NetworkConfig(persistent_retries=False))
        with mock.patch("archive_scout.operations.index_archive", side_effect=ConnectivityPaused("offline")):
            with self.assertRaises(ConnectivityPaused):
                run_project(config, "index")
        self.assertIn("partial", (self.root / "reports" / "summary.txt").read_text(encoding="utf-8"))

    def test_partial_report_failure_preserves_original_stop_and_operation_status(self):
        events = []
        with mock.patch("archive_scout.operations.index_archive", side_effect=Stopped), \
             mock.patch("archive_scout.operations.generate_index_reports", side_effect=OSError("report output unavailable")):
            with self.assertRaises(Stopped):
                run_project(self.config, "index", threading.Event(), events.append)
        self.assertEqual(self.counts()["operations"], [("index", "interrupted")])
        self.assertTrue(any(event.detail.get("reason_code") == "index_report_write_failed" for event in events))

    def test_index_only_scope_uses_index_reports_without_scans_or_media(self):
        config = replace(self.config, download_scope="index_only", media=MediaConfig(enabled=True))
        for mode in ("all", "external_media_after_scan"):
            with self.subTest(mode=mode), mock.patch("archive_scout.operations.index_archive", side_effect=self.index_one), \
                 mock.patch("archive_scout.operations.prepare_scan_jobs", side_effect=AssertionError("scan created")), \
                 mock.patch("archive_scout.operations._run_standard_media_phase", side_effect=AssertionError("media downloaded")):
                paths = run_project(config, mode)
                self.assertTrue(paths["all_indexed_urls"].is_file())
        self.assertEqual(self.counts()["scans"], 0)

    def test_regenerate_after_index_uses_inventory_even_with_older_scan(self):
        database = open_database(self.root)
        try:
            keyword = get_or_create_keyword_set(database, "Older scan", ["needle"])
            scan = start_scan_run(database, keyword, "Older scan", 1, "rescan")
            finish_scan_run(database, scan, "complete")
            database.commit()
        finally:
            database.close()
        with mock.patch("archive_scout.operations.index_archive", side_effect=self.index_one):
            run_project(self.config, "index")
        for _ in range(2):
            with mock.patch("archive_scout.operations.generate_reports", side_effect=AssertionError("used historical scan")):
                paths = run_project(self.config, "report")
            self.assertTrue(paths["all_indexed_urls"].is_file())
            self.assertNotIn("matches_ranked", paths)
        self.assertEqual(self.counts()["scans"], 1)

    def test_regenerate_partial_inventory_retains_partial_label(self):
        with mock.patch("archive_scout.operations.index_archive", side_effect=Stopped):
            with self.assertRaises(Stopped):
                run_project(self.config, "index")
        paths = run_project(self.config, "report")
        self.assertIn("partial", paths["summary"].read_text(encoding="utf-8"))

    def test_disabled_reports_remain_disabled_and_explain_empty_folder(self):
        for outputs in ([], ["matched_urls"]):
            events = []
            config = replace(self.config, report=ReportConfig(outputs=outputs))
            with self.subTest(outputs=outputs), mock.patch("archive_scout.operations.index_archive", side_effect=self.index_one):
                paths = run_project(config, "index", callback=events.append)
            self.assertEqual(set(paths), {"project"})
            self.assertEqual(list((self.root / "reports").iterdir()), [])
            self.assertTrue(any(event.detail.get("reason_code") == "index_reports_disabled" for event in events))

    def test_plain_index_url_field_selection_is_respected(self):
        config = replace(self.config, report=ReportConfig(outputs=["all_indexed_urls"], fields={"all_indexed_urls": ["original_url"]}))
        with mock.patch("archive_scout.operations.index_archive", side_effect=self.index_one):
            paths = run_project(config, "index")
        self.assertEqual(paths["all_indexed_urls"].read_text(encoding="utf-8").splitlines(), ["http://example.com/indexed.txt"])

    def test_index_url_preset_selects_plain_inventory_and_none_stays_empty(self):
        class Variable:
            def __init__(self): self.value = True
            def set(self, value): self.value = value
        window = SimpleNamespace(
            report_output_vars={name: Variable() for name in REPORT_OUTPUT_NAMES},
            report_field_vars={name: {field: Variable() for field in fields} for name, fields in REPORT_FIELD_NAMES.items()},
        )
        window.set_report_fields = lambda name, value: ArchiveScoutApp.set_report_fields(window, name, value)
        ArchiveScoutApp.set_report_preset(window, "index_urls")
        self.assertEqual([name for name, value in window.report_output_vars.items() if value.value], ["all_indexed_urls"])
        self.assertEqual([name for name, value in window.report_field_vars["all_indexed_urls"].items() if value.value], ["original_url"])
        ArchiveScoutApp.set_report_preset(window, "none")
        self.assertFalse(any(value.value for value in window.report_output_vars.values()))


if __name__ == "__main__":
    unittest.main()
