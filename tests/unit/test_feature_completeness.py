import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from archive_scout.cdx.client import CDXRows, HttpClient, PermanentRequestError, TransientRequestError
from archive_scout.cdx.discovery import run_fast_discovery
from archive_scout.cdx.indexer import IndexPlan, PendingWindow, encode_plan, index_archive, save_coverage_state
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.config import MediaConfig, NetworkConfig, ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.database.lease import project_lease
from archive_scout.database.repositories import get_or_create_media_target, get_or_create_target, upsert_captures
from archive_scout.downloads.diagnostics import AcquisitionDiagnostics
from archive_scout.events import Stopped
from archive_scout.media.indexer import _save_media_state, index_media, media_index_state_signature, media_query_signature


def image_row(host, name):
    return ("20020101000000", f"http://{host}/{name}.jpg", "image/jpeg", "200", "", "16")


class FeatureCompletenessTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.config = ProjectConfig(self.root, ["slow.example/*"], [], from_date="2001", to_date="2003",
            cdx_delay=0, cdx_collapses=[], network=NetworkConfig(index_strategy="auto", cdx_workers=4),
            media=MediaConfig(enabled=True, include_extensions=["jpg"], discover_embedded=False)).normalized()
        self.db = open_database(self.root)
        self.addCleanup(self.db.close)

    def test_resumed_heartbeat_records_gap_without_attributing_a_stack(self):
        diagnostic = AcquisitionDiagnostics(self.root, None)
        self.addCleanup(diagnostic.close)
        clock = [0.0]
        monitor = diagnostic.watchdog
        monitor.clock = lambda: clock[0]
        monitor.start()
        monitor.beat("queue admission")
        clock[0] = 310
        monitor.beat("outcome processing")
        diagnostic.summary(0, force=True)
        latest = json.loads((monitor.directory / "latest.json").read_text())
        gap = latest["last_owner_heartbeat_gap"]
        self.assertEqual((gap["previous_stage"], gap["next_stage"], gap["seconds"]),
                         ("queue admission", "outcome processing", 310))
        self.assertIn("observed_at", latest)
        self.assertFalse((monitor.directory / "stall-latest.json").exists())
        monitor.close()
        clock[0] = 1000
        monitor.start()
        self.assertEqual(monitor.last_gap, gap)

    def test_final_diagnostics_preserve_batch_metrics_and_bind_records_to_the_run(self):
        first = AcquisitionDiagnostics(self.root, None)
        first.batch_metrics = lambda: dict(batch_phase='stopped', running_tasks=0)
        first.summary(1, force=True)
        first.failure({'id': 1, 'timestamp': '20010101000000', 'original_url': 'http://example.org/'},
                      'connection', ConnectionRefusedError('fixture'))
        first.watchdog.pulse = (first.watchdog.clock() - 121, 'queue admission')
        self.assertTrue(first.watchdog.check())
        first.finish_batch(2)
        self.assertIsNone(first.batch_metrics)
        first.close()
        directory = first.summary_path
        records = [json.loads(line) for line in (directory / 'history.jsonl').read_text().splitlines()]
        self.assertEqual([(record['new_saves'], record['acquisition_run_finished']) for record in records],
                         [(1, False), (2, True)])
        self.assertEqual(records[-1]['acquisition'], {'batch_phase': 'stopped', 'running_tasks': 0})
        paths = [directory / 'latest.json', next(directory.glob('sample-*.json')), directory / 'stall-latest.json']
        old_id = records[-1]['acquisition_run_id']
        self.assertTrue(all(json.loads(path.read_text())['acquisition_run_id'] == old_id for path in paths))
        second = AcquisitionDiagnostics(self.root, None)
        second.finish_batch(1, final=True)
        self.assertNotEqual(json.loads(paths[0].read_text())['acquisition_run_id'], old_id)

    def test_open_from_another_process_preserves_the_active_operation(self):
        with project_lease(self.root), self.db:
            target_id = get_or_create_target(self.db, self.config.targets[0])
            upsert_captures(self.db, [image_row("slow.example", "work")], target_id, cdx_query_signature(self.config))
            self.db.execute("UPDATE captures SET state='downloading'")
            self.db.execute("""INSERT INTO operation_runs(mode,status,started_at,updated_at,process_id,app_version)
                VALUES('all','running',datetime('now'),datetime('now'),?,'1.3.1')""", (os.getpid(),))
            self.db.commit()
            result = subprocess.run([sys.executable, "-c",
                "import sys; from pathlib import Path; from archive_scout.database.connection import open_database; "
                "db=open_database(Path(sys.argv[1])); print(*db.execute('select state from captures').fetchone(), "
                "*db.execute('select status from operation_runs').fetchone()); db.close()", str(self.root)],
                capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.strip(), "downloading running")

    def test_blocked_page_count_allows_an_unrelated_target_to_commit(self):
        cfg = replace(self.config, targets=["slow.example/*", "fast.example/*"])
        plan = IndexPlan([PendingWindow(cfg.from_date, cfg.to_date, strategy="paged", page_blocks=9)], 0, 1)
        with self.db:
            tid = get_or_create_target(self.db, cfg.targets[0])
            save_coverage_state(self.db, tid, cdx_query_signature(cfg), cfg.from_date, cfg.to_date,
                                encode_plan(plan), False, 0, None)
        fast_saved = threading.Event()
        count_started = threading.Event()

        def fetch(_client, _endpoints, params, **kwargs):
            query = dict(params)
            if "showNumPages" in query:
                count_started.set()
                self.assertTrue(fast_saved.wait(3), "a blocked count stopped ordinary indexing")
                return 0
            host = query["url"].split("/")[0]
            if host == "fast.example":
                self.assertTrue(count_started.wait(3))
            return CDXRows([image_row(host, "found")])

        def progress(event):
            data = event.detail or {}
            if data.get("phase") == "inventory complete" and data["target"] == "fast.example/*":
                self.assertEqual(self.db.execute("SELECT count(*) FROM captures WHERE original_url LIKE '%fast.example%'").fetchone()[0], 1)
                fast_saved.set()

        with patch.object(HttpClient, "get_cdx_any", fetch):
            self.assertFalse(index_archive(cfg, self.db, threading.Event(), progress))
        self.assertEqual(self.db.execute("SELECT SUM(complete) FROM index_coverage").fetchone()[0], 2)

    def test_advanced_urlkey_collapse_cannot_reuse_a_broader_date_range(self):
        broad = replace(self.config, cdx_extra_params=["collapse=urlkey"])
        narrow = replace(broad, from_date="20020101000000", to_date="20021231235959")
        calls = []
        def fetch(_client, _endpoints, params, **kwargs):
            query = dict(params)
            calls.append(query["from"])
            row = (query["from"], "http://slow.example/original", "text/html", "200", "", "16")
            return CDXRows([row])
        with patch.object(HttpClient, "get_cdx_any", fetch):
            for cfg in (broad, narrow, narrow):
                self.assertFalse(index_archive(cfg, self.db, threading.Event()))
        self.assertEqual(calls, [broad.from_date, narrow.from_date])
        rows = list(self.db.execute("SELECT timestamp FROM captures WHERE query_signature=?", (cdx_query_signature(narrow),)))
        self.assertEqual([row[0] for row in rows], [narrow.from_date])

    def test_new_targets_do_not_search_capture_history_for_legacy_window_repair(self):
        cfg = replace(self.config, targets=[f"host{i}.example/*" for i in range(12)])
        statements = []
        self.db.set_trace_callback(statements.append)
        with patch.object(HttpClient, "get_cdx_any", return_value=CDXRows([])):
            self.assertFalse(index_archive(cfg, self.db, threading.Event()))
        self.assertEqual(self.db.execute("SELECT SUM(complete) FROM index_coverage").fetchone()[0], 12)
        self.assertFalse(any("SELECT 1 FROM captures WHERE target_id=" in query for query in statements))

    def check_media_pages(self, count):
        ordinary = []
        def fetch(_client, _endpoints, params, **kwargs):
            query = dict(params)
            if "showNumPages" in query:
                return count
            if "page" in query:
                return CDXRows([image_row("slow.example", "numbered")])
            ordinary.append(query.get("resumeKey"))
            if len(ordinary) == 1:
                raise TransientRequestError("fixture gateway timeout", status=504)
            if "resumeKey" not in query:
                return CDXRows([image_row("slow.example", "numbered")], "next")
            return CDXRows([image_row("slow.example", "additional")])

        with patch.object(HttpClient, "get_cdx_any", fetch):
            self.assertEqual(index_media(self.config, self.db, threading.Event()), media_query_signature(self.config))
        self.assertEqual(ordinary, [None, None, "next"])
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM media_captures").fetchone()[0], 2)

    def test_zero_media_page_count_is_verified(self):
        self.check_media_pages(0)

    def test_positive_automatic_media_page_count_is_verified(self):
        self.check_media_pages(1)

    def test_old_empty_media_checkpoint_is_reverified_once(self):
        cfg = self.config
        with self.db:
            tid = get_or_create_media_target(self.db, cfg.targets[0])
            _save_media_state(self.db, tid, 0, media_index_state_signature(cfg, revision=8), None, True, 0, None)
        with patch.object(HttpClient, "get_cdx_any", return_value=CDXRows([image_row("slow.example", "missed")])) as fetch:
            index_media(cfg, self.db, threading.Event())
            index_media(cfg, self.db, threading.Event())
        self.assertEqual(fetch.call_count, 1)
        self.assertEqual(self.db.execute("SELECT query_signature FROM media_captures").fetchone()[0], media_query_signature(cfg))

    def test_old_partial_media_plan_keeps_cursor_and_resumes_interrupted_verification(self):
        cfg = self.config
        prior = PendingWindow("20020101000000", cfg.to_date, strategy="resume", resume_key="OLD")
        with self.db:
            tid = get_or_create_media_target(self.db, cfg.targets[0])
            _save_media_state(self.db, tid, 0, media_index_state_signature(cfg, revision=8),
                              encode_plan(IndexPlan([prior], 1, 2)), False, 3, None)
        cursors = []
        def fetch(_client, _endpoints, params, **kwargs):
            cursor = dict(params).get("resumeKey")
            cursors.append(cursor)
            if cursor == "OLD":
                return CDXRows([image_row("slow.example", "old")])
            if cursor is None:
                return CDXRows([image_row("slow.example", "rechecked")], "PROOF")
            if cursors.count("PROOF") == 1:
                raise Stopped
            return CDXRows([image_row("slow.example", "last")])
        with patch.object(HttpClient, "get_cdx_any", fetch):
            with self.assertRaises(Stopped):
                index_media(cfg, self.db, threading.Event())
            with closing(open_database(self.root)) as reopened:
                index_media(cfg, reopened, threading.Event())
                index_media(cfg, reopened, threading.Event())
        self.assertEqual(cursors, ["OLD", None, "PROOF", "PROOF"])
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM media_captures").fetchone()[0], 3)

    def test_persistent_media_index_recovers_after_repeated_503_at_same_cursor(self):
        cfg = replace(self.config, max_attempts=2,
            network=replace(self.config.network, index_strategy="resume", persistent_retries=True))
        requests = []
        def fetch(_client, _endpoints, params, **kwargs):
            cursor = dict(params).get("resumeKey")
            requests.append(cursor)
            if cursor is None:
                return CDXRows([image_row("slow.example", "first")], "SAVED")
            if len(requests) < 6:
                raise TransientRequestError("temporary service failure", status=503)
            return CDXRows([image_row("slow.example", "last")])
        with patch.object(HttpClient, "get_cdx_any", fetch), patch("archive_scout.media.indexer._wait_seconds", return_value=0):
            index_media(cfg, self.db, threading.Event())
        self.assertEqual(requests, [None] + ["SAVED"] * 5)
        self.assertEqual(self.db.execute("SELECT complete FROM media_index_state").fetchone()[0], 1)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM media_captures").fetchone()[0], 2)

    def test_media_target_rejections_are_isolated_and_resolve_independently(self):
        cfg = replace(self.config, targets=["first.example/*", "second.example/*", "good.example/*"])
        denied = {"first.example", "second.example"}
        def fetch(_client, _endpoints, params, **kwargs):
            host = dict(params)["url"].split("/")[0]
            if host in denied:
                raise PermanentRequestError("invalid host", status=400, category="invalid_index_target")
            return CDXRows([image_row(host, "saved")])
        with patch.object(HttpClient, "get_cdx_any", fetch):
            index_media(cfg, self.db, threading.Event())
            errors = dict(self.db.execute("""SELECT t.pattern,s.error_id FROM media_index_state s
                JOIN media_targets t ON t.id=s.target_id WHERE complete=0"""))
            self.assertEqual(len(set(errors.values())), 2)
            denied.remove("first.example")
            index_media(cfg, self.db, threading.Event())
        for host, error_id in errors.items():
            self.assertEqual(self.db.execute("SELECT resolved FROM errors WHERE id=?", (error_id,)).fetchone()[0],
                             int(host == "first.example/*"))
        self.assertEqual(self.db.execute("SELECT SUM(complete) FROM media_index_state").fetchone()[0], 2)
        error = PermanentRequestError("invalid regex", status=400, category="invalid_index_parameters")
        with patch.object(HttpClient, "get_cdx_any", side_effect=error), self.assertRaises(PermanentRequestError):
            index_media(cfg, self.db, threading.Event())

    def test_discovery_recovers_previously_rejected_wildcard_list_on_resume(self):
        source = self.root / "domains.txt"
        source.write_text("plain.example\n*.wild.example/*\nPLAIN.EXAMPLE/*\n*.bad.example/shared/*\n", encoding="utf-8")
        hosts = []
        def get(url, *_args):
            host = parse_qs(urlsplit(url).query)["url"][0]
            hosts.append(host)
            return {"status": 200, "data": json.dumps([["timestamp", "original"],
                ["20020101000000", f"http://{host}/item.jpg"]]).encode()}

        with patch("archive_scout.cdx.discovery.HttpClient") as factory:
            client = factory.return_value
            client.get.side_effect = get
            client.host_gate.remaining.return_value = 0
            with patch("archive_scout.cdx.discovery._domain", side_effect=lambda line: "plain.example" if line.strip() == b"plain.example" else ""):
                paths, _ = run_fast_discovery(source, self.root / "quick", "jpg")
            with sqlite3.connect(paths["urls"].parent / "discovery.sqlite3") as old:
                old.execute("PRAGMA user_version=0")
            paths, counts = run_fast_discovery(source, self.root / "quick", "jpg")
            run_fast_discovery(source, self.root / "quick", "jpg")
        self.assertEqual(hosts, ["plain.example", "wild.example"])
        self.assertEqual(counts["invalid_input_lines"], 1)
        self.assertEqual(len(paths["urls"].read_text().splitlines()), 2)
