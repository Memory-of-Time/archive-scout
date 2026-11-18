import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from archive_scout.cdx.client import CDXRows, HttpClient, TransientRequestError, _permanent_http_rejection
from archive_scout.cdx.indexer import PendingWindow, _ledger_complete, index_archive
from archive_scout.cdx.parameters import cdx_query_signature, parse_num_pages
from archive_scout.config import NetworkConfig, ProjectConfig
from archive_scout.database.classification import classify_indexed_captures
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, upsert_captures
from archive_scout.events import Stopped
from archive_scout.operations import run_project
from archive_scout.projects.merge import merge_projects
from archive_scout.reports.text import generate_index_reports
from archive_scout.utils import utc_now


def capture(host, n=0, mime="text/html", digest="A"):
    return ("20020101000000", f"http://{host}/{n}", mime, "200", digest, str(n + 1))


class NormalInventoryTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.cfg = ProjectConfig(self.root, ["*.a.example/*"], [], from_date="2001",
            to_date="2003", cdx_delay=0, cdx_collapses=[], page_size=10000,
            network=NetworkConfig(index_strategy="auto", trust_environment=False)).normalized()
        self.db = open_database(self.root)
        self.addCleanup(lambda: self.db.close())

    def test_count_wire_types_are_unambiguous(self):
        for bad in (False, True, -1, "-1", 0.0, None, {}, [], "٢", "0.5",
                    2**63, [["ignored"], [0]], {"error": "no index"}):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                parse_num_pages(bad)
        self.assertEqual(parse_num_pages([["numpages"], ["1"]]), 1)
        for valid in (0, "0", {"pages": "0"}, [["pages"], [0]]):
            self.assertEqual(parse_num_pages(valid), 0)

    def test_malformed_and_zero_count_follow_ordinary_cursor_until_terminal(self):
        for count in (False, -1, 0):
            with self.subTest(count=count):
                with self.db:
                    self.db.execute("DELETE FROM index_coverage")
                    self.db.execute("DELETE FROM captures")
                calls = []
                def fetch(_client, endpoints, params, **kwargs):
                    q = dict(params)
                    calls.append(q)
                    if len(calls) == 1:
                        raise TransientRequestError("gateway", status=504)
                    if "showNumPages" in q:
                        return count
                    if "resumeKey" not in q:
                        return CDXRows([capture("a.example")], "opaque")
                    return CDXRows([capture("a.example", 1)])
                with patch.object(HttpClient, "get_cdx_any", fetch):
                    self.assertFalse(index_archive(self.cfg, self.db, threading.Event()))
                self.assertEqual([q.get("resumeKey") for q in calls], [None, None, None, "opaque"])
                self.assertEqual(self.db.execute("SELECT count(*) FROM captures").fetchone()[0], 2)
                self.assertEqual(tuple(self.db.execute("SELECT complete,terminal_method FROM index_coverage").fetchone()),
                                 (1, "paged_then_resume"))

    def test_scoped_denials_recover_independently_and_invalid_shared_filter_raises(self):
        domains = ["a.example", "b.example", "c.example", "d.example"]
        cfg = replace(self.cfg, targets=[domain + "/*" for domain in domains])
        denied = {"a.example": 403, "b.example": 403, "c.example": 400}
        def fetch(_client, endpoints, params, **kwargs):
            host = dict(params)["url"].split("/")[0]
            if host in denied:
                raise _permanent_http_rejection(denied[host], "https://example.test/cdx", {}, b"invalid host")
            return CDXRows([capture(host)])
        with patch.object(HttpClient, "get_cdx_any", fetch):
            self.assertTrue(index_archive(cfg, self.db, threading.Event()))
            before = dict(self.db.execute("SELECT t.pattern,c.error_id FROM index_coverage c JOIN targets t ON t.id=c.target_id"))
            self.assertEqual(len({before[h + "/*"] for h in denied}), 3)
            del denied["a.example"]
            self.assertFalse(index_archive(replace(cfg, targets=["a.example/*"]), self.db, threading.Event()))
        for host in domains[:3]:
            state = self.db.execute("SELECT resolved FROM errors WHERE id=?", (before[host + "/*"],)).fetchone()
            self.assertEqual(state[0], int(host == "a.example"))
        rejected = _permanent_http_rejection(400, "fixture", {}, b"Invalid regex")
        with patch.object(HttpClient, "get_cdx_any", side_effect=rejected), self.assertRaises(type(rejected)):
            index_archive(replace(cfg, cdx_filters=["original:invalid"]), self.db, threading.Event())

    def test_persistent_gateway_recovers_after_immediate_budget_at_exact_cursor(self):
        cfg = replace(self.cfg, network=replace(self.cfg.network, index_strategy="resume"))
        calls = []
        def fetch(_client, endpoints, params, **kwargs):
            q = dict(params)
            calls.append(q.get("resumeKey"))
            if len(calls) == 1:
                return CDXRows([capture("a.example")], "next")
            if len(calls) < 8:
                raise TransientRequestError("bad gateway", status=502)
            return CDXRows([capture("a.example", 1)])
        with patch.object(HttpClient, "get_cdx_any", fetch), patch("archive_scout.cdx.indexer.transient_backoff", return_value=0):
            self.assertFalse(index_archive(cfg, self.db, threading.Event()))
        self.assertEqual(calls, [None] + ["next"] * 7)
        self.assertEqual(self.db.execute("SELECT count(*) FROM captures").fetchone()[0], 2)
        self.assertFalse(self.db.execute("SELECT 1 FROM errors WHERE resolved=0").fetchone())

    def test_healthy_scope_commits_before_slow_request_finishes(self):
        cfg = replace(self.cfg, targets=["slow.example/*", "fast.example/*", "third.example/*"])
        release = threading.Event()
        begun = threading.Event()
        committed = []
        def fetch(_client, endpoints, params, **kwargs):
            host = dict(params)["url"].split("/")[0]
            if host == "slow.example":
                begun.set()
                self.assertTrue(release.wait(5))
            else:
                self.assertTrue(begun.wait(5))
            return CDXRows([capture(host)])
        def progress(e):
            if (e.detail or {}).get("phase") == "inventory complete":
                committed.append(e.detail["target"])
                if "fast.example/*" in committed:
                    self.assertEqual(self.db.execute("SELECT COUNT(*) FROM captures WHERE original_url LIKE '%fast.example%'").fetchone()[0],1)
                    release.set()
        with patch.object(HttpClient, "get_cdx_any", fetch):
            self.assertFalse(index_archive(cfg, self.db, threading.Event(), progress))
        self.assertLess(committed.index("fast.example/*"), committed.index("slow.example/*"))
        self.assertEqual(len(committed), 3)

    def test_lazy_manifest_survives_stop_before_unregistered_targets(self):
        cfg = replace(self.cfg, targets=[f"host{i}.example/*" for i in range(200)])
        stop = threading.Event()
        def interrupt(*args, **kwargs):
            stop.set()
            raise Stopped
        with patch.object(HttpClient, "get_cdx_any", interrupt), self.assertRaises(Stopped):
            run_project(cfg, "index", stop)
        self.assertLessEqual(self.db.execute("SELECT count(*) FROM targets").fetchone()[0], 4)
        self.assertIn("host199.example", self.db.execute("SELECT value FROM project_meta WHERE key='resume_request'").fetchone()[0])
        with patch.object(HttpClient, "get_cdx_any", return_value=CDXRows([])) as requested:
            result = run_project(replace(cfg, targets=["different.example/*"]), "resume")
        self.assertEqual(result.status, "complete")
        self.assertEqual(requested.call_count, 200)
        self.assertEqual(self.db.execute("SELECT SUM(complete) FROM index_coverage").fetchone()[0], 200)

    def test_conflict_observations_survive_repeated_upsert_and_merge(self):
        a = get_or_create_target(self.db, "*.a.example/*")
        b = get_or_create_target(self.db, "a.example/*")
        sig = cdx_query_signature(self.cfg)
        for target, digest in ((a, "A"), (b, "B"), (a, "A")):
            with self.db:
                upsert_captures(self.db, [capture("a.example", digest=digest)], target, sig)
        query = "SELECT target_pattern,digest FROM capture_cdx_observations ORDER BY digest"
        expected = [("*.a.example/*", "A"), ("a.example/*", "B")]
        self.assertEqual(list(map(tuple, self.db.execute(query))), expected)
        self.assertEqual(self.db.execute("SELECT count(*) FROM captures").fetchone()[0], 1)
        dest = self.root / "merge"
        out = open_database(dest)
        try:
            merge_projects(dest, self.root, out)
            self.assertEqual(list(map(tuple, out.execute(query))), expected)
        finally:
            out.close()

    def test_routing_checkpoint_and_inventory_cache_follow_old_row_edits(self):
        a = get_or_create_target(self.db, "*.a.example/*")
        sig = cdx_query_signature(self.cfg)
        with self.db:
            upsert_captures(self.db, [capture("a.example", i) for i in range(20)], a, sig)
        self.assertEqual(classify_indexed_captures(self.db, sig, batch_size=3), 20)
        report = generate_index_reports(self.cfg, self.db, reuse_inventory=True)["all_indexed_urls"]
        previous = report.read_bytes()
        with patch("archive_scout.reports.text._indexed_inventory_lines", side_effect=AssertionError("redundant export")):
            generate_index_reports(self.cfg, self.db, reuse_inventory=True)
        with self.db:
            upsert_captures(self.db, [capture("a.example", mime="image/png")], a, sig)
        self.assertEqual(classify_indexed_captures(self.db, sig, batch_size=4), 1)
        self.assertEqual(self.db.execute("SELECT resource_class FROM capture_routing WHERE capture_id=1").fetchone()[0], "image")
        generate_index_reports(self.cfg, self.db, reuse_inventory=True)
        current = report.read_bytes()
        self.assertNotEqual(current, previous)
        report.write_bytes(b"altered externally")
        generate_index_reports(self.cfg, self.db, reuse_inventory=True)
        self.assertEqual(report.read_bytes(), current)
        with self.db:
            self.db.execute("UPDATE capture_routing SET routing='downloaded' WHERE capture_id=1")
            upsert_captures(self.db, [capture("a.example", mime="text/html")], a, sig)
        classify_indexed_captures(self.db, sig)
        self.assertEqual(self.db.execute("SELECT routing FROM capture_routing WHERE capture_id=1").fetchone()[0], "downloaded")

    def test_page_gap_check_is_layout_bound_and_accepts_only_contiguous_ledger(self):
        target = get_or_create_target(self.db, "a.example/*")
        key = ("sig", target, self.cfg.from_date, self.cfg.to_date, "layout")
        current = PendingWindow(key[2], key[3], page_count=4, page=4)
        def save(page, layout):
            self.db.execute("""INSERT INTO index_pages(query_signature,target_id,window_start,window_end,
                layout_signature,page,row_count,status,updated_at) VALUES(?,?,?,?,?,?,1,'complete',?)""",
                (*key[:4], layout, page, utc_now()))
        for page in (0, 2, 3):
            save(page, "layout")
        save(1, "old")
        self.assertFalse(_ledger_complete(self.db, current, key))
        self.assertEqual(current.page, 1)
        save(1, "layout")
        self.assertTrue(_ledger_complete(self.db, current, key))

    def test_local_write_rollback_restores_last_committed_cursor(self):
        import sqlite3
        calls = []
        def fetch(_client, endpoints, params, **kwargs):
            token = dict(params).get("resumeKey")
            calls.append(token)
            if token is None:
                return CDXRows([capture("a.example")], "next")
            return CDXRows([capture("a.example", 1)])
        def fail_after_insert(db, rows, target, sig):
            result = upsert_captures(db, rows, target, sig)
            if rows[0][1].endswith("/1"):
                raise sqlite3.OperationalError("injected disk I/O failure")
            return result
        with patch.object(HttpClient, "get_cdx_any", fetch):
            with patch("archive_scout.cdx.indexer.upsert_captures", fail_after_insert), self.assertRaises(sqlite3.OperationalError):
                index_archive(self.cfg, self.db, threading.Event())
            self.assertEqual(self.db.execute("SELECT count(*) FROM captures").fetchone()[0], 1)
            saved = self.db.execute("SELECT complete,plan_json FROM index_coverage").fetchone()
            self.assertFalse(saved[0])
            self.assertIn('"resume_key":"next"', saved[1])
            self.assertFalse(index_archive(self.cfg, self.db, threading.Event()))
        self.assertEqual(calls, [None, "next", "next"])
        self.assertEqual(self.db.execute("SELECT count(*) FROM captures").fetchone()[0], 2)

    def test_unknown_cdx_length_and_numeric_zero_are_distinct_observations(self):
        a = get_or_create_target(self.db, "a.example/*")
        sig = cdx_query_signature(self.cfg)
        fields = capture("a.example")[:5]
        with self.db:
            upsert_captures(self.db, [(*fields, length) for length in ("-", "0", "00", "-")], a, sig)
        self.assertEqual({r[0] for r in self.db.execute("SELECT length FROM capture_cdx_observations")}, {"-", "0", "00"})
        self.assertEqual(tuple(self.db.execute("SELECT length,cdx_length_raw FROM captures").fetchone()), (0, "-"))

    def test_legacy_shared_error_waits_for_last_outstanding_owner(self):
        from archive_scout.database.repositories import record_error
        from archive_scout.cdx.indexer import save_coverage_state
        sig = cdx_query_signature(self.cfg)
        with self.db:
            shared = record_error(self.db, "index", "wayback_forbidden", "legacy restriction", http_status=403)
            for host in ("a.example", "b.example"):
                target = get_or_create_target(self.db, host + "/*")
                save_coverage_state(self.db, target, sig, self.cfg.from_date, self.cfg.to_date, None, False, 0, shared)
        for host, resolved in (("a.example", 0), ("b.example", 1)):
            with patch.object(HttpClient, "get_cdx_any", return_value=CDXRows([])):
                self.assertFalse(index_archive(replace(self.cfg, targets=[host+"/*"]), self.db, threading.Event()))
            self.assertEqual(self.db.execute("SELECT resolved FROM errors WHERE id=?", (shared,)).fetchone()[0], resolved)

    def test_schema12_completed_zero_and_numbered_scopes_recheck_once(self):
        from archive_scout.cdx.indexer import save_coverage_state
        import sqlite3
        cfg = replace(self.cfg, targets=[f"{n}.example/*" for n in ("empty", "numbered", "ordinary")])
        sig = cdx_query_signature(cfg)
        with self.db:
            for host, strategy, seen in (("empty", "resume", 0), ("numbered", "paged", 1), ("ordinary", "resume", 1)):
                target = get_or_create_target(self.db, host + ".example/*")
                if seen:
                    upsert_captures(self.db, [capture(host + ".example")], target, sig)
                save_coverage_state(self.db, target, sig, cfg.from_date, cfg.to_date, None, True, seen, None, strategy)
            self.db.execute("UPDATE schema_info SET version=12")
        self.db.close()
        self.db = open_database(self.root)
        backups = list((self.root / "backups").glob("*before_schema_13*.sqlite3"))
        self.assertEqual(len(backups), 1)
        with sqlite3.connect(backups[0]) as saved:
            self.assertEqual(saved.execute("SELECT version FROM schema_info").fetchone()[0], 12)
            self.assertEqual(saved.execute("SELECT SUM(complete) FROM index_coverage").fetchone()[0], 3)
        self.assertEqual(self.db.execute("SELECT SUM(complete) FROM index_coverage").fetchone()[0], 1)
        calls = []
        def fetch(_client, endpoints, params, **kwargs):
            q = dict(params)
            self.assertIn("showResumeKey", q)
            host = q["url"].split("/")[0]
            calls.append(host)
            return CDXRows([capture(host, 1)])
        with patch.object(HttpClient, "get_cdx_any", fetch):
            self.assertFalse(index_archive(cfg, self.db, threading.Event()))
            self.assertFalse(index_archive(cfg, self.db, threading.Event()))
        self.assertCountEqual(calls, ["empty.example", "numbered.example"])
        self.assertEqual(self.db.execute("SELECT count(*) FROM captures").fetchone()[0], 4)

    def test_legacy_partial_window_finishes_saved_cursor_before_full_verification(self):
        from archive_scout.cdx.indexer import IndexPlan, encode_plan, save_coverage_state
        sig = cdx_query_signature(self.cfg)
        with self.db:
            target = get_or_create_target(self.db, self.cfg.targets[0])
            old = IndexPlan([PendingWindow("20020101000000", self.cfg.to_date, "saved")], 1, 2)
            save_coverage_state(self.db, target, sig, self.cfg.from_date, self.cfg.to_date,
                                encode_plan(old), False, 1, None)
            self.db.execute("UPDATE schema_info SET version=12")
        self.db.close()
        self.db = open_database(self.root)
        calls = []
        def fetch(_client, endpoints, params, **kwargs):
            q = dict(params)
            calls.append((q["from"], q.get("resumeKey")))
            return CDXRows([capture("a.example", len(calls))])
        with patch.object(HttpClient, "get_cdx_any", fetch):
            self.assertFalse(index_archive(self.cfg, self.db, threading.Event()))
        self.assertEqual(calls, [("20020101000000", "saved"), (self.cfg.from_date, None)])
        self.assertEqual(self.db.execute("SELECT complete FROM index_coverage").fetchone()[0], 1)
