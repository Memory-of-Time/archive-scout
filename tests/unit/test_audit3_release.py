from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from archive_scout.cdx.indexer import index_archive, uncovered_index_ranges
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.classification import (
    PREVIEW_BUDGET,
    RESOURCE_CLASSIFIER_REVISION,
    classify_indexed_resource,
    classify_payload_prefix,
)
from archive_scout.config import MediaConfig, NetworkConfig, ProjectConfig, load_project_config, save_project_config
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_keyword_set, start_scan_run
from archive_scout.downloads.downloader import (
    _commit_non_text_discard,
    _finish_discard_cleanup,
    download_archive,
    download_archive_only,
    recover_pending_discard_cleanup,
)
from archive_scout.media.indexer import index_media, media_index_state_signature
from archive_scout.network.transports import PreviewRejected, _write_limited
from archive_scout.scanning.hitlist import search_with_hitlist
from archive_scout.scanning.jobs import ScanJob
from archive_scout.reports.text import generate_reports
from archive_scout.ui.dashboard_refresh import DashboardRefreshController
from archive_scout.utils import utc_now


HEADER = ["timestamp", "original", "mimetype", "statuscode", "digest", "length"]


def _insert_capture(database, signature: str, url: str, *, mimetype: str = "text/html", state: str = "pending", local_path: str | None = None, availability: str = "not_acquired") -> int:
    now = utc_now()
    cur = database.execute(
        """INSERT INTO captures(
               original_url,timestamp,query_signature,mimetype,statuscode,length,state,local_path,
               payload_availability,created_at,updated_at
           ) VALUES(?, '20010101000000', ?, ?, '200', 100, ?, ?, ?, ?, ?)""",
        (url, signature, mimetype, state, local_path, availability, now, now),
    )
    return int(cur.lastrowid)


class _PayloadClient:
    bodies: dict[str, bytes] = {}
    calls = 0

    def __init__(self, *args, **kwargs):
        del args, kwargs

    def close(self):
        return None

    def metrics_snapshot(self):
        return {
            "request_starts": type(self).calls,
            "request_completions": type(self).calls,
            "request_failures": 0,
            "network_bytes": 0,
            "retry_waits": 0,
            "rate_limit_events": 0,
            "pacing_wait_seconds": 0.0,
            "host_gate_wait_seconds": 0.0,
            "retry_wait_seconds": 0.0,
            "rate_limit_wait_seconds": 0.0,
            "network_seconds": 0.0,
        }

    def download_to_path(self, url, destination, max_bytes, compute_hash=False, preview_validator=None):
        del max_bytes, compute_hash
        type(self).calls += 1
        original = url.split("id_/", 1)[-1]
        body = type(self).bodies.get(original, b"<html><body>plain text</body></html>")
        headers = {"content-type": "text/html; charset=utf-8"}
        if preview_validator is not None:
            rejected = preview_validator(headers, body[:PREVIEW_BUDGET])
            if rejected:
                raise PreviewRejected(str(rejected))
        Path(destination).parent.mkdir(parents=True, exist_ok=True)
        Path(destination).write_bytes(body)
        return {
            "headers": headers,
            "preview": body[:PREVIEW_BUDGET],
            "bytes": len(body),
            "content_hash": "",
            "status": 200,
            "final_url": url,
        }


class Audit3ReleaseTests(unittest.TestCase):
    def test_twenty_year_sparse_range_completes_in_one_data_request(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(
                root, ["example.com/*"], [], from_date="2000", to_date="2019", cdx_delay=0,
                network=NetworkConfig(index_strategy="auto"),
            ).normalized()
            db = open_database(root)
            calls: list[dict[str, str]] = []
            rows = [
                [f"{2000 + (i % 20):04d}0101000000", f"http://example.com/{i}", "text/html", "200", f"d{i}", "10"]
                for i in range(100)
            ]

            def fake_get(_self, _urls, params, max_bytes=0, prefer_text=False):
                del max_bytes, prefer_text
                calls.append(dict(params))
                return [HEADER, *rows]

            with mock.patch("archive_scout.cdx.client.HttpClient.get_cdx_any", new=fake_get):
                index_archive(config, db, threading.Event())
            self.assertEqual(len(calls), 1)
            self.assertNotIn("showNumPages", calls[0])
            self.assertEqual(calls[0]["from"], "20000101000000")
            self.assertEqual(calls[0]["to"], "20191231235959")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM captures").fetchone()[0], 100)
            coverage = db.execute("SELECT complete,range_start,range_end FROM index_coverage").fetchone()
            self.assertEqual(tuple(coverage), (1, "20000101000000", "20191231235959"))
            db.close()

    def test_range_extension_queries_only_uncovered_gap(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            calls: list[dict[str, str]] = []

            def fake_get(_self, _urls, params, max_bytes=0, prefer_text=False):
                del max_bytes, prefer_text
                calls.append(dict(params))
                return []

            first = ProjectConfig(root, ["example.com/*"], [], from_date="2000", to_date="2009", cdx_delay=0, cdx_collapses=[]).normalized()
            second = ProjectConfig(root, ["example.com/*"], [], from_date="2000", to_date="2019", cdx_delay=0, cdx_collapses=[]).normalized()
            self.assertEqual(cdx_query_signature(first), cdx_query_signature(second))
            db = open_database(root)
            with mock.patch("archive_scout.cdx.client.HttpClient.get_cdx_any", new=fake_get):
                index_archive(first, db, threading.Event())
                index_archive(second, db, threading.Event())
            self.assertEqual(len(calls), 2)
            self.assertEqual(calls[0]["from"], "20000101000000")
            self.assertEqual(calls[0]["to"], "20091231235959")
            self.assertEqual(calls[1]["from"], "20100101000000")
            self.assertEqual(calls[1]["to"], "20191231235959")
            target_id = int(db.execute("SELECT id FROM targets WHERE pattern='example.com/*'").fetchone()[0])
            self.assertEqual(uncovered_index_ranges(db, target_id, cdx_query_signature(second), second.from_date, second.to_date), [])
            db.close()

    def test_year_scope_signature_retains_date_partition_identity(self):
        a = ProjectConfig(Path('.'), ["example.com/*"], [], from_date="2000", to_date="2009", text_collapse_scope="year").normalized()
        b = ProjectConfig(Path('.'), ["example.com/*"], [], from_date="2000", to_date="2019", text_collapse_scope="year").normalized()
        self.assertNotEqual(cdx_query_signature(a), cdx_query_signature(b))

    def test_media_range_uses_one_combined_data_request(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(
                root, ["media.example/*"], [], from_date="2000", to_date="2019", cdx_delay=0,
                media=MediaConfig(
                    enabled=True, include_images=True, include_videos=False,
                    include_extensions=["jpg"], discover_embedded=False,
                ),
            ).normalized()
            db = open_database(root)
            calls: list[dict[str, str]] = []

            def fake_get(_self, _urls, params, max_bytes=0, prefer_text=False):
                del max_bytes, prefer_text
                calls.append(dict(params))
                return [HEADER, ["20050101000000", "http://media.example/a.jpg", "image/jpeg", "200", "x", "20"]]

            with mock.patch("archive_scout.cdx.client.HttpClient.get_cdx_any", new=fake_get):
                index_media(config, db, threading.Event())
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0]["from"], "20000101000000")
            self.assertEqual(calls[0]["to"], "20191231235959")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM media_captures").fetchone()[0], 1)
            row = db.execute(
                "SELECT complete,range_start,range_end FROM media_index_coverage WHERE query_signature=?",
                (media_index_state_signature(config),),
            ).fetchone()
            self.assertEqual(tuple(row), (1, "20000101000000", "20191231235959"))
            db.close()

    def test_classification_decision_table_preserves_weak_and_conflicting_evidence(self):
        self.assertEqual(classify_indexed_resource("http://x/a.svg", "image/svg+xml").resource_class, "image")
        suffix_only = classify_indexed_resource("http://x/a.jpg", "")
        self.assertEqual(suffix_only.resource_class, "image")
        self.assertFalse(suffix_only.confident)
        watch = classify_indexed_resource("http://x/watch?v=1", "text/html")
        self.assertEqual((watch.resource_class, watch.confident), ("text", True))
        conflict = classify_indexed_resource("http://x/movie.mp4", "text/html")
        self.assertEqual((conflict.resource_class, conflict.confident), ("unknown", False))
        descriptor = classify_indexed_resource("http://x/list.m3u8", "application/vnd.apple.mpegurl")
        self.assertEqual((descriptor.resource_class, descriptor.confident), ("media_descriptor", True))
        self.assertEqual(classify_payload_prefix(b"\xff\xfeh\x00i\x00", "application/octet-stream", "http://x/a").resource_class, "text")
        self.assertEqual(classify_payload_prefix(b"\x89PNG\r\n\x1a\n" + b"x" * 20, "text/plain", "http://x/a.txt").resource_class, "image")

    def test_stream_prefix_rejection_stops_before_whole_binary_fixture(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "payload.part"
            consumed = 0
            total = PREVIEW_BUDGET * 8

            def chunks():
                nonlocal consumed
                for _ in range(total // 8192):
                    consumed += 8192
                    yield b"\x89PNG\r\n\x1a\n" + b"x" * (8192 - 8)

            def validate(_headers, prefix):
                return classify_payload_prefix(prefix, "text/plain", "http://x/file").resource_class

            with self.assertRaises(PreviewRejected):
                _write_limited(chunks(), path, total + 1, threading.Event(), preview_validator=validate)
            self.assertLessEqual(consumed, PREVIEW_BUDGET)
            self.assertLess(consumed, total)
            self.assertFalse(path.exists())

    def test_confident_media_metadata_never_enters_text_replay(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(root, ["example.com/*"], [], from_date="2001", to_date="2001", download_delay=0).normalized()
            db = open_database(root)
            sig = cdx_query_signature(config)
            with db:
                capture_id = _insert_capture(db, sig, "http://example.com/a.jpg", mimetype="image/jpeg")

            class NoReplayClient(_PayloadClient):
                def download_to_path(self, *args, **kwargs):
                    raise AssertionError("confident media must not enter text replay")

            NoReplayClient.calls = 0
            with mock.patch("archive_scout.downloads.downloader.HttpClient", NoReplayClient):
                result = download_archive_only(config, db, threading.Event(), None)
            row = db.execute("SELECT state,skip_reason,resource_class,resource_classifier_revision FROM captures WHERE id=?", (capture_id,)).fetchone()
            self.assertEqual((row["state"], row["skip_reason"], row["resource_class"]), ("skipped", "classified_media", "image"))
            self.assertEqual(int(row["resource_classifier_revision"]), RESOURCE_CLASSIFIER_REVISION)
            self.assertEqual(int(result["http_starts"]), 0)
            db.close()

    def test_discard_mode_deletes_match_and_nonmatch_after_durable_scan_evidence(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(
                root, ["example.com/*"], ["needle"], from_date="2001", to_date="2001",
                text_retention="discard_after_scan", workers=1, scan_workers=1, download_delay=0,
                media=MediaConfig(enabled=False),
            ).normalized()
            db = open_database(root)
            sig = cdx_query_signature(config)
            with db:
                first = _insert_capture(db, sig, "http://example.com/match.html")
                second = _insert_capture(db, sig, "http://example.com/miss.html")
                keyword_set_id = get_or_create_keyword_set(db, "Audit3", ["needle"])
                scan_run_id = start_scan_run(db, keyword_set_id, "Audit3", 1, "test")
            job = ScanJob.create(scan_run_id, "Audit3", ["needle"])
            _PayloadClient.bodies = {
                "http://example.com/match.html": b"<html><title>A</title><body>needle here</body></html>",
                "http://example.com/miss.html": b"<html><title>B</title><body>nothing here</body></html>",
            }
            _PayloadClient.calls = 0
            with mock.patch("archive_scout.downloads.downloader.HttpClient", _PayloadClient):
                download_archive(config, db, scan_run_id, threading.Event(), None, scan_jobs=[job])
            rows = db.execute(
                "SELECT id,payload_availability,local_path,cleanup_pending FROM captures WHERE id IN (?,?) ORDER BY id",
                (first, second),
            ).fetchall()
            self.assertTrue(all(row["payload_availability"] == "discarded" for row in rows))
            self.assertTrue(all(row["local_path"] is None and int(row["cleanup_pending"]) == 0 for row in rows))
            self.assertEqual(db.execute("SELECT COUNT(*) FROM documents WHERE capture_id IN (?,?)", (first, second)).fetchone()[0], 2)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM document_matches WHERE scan_run_id=?", (scan_run_id,)).fetchone()[0], 2)
            self.assertFalse(any((root / "captures").rglob("*.txt")))
            db.close()

    def test_discard_scan_failure_keeps_complete_source_for_resume(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(
                root, ["example.com/*"], ["needle"], from_date="2001", to_date="2001",
                text_retention="discard_after_scan", workers=1, scan_workers=1, download_delay=0,
            ).normalized()
            db = open_database(root)
            sig = cdx_query_signature(config)
            with db:
                capture_id = _insert_capture(db, sig, "http://example.com/fail.html")
                ks = get_or_create_keyword_set(db, "Audit3", ["needle"])
                scan_run_id = start_scan_run(db, ks, "Audit3", 1, "test")
            job = ScanJob.create(scan_run_id, "Audit3", ["needle"])
            _PayloadClient.bodies = {"http://example.com/fail.html": b"<html>needle</html>"}
            with mock.patch("archive_scout.downloads.downloader.HttpClient", _PayloadClient), \
                 mock.patch("archive_scout.downloads.downloader._scan_saved_capture", side_effect=RuntimeError("scanner failed")):
                download_archive(config, db, scan_run_id, threading.Event(), None, scan_jobs=[job])
            row = db.execute("SELECT state,payload_availability,local_path FROM captures WHERE id=?", (capture_id,)).fetchone()
            self.assertEqual((row["state"], row["payload_availability"]), ("downloaded_unscanned", "retained_unscanned"))
            self.assertTrue(Path(row["local_path"]).is_file())
            db.close()

    def test_non_text_discard_uses_cleanup_intent_and_recovery_is_idempotent(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(root, ["example.com/*"], ["x"], text_retention="discard_after_scan").normalized()
            db = open_database(root)
            sig = cdx_query_signature(config)
            path = root / "captures" / "2001" / "01" / "binary.txt"
            path.parent.mkdir(parents=True)
            path.write_bytes(b"binary")
            with db:
                capture_id = _insert_capture(db, sig, "http://example.com/binary", state="downloaded_unscanned", local_path=str(path), availability="spooled_unscanned")
            cleanup = _commit_non_text_discard(db, config, capture_id, path)
            row = db.execute("SELECT cleanup_pending,payload_availability FROM captures WHERE id=?", (capture_id,)).fetchone()
            self.assertEqual((int(row["cleanup_pending"]), row["payload_availability"]), (1, "cleanup_pending"))
            _finish_discard_cleanup(db, config, cleanup)
            row = db.execute("SELECT cleanup_pending,payload_availability,local_path FROM captures WHERE id=?", (capture_id,)).fetchone()
            self.assertEqual((int(row["cleanup_pending"]), row["payload_availability"], row["local_path"]), (0, "discarded", None))
            self.assertEqual(recover_pending_discard_cleanup(db, root), 0)
            db.close()

    def test_failed_unlink_stays_cleanup_pending_without_reacquisition_state(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(root, ["example.com/*"], ["x"], text_retention="discard_after_scan").normalized()
            db = open_database(root)
            sig = cdx_query_signature(config)
            path = root / "captures" / "2001" / "01" / "held.txt"
            path.parent.mkdir(parents=True)
            path.write_text("x", encoding="utf-8")
            with db:
                capture_id = _insert_capture(db, sig, "http://example.com/held", state="skipped", local_path=str(path), availability="cleanup_pending")
                db.execute("UPDATE captures SET cleanup_pending=1 WHERE id=?", (capture_id,))
            with mock.patch("pathlib.Path.unlink", side_effect=PermissionError("open handle")):
                self.assertEqual(recover_pending_discard_cleanup(db, root), 0)
            row = db.execute("SELECT state,payload_availability,cleanup_pending,download_attempts FROM captures WHERE id=?", (capture_id,)).fetchone()
            self.assertEqual((row["state"], row["payload_availability"], int(row["cleanup_pending"])), ("skipped", "cleanup_pending", 1))
            self.assertEqual(int(row["download_attempts"]), 0)
            db.close()

    def test_download_only_rejects_discard_policy(self):
        with tempfile.TemporaryDirectory() as temp:
            config = ProjectConfig(Path(temp), ["example.com/*"], [], text_retention="discard_after_scan").normalized()
            db = open_database(config.output_dir)
            with self.assertRaisesRegex(ValueError, "discard"):
                download_archive_only(config, db, threading.Event(), None)
            db.close()

    def test_hitlist_reports_discarded_body_gap_but_still_checks_url(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = open_database(root)
            sig = "audit3"
            with db:
                capture_id = _insert_capture(db, sig, "http://example.com/needle", state="downloaded", availability="discarded")
            result = search_with_hitlist(root, db, ["needle"], threading.Event())
            self.assertEqual(result["discarded"], 1)
            self.assertEqual(result["local_checked"], 0)
            self.assertEqual(result["matches"], 1)
            hit = db.execute("SELECT fields FROM quick_search_hits WHERE capture_id=?", (capture_id,)).fetchone()
            self.assertEqual(hit["fields"], "url")
            db.close()

    def test_dashboard_controller_manual_interval_coalescing_and_project_switch(self):
        ctl = DashboardRefreshController(mode="manual", interval_seconds=10)
        self.assertFalse(ctl.automatic_due(0.0, visible=True, operation_active=False))
        generation = ctl.begin(0.0, manual=True)
        self.assertEqual(generation, 0)
        self.assertIsNone(ctl.begin(0.1, manual=True))
        self.assertTrue(ctl.finish(0))
        ctl.configure("auto", 5)
        self.assertFalse(ctl.automatic_due(4.9, visible=True, operation_active=False))
        self.assertTrue(ctl.automatic_due(5.0, visible=True, operation_active=False))
        generation = ctl.begin(5.0)
        self.assertEqual(generation, 0)
        self.assertEqual(ctl.switch_project(), 1)
        self.assertFalse(ctl.finish(generation))
        self.assertTrue(ctl.automatic_due(5.1, visible=True, operation_active=False))
        self.assertTrue(ctl.automatic_due(5.1, visible=True, operation_active=True))

    def test_audit3_config_round_trip_retention_and_dashboard(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(
                root, ["example.com/*"], ["x"], text_retention="discard_after_scan",
                dashboard_refresh_mode="manual", dashboard_refresh_seconds=30,
                search_media_descriptors=True,
            ).normalized()
            path = save_project_config(config)
            loaded = load_project_config(path)
            self.assertEqual(loaded.text_retention, "discard_after_scan")
            self.assertEqual(loaded.dashboard_refresh_mode, "manual")
            self.assertEqual(loaded.dashboard_refresh_seconds, 30)
            self.assertTrue(loaded.search_media_descriptors)
            self.assertEqual(loaded.text_collapse_scope, "range")

    def test_ambiguous_image_is_deferred_to_standard_media_phase_without_text_file(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(
                root, ["example.com/*"], [], from_date="2001", to_date="2001", download_delay=0,
                media=MediaConfig(
                    enabled=True, include_images=True, include_videos=False,
                    include_extensions=["png"], discover_embedded=False,
                ),
            ).normalized()
            db = open_database(root)
            sig = cdx_query_signature(config)
            with db:
                capture_id = _insert_capture(
                    db, sig, "http://example.com/asset?id=1", mimetype="application/octet-stream"
                )
            _PayloadClient.bodies = {
                "http://example.com/asset?id=1": b"\x89PNG\r\n\x1a\n" + b"x" * 2048
            }
            _PayloadClient.calls = 0
            with mock.patch("archive_scout.downloads.downloader.HttpClient", _PayloadClient):
                download_archive_only(config, db, threading.Event(), None)
            self.assertEqual(_PayloadClient.calls, 1)
            row = db.execute(
                "SELECT state,skip_reason,resource_class,local_path FROM captures WHERE id=?", (capture_id,)
            ).fetchone()
            self.assertEqual((row["state"], row["skip_reason"], row["resource_class"]), ("skipped", "deferred_to_media", "image"))
            self.assertIsNone(row["local_path"])
            self.assertEqual(db.execute("SELECT COUNT(*) FROM media_captures").fetchone()[0], 0)
            queued = db.execute(
                "SELECT state,source_type,kind_hint FROM media_discovery_queue WHERE original_url=?",
                ("http://example.com/asset?id=1",),
            ).fetchone()
            self.assertEqual((queued["state"], queued["source_type"], queued["kind_hint"]),
                             ("pending", "text_validation_deferred", "image"))
            self.assertFalse(any((root / "captures").rglob("*.txt")))
            self.assertFalse(any((root / "media").rglob("*.*")))
            db.close()

    def test_discard_does_not_sweep_preexisting_retained_capture(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(
                root, ["example.com/*"], ["needle"], from_date="2001", to_date="2001",
                text_retention="discard_after_scan", workers=1, scan_workers=1, download_delay=0,
            ).normalized()
            db = open_database(root)
            sig = cdx_query_signature(config)
            old_path = root / "captures" / "2001" / "01" / "preexisting.txt"
            old_path.parent.mkdir(parents=True)
            old_path.write_text("needle", encoding="utf-8")
            with db:
                capture_id = _insert_capture(
                    db, sig, "http://example.com/old.html", state="downloaded_unscanned",
                    local_path=str(old_path), availability="retained_unscanned"
                )
                ks = get_or_create_keyword_set(db, "Audit3", ["needle"])
                scan_run_id = start_scan_run(db, ks, "Audit3", 1, "test")
            job = ScanJob.create(scan_run_id, "Audit3", ["needle"])
            with mock.patch("archive_scout.downloads.downloader.HttpClient", _PayloadClient):
                download_archive(config, db, scan_run_id, threading.Event(), None, scan_jobs=[job])
            row = db.execute(
                "SELECT state,payload_availability,local_path FROM captures WHERE id=?", (capture_id,)
            ).fetchone()
            self.assertEqual((row["state"], row["payload_availability"]), ("downloaded_unscanned", "retained_unscanned"))
            self.assertEqual(row["local_path"], str(old_path))
            self.assertTrue(old_path.is_file())
            db.close()

    def test_reports_regenerate_after_discard_and_label_local_source_unavailable(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(
                root, ["example.com/*"], ["needle"], from_date="2001", to_date="2001",
                text_retention="discard_after_scan", workers=1, scan_workers=1, download_delay=0,
            ).normalized()
            db = open_database(root)
            sig = cdx_query_signature(config)
            with db:
                _insert_capture(db, sig, "http://example.com/report.html")
                ks = get_or_create_keyword_set(db, "Audit3", ["needle"])
                scan_run_id = start_scan_run(db, ks, "Audit3", 1, "test")
            job = ScanJob.create(scan_run_id, "Audit3", ["needle"])
            _PayloadClient.bodies = {
                "http://example.com/report.html": b"<html><title>Report</title><body>needle context</body></html>"
            }
            with mock.patch("archive_scout.downloads.downloader.HttpClient", _PayloadClient):
                download_archive(config, db, scan_run_id, threading.Event(), None, scan_jobs=[job])
            paths = generate_reports(config, db, scan_run_id)
            ranked = paths["matches_ranked"].read_text(encoding="utf-8")
            self.assertIn("WAYBACK URL:", ranked)
            self.assertIn("LOCAL FILE: (intentionally discarded after successful scan)", ranked)
            self.assertIn("needle", ranked.casefold())
            db.close()


if __name__ == "__main__":
    unittest.main()
