"""Regression tests for coverage integrity and crash-safe local scanning."""
from __future__ import annotations

import tempfile
import base64
import hashlib
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from archive_scout.cdx.client import CDXRows, MalformedCDXResponse, PermanentRequestError, parse_cdx_rows_payload
from archive_scout.cdx.indexer import index_archive
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.config import NetworkConfig, ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import (
    get_or_create_target, get_or_create_keyword_set, upsert_capture, upsert_document, start_scan_run, save_match,
)
from archive_scout.downloads.downloader import capture_path
from archive_scout.operations import prepare_scan_jobs, run_project
from archive_scout.scanning.jobs import ScanJob
from archive_scout.scanning.pending import scan_pending_saved
from archive_scout.utils import hash_text


class CoverageIntegrityTests(unittest.TestCase):
    def config(self, root, targets=None):
        return ProjectConfig(root, targets=targets or ["example.org/*"], keywords=["needle"],
                             from_date="20010101000000", to_date="20011231235959",
                             workers=2, cdx_delay=0, download_delay=0,
                             network=NetworkConfig(index_strategy="resume")).normalized()

    def add(self, db, cfg, name="first", target="example.org/*", state="downloaded_unscanned"):
        target_id = get_or_create_target(db, target)
        upsert_capture(db, {
            "original": f"http://{target.rstrip('/*')}/{name}.html",
            "timestamp": "20010101000000", "mimetype": "text/html",
            "statuscode": "200", "digest": "D", "length": "100",
        }, target_id, cdx_query_signature(cfg.for_target(target)))
        row = db.execute("SELECT * FROM captures WHERE original_url LIKE ?", (f"%/{name}.html",)).fetchone()
        path = capture_path(cfg.output_dir, row["id"], row["timestamp"], row["original_url"])
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"<html><body>needle and other evidence</body></html>")
        digest=base64.b32encode(hashlib.sha1(path.read_bytes()).digest()).decode()
        db.execute("UPDATE captures SET state=?,digest=? WHERE id=?", (state, digest, row["id"]))
        db.commit()
        return int(row["id"]), path

    def job(self, db):
        kid = get_or_create_keyword_set(db, "Scan", ["needle"])
        run = start_scan_run(db, kid, "Scan", 1, "rescan")
        db.commit()
        return ScanJob.create(run, "Scan", ["needle"])

    def test_failed_sql_match_write_leaves_capture_unscanned_and_retryable(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = self.config(Path(tmp))
            db = open_database(cfg.output_dir)
            cap, _ = self.add(db, cfg)
            job = self.job(db)
            with patch("archive_scout.scanning.pending.save_match", side_effect=sqlite_failure):
                scan_pending_saved(db, cfg, [job], threading.Event(), workers=1)
            self.assertEqual(db.execute("SELECT state FROM captures WHERE id=?", (cap,)).fetchone()[0],
                             "downloaded_unscanned")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM documents").fetchone()[0], 0)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM document_matches").fetchone()[0], 0)
            scan_pending_saved(db, cfg, [job], threading.Event(), workers=1)
            self.assertEqual(db.execute("SELECT state FROM captures WHERE id=?", (cap,)).fetchone()[0], "downloaded")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM document_matches").fetchone()[0], 1)
            db.close()

    def test_saved_downloaded_capture_without_document_is_repaired_locally(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = self.config(Path(tmp)); db = open_database(cfg.output_dir)
            cap, _ = self.add(db, cfg, state="downloaded")
            scan_pending_saved(db, cfg, [self.job(db)], threading.Event(), workers=1)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM documents WHERE capture_id=?", (cap,)).fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM document_matches").fetchone()[0], 1)
            db.close()

    def test_missing_local_body_is_marked_for_reacquisition(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = self.config(Path(tmp)); db = open_database(cfg.output_dir)
            cap, path = self.add(db, cfg)
            path.unlink()
            scan_pending_saved(db, cfg, [self.job(db)], threading.Event(), workers=1)
            self.assertEqual(db.execute("SELECT state FROM captures WHERE id=?", (cap,)).fetchone()[0], "error")
            self.assertEqual(tuple(db.execute("SELECT category,retryable FROM errors WHERE capture_id=?", (cap,)).fetchone()),
                             ("missing_local_file", 1))
            db.close()

    def test_pending_scanner_respects_selected_target(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); cfg = self.config(root,["example.org/*", "other.org/*"]); db=open_database(root)
            self.add(db,cfg,"one","example.org/*")
            self.add(db,cfg,"two","other.org/*")
            scan_pending_saved(db,cfg.for_target("example.org/*"),[self.job(db)],threading.Event(),workers=1)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM documents").fetchone()[0],1)
            db.close()

    def test_rescan_reuses_interrupted_run_and_only_unscored_documents(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); cfg=self.config(root); db=open_database(root)
            ids = [self.add(db,cfg,name)[0] for name in ("one","two")]
            for cap in ids:
                row=db.execute("SELECT * FROM captures WHERE id=?",(cap,)).fetchone()
                path=capture_path(root,cap,row['timestamp'],row['original_url'])
                upsert_document(db,cap,path,"", "needle",[],hash_text("needle"),hash_text("needle"),path.stat().st_size)
            jobs=prepare_scan_jobs(db,cfg,"rescan")
            first_doc=db.execute("SELECT id FROM documents ORDER BY id LIMIT 1").fetchone()[0]
            save_match(db,jobs[0].scan_run_id,first_doc,{"score":10,"hits":{"needle":1}})
            db.execute("UPDATE scan_runs SET status='interrupted' WHERE id=?",(jobs[0].scan_run_id,))
            db.commit(); db.close()
            from archive_scout.scanning import rescanner
            with patch.object(rescanner,"_analyze_saved_document",wraps=rescanner._analyze_saved_document) as work:
                run_project(cfg,"rescan")
                self.assertEqual(work.call_count,1)
            db=open_database(root)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM scan_runs").fetchone()[0],1)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM document_matches").fetchone()[0],2)
            self.assertEqual(db.execute("SELECT status FROM scan_runs").fetchone()[0],"complete")
            db.close()

    def test_cdx_error_object_cannot_be_treated_as_empty_inventory(self):
        with self.assertRaises(MalformedCDXResponse):
            parse_cdx_rows_payload({"error":"not found: upstream CDX API failed"})
        self.assertEqual(parse_cdx_rows_payload([]).rows,[])

    def test_forbidden_index_is_checkpointed_but_not_marked_complete(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg=self.config(Path(tmp)); db=open_database(cfg.output_dir)
            with patch("archive_scout.cdx.client.HttpClient.get_cdx_any",side_effect=PermanentRequestError(
                "HTTP 403", status=403, category="wayback_forbidden")):
                self.assertTrue(index_archive(cfg,db,threading.Event()))
            state=db.execute("SELECT complete FROM index_coverage").fetchone()
            self.assertIsNotNone(state)
            self.assertEqual(int(state[0]),0)
            db.close()


def sqlite_failure(*_args, **_kwargs):
    raise RuntimeError("injected database match failure")


if __name__ == "__main__":
    unittest.main()

class TargetScopeAndOfflineRecoveryTests(unittest.TestCase):
    config = CoverageIntegrityTests.config
    add = CoverageIntegrityTests.add

    def test_per_target_signatures_select_captures_for_download_and_retry(self):
        from archive_scout.downloads.downloader import prepare_download_rows, cumulative_download_progress
        from archive_scout.operations import _retryable_text_batches
        from archive_scout.database.repositories import record_error
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = ProjectConfig(
                root, targets=["alpha.example/*", "beta.example/*"],
                target_settings={"beta.example/*": {"cdx_filters": ["mimetype:text/html"]}},
                keywords=["needle"], from_date="2001", to_date="2001",
            ).normalized()
            db = open_database(root)
            ids = []
            for name in cfg.targets:
                target_id = get_or_create_target(db, name)
                upsert_capture(db, {
                    "original": "http://" + name.replace("/*", "/page"),
                    "timestamp": "20010101000000", "mimetype": "text/html", "statuscode": "200",
                    "digest": name, "length": "100",
                }, target_id, cdx_query_signature(cfg.for_target(name)))
                ids.append(db.execute("SELECT id FROM captures WHERE target_id=?", (target_id,)).fetchone()[0])
            db.commit()
            self.assertNotEqual(cdx_query_signature(cfg.for_target(cfg.targets[0])),
                                cdx_query_signature(cfg.for_target(cfg.targets[1])))
            queued, rows = prepare_download_rows(db, cfg, [])
            self.assertEqual(queued, 2)
            self.assertEqual({row["id"] for row in rows}, set(ids))
            self.assertEqual(cumulative_download_progress(db, cfg, queued_total=2), (0, 2))
            with db:
                for cap in ids:
                    db.execute("UPDATE captures SET state='error' WHERE id=?", (cap,))
                    record_error(db, "download", "transport_failure", "injected", capture_id=cap, retryable=True)
            self.assertEqual(set(sum(_retryable_text_batches(cfg, db), [])), set(ids))
            db.close()

    def test_preexisting_saved_body_scans_before_failed_cdx_indexing(self):
        with tempfile.TemporaryDirectory() as temp:
            cfg = self.config(Path(temp))
            db = open_database(cfg.output_dir)
            cap, _ = self.add(db, cfg)
            db.close()
            with patch("archive_scout.operations.index_archive", side_effect=RuntimeError("injected CDX outage")):
                with self.assertRaisesRegex(RuntimeError, "injected CDX outage"):
                    run_project(cfg, "resume")
            db = open_database(cfg.output_dir)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM documents WHERE capture_id=?", (cap,)).fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM document_matches").fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT status FROM scan_runs ORDER BY id DESC LIMIT 1").fetchone()[0], "failed")
            db.close()

class ReportCoverageTruthTests(unittest.TestCase):
    def test_unfinished_cdx_checkpoint_is_visible_in_summary(self):
        from archive_scout.reports.text import _index_coverage_status
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = open_database(root)
            self.assertIn("unverified", _index_coverage_status(db))
            target = get_or_create_target(db, "example.org/*")
            db.execute("""INSERT INTO index_coverage(
                target_id,query_signature,range_start,range_end,
                strategy,complete,seen,updated_at)
                VALUES(?,?,?,?,?,?,?,?)""",
                (target, "example-signature", "2001", "2002", "resume", 0, 50, "2026-10-12"))
            self.assertIn("1 incomplete", _index_coverage_status(db))
            db.execute("UPDATE index_coverage SET complete=1")
            self.assertIn("0 incomplete", _index_coverage_status(db))
            db.close()
