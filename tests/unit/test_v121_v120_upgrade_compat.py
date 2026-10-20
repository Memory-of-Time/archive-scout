"""Schema-9 Scout 1.2.0 project resumes without discarding inventory or state."""
from __future__ import annotations

import json
import shutil
import sqlite3
import tempfile
import unittest
from pathlib import Path

from archive_scout.cdx.indexer import IndexPlan, PendingWindow, decode_plan, encode_plan, uncovered_index_ranges
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.config import ProjectConfig, load_project_config, save_project_config
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, upsert_captures, keyword_fingerprint
from archive_scout.utils import utc_now
from scripts.check_project_resume import compare, snapshot
from archive_scout.operations import prepare_scan_jobs


class V120V121ResumeCompatibility(unittest.TestCase):
    def make_old_project(self, root: Path, rows: int = 2100) -> tuple[str, int]:
        config = ProjectConfig(
            output_dir=root, targets=["example.net/*"], keywords=["needle"],
            from_date="20000101000000", to_date="20091231235959",
            text_collapse_scope="range", cdx_collapses=["urlkey"],
            cdx_delay=2.5, download_delay=0.125,
        ).normalized()
        path = save_project_config(config)
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["version"] = "1.2.0"
        path.write_text(json.dumps(payload), encoding="utf-8")
        db = open_database(root)
        try:
            target = get_or_create_target(db, config.targets[0])
            sig = cdx_query_signature(config)
            now = utc_now()
            pending_window = PendingWindow(
                start=config.from_date, end=config.to_date,
                resume_key="2005-last-page-row", strategy="resume", page_size=100000,
            )
            plan = encode_plan(IndexPlan(pending=[pending_window], completed=3, planned=4))
            db.execute(
                """INSERT INTO index_coverage(target_id,query_signature,range_start,range_end,plan_json,
                   strategy,layout_signature,complete,seen,updated_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?)""",
                (target, sig, config.from_date, config.to_date, plan, "resume", "layout-old", 0, rows, now),
            )
            # Each row has its own archived identity; deliberate mixed states.
            for start in range(0, rows, 1000):
                chunk = [
                    (f"2005{i//1000000:02d}0101000000", f"http://example.net/page/{i}",
                     "text/html", "200", "digest", "125")
                    for i in range(start, min(rows, start+1000))
                ]
                upsert_captures(db, chunk, target, sig)
            db.execute("UPDATE captures SET state='downloaded_unscanned',bytes_saved=125 WHERE id%5=0")
            db.execute("UPDATE captures SET state='error',download_attempts=4 WHERE id%7=0")
            db.execute("UPDATE captures SET state='downloading' WHERE id%11=0")
            db.execute("INSERT INTO keyword_sets(name,fingerprint,keywords_json,created_at,updated_at) VALUES(?,?,?,?,?)",
                       ("needle", keyword_fingerprint(["needle"]), '["needle"]', now, now))
            keyword_set_id = db.execute("SELECT id FROM keyword_sets LIMIT 1").fetchone()[0]
            db.execute("""INSERT INTO scan_runs(keyword_set_id,name,status,started_at,source_operation,document_count,match_count)
                          VALUES(?,?,?,?,?,?,?)""", (keyword_set_id, "old search", "complete", now, "download", 5, 2))
            doc_dir = root / "captures"
            doc_dir.mkdir(parents=True, exist_ok=True)
            (doc_dir / "resume_test.txt").write_text("source saved", encoding="utf-8")
            db.execute("""INSERT INTO documents(capture_id,path,content_hash,size_bytes,created_at,updated_at)
                          VALUES(?,?,?,?,?,?)""",
                       (1, str(doc_dir / "resume_test.txt"), "digest", 12, now, now))
            db.execute("UPDATE captures SET document_id=1 WHERE id=1")
            db.commit()
            return sig, target
        finally:
            db.close()

    def test_v120_config_and_checkpoint_survive_reopen_and_relocation(self):
        with tempfile.TemporaryDirectory() as temp:
            old_root = Path(temp)/"old"; new_root=Path(temp)/"relocated"
            signature, _ = self.make_old_project(old_root)
            original = snapshot(old_root)
            self.assertEqual(original["schema_version"], 10)
            self.assertEqual(original["counts"]["captures"], 2100)
            self.assertEqual(original["counts"]["documents"], 1)
            shutil.move(str(old_root), str(new_root))
            loaded = load_project_config(new_root / "project.json")
            self.assertTrue(loaded.output_dir.samefile(new_root),
                            "Reopened project must point to the relocated directory")
            self.assertEqual(loaded.cdx_delay, 2.5)
            self.assertEqual(cdx_query_signature(loaded), signature)
            db = open_database(new_root)
            try:
                self.assertEqual(db.execute("SELECT version FROM schema_info").fetchone()[0], 10)
                self.assertEqual(db.execute("SELECT COUNT(*) FROM captures").fetchone()[0], 2100)
                self.assertEqual(db.execute("SELECT COUNT(*) FROM scan_runs").fetchone()[0], 1)
                self.assertEqual(db.execute("SELECT COUNT(*) FROM documents").fetchone()[0], 1)
                path = db.execute("SELECT path FROM documents WHERE capture_id=1").fetchone()[0]
                self.assertTrue(Path(path).samefile(new_root / "captures/resume_test.txt"),
                                "Stored document must resolve to the existing relocated file")
                self.assertEqual(db.execute("SELECT COUNT(*) FROM captures WHERE state='downloading'").fetchone()[0], 0)
                self.assertGreater(db.execute("SELECT COUNT(*) FROM captures WHERE state='downloaded_unscanned'").fetchone()[0], 0)
                self.assertGreater(db.execute("SELECT COUNT(*) FROM captures WHERE state='error'").fetchone()[0], 0)
                checkpoint = db.execute("SELECT plan_json,seen FROM index_coverage WHERE query_signature=?", (signature,)).fetchone()
                self.assertEqual(checkpoint["seen"], 2100)
                recovered=decode_plan(checkpoint["plan_json"], [(loaded.from_date, loaded.to_date)])
                self.assertEqual(recovered.pending[0].resume_key, "2005-last-page-row")
                self.assertEqual(uncovered_index_ranges(db,1,signature,loaded.from_date,loaded.to_date),
                                 [(loaded.from_date,loaded.to_date)])
            finally:
                db.close()
            current = snapshot(new_root, [int(key) for key in original["capture_identity_samples"]])
            self.assertEqual(compare(original, current), [])

    def test_resume_reuses_matching_interrupted_scan_without_resetting_results(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.make_old_project(root, 36)
            cfg = load_project_config(root/"project.json")
            db = open_database(root)
            try:
                original_run_id = int(db.execute("SELECT id FROM scan_runs").fetchone()[0])
                db.execute("UPDATE scan_runs SET status='interrupted' WHERE id=?", (original_run_id,))
                db.execute("""INSERT INTO document_matches(scan_run_id,document_id,score,created_at,updated_at)
                              VALUES(?,?,?,?,?)""", (original_run_id,1,2,utc_now(),utc_now()))
                db.commit()
                jobs = prepare_scan_jobs(db, cfg, "resume")
                self.assertEqual(len(jobs), 1)
                self.assertEqual(jobs[0].scan_run_id, original_run_id)
                self.assertEqual(db.execute("SELECT COUNT(*) FROM scan_runs").fetchone()[0],1)
                self.assertEqual(db.execute("SELECT COUNT(*) FROM document_matches").fetchone()[0],1)
                self.assertEqual(db.execute("SELECT status FROM scan_runs").fetchone()[0],"running")
                db.execute("UPDATE scan_runs SET status='interrupted' WHERE id=?", (original_run_id,))
                db.commit()
                incompatible = ProjectConfig(root, targets=["other.example/*"], keywords=["needle"],
                                             from_date=cfg.from_date,to_date=cfg.to_date,
                                             cdx_collapses=cfg.cdx_collapses,
                                             text_collapse_scope=cfg.text_collapse_scope).normalized()
                newer = prepare_scan_jobs(db, incompatible, "resume")
                self.assertNotEqual(newer[0].scan_run_id,original_run_id)
                self.assertEqual(db.execute("SELECT COUNT(*) FROM document_matches WHERE scan_run_id=?",(original_run_id,)).fetchone()[0],1)
            finally:
                db.close()

    def test_read_only_resume_guard_refuses_missing_database(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "misplaced_project"
            root.mkdir()
            (root/"project.json").write_text('{"version":"1.2.0"}', encoding="utf-8")
            with self.assertRaises(FileNotFoundError):
                snapshot(root)
            self.assertFalse((root / "archive_scout.sqlite3").exists())


if __name__ == "__main__":
    unittest.main()
