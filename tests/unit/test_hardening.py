from __future__ import annotations

import json
import subprocess
import sys
import os
import tempfile
import unittest
import zipfile
from pathlib import Path

from archive_scout.database.connection import open_database
from archive_scout.database.repositories import (
    get_or_create_keyword_set,
    get_or_create_target,
    list_error_categories,
    list_errors,
    result_rows,
    save_match,
    save_note,
    set_match_tags,
    start_scan_run,
    upsert_capture,
    upsert_document,
)
from archive_scout.projects.diagnostics import export_diagnostics
from archive_scout.projects.merge import merge_projects
from archive_scout.reports.export import export_review_package, export_scan
from archive_scout.ui.event_queue import CoalescingEventQueue
from archive_scout.utils import hash_text, normalize_search


class HardeningTests(unittest.TestCase):
    def _project_with_match(self, root: Path):
        root.mkdir(parents=True, exist_ok=True)
        path = root / "page.txt"
        body = "benchmark body " + ("x" * 10000)
        path.write_text(body, encoding="utf-8")
        database = open_database(root)
        target_id = get_or_create_target(database, "example.com/*")
        upsert_capture(
            database,
            {
                "original": "http://example.com/thread",
                "timestamp": "20060101000000",
                "mimetype": "text/html",
                "statuscode": "200",
                "digest": "",
                "length": str(path.stat().st_size),
            },
            target_id,
            "sig",
        )
        capture_id = int(database.execute("SELECT id FROM captures").fetchone()[0])
        document_id = upsert_document(
            database,
            capture_id,
            path,
            "Thread",
            body,
            [],
            hash_text(body),
            hash_text(normalize_search(body)),
            path.stat().st_size,
        )
        set_id = get_or_create_keyword_set(database, "Set", ["benchmark"])
        run_id = start_scan_run(database, set_id, "Run", 1, "rescan")
        match_id = save_match(
            database,
            run_id,
            document_id,
            {
                "score": 10,
                "hits": {"benchmark": 1},
                "hit_fields": {"benchmark": ["body"]},
                "snippets": ["benchmark body"],
                "interesting_links": [],
            },
        )
        save_note(database, match_id, "note")
        set_match_tags(database, match_id, ["research", "research", "performance"])
        database.commit()
        return database, run_id

    def test_result_page_does_not_load_full_document_body(self):
        with tempfile.TemporaryDirectory() as temp:
            database, run_id = self._project_with_match(Path(temp))
            row = result_rows(database, run_id, limit=1)[0]
            self.assertNotIn("body_text", row.keys())
            self.assertEqual(row["note"], "note")
            self.assertEqual(set(row["tags"].split(", ")), {"research", "performance"})
            database.close()

    def test_streamed_exports_are_valid(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database, run_id = self._project_with_match(root)
            json_path = export_scan(database, run_id, root / "review.json", "json")
            csv_path = export_scan(database, run_id, root / "review.csv", "csv")
            md_path = export_scan(database, run_id, root / "review.md", "markdown")
            self.assertEqual(json.loads(json_path.read_text(encoding="utf-8"))[0]["note"], "note")
            self.assertIn("match_id,score", csv_path.read_text(encoding="utf-8"))
            self.assertIn("# Archive Scout review export", md_path.read_text(encoding="utf-8"))
            database.close()

    def test_review_package_does_not_delete_neighboring_exports(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database, run_id = self._project_with_match(root)
            neighbors = [root / "review.json", root / "review.csv", root / "review.md"]
            for path in neighbors:
                path.write_text("keep me", encoding="utf-8")
            destination = export_review_package(database, run_id, root / "review.zip")
            for path in neighbors:
                self.assertEqual(path.read_text(encoding="utf-8"), "keep me")
            with zipfile.ZipFile(destination) as archive:
                self.assertEqual(set(archive.namelist()), {f"scan-{run_id}.json", f"scan-{run_id}.csv", f"scan-{run_id}.md"})
            database.close()

    def test_error_table_query_is_bounded_and_server_filtered(self):
        with tempfile.TemporaryDirectory() as temp:
            database = open_database(Path(temp))
            now = "2026-01-01T00:00:00+00:00"
            with database:
                database.executemany(
                    """
                    INSERT INTO errors(operation,category,message,attempt_count,retryable,resolved,ignored,first_seen,last_seen)
                    VALUES('download',?,?,1,1,0,0,?,?)
                    """,
                    (("timeout" if index % 2 else "http", f"error {index}", now, now) for index in range(2500)),
                )
            self.assertEqual(list_error_categories(database), ["http", "timeout"])
            self.assertEqual(len(list_errors(database)), 2000)
            filtered = list_errors(database, category="timeout", limit=100)
            self.assertEqual(len(filtered), 100)
            self.assertTrue(all(row["category"] == "timeout" for row in filtered))
            database.close()

    def test_second_connection_does_not_interrupt_active_process(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database, run_id = self._project_with_match(root)
            database.execute("UPDATE captures SET state='downloading'")
            database.execute("UPDATE scan_runs SET status='running' WHERE id=?", (run_id,))
            database.execute(
                "INSERT INTO operation_runs(mode,status,started_at,updated_at,process_id,app_version) VALUES(?,'running',datetime('now'),datetime('now'),?,?)",
                ("rescan", os.getpid(), "test"),
            )
            database.commit()

            second = open_database(root)
            self.assertEqual(second.execute("SELECT state FROM captures").fetchone()[0], "downloading")
            self.assertEqual(second.execute("SELECT status FROM scan_runs WHERE id=?", (run_id,)).fetchone()[0], "running")
            self.assertEqual(second.execute("SELECT status FROM operation_runs").fetchone()[0], "running")
            second.close()
            database.close()

    def test_stale_process_states_are_recovered(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database, run_id = self._project_with_match(root)
            database.execute("UPDATE captures SET state='downloading'")
            database.execute("UPDATE scan_runs SET status='running' WHERE id=?", (run_id,))
            database.execute(
                "INSERT INTO operation_runs(mode,status,started_at,updated_at,process_id,app_version) VALUES(?,'running',datetime('now'),datetime('now'),?,?)",
                ("rescan", os.getpid() + 100000, "test"),
            )
            database.commit()
            database.close()

            recovered = open_database(root)
            self.assertEqual(recovered.execute("SELECT state FROM captures").fetchone()[0], "pending")
            self.assertEqual(recovered.execute("SELECT status FROM scan_runs WHERE id=?", (run_id,)).fetchone()[0], "interrupted")
            self.assertEqual(recovered.execute("SELECT status FROM operation_runs").fetchone()[0], "interrupted")
            recovered.close()

    def test_merge_does_not_read_files_outside_source_project(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            source_root = base / "source"
            destination_root = base / "destination"
            source, _run_id = self._project_with_match(source_root)
            secret = base / "private.txt"
            secret.write_text("top secret outside project", encoding="utf-8")
            source.execute("UPDATE documents SET path=?", (str(secret),))
            source.commit()
            source.close()

            destination = open_database(destination_root)
            merge_projects(destination_root, source_root, destination)
            merged = destination.execute("SELECT path,body_text FROM documents").fetchone()
            merged_path = Path(merged["path"])
            self.assertTrue(merged_path.is_relative_to(destination_root))
            self.assertEqual(merged_path.read_text(encoding="utf-8"), merged["body_text"])
            self.assertNotIn("top secret outside project", merged_path.read_text(encoding="utf-8"))
            destination.close()

    def test_progress_events_are_coalesced_and_bounded(self):
        events: CoalescingEventQueue[int] = CoalescingEventQueue(max_events=8)
        for value in range(10000):
            events.put_progress(value)
        self.assertEqual(events.qsize(), 1)
        self.assertEqual(events.get_nowait(), ("progress", 9999))
        events.put_progress(10000)
        events.put(("complete", {"ok": True}))
        self.assertEqual(events.get_nowait(), ("progress", 10000))
        self.assertEqual(events.get_nowait(), ("complete", {"ok": True}))

    def test_offline_benchmark_runs_from_repository_checkout(self):
        root = Path(__file__).resolve().parents[2]
        completed = subprocess.run(
            [
                sys.executable,
                str(root / "scripts" / "benchmark_offline.py"),
                "--cdx-rows",
                "10",
                "--result-rows",
                "5",
                "--body-bytes",
                "128",
            ],
            cwd=root,
            capture_output=True,
            text=True,
            check=True,
        )
        report = json.loads(completed.stdout)
        self.assertEqual(report["fixture"]["cdx_rows"], 10)
        self.assertEqual(report["http_attempts"], 0)


    def test_diagnostics_exclude_paths_and_project_content(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            secret_path = root / "private" / "archive"
            secret_url = "http://private.example/research"
            project = {
                "version": "test",
                "output_dir": str(secret_path),
                "targets": [secret_url],
                "keywords": ["private keyword"],
                "keyword_sets": [{"name": "Private", "selected": True, "rules": ["private keyword"]}],
                "network": {"backend": "auto"},
            }
            (root / "project.json").write_text(json.dumps(project), encoding="utf-8")
            database = open_database(root)
            now = "2026-01-01T00:00:00+00:00"
            with database:
                database.execute(
                    """
                    INSERT INTO errors(operation,category,message,retryable,resolved,ignored,first_seen,last_seen)
                    VALUES('download','filesystem',?,0,0,0,?,?)
                    """,
                    (f"failed at {secret_path}", now, now),
                )
            package = export_diagnostics(root, database)
            with zipfile.ZipFile(package) as archive:
                names = set(archive.namelist())
                combined = b"\n".join(archive.read(name) for name in names).decode("utf-8")
            self.assertIn("project-summary.json", names)
            self.assertNotIn("project-sanitized.json", names)
            self.assertNotIn(str(secret_path), combined)
            self.assertNotIn(secret_url, combined)
            self.assertNotIn("private keyword", combined)
            self.assertNotIn("failed at", combined)
            database.close()


    def test_pull_request_ci_covers_supported_matrix(self):
        root = Path(__file__).resolve().parents[2]
        workflow = (root / ".github" / "workflows" / "tests.yml").read_text(encoding="utf-8")
        for value in ("ubuntu-22.04", "windows-2022", "macos-15-intel", "macos-15", '"3.11"', '"3.12"'):
            self.assertIn(value, workflow)
        self.assertIn("uses: actions/checkout@v6", workflow)
        self.assertIn("uses: actions/setup-python@v6", workflow)
        self.assertIn("python -m pip install .", workflow)
        self.assertIn("scripts/benchmark_offline.py", workflow)



if __name__ == "__main__":
    unittest.main()
