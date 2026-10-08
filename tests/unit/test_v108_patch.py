from __future__ import annotations

import hashlib
import gzip
import os
import sqlite3
import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path
from unittest import mock

from archive_scout.ai.relevance import _fts_match_ids
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.config import ProjectConfig, load_project_config, save_project_config
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import (
    get_or_create_keyword_set, result_rows, save_match, start_scan_run, upsert_document,
)
from archive_scout.database.schema import initialize_schema
from archive_scout.downloads import downloader
from archive_scout.downloads.retry import retry_error_downloads
from archive_scout.events import ProgressEvent, Stopped
from archive_scout.media.downloader import iter_media_download_rows
from archive_scout.projects.repair import rebuild_full_text_index
from archive_scout.projects.backups import create_project_backup
from archive_scout.analysis.duplicates import cluster_duplicates
from archive_scout.research.embeddings import local_hash_vector
from archive_scout.research.search import _candidate_ids
from archive_scout.scanning.batches import BoundedResultWriter
from archive_scout.scanning.full_text import search_documents
from archive_scout.scanning.hitlist import search_with_hitlist
from archive_scout.scanning.jobs import ScanJob
from archive_scout.scanning.keywords import compile_keywords, compile_prefilter
from archive_scout.scanning.rescanner import rescan_keyword_sets
from archive_scout.ui.eta import OperationEtaTracker
from archive_scout.utils import hash_text, utc_now


class PatchDatabaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.config = ProjectConfig(self.root, ["example.com/*"], ["needle"], scan_workers=2).normalized()
        self.db = open_database(self.root)
        set_id = get_or_create_keyword_set(self.db, "Patch", ["needle"])
        run_id = start_scan_run(self.db, set_id, "Patch", 1, "rescan")
        self.job = ScanJob.create(run_id, "Patch", ["needle"])
        self.db.commit()

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()

    def capture(self, body: str | None = None, *, url: str | None = None) -> int:
        now = utc_now()
        number = int(self.db.execute("SELECT COALESCE(MAX(id),0)+1 FROM captures").fetchone()[0])
        cursor = self.db.execute(
            """INSERT INTO captures(original_url,timestamp,query_signature,mimetype,statuscode,
                   resource_class,created_at,updated_at) VALUES(?,'20010101000000',?,'text/plain','200','text',?,?)""",
            (url or f"http://example.com/page-{number}", cdx_query_signature(self.config), now, now),
        )
        cid = int(cursor.lastrowid)
        if body is not None:
            path = self.root / "captures" / f"{cid}.txt"
            path.parent.mkdir(exist_ok=True)
            path.write_text(body, encoding="utf-8")
            self.db.execute(
                """UPDATE captures SET state='downloaded_unscanned',local_path=?,
                       payload_availability='retained_unscanned',bytes_saved=? WHERE id=?""",
                (str(path), path.stat().st_size, cid),
            )
        self.db.commit()
        return cid

    def path(self, cid: int) -> Path:
        return self.root / "captures" / f"{cid}.txt"

    def document(self, cid: int, body: str, *, indexed: bool = True) -> int:
        path = self.path(cid)
        path.write_text(body, encoding="utf-8")
        did = upsert_document(self.db, cid, path, "Page", body, [], hash_text(body),
                              hash_text(body), len(body.encode()), index_full_text=indexed)
        self.db.commit()
        return did

    def test_hitlist_resume_detects_same_size_same_mtime_edit_behind_checkpoint(self):
        first = self.capture("aaaaaa")
        self.capture("second")
        stop = threading.Event()
        with self.assertRaises(Stopped):
            search_with_hitlist(self.root, self.db, ["needle"], stop, lambda _: stop.set(), batch_size=1)
        path = self.path(first)
        stat = path.stat()
        path.write_text("needle", encoding="utf-8")
        os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
        result = search_with_hitlist(self.root, self.db, ["needle"], threading.Event(), batch_size=1)
        self.assertEqual((result["matches"], result["indexed_checked"], result["local_checked"]), (1, 2, 2))
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM quick_search_coverage").fetchone()[0], 2)

    def test_hitlist_resume_removes_stale_hits_and_preserves_corpus_boundary(self):
        first = self.capture("needle")
        self.capture("second")
        stop = threading.Event()
        with self.assertRaises(Stopped):
            search_with_hitlist(self.root, self.db, ["needle"], stop, lambda _: stop.set(), batch_size=1)
        self.path(first).write_text("absent", encoding="utf-8")
        self.capture("needle")
        result = search_with_hitlist(self.root, self.db, ["needle"], threading.Event(), batch_size=1)
        self.assertEqual((result["matches"], result["indexed_checked"], result["local_checked"]), (0, 2, 2))

    def test_hitlist_resume_checks_url_edits_without_database_revision_change(self):
        first = self.capture("absent")
        self.capture("second")
        stop = threading.Event()
        with self.assertRaises(Stopped):
            search_with_hitlist(self.root, self.db, ["needle"], stop, lambda _: stop.set(), batch_size=1)
        self.db.execute("UPDATE captures SET original_url='http://example.com/needle' WHERE id=?", (first,))
        self.db.commit()
        result = search_with_hitlist(self.root, self.db, ["needle"], threading.Event(), batch_size=1)
        self.assertEqual(result["matches"], 1)

    def test_saved_scan_hashes_actual_bytes_instead_of_cached_hash(self):
        cid = self.capture("needle actual bytes")
        row = dict(self.db.execute("SELECT * FROM captures WHERE id=?", (cid,)).fetchone())
        row["content_hash"] = "obsolete"
        result = downloader._scan_saved_capture(row, self.path(cid), self.config, [self.job])
        self.assertEqual(result["content_hash"], hashlib.sha256(self.path(cid).read_bytes()).hexdigest())
        self.assertEqual(result["bytes_saved"], len(self.path(cid).read_bytes()))

    def test_replaced_body_is_current_in_all_four_full_text_search_paths(self):
        cid = self.capture("oldword")
        did = self.document(cid, "oldword")
        mid = save_match(self.db, self.job.scan_run_id, did, {"score": 10})
        self.db.commit()
        self.document(cid, "freshword")
        self.assertEqual(search_documents(self.db, "oldword"), [])
        self.assertEqual([r["id"] for r in search_documents(self.db, "freshword")], [did])
        self.assertEqual(result_rows(self.db, self.job.scan_run_id, search="oldword"), [])
        self.assertEqual([r["id"] for r in result_rows(self.db, self.job.scan_run_id, search="freshword")], [mid])
        for term, expected in (("oldword", set()), ("freshword", {did})):
            candidates, _ = _candidate_ids(self.db, term, local_hash_vector(term).values, 100)
            self.assertEqual(candidates, expected)
        self.assertEqual(_fts_match_ids(self.db, self.job.scan_run_id, "oldword", 100), [])
        self.assertEqual(_fts_match_ids(self.db, self.job.scan_run_id, "freshword", 100), [mid])
        count = self.db.execute("SELECT COUNT(*) FROM documents_fts").fetchone()[0]
        self.document(cid, "freshword")
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM documents_fts").fetchone()[0], count)
        self.assertEqual(rebuild_full_text_index(self.db), 1)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM documents_fts").fetchone()[0], 1)
        self.db.commit()
        initialize_schema(self.db)
        self.assertEqual(search_documents(self.db, "oldword"), [])

    def test_full_text_replacement_rollback_keeps_previous_searchable_version(self):
        cid = self.capture("oldword")
        did = self.document(cid, "oldword")
        path = self.path(cid)
        path.write_text("freshword", encoding="utf-8")
        with self.assertRaises(OSError):
            with self.db:
                upsert_document(self.db, cid, path, "Page", "freshword", [], hash_text("freshword"),
                                hash_text("freshword"), 9)
                raise OSError("write failed")
        self.assertEqual([r["id"] for r in search_documents(self.db, "oldword")], [did])
        self.assertEqual(search_documents(self.db, "freshword"), [])

    def test_nonindexed_document_does_not_leave_current_body_searchable(self):
        cid = self.capture("oldword")
        self.document(cid, "oldword")
        self.document(cid, "freshword", indexed=False)
        self.assertEqual(search_documents(self.db, "oldword"), [])
        self.assertEqual(search_documents(self.db, "freshword"), [])

    def test_download_only_retry_has_no_large_in_clause_or_materialized_id_list(self):
        ids = [self.capture() for _ in range(160)]
        now = utc_now()
        self.db.executemany(
            """INSERT INTO errors(capture_id,operation,category,message,retryable,first_seen,last_seen)
               VALUES(?,'download','network','offline',1,?,?)""", ((cid, now, now) for cid in ids)
        )
        self.db.execute("UPDATE errors SET ignored=1 WHERE capture_id=?", (ids[-1],))
        self.db.execute("UPDATE captures SET state='error'")
        self.db.commit()
        old_limit = self.db.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 64)
        def recovered(config, database, stop, callback, *, states, capture_ids):
            self.assertNotIsInstance(capture_ids, list)
            self.assertEqual(list(capture_ids), ids[:-1])
            self.assertEqual(list(capture_ids), ids[:-1])
            return {"queued": len(capture_ids), "downloaded": len(capture_ids), "skipped": 0, "errors": 0, "elapsed": 0.0}
        try:
            with mock.patch("archive_scout.downloads.retry.download_archive_only", side_effect=recovered):
                result = retry_error_downloads(replace(self.config, retry_capture_ids=[*ids, *ids]), self.db, threading.Event(), None)
            self.assertEqual(result["queued"], 159)
            self.assertEqual(self.db.execute("SELECT state FROM captures WHERE id=?", (ids[-1],)).fetchone()[0], "error")
        finally:
            self.db.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, old_limit)

    def test_scan_batch_write_failure_rolls_back_and_reopens_without_lost_files(self):
        ids = [self.capture(f"needle {i}") for i in range(24)]
        real_save = downloader.save_success
        calls = 0
        def fail_third(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 3:
                raise sqlite3.OperationalError("database or disk is full")
            return real_save(*args, **kwargs)
        with mock.patch.object(downloader, "save_success", side_effect=fail_third), self.assertRaises(sqlite3.OperationalError):
            downloader._scan_pending_captures(self.config, self.db, [self.job], threading.Event(), None)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM documents").fetchone()[0], 0)
        self.assertTrue(all(self.path(cid).is_file() for cid in ids))
        self.db.close()
        self.db = open_database(self.root)
        result = downloader._scan_pending_captures(self.config, self.db, [self.job], threading.Event(), None)
        self.assertEqual(result["scanned"], 24)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM document_matches").fetchone()[0], 24)

    def test_retained_scan_cancel_flushes_completed_results_and_resume_is_complete(self):
        ids = [self.capture(f"needle {i}") for i in range(80)]
        stop = threading.Event()
        def pause(event):
            if event.current:
                stop.set()
        with self.assertRaises(Stopped):
            downloader._scan_pending_captures(self.config, self.db, [self.job], stop, pause)
        completed = self.db.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
        self.assertGreater(completed, 0)
        self.assertLess(completed, 80)
        downloader._scan_pending_captures(self.config, self.db, [self.job], threading.Event(), None)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM document_matches").fetchone()[0], 80)
        self.assertTrue(all(self.path(cid).is_file() for cid in ids))

    def test_rescan_cancel_and_resume_preserve_complete_unique_results(self):
        for i in range(80):
            self.capture(f"needle {i}")
        stop = threading.Event()
        def pause(event):
            if event.current:
                stop.set()
        with self.assertRaises(Stopped):
            rescan_keyword_sets(self.db, [self.job], stop, pause, workers=2)
        rescan_keyword_sets(self.db, [self.job], threading.Event(), workers=2)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM document_matches").fetchone()[0], 80)

    def test_warm_schema_open_keeps_classification_index(self):
        statements = []
        self.db.set_trace_callback(statements.append)
        initialize_schema(self.db)
        self.db.set_trace_callback(None)
        self.assertFalse(any("DROP INDEX" in sql and "captures_classification_idx" in sql for sql in statements))

    def test_database_mapping_budget_is_bounded(self):
        self.assertLessEqual(self.db.execute("PRAGMA mmap_size").fetchone()[0], 64 * 1024 * 1024)
        self.assertEqual(self.db.execute("PRAGMA cache_size").fetchone()[0], -65536)

    def test_fts_rebuild_uses_recorded_charset_for_plain_replay_files(self):
        cid = self.capture("placeholder")
        body = "старый архив игла"
        self.path(cid).write_bytes(body.encode("windows-1251"))
        self.db.execute("UPDATE captures SET detected_encoding='windows-1251' WHERE id=?", (cid,))
        did = upsert_document(self.db, cid, self.path(cid), "Page", body, [], "actual", "normalized", len(self.path(cid).read_bytes()))
        self.db.commit()
        events = []
        rebuild_full_text_index(self.db, callback=events.append)
        self.assertEqual([r["id"] for r in search_documents(self.db, "игла")], [did])
        self.assertEqual((events[-1].current, events[-1].total), (1, 1))

    def test_backup_byte_and_page_progress_preserves_a_valid_database(self):
        self.capture("needle")
        events = []
        backup = create_project_backup(self.root, callback=events.append)
        stages = {event.stage for event in events}
        self.assertEqual(stages, {"backup_copy", "backup_compress", "backup_verify"})
        for stage in stages:
            last = [event for event in events if event.stage == stage][-1]
            self.assertEqual(last.current, last.total)
        restored = self.root / "backup-verification.sqlite3"
        with gzip.open(backup, "rb") as handle:
            restored.write_bytes(handle.read())
        db = sqlite3.connect(restored)
        try:
            self.assertEqual(db.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM captures").fetchone()[0], 1)
        finally:
            db.close()

    def test_equal_simhash_groups_scale_past_old_bucket_cap_without_lost_members(self):
        now = utc_now()
        rows = [(f"http://example.com/duplicate-{i}", cdx_query_signature(self.config), now, now) for i in range(2105)]
        self.db.executemany("INSERT INTO captures(original_url,timestamp,query_signature,created_at,updated_at) VALUES(?,'20010101000000',?,?,?)", rows)
        self.db.execute("""INSERT INTO documents(capture_id,path,body_text,content_hash,created_at,updated_at)
                           SELECT id,'','body-'||id,'unique-'||id,created_at,updated_at FROM captures""")
        self.db.commit()
        from archive_scout.analysis import duplicates
        with mock.patch.object(duplicates, "simhash64", return_value=0), mock.patch.object(duplicates, "hamming_similarity", wraps=duplicates.hamming_similarity) as similarity:
            summary = cluster_duplicates(self.db)
        self.assertEqual(summary.grouped_documents, 2105)
        self.assertEqual(summary.near_groups, 1)
        self.assertLess(similarity.call_count, 2 * 2105)

    def test_simhash_representatives_preserve_transitive_near_groups(self):
        for i, body in enumerate(("value-0", "value-1", "value-3", "value-1")):
            cid = self.capture(body)
            self.document(cid, body)
        from archive_scout.analysis import duplicates
        with mock.patch.object(duplicates, "simhash64", side_effect=lambda body: int(body.rsplit("-", 1)[-1])):
            summary = cluster_duplicates(self.db, threshold=0.98)
        self.assertEqual(summary.grouped_documents, 4)
        self.assertEqual(summary.near_groups, 1)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM duplicate_members").fetchone()[0], 4)

    def test_media_keysets_cover_equal_lengths_unknown_lengths_states_and_signatures(self):
        now = utc_now()
        for i, (signature, state, length) in enumerate([
            ("sig", "pending", 0), ("sig", "pending", 10), ("sig", "pending", 10),
            ("sig", "error", 2), ("other", "pending", 1), ("sig", "pending", -1),
        ]):
            self.db.execute(
                """INSERT INTO media_captures(original_url,timestamp,query_signature,media_kind,extension,
                       state,length,created_at,updated_at) VALUES(?,'20010101000000',?,'image','.jpg',?,?,?,?)""",
                (f"http://example.com/media-{i}.jpg", signature, state, length, now, now),
            )
        self.db.commit()
        total, rows = iter_media_download_rows(self.db, ["query_signature=?", "state IN ('pending','error')"], ["sig"], batch_size=1)
        selected = list(rows)
        self.assertEqual(total, 5)
        self.assertEqual([r["length"] for r in selected], [2, 10, 10, 0, -1])
        self.assertEqual(len({r["id"] for r in selected}), 5)


class BoundedWriterTests(unittest.TestCase):
    def test_count_byte_and_elapsed_limits_commit_without_holding_a_transaction(self):
        now = [0.0]
        groups = []
        writer = BoundedResultWriter(lambda rows: groups.append(list(rows)), max_count=3,
                                     max_bytes=1000, max_delay=0.25, clock=lambda: now[0])
        for value in (1, 2, 3):
            writer.add(value)
        self.assertEqual(groups, [[1, 2, 3]])
        writer.add("a" * 700)
        writer.add("b" * 700)
        self.assertEqual(len(groups), 2)
        now[0] = 0.25
        writer.flush_if_due()
        self.assertEqual(len(groups), 3)
        writer.add("x" * 2000)
        self.assertEqual(len(writer.items), 0)
        self.assertEqual(len(groups), 4)

    def test_identical_positive_and_candidate_rules_share_one_immutable_automaton(self):
        prefilter = compile_prefilter(compile_keywords(["needle", "required: archive"]))
        self.assertIs(prefilter.positive_automaton, prefilter.candidate_automaton)
        excluded = compile_prefilter(compile_keywords(["needle", "exclude: archive"]))
        self.assertIsNot(excluded.positive_automaton, excluded.candidate_automaton)


class EtaTests(unittest.TestCase):
    def test_ai_progress_and_completion_keep_the_worker_project_identity(self):
        from archive_scout.ui import main_window
        app = main_window.ArchiveScoutApp.__new__(main_window.ArchiveScoutApp)
        app.events = mock.Mock()
        app.stop_event = threading.Event()
        app.active_operation_project_identity = "a different visible project"
        with tempfile.TemporaryDirectory() as temp:
            config = ProjectConfig(Path(temp), [], []).normalized()
            identity = app.project_identity(config.output_dir)
            event = ProgressEvent("ai", "Review", 1, 2)
            def review(config, database, scan_id, prompt, api_key, stop, callback):
                callback(event)
                return 7
            database = mock.Mock()
            with mock.patch.object(main_window, "open_database", return_value=database), \
                 mock.patch.object(main_window, "run_ai_review", side_effect=review), \
                 mock.patch.object(main_window, "generate_ai_reports", return_value={"report": "ready"}):
                app.run_ai_worker(config, 1, "Research", "")
            app.events.put_progress.assert_called_once_with((identity, event))
            app.events.put.assert_called_once_with(("ai_complete", (identity, {"run_id": 7, "paths": {"report": "ready"}})))
            database.close.assert_called_once()

    def test_optional_config_round_trip_and_legacy_default(self):
        with tempfile.TemporaryDirectory() as temp:
            config = ProjectConfig(Path(temp), [], [], dashboard_eta_enabled=True).normalized()
            self.assertTrue(load_project_config(save_project_config(config)).dashboard_eta_enabled)
            self.assertFalse(ProjectConfig(Path(temp), [], []).normalized().dashboard_eta_enabled)

    def test_measured_rate_and_duplicate_counters(self):
        tracker = OperationEtaTracker(True)
        tracker.observe(ProgressEvent("scan", "", 0, 100), now=0)
        tracker.observe(ProgressEvent("scan", "", 10, 100), now=10)
        self.assertEqual(tracker.seconds_remaining(10), 90)
        tracker.observe(ProgressEvent("scan", "", 10, 100), now=12)
        self.assertEqual(len(tracker.samples), 2)
        self.assertEqual(tracker.seconds_remaining(12), 108)

    def test_recovery_deadline_counts_once_and_excludes_paused_clock(self):
        tracker = OperationEtaTracker(True)
        tracker.observe(ProgressEvent("scan", "", 0, 100), now=0)
        tracker.observe(ProgressEvent("scan", "", 10, 100), now=10)
        wait = ProgressEvent("network_waiting", "", detail={"eligible_at_epoch": 120})
        tracker.observe(wait, now=10, epoch=100)
        tracker.observe(wait, now=15, epoch=105)
        self.assertEqual(tracker.seconds_remaining(15), 105)
        tracker.observe(ProgressEvent("scan", "", 20, 100), now=40)
        self.assertEqual(tracker.seconds_remaining(40), 80)

    def test_unknown_total_phase_change_reset_and_stale_operation(self):
        tracker = OperationEtaTracker(True)
        tracker.observe(ProgressEvent("index", "", detail={"operation_run_id": 4}), now=0)
        self.assertIsNone(tracker.seconds_remaining(1))
        self.assertIn("Estimating", tracker.label(1))
        tracker.observe(ProgressEvent("scan", "", 0, 10, {"operation_run_id": 4}), now=2)
        tracker.observe(ProgressEvent("scan", "", 5, 10, {"operation_run_id": 4}), now=7)
        tracker.observe(ProgressEvent("index", "", 0, 100, {"operation_run_id": 3}), now=8)
        self.assertEqual(tracker.stage, "scan")
        tracker.observe(ProgressEvent("media_download", "", 0, 10, {"operation_run_id": 5}), now=9)
        self.assertIsNone(tracker.seconds_remaining(9))
        tracker.reset()
        self.assertEqual(tracker.label(10), "Estimated time remaining: Ready")

    def test_history_is_bounded_and_disabled_tracker_does_no_work(self):
        tracker = OperationEtaTracker()
        tracker.observe(ProgressEvent("scan", "", 0, 2000), now=0)
        self.assertEqual(len(tracker.samples), 0)
        tracker.set_enabled(True)
        for n in range(1000):
            tracker.observe(ProgressEvent("scan", "", n, 2000), now=n)
        self.assertLessEqual(len(tracker.samples), 64)
        tracker.finish("Complete")
        self.assertIn("Complete", tracker.label(1000))
        tracker.set_enabled(False)
        self.assertEqual(len(tracker.samples), 0)


if __name__ == "__main__":
    unittest.main()
