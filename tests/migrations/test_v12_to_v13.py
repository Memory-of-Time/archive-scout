from __future__ import annotations

import gzip
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from archive_scout.database.connection import open_database
from archive_scout.database.repositories import (
    get_or_create_keyword_set, save_match, save_note, set_review, start_scan_run, upsert_document,
)
from archive_scout.database.schema import initialize_schema, migrate_v12_to_v13
from archive_scout.scanning.full_text import search_documents
from archive_scout.utils import utc_now


class V12ToV13MigrationTests(unittest.TestCase):
    def legacy_project(self, root: Path) -> tuple[int, int]:
        database = open_database(root)
        now = utc_now()
        path = root / "retained.txt"
        path.write_text("needle retained evidence", encoding="utf-8")
        cid = database.execute(
            """INSERT INTO captures(original_url,timestamp,query_signature,mimetype,state,local_path,
                   payload_availability,created_at,updated_at)
               VALUES('http://example.com/page','20010101000000','sig','text/plain','downloaded',?,'retained',?,?)""",
            (str(path), now, now),
        ).lastrowid
        did = upsert_document(database, cid, path, "Evidence", "needle retained evidence", [], "body-hash", "normalized", 24)
        kid = get_or_create_keyword_set(database, "Preserved", ["needle"])
        run_id = start_scan_run(database, kid, "Preserved", 1, "rescan")
        mid = save_match(database, run_id, did, {"score": 17, "hits": {"needle": 1}})
        set_review(database, mid, "relevant", "researcher")
        save_note(database, mid, "Keep this note", "researcher")
        database.execute("UPDATE scan_runs SET status='complete' WHERE id=?", (run_id,))
        database.execute("DROP TABLE document_fts_versions")
        database.execute("DELETE FROM project_meta WHERE key='fts_current_mapping'")
        database.execute("ALTER TABLE quick_search_coverage DROP COLUMN content_fingerprint")
        database.execute("DROP INDEX captures_classification_idx")
        database.execute("CREATE INDEX captures_classification_idx ON captures(query_signature,resource_classifier_revision,id)")
        database.execute("DROP INDEX media_captures_download_length_idx")
        database.execute("CREATE INDEX media_captures_download_length_idx ON media_captures(query_signature,state,download_attempts,length,id)")
        database.execute("UPDATE schema_info SET version=12")
        database.commit()
        database.close()
        return int(did), int(mid)

    def test_previous_schema_is_backed_up_and_all_evidence_survives(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            did, mid = self.legacy_project(root)
            database = open_database(root)
            try:
                self.assertEqual(database.execute("SELECT version FROM schema_info").fetchone()[0], 13)
                self.assertEqual(database.execute("SELECT score FROM document_matches WHERE id=?", (mid,)).fetchone()[0], 17)
                self.assertEqual(database.execute("SELECT status FROM reviews WHERE match_id=?", (mid,)).fetchone()[0], "relevant")
                self.assertEqual(database.execute("SELECT text FROM notes WHERE match_id=?", (mid,)).fetchone()[0], "Keep this note")
                self.assertEqual([r["id"] for r in search_documents(database, "needle")], [did])
                columns = {r[1] for r in database.execute("PRAGMA table_info(quick_search_coverage)")}
                self.assertIn("content_fingerprint", columns)
                self.assertEqual([r[2] for r in database.execute("PRAGMA index_info(media_captures_download_length_idx)")],
                                 ["query_signature", "state", "length", "id", "download_attempts"])
                initialize_schema(database)
                self.assertEqual(database.execute("SELECT COUNT(*) FROM document_fts_versions").fetchone()[0], 1)
                database.commit()
            finally:
                database.close()
            backups = list((root / "backups").glob("*before_schema_13.sqlite3.gz"))
            self.assertEqual(len(backups), 1)
            before = root / "backup-check.sqlite3"
            with gzip.open(backups[0], "rb") as handle:
                before.write_bytes(handle.read())
            old = sqlite3.connect(before)
            try:
                self.assertEqual(old.execute("SELECT version FROM schema_info").fetchone()[0], 12)
                self.assertEqual(old.execute("SELECT text FROM notes").fetchone()[0], "Keep this note")
            finally:
                old.close()

    def test_backup_failure_does_not_change_schema_or_evidence(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.legacy_project(root)
            with mock.patch("archive_scout.projects.backups.create_project_backup", side_effect=OSError("no backup space")):
                with self.assertRaisesRegex(RuntimeError, "no schema changes"):
                    open_database(root)
            database = sqlite3.connect(root / "archive_scout.sqlite3")
            try:
                self.assertEqual(database.execute("SELECT version FROM schema_info").fetchone()[0], 12)
                self.assertEqual(database.execute("SELECT text FROM notes").fetchone()[0], "Keep this note")
                self.assertFalse(database.execute("SELECT 1 FROM sqlite_master WHERE name='document_fts_versions'").fetchone())
            finally:
                database.close()

    def test_forward_migration_can_be_repeated_after_partial_initialization(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.legacy_project(root)
            database = sqlite3.connect(root / "archive_scout.sqlite3")
            database.row_factory = sqlite3.Row
            try:
                migrate_v12_to_v13(database)
                database.execute("UPDATE schema_info SET version=12")
                database.commit()
                migrate_v12_to_v13(database)
                initialize_schema(database)
                database.commit()
                self.assertEqual(database.execute("SELECT COUNT(*) FROM notes").fetchone()[0], 1)
                self.assertEqual(database.execute("SELECT COUNT(*) FROM document_fts_versions").fetchone()[0], 1)
            finally:
                database.close()


if __name__ == "__main__":
    unittest.main()
