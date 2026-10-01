from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from archive_scout.database.connection import open_database
from archive_scout.database.schema import initialize_schema
from archive_scout.utils import utc_now


class V9ToV10MigrationTests(unittest.TestCase):
    def test_v9_database_adds_audit3_state_and_preserves_research_work(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path = root / "archive_scout.sqlite3"
            db = sqlite3.connect(path)
            db.row_factory = sqlite3.Row
            initialize_schema(db)
            # Turn a freshly-created schema into the physical v9 shape. Python
            # 3.11+'s bundled SQLite supports DROP COLUMN on every supported CI
            # platform; this keeps the fixture focused on the real forward
            # migration instead of carrying a second giant schema copy.
            db.execute("DROP INDEX IF EXISTS captures_classification_idx")
            db.execute("DROP INDEX IF EXISTS captures_payload_idx")
            db.execute("DROP TABLE IF EXISTS index_coverage")
            db.execute("DROP TABLE IF EXISTS media_index_coverage")
            for table, columns in {
                "captures": [
                    "resource_class", "classification_reason", "resource_classifier_revision",
                    "payload_availability", "cleanup_pending", "discarded_at",
                ],
                "operation_runs": ["retention_policy", "config_json"],
                "index_pages": ["layout_signature"],
                "media_index_pages": ["layout_signature"],
                "quick_search_runs": ["discarded_count", "missing_count", "non_text_count", "incomplete_count"],
            }.items():
                for column in columns:
                    db.execute(f"ALTER TABLE {table} DROP COLUMN {column}")
            db.execute("UPDATE schema_info SET version=9")
            now = utc_now()
            target_id = db.execute(
                "INSERT INTO targets(pattern,settings_json,created_at) VALUES('example.com/*','{}',?)", (now,)
            ).lastrowid
            capture_id = db.execute(
                """INSERT INTO captures(original_url,timestamp,target_id,query_signature,mimetype,statuscode,length,
                       state,local_path,created_at,updated_at)
                   VALUES('http://example.com/a','20010101000000',?,'sig','text/html','200',10,
                       'downloaded',?, ?, ?)""",
                (target_id, str(root / "captures" / "a.txt"), now, now),
            ).lastrowid
            document_id = db.execute(
                """INSERT INTO documents(capture_id,path,title,body_text,body_chars,original_url,size_bytes,created_at,updated_at)
                   VALUES(?,?,?,?,?,?,?,?,?)""",
                (capture_id, str(root / "captures" / "a.txt"), "Title", "legacy body", 11,
                 "http://example.com/a", 11, now, now),
            ).lastrowid
            db.execute("UPDATE captures SET document_id=? WHERE id=?", (document_id, capture_id))
            db.execute(
                "INSERT INTO notes(capture_id,text,author,created_at,updated_at) VALUES(?,?,?,?,?)",
                (capture_id, "keep this note", "tester", now, now),
            )
            db.commit()
            db.close()

            modern = open_database(root)
            try:
                self.assertEqual(modern.execute("SELECT version FROM schema_info").fetchone()[0], 11)
                row = modern.execute(
                    "SELECT resource_class,payload_availability,cleanup_pending,document_id FROM captures WHERE id=?",
                    (capture_id,),
                ).fetchone()
                self.assertEqual(row["resource_class"], "unknown")
                self.assertEqual(row["payload_availability"], "retained")
                self.assertEqual(row["cleanup_pending"], 0)
                self.assertEqual(row["document_id"], document_id)
                self.assertEqual(
                    modern.execute("SELECT text FROM notes WHERE capture_id=?", (capture_id,)).fetchone()[0],
                    "keep this note",
                )
                self.assertIsNotNone(modern.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='index_coverage'"
                ).fetchone())
                self.assertIsNotNone(modern.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='media_index_coverage'"
                ).fetchone())
            finally:
                modern.close()


if __name__ == "__main__":
    unittest.main()
