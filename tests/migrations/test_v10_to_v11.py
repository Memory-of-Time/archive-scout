from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from archive_scout.database.connection import open_database
from archive_scout.database.schema import initialize_schema
from archive_scout.utils import utc_now


class V10ToV11MigrationTests(unittest.TestCase):
    def test_v10_database_adds_audit4_ownership_and_urlkey_without_losing_review_data(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path = root / "archive_scout.sqlite3"
            db = sqlite3.connect(path)
            db.row_factory = sqlite3.Row
            initialize_schema(db)
            db.execute("DROP TRIGGER IF EXISTS captures_body_revision_update")
            # Recreate the physical v10 shape from the current schema so this
            # fixture stays small while exercising the real forward migration.
            db.execute("DROP INDEX IF EXISTS captures_classification_idx")
            for table, columns in {
                "captures": ["urlkey", "payload_origin", "payload_retention"],
                "media_captures": ["urlkey"],
            }.items():
                for column in columns:
                    db.execute(f"ALTER TABLE {table} DROP COLUMN {column}")
            db.execute("UPDATE schema_info SET version=10")
            now = utc_now()
            target_id = db.execute(
                "INSERT INTO targets(pattern,settings_json,created_at) VALUES('example.com/*','{}',?)",
                (now,),
            ).lastrowid
            retained_path = root / "captures" / "retained.txt"
            retained_path.parent.mkdir(parents=True)
            retained_path.write_text("needle", encoding="utf-8")
            retained_id = db.execute(
                """INSERT INTO captures(original_url,timestamp,target_id,query_signature,mimetype,statuscode,length,
                       state,local_path,payload_availability,created_at,updated_at)
                   VALUES('http://example.com/retained','20010101000000',?,'sig','text/plain','200',6,
                          'downloaded',?,'retained',?,?)""",
                (target_id, str(retained_path), now, now),
            ).lastrowid
            spool_path = root / "captures" / "spool.txt"
            spool_path.write_text("needle", encoding="utf-8")
            spool_id = db.execute(
                """INSERT INTO captures(original_url,timestamp,target_id,query_signature,mimetype,statuscode,length,
                       state,local_path,payload_availability,created_at,updated_at)
                   VALUES('http://example.com/spool','20010102000000',?,'sig','text/plain','200',6,
                          'downloaded_unscanned',?,'spooled_unscanned',?,?)""",
                (target_id, str(spool_path), now, now),
            ).lastrowid
            db.execute(
                "INSERT INTO notes(capture_id,text,author,created_at,updated_at) VALUES(?,?,?,?,?)",
                (retained_id, "preserve me", "tester", now, now),
            )
            db.commit()
            db.close()

            modern = open_database(root)
            try:
                self.assertEqual(modern.execute("SELECT version FROM schema_info").fetchone()[0], 12)
                retained = modern.execute(
                    "SELECT urlkey,payload_origin,payload_retention,local_path FROM captures WHERE id=?",
                    (retained_id,),
                ).fetchone()
                self.assertEqual((retained["urlkey"], retained["payload_origin"], retained["payload_retention"]), ("", "legacy", "keep"))
                self.assertEqual(retained["local_path"], str(retained_path))
                spool = modern.execute(
                    "SELECT payload_origin,payload_retention,payload_availability,local_path FROM captures WHERE id=?",
                    (spool_id,),
                ).fetchone()
                self.assertEqual(
                    (spool["payload_origin"], spool["payload_retention"], spool["payload_availability"]),
                    ("acquired", "discard_after_scan", "spooled_unscanned"),
                )
                self.assertEqual(spool["local_path"], str(spool_path))
                self.assertEqual(
                    modern.execute("SELECT text FROM notes WHERE capture_id=?", (retained_id,)).fetchone()[0],
                    "preserve me",
                )
                columns = {row[1] for row in modern.execute("PRAGMA table_info(media_captures)")}
                self.assertIn("urlkey", columns)
            finally:
                modern.close()


if __name__ == "__main__":
    unittest.main()
