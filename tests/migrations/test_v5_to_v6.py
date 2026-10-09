from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from archive_scout.database.connection import open_database
from archive_scout.database.schema import BASE_SCHEMA_SQL


class V5ToV6MigrationTests(unittest.TestCase):
    def test_v5_database_adds_official_release_tables(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = sqlite3.connect(root / "archive_scout.sqlite3")
            database.executescript(BASE_SCHEMA_SQL)
            for table in ("ai_results", "ai_runs", "media_discovery_documents", "media_discovery_queue", "site_issues"):
                database.execute(f"DROP TABLE IF EXISTS {table}")
            database.execute("DELETE FROM schema_info")
            database.execute("INSERT INTO schema_info(version) VALUES(5)")
            database.commit()
            database.close()

            modern = open_database(root)
            self.assertEqual(modern.execute("SELECT version FROM schema_info").fetchone()[0], 13)
            for table in ("ai_runs", "ai_results", "media_discovery_queue", "media_discovery_documents", "site_issues"):
                row = modern.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
                self.assertIsNotNone(row)
            modern.close()


if __name__ == "__main__":
    unittest.main()
