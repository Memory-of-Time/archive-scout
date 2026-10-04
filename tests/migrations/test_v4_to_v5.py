from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from archive_scout.database.connection import open_database
from archive_scout.database.schema import BASE_SCHEMA_SQL


class V4ToV5MigrationTests(unittest.TestCase):
    def test_v4_database_adds_recovery_tables(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = sqlite3.connect(root / "archive_scout.sqlite3")
            database.executescript(BASE_SCHEMA_SQL)
            database.execute("DELETE FROM schema_info")
            database.execute("INSERT INTO schema_info(version) VALUES(4)")
            database.commit()
            database.close()

            modern = open_database(root)
            self.assertEqual(modern.execute("SELECT version FROM schema_info").fetchone()[0], 12)
            for table in ("operation_runs", "network_events", "project_backups", "repair_actions"):
                row = modern.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
                self.assertIsNotNone(row)
            modern.close()


if __name__ == "__main__":
    unittest.main()
