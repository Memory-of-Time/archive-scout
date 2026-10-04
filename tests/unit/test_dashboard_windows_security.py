from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from archive_scout.ui.dashboard import read_dashboard_counts


class DashboardTests(unittest.TestCase):
    def test_dashboard_counts_follow_database_changes(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "archive_scout.sqlite3"
            database = sqlite3.connect(path)
            database.executescript(
                """
                CREATE TABLE captures (id INTEGER PRIMARY KEY);
                CREATE TABLE documents (id INTEGER PRIMARY KEY);
                CREATE TABLE document_matches (id INTEGER PRIMARY KEY);
                CREATE TABLE errors (id INTEGER PRIMARY KEY, resolved INTEGER NOT NULL, ignored INTEGER NOT NULL);
                INSERT INTO captures DEFAULT VALUES;
                INSERT INTO documents DEFAULT VALUES;
                INSERT INTO document_matches DEFAULT VALUES;
                INSERT INTO errors(resolved, ignored) VALUES (0, 0);
                INSERT INTO errors(resolved, ignored) VALUES (1, 0);
                """
            )
            database.commit()
            counts = read_dashboard_counts(path)
            self.assertEqual(counts["captures"], 1)
            self.assertEqual(counts["documents"], 1)
            self.assertEqual(counts["matches"], 1)
            self.assertEqual(counts["errors"], 1)
            self.assertIsNone(counts["recovery_events"])
            self.assertFalse(counts["_exact"])
            self.assertEqual(counts["_status"], "database_error")
            database.execute("INSERT INTO captures DEFAULT VALUES")
            database.execute("INSERT INTO documents DEFAULT VALUES")
            database.commit()
            self.assertEqual(read_dashboard_counts(path)["captures"], 2)
            self.assertEqual(read_dashboard_counts(path)["documents"], 2)
            database.close()

    def test_missing_database_returns_zeroes(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "missing.sqlite3"
            counts = read_dashboard_counts(path)
            for key, value in counts.items():
                if key.startswith("_"):
                    continue
                self.assertEqual(value, 0)
            self.assertTrue(counts["_exact"])
            self.assertEqual(counts["_status"], "missing_database")


if __name__ == "__main__":
    unittest.main()
