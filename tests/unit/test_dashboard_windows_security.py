from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from archive_scout.ui.dashboard import read_dashboard_counts


class DashboardTests(unittest.TestCase):
    def test_manual_snapshot_counts_saved_downloads_and_unique_keyword_pages(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'archive_scout.sqlite3'
            database = sqlite3.connect(path)
            try:
                database.executescript('''
                    CREATE TABLE captures (id INTEGER PRIMARY KEY, state TEXT NOT NULL);
                    CREATE TABLE documents (id INTEGER PRIMARY KEY, capture_id INTEGER);
                    CREATE TABLE document_matches (
                        id INTEGER PRIMARY KEY, document_id INTEGER, score INTEGER,
                        excluded INTEGER, required_missing INTEGER
                    );
                    CREATE TABLE errors (
                        id INTEGER PRIMARY KEY, resolved INTEGER NOT NULL, ignored INTEGER NOT NULL
                    );
                    INSERT INTO captures VALUES (1, 'downloaded_unscanned');
                    INSERT INTO captures VALUES (2, 'downloaded');
                    INSERT INTO captures VALUES (3, 'pending');
                    INSERT INTO captures VALUES (4, 'skipped');
                    INSERT INTO captures VALUES (5, 'error');
                    INSERT INTO documents VALUES (1, 1);
                    INSERT INTO documents VALUES (2, 2);
                    INSERT INTO document_matches VALUES (1, 1, 6, 0, 0);
                    INSERT INTO document_matches VALUES (2, 1, 12, 0, 0);
                    INSERT INTO document_matches VALUES (3, 2, 0, 0, 0);
                    INSERT INTO document_matches VALUES (4, 2, 5, 1, 0);
                    INSERT INTO errors VALUES (1, 0, 0);
                    INSERT INTO errors VALUES (2, 1, 0);
                ''')
                database.commit()
                snapshot = read_dashboard_counts(path, include_classification=False)
                self.assertEqual(snapshot, {
                    'captures': 5, 'documents': 2, 'matches': 1, 'errors': 1,
                })
                # Reading counts does not mutate or migrate the database, nor
                # does retaining a Python snapshot magically refresh it.
                database.execute("INSERT INTO captures VALUES (6, 'downloaded_unscanned')")
                database.execute("INSERT INTO documents VALUES (3, 6)")
                database.execute("INSERT INTO document_matches VALUES (5, 3, 9, 0, 0)")
                database.commit()
                self.assertEqual(snapshot['documents'], 2)
                self.assertEqual(snapshot['matches'], 1)
                fresh = read_dashboard_counts(path, include_classification=False)
                self.assertEqual(fresh['documents'], 3)
                self.assertEqual(fresh['matches'], 2)
            finally:
                database.close()  # Windows cannot unlink an open SQLite file.

    def test_missing_database_returns_zeroes(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'missing.sqlite3'
            self.assertEqual(
                {key: read_dashboard_counts(path)[key] for key in ('captures', 'documents', 'matches', 'errors')},
                {'captures': 0, 'documents': 0, 'matches': 0, 'errors': 0},
            )


if __name__ == '__main__':
    unittest.main()
