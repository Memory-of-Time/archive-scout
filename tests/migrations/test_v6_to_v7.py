from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from archive_scout.database.connection import open_database
from archive_scout.database.schema import BASE_SCHEMA_SQL


class V6ToV7MigrationTests(unittest.TestCase):
    def test_v6_database_adds_research_intelligence_tables(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = sqlite3.connect(root / "archive_scout.sqlite3")
            database.executescript(BASE_SCHEMA_SQL)
            for table in (
                "research_ai_claims", "research_ai_runs", "research_query_results", "research_queries",
                "research_edges", "research_document_entities", "research_entities",
                "research_vector_bands", "research_vectors",
            ):
                database.execute(f"DROP TABLE IF EXISTS {table}")
            database.execute("DELETE FROM schema_info")
            database.execute("INSERT INTO schema_info(version) VALUES(6)")
            database.commit()
            database.close()

            modern = open_database(root)
            self.assertEqual(modern.execute("SELECT version FROM schema_info").fetchone()[0], 11)
            for table in (
                "research_vectors", "research_vector_bands", "research_entities", "research_document_entities",
                "research_edges", "research_queries", "research_query_results", "research_ai_runs", "research_ai_claims",
            ):
                self.assertIsNotNone(
                    modern.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone(),
                    table,
                )
            modern.close()


if __name__ == "__main__":
    unittest.main()
