from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from archive_scout.config import KeywordSetConfig, ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, upsert_capture, upsert_document
from archive_scout.operations import run_project
from archive_scout.utils import hash_text, normalize_search


class SingleScanIntegrationTests(unittest.TestCase):
    def test_one_rescan_scans_only_one_selected_keyword_set(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path = root / "saved.txt"
            path.write_text("<html><body>WTC jumper skylight.mov canopy</body></html>", encoding="utf-8")
            database = open_database(root)
            target_id = get_or_create_target(database, "example.com/*")
            upsert_capture(database, {
                "original": "http://example.com/thread", "timestamp": "20060101000000", "mimetype": "text/html",
                "statuscode": "200", "digest": "", "length": str(path.stat().st_size),
            }, target_id, "legacy")
            capture_id = database.execute("SELECT id FROM captures").fetchone()[0]
            upsert_document(database, capture_id, path, "", "WTC jumper skylight.mov canopy", [], hash_text(path.read_text()), hash_text(normalize_search(path.read_text())), path.stat().st_size)
            database.commit()
            database.close()
            config = ProjectConfig(
                output_dir=root,
                targets=[],
                keywords=[],
                keyword_sets=[
                    KeywordSetConfig("General", ["WTC", "jumper"], True),
                    KeywordSetConfig("Media", ["skylight.mov", "canopy"], True),
                ],
                from_date="2000",
                to_date="2010",
            )
            run_project(config, "rescan")
            database = open_database(root)
            self.assertEqual(database.execute("SELECT COUNT(*) FROM scan_runs WHERE status='complete'").fetchone()[0], 1)
            self.assertEqual(database.execute("SELECT COUNT(*) FROM document_matches").fetchone()[0], 1)
            selected = database.execute("""SELECT ks.name,ks.keywords_json FROM scan_runs sr
                    JOIN keyword_sets ks ON ks.id=sr.keyword_set_id""").fetchone()
            self.assertEqual(selected["name"], "General")
            self.assertNotIn("skylight.mov", selected["keywords_json"])
            run_folders = list((root / "reports").glob("scan-*-*"))
            self.assertEqual(len([p for p in run_folders if p.is_dir()]), 1)
            database.close()

            # A later user-selected set gets its own single run. Existing
            # evidence and prior reports remain available, not overwritten.
            config.keyword_sets = [
                KeywordSetConfig("General", ["WTC", "jumper"], False),
                KeywordSetConfig("Media", ["skylight.mov", "canopy"], True),
            ]
            run_project(config, "rescan")
            database = open_database(root)
            names = [row[0] for row in database.execute("""SELECT ks.name FROM scan_runs sr
                        JOIN keyword_sets ks ON ks.id=sr.keyword_set_id ORDER BY sr.id""")]
            self.assertEqual(names, ["General", "Media"])
            self.assertEqual(database.execute("SELECT COUNT(*) FROM document_matches").fetchone()[0], 2)
            database.close()
            self.assertEqual(len([p for p in (root / "reports").glob("scan-*-*") if p.is_dir()]), 2)


if __name__ == "__main__":
    unittest.main()
