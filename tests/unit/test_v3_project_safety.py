from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from archive_scout.config import AnalysisConfig, ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.operations import run_project
from archive_scout.projects.backups import create_project_backup, restore_project_backup
from archive_scout.projects.diagnostics import export_diagnostics
from archive_scout.projects.repair import repair_project


class V3ProjectSafetyTests(unittest.TestCase):
    def test_backup_restore_repair_and_diagnostics(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = open_database(root)
            database.execute("INSERT INTO project_meta(key,value) VALUES('sample','before')")
            database.commit()
            database.close()
            backup = create_project_backup(root, reason="test", keep=3)
            database = open_database(root)
            database.execute("UPDATE project_meta SET value='after' WHERE key='sample'")
            database.commit()
            database.close()
            restore_project_backup(root, backup)
            database = open_database(root)
            self.assertEqual(database.execute("SELECT value FROM project_meta WHERE key='sample'").fetchone()[0], "before")
            report = repair_project(root, database, keep_backups=3)
            self.assertTrue(report.exists())
            package = export_diagnostics(root, database)
            self.assertTrue(package.exists())
            self.assertGreater(package.stat().st_size, 0)
            database.close()

    def test_project_merge_creates_automatic_safety_backup(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            destination = root / "destination"
            source = root / "source"
            source.mkdir()
            config = ProjectConfig(
                output_dir=destination,
                targets=[],
                keywords=[],
                analysis=AnalysisConfig(merge_source=str(source)),
                auto_backup=True,
                backup_keep=4,
            )
            with patch("archive_scout.operations.create_project_backup") as backup, patch(
                "archive_scout.operations.merge_projects", return_value={"documents": 0}
            ):
                paths = run_project(config, mode="merge_project")
            backup.assert_called_once_with(destination.resolve(), reason="before_merge", keep=4)
            self.assertTrue(paths["merge_summary"].exists())


if __name__ == "__main__":
    unittest.main()
