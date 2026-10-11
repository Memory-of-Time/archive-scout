from __future__ import annotations
import unittest
from pathlib import Path
from archive_scout.constants import APP_NAME, VERSION, SCHEMA_VERSION

class ScoutReleaseBrandTests(unittest.TestCase):
    def test_new_brand_preserves_schema(self):
        self.assertEqual(APP_NAME, "Scout")
        self.assertEqual(VERSION, "1.2.0")
        self.assertEqual(SCHEMA_VERSION, 9)

    def test_windows_numeric_version_metadata(self):
        source = (Path(__file__).resolve().parents[2] / "packaging/windows/version_info.txt").read_text()
        self.assertIn("filevers=(1, 2, 0, 0)", source)
        self.assertIn("prodvers=(1, 2, 0, 0)", source)

    def test_three_platform_download_links_match_release_assets(self):
        root = Path(__file__).resolve().parents[2]
        readme = (root / "README.md").read_text(encoding="utf8")
        release = (root / ".github/workflows/build-and-release.yml").read_text(encoding="utf8")
        for name in ["Scout-Windows-x64.zip", "Scout-macOS-Universal.zip", "Scout-Linux-x64.tar.gz"]:
            self.assertIn('/releases/latest/download/' + name, readme)
            self.assertIn(name, release)

    def test_installed_source_asset_lookup(self):
        from archive_scout.runtime import bundled_resource
        self.assertTrue(bundled_resource("assets", "scout.png").is_file())

    def test_legacy_imports_and_commands_preserved(self):
        project = (Path(__file__).resolve().parents[2] / 'pyproject.toml').read_text()
        self.assertIn('archive-scout = "archive_scout.cli:main"', project)
        self.assertIn('scout = "archive_scout.cli:main"', project)
