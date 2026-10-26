from __future__ import annotations
import unittest
from pathlib import Path
from archive_scout.constants import APP_NAME, VERSION, SCHEMA_VERSION

class ScoutReleaseBrandTests(unittest.TestCase):
    def test_new_brand_preserves_schema(self):
        self.assertEqual(APP_NAME, "Archive Scout")
        self.assertEqual(VERSION, "1.2.6")
        self.assertEqual(SCHEMA_VERSION, 11)

    def test_windows_numeric_version_metadata(self):
        source = (Path(__file__).resolve().parents[2] / "packaging/windows/version_info.txt").read_text()
        numeric_version = (*map(int, VERSION.split('.')), 0)
        self.assertIn(f"filevers={numeric_version}", source)
        self.assertIn(f"prodvers={numeric_version}", source)

    def test_three_platform_download_links_match_release_assets(self):
        root = Path(__file__).resolve().parents[2]
        readme = (root / "README.md").read_text(encoding="utf8")
        release = (root / ".github/workflows/build-and-release.yml").read_text(encoding="utf8")
        for name in ["ArchiveScout-Windows-x64.zip", "ArchiveScout-macOS-Universal.zip", "ArchiveScout-Linux-x64.tar.gz"]:
            self.assertIn('/releases/latest/download/' + name, readme)
            self.assertIn(name, release)

    def test_installed_source_asset_lookup(self):
        from archive_scout.runtime import bundled_resource
        self.assertTrue(bundled_resource("assets", "archive_scout.png").is_file())

    def test_public_installer_paths_are_consistent(self):
        root = Path(__file__).resolve().parents[2]
        windows = (root / "scripts/build_windows.ps1").read_text(encoding="utf-8")
        linux = (root / "scripts/build_linux.sh").read_text(encoding="utf-8")
        macos = (root / "scripts/build_macos.sh").read_text(encoding="utf-8")
        workflow = (root / ".github/workflows/build-and-release.yml").read_text(encoding="utf-8")
        self.assertIn("dist\\ArchiveScout\\ArchiveScout.exe", windows)
        self.assertIn("release/ArchiveScout-Linux-x64", linux)
        self.assertIn('APP="dist/Archive Scout.app"', macos)
        self.assertIn("dist\\ArchiveScout\\ArchiveScout.exe", workflow)

    def test_legacy_imports_and_commands_preserved(self):
        project = (Path(__file__).resolve().parents[2] / 'pyproject.toml').read_text()
        self.assertIn('archive-scout = "archive_scout.cli:main"', project)
        self.assertIn('scout = "archive_scout.cli:main"', project)
