from __future__ import annotations
import unittest
from pathlib import Path
from archive_scout.constants import APP_NAME, VERSION, SCHEMA_VERSION

class ScoutReleaseBrandTests(unittest.TestCase):
    def test_new_brand_preserves_schema(self):
        self.assertEqual(APP_NAME, "Archive Scout")
        self.assertEqual(VERSION, "1.3.0")
        self.assertEqual(SCHEMA_VERSION, 12)

    def test_default_user_agent_and_saved_version_follow_release(self):
        from archive_scout.config import ProjectConfig, project_config_from_payload
        config=ProjectConfig(Path('.'),targets=[],keywords=[]).normalized()
        self.assertIn(f'ArchiveScout/{VERSION} ',config.user_agent)
        self.assertEqual(config.to_payload()['version'],VERSION)
        restored=project_config_from_payload({'user_agent':'ArchiveScout/1.2.6 public web archive research client'},Path('project.json'))
        self.assertEqual(restored.user_agent,config.user_agent)
        custom=project_config_from_payload({'user_agent':'custom research client'},Path('project.json'))
        self.assertEqual(custom.user_agent,'custom research client')

    def test_v126_deliberately_saved_pacing_survives_version_bump(self):
        from archive_scout.config import project_config_from_payload
        config=project_config_from_payload({'version':'1.2.6','cdx_delay':.75,'download_delay':.2},Path('project.json'))
        self.assertEqual(config.cdx_delay,.75)
        self.assertEqual(config.download_delay,.2)

    def test_windows_numeric_version_metadata(self):
        source = (Path(__file__).resolve().parents[2] / "packaging/windows/version_info.txt").read_text()
        numeric_version = (*map(int, VERSION.split('.')), 0)
        self.assertIn(f"filevers={numeric_version}", source)
        self.assertIn(f"prodvers={numeric_version}", source)

    def test_macos_bundle_metadata_uses_current_release(self):
        import plistlib
        import subprocess
        import sys
        import tempfile
        root=Path(__file__).resolve().parents[2]
        source=(root/'scripts/build_macos.sh').read_text()
        block=source.split("<<'PYVERSION'\n",1)[1].split('\nPYVERSION',1)[0]
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/'Info.plist'
            path.write_bytes(plistlib.dumps({'CFBundleName':'Archive Scout','CFBundleVersion':'0.0.0'}))
            result=subprocess.run([sys.executable,'-',str(path)],input=block,text=True,cwd=root,capture_output=True)
            self.assertEqual(result.returncode,0,result.stderr)
            info=plistlib.loads(path.read_bytes())
            self.assertEqual(info['CFBundleShortVersionString'],VERSION)
            self.assertEqual(info['CFBundleVersion'],VERSION)
            self.assertEqual(info['CFBundleName'],'Archive Scout')

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
