"""Validate Archive Scout branding, source packaging, release assets, and CI contracts."""
from __future__ import annotations

import re
import sys
from pathlib import Path


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(root))
    from archive_scout.constants import APP_NAME, VERSION, SCHEMA_VERSION

    assert (APP_NAME, VERSION, SCHEMA_VERSION) == ("Archive Scout", "1.2.4", 9), "release metadata mismatch"
    project = (root / "pyproject.toml").read_text(encoding="utf-8")
    assert re.search(r'(?m)^name\s*=\s*"archive-scout"', project), "distribution name"
    assert re.search(r'(?m)^version\s*=\s*"' + re.escape(VERSION) + r'"', project), "distribution version"
    for name in ("archive-scout", "scout"):
        assert f'{name} = "archive_scout.cli:main"' in project, f"missing CLI alias {name}"
    for name in ("archive-scout-gui", "scout-gui"):
        assert f'{name} = "archive_scout.app:main"' in project, f"missing GUI alias {name}"
    version_info = (root / "packaging/windows/version_info.txt").read_text(encoding="utf-8")
    for marker in ("filevers=(1, 2, 3, 0)", "prodvers=(1, 2, 3, 0)",
                   "'Archive Scout'", "'ArchiveScout.exe'"):
        assert marker in version_info, f"Windows version_info missing {marker}"
    readme = (root / "README.md").read_text(encoding="utf-8")
    workflow = (root / ".github/workflows/build-and-release.yml").read_text(encoding="utf-8")
    for filename in ("ArchiveScout-Windows-x64.zip", "ArchiveScout-macOS-Universal.zip",
                     "ArchiveScout-Linux-x64.tar.gz"):
        assert f"/releases/latest/download/{filename}" in readme, f"README link missing {filename}"
        assert filename in workflow, f"workflow missing {filename}"
    for script, expected in (
        ("scripts/build_windows.ps1", ("--name ArchiveScout", "--name ArchiveScoutCLI", "ArchiveScout-Windows-x64.zip")),
        ("scripts/build_macos.sh", ('--name "Archive Scout"', '--name "ArchiveScoutCLI"', "ArchiveScout-macOS-Universal.zip")),
        ("scripts/build_linux.sh", ("--name ArchiveScout", "--name ArchiveScoutCLI", "ArchiveScout-Linux-x64.tar.gz")),
    ):
        contents = (root / script).read_text(encoding="utf-8")
        for name in expected:
            assert name in contents, f"{script} missing {name}"
    for ext in ("png", "ico", "icns"):
        assert (root / "archive_scout/assets" / f"archive_scout.{ext}").is_file(), "missing platform icon"
    for file in ("Install Archive Scout.cmd", "Uninstall Archive Scout.cmd", "install.ps1", "uninstall.ps1"):
        assert (root / "packaging/windows" / file).is_file(), f"missing installer {file}"
    tests = (root / ".github/workflows/tests.yml").read_text(encoding="utf-8")
    assert "python scripts/run_tests.py" in tests, "CI canonical test runner missing"
    assert "xvfb-run" in tests, "Linux GUI test runner missing"
    assert (root / "archive_scout/ui/main_window.py").is_file()
    print(f"{APP_NAME} v{VERSION}: release metadata, installers, assets, aliases and workflows verified")
    return 0


if __name__ == "__main__":
    sys.exit(main())
