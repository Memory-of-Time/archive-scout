"""Verify Scout version alignment, release assets, links and CI workflow."""
from __future__ import annotations
import re
import sys
from pathlib import Path


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(root))
    from archive_scout.constants import APP_NAME, VERSION
    assert APP_NAME == "Scout" and VERSION == "1.2.0"
    pyproject = (root / "pyproject.toml").read_text(encoding="utf-8")
    assert re.search(r'(?m)^version\s*=\s*"' + re.escape(VERSION) + r'"', pyproject), "pyproject version mismatch"
    assert 'scout = "archive_scout.cli:main"' in pyproject
    version_info = (root / "packaging/windows/version_info.txt").read_text(encoding="utf-8")
    assert "'" + VERSION + "'" in version_info, "Windows version mismatch"
    assert "'Scout'" in version_info, "Windows product name mismatch"
    readme = (root / "README.md").read_text(encoding="utf-8")
    workflow = (root / ".github/workflows/build-and-release.yml").read_text(encoding="utf-8")
    for asset in ("Scout-Windows-x64.zip", "Scout-macOS-Universal.zip", "Scout-Linux-x64.tar.gz"):
        assert re.search(r'https://github\.com/[^/]+/[^/]+/releases/latest/download/' + re.escape(asset), readme), asset + " README link"
        assert asset in workflow, asset + " release workflow"
    tests = (root / ".github/workflows/tests.yml").read_text(encoding="utf-8")
    assert 'python scripts/run_tests.py' in tests, "CI test runner"
    for path in ('archive_scout/ui/main_window.py', 'scripts/build_windows.ps1', 'scripts/build_linux.sh', 'scripts/build_macos.sh'):
        assert (root / path).is_file(), path
    print(f"Scout {VERSION}: source, package, platform links and release workflow verified")
    return 0


if __name__ == "__main__":
    sys.exit(main())
