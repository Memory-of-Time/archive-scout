"""Check release identity, installed metadata, and canonical workflow placement."""
from __future__ import annotations

import argparse
import ast
import importlib.metadata
import json
from pathlib import Path
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from archive_scout.constants import SCHEMA_VERSION, VERSION


def verify(*, source_only: bool = False) -> dict:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    if project["version"] != VERSION:
        raise RuntimeError("pyproject.toml and runtime VERSION disagree")
    metadata = ast.parse((ROOT / "packaging/windows/version_info.txt").read_text(encoding="utf-8"))
    expected_numbers = (*map(int, VERSION.split(".")), 0)
    fields = {}
    for node in ast.walk(metadata):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
            continue
        if node.func.id == "FixedFileInfo":
            for item in node.keywords:
                if item.arg in {"filevers", "prodvers"}:
                    fields[item.arg] = ast.literal_eval(item.value)
        if node.func.id == "StringStruct" and len(node.args) == 2:
            key, value = map(ast.literal_eval, node.args)
            if key in {"FileVersion", "ProductVersion"}:
                fields[key] = value
    if any(fields.get(key) != expected_numbers for key in ("filevers", "prodvers")):
        raise RuntimeError("Windows numeric versions do not match the release")
    if any(fields.get(key) != VERSION for key in ("FileVersion", "ProductVersion")):
        raise RuntimeError("Windows display versions do not match the release")
    workflows = []
    for name in ("tests.yml", "build-and-release.yml"):
        canonical = ROOT / ".github/workflows" / name
        if not canonical.is_file():
            raise RuntimeError(f"Missing canonical workflow: {name}")
        if (ROOT / "github/workflows" / name).exists():
            raise RuntimeError(f"Duplicate workflow mirror: {name}; keep only .github/workflows")
        if name == "tests.yml":
            expected = f"assert importlib.metadata.version('archive-scout') == '{VERSION}'"
            if expected not in canonical.read_text(encoding="utf-8-sig"):
                raise RuntimeError("Tests workflow package version does not match the release")
        if (ROOT / name).is_file():
            raise RuntimeError(f"Misplaced root workflow: {name}; keep the workflow in .github/workflows")
        workflows.append(str(canonical.relative_to(ROOT)))
    if not source_only and importlib.metadata.version("archive-scout") != VERSION:
        raise RuntimeError("Installed package metadata does not match runtime VERSION")
    return {"version": VERSION, "schema": SCHEMA_VERSION, "workflow_paths": workflows,
            "installed_metadata_checked": not source_only, "status": "passed"}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-only", action="store_true", help="Skip installed metadata for an uninstalled source checkout")
    args = parser.parse_args()
    print(json.dumps(verify(source_only=args.source_only), indent=2))


if __name__ == "__main__":
    main()
