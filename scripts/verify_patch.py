"""Verify and optionally apply the v1.0.8 replacement files to v1.0.7 source.

Run this script from the extracted patch, pointing --project at your checkout.
Only Python's standard library is required. Project databases are never opened.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import tempfile

ROOT = Path(__file__).resolve().parents[1]
METADATA = ("PATCH_MANIFEST.json", "SHA256SUMS.txt")


def digest(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def checked_path(root: Path, name: str) -> Path:
    relative = PurePosixPath(name)
    if (not name or "\\" in name or ":" in name or relative.is_absolute()
            or any(part in {".", "..", ""} for part in name.split("/"))):
        raise RuntimeError(f"Unsafe patch path: {name!r}")
    path = root
    for part in relative.parts:
        path = path / part
        if path.is_symlink():
            raise RuntimeError(f"Refusing a symlink in patch path: {name}")
    if path.exists() and not path.is_file():
        raise RuntimeError(f"Patch file path is occupied by a directory: {name}")
    return path


def source_identity(project: Path) -> tuple[str, int]:
    fields = {}
    path = checked_path(project, "archive_scout/constants.py")
    for node in ast.parse(path.read_text(encoding="utf-8")).body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id in {"VERSION", "SCHEMA_VERSION"}:
                    fields[target.id] = ast.literal_eval(node.value)
    return fields.get("VERSION"), fields.get("SCHEMA_VERSION")


def preflight(project: Path) -> tuple[dict, list[str]]:
    manifest = json.loads((ROOT / METADATA[0]).read_text(encoding="utf-8"))
    if manifest.get("release") != "1.0.8" or manifest.get("base_release") != "1.0.7" or manifest.get("deletions"):
        raise RuntimeError("This helper requires the v1.0.8-over-v1.0.7 replacement manifest")
    entries = manifest["files"]
    names = [entry["path"] for entry in entries]
    if len(set(names)) != len(names) or set(names) & set(METADATA):
        raise RuntimeError("Duplicate or invalid metadata entry in patch manifest")
    checksums = {}
    for line in (ROOT / METADATA[1]).read_text(encoding="utf-8").splitlines():
        expected, separator, name = line.partition("  ")
        if not separator or name in checksums:
            raise RuntimeError("Malformed or duplicate patch checksum")
        checksums[name] = expected
    if set(checksums) != set(names) | {METADATA[0]}:
        raise RuntimeError("Checksum inventory differs from the patch manifest")
    for name, expected in checksums.items():
        path = checked_path(ROOT, name)
        if not path.is_file() or digest(path) != expected:
            raise RuntimeError(f"Patch integrity check failed: {name}")
    if source_identity(project) not in {("1.0.7", 12), ("1.0.8", 13)}:
        raise RuntimeError("Target must be the supplied v1.0.7 source or an already-applied v1.0.8 patch")
    pending = []
    for entry in entries:
        name = entry["path"]
        if checksums[name] != entry["sha256"]:
            raise RuntimeError(f"Manifest and checksum disagree: {name}")
        target = checked_path(project, name)
        current = digest(target) if target.exists() else None
        if current == entry["sha256"]:
            continue
        if current != entry["base_sha256"]:
            raise RuntimeError(f"Target has a different/local version of {name}; no files were changed")
        pending.append(name)
    # Old release metadata is replaced too, after all source files pass.
    for name in METADATA:
        target = checked_path(project, name)
        if not target.exists() or digest(target) != digest(ROOT / name):
            pending.append(name)
    return manifest, pending


def atomic_copy(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".archive-scout-patch-", dir=target.parent)
    os.close(fd)
    try:
        shutil.copy2(source, temporary)
        os.replace(temporary, target)
    finally:
        Path(temporary).unlink(missing_ok=True)


def apply(project: Path, pending: list[str]) -> Path | None:
    if not pending:
        return None
    backup = Path(tempfile.mkdtemp(prefix="ArchiveScout-v1.0.8-source-backup-", dir=project.parent))
    existing = set()
    for name in pending:
        target = project / name
        if target.exists():
            existing.add(name)
            saved = backup / name
            saved.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(target, saved)
    written = []
    try:
        for name in pending:
            atomic_copy(ROOT / name, project / name)
            written.append(name)
    except Exception as exc:
        try:
            for name in reversed(written):
                if name in existing:
                    atomic_copy(backup / name, project / name)
                else:
                    (project / name).unlink(missing_ok=True)
        except Exception as rollback:
            raise RuntimeError(f"Apply and rollback failed; restore source files from {backup}: {rollback}") from exc
        raise RuntimeError(f"Apply failed and original files were restored; source backup: {backup}") from exc
    return backup


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True, type=Path, help="Root of the v1.0.7 source checkout")
    parser.add_argument("--apply", action="store_true", help="Apply after validation, keeping a source-file backup")
    args = parser.parse_args()
    try:
        project = args.project.expanduser().resolve(strict=True)
        manifest, pending = preflight(project)
        backup = apply(project, pending) if args.apply else None
    except (OSError, ValueError, KeyError, RuntimeError, SyntaxError) as exc:
        parser.exit(1, f"Patch verification failed: {exc}\n")
    print(json.dumps({"status": "applied" if args.apply and pending else "verified",
                      "release": manifest["release"], "replacement_files": manifest["replacement_count"],
                      "files_requiring_copy": len(pending), "project": str(project),
                      "source_backup": str(backup) if backup else None}, indent=2))


if __name__ == "__main__":
    main()
