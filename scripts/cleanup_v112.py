"""Preview or apply the deletion-only v1.1.2 follow-up to known rollback source."""
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


def digest(path: Path) -> str:
    with path.open('rb') as handle:
        return hashlib.file_digest(handle, 'sha256').hexdigest()


def text_digest(path: Path) -> str | None:
    try:
        value = path.read_bytes().decode('utf-8-sig').replace('\r\n', '\n').replace('\r', '\n')
        return hashlib.sha256(value.encode()).hexdigest()
    except (OSError, UnicodeDecodeError):
        return None


def checked_path(root: Path, name: str) -> Path:
    relative = PurePosixPath(name)
    if (not name or '\\' in name or ':' in name or relative.is_absolute()
            or any(p in {'.', '..', ''} for p in name.split('/'))):
        raise RuntimeError(f'Unsafe cleanup path: {name!r}')
    if (relative.suffix.lower() in {'.sqlite', '.sqlite3', '.db', '.wal', '.shm'}
            or relative.name == 'project.json'
            or relative.parts[0] in {'.git', 'captures', 'media', 'reports', 'backups'}):
        raise RuntimeError(f'Refusing project/evidence path: {name}')
    path = root
    for part in relative.parts:
        path = path / part
        if path.is_symlink() or (hasattr(path, 'is_junction') and path.is_junction()):
            raise RuntimeError(f'Refusing redirected cleanup path: {name}')
    if not path.resolve().is_relative_to(root.resolve()):
        raise RuntimeError(f'Cleanup path escapes repository: {name}')
    if path.exists() and not path.is_file():
        raise RuntimeError(f'Cleanup path is not a file: {name}')
    return path


def identity(project: Path) -> tuple:
    fields = {}
    for node in ast.parse(checked_path(project, 'archive_scout/constants.py').read_text(encoding='utf-8-sig')).body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id in {'VERSION', 'SCHEMA_VERSION'}:
                    fields[target.id] = ast.literal_eval(node.value)
    return fields.get('VERSION'), fields.get('SCHEMA_VERSION')


def preflight(project: Path, *, include_optional: bool = False) -> list[str]:
    path = ROOT / 'CLEANUP_MANIFEST.json'
    if digest(path) != (ROOT / 'CLEANUP_MANIFEST.sha256').read_text().strip():
        raise RuntimeError('Cleanup manifest integrity check failed; no files changed')
    manifest = json.loads(path.read_text(encoding='utf-8'))
    if manifest.get('release') != '1.1.2' or manifest.get('schema') != 13:
        raise RuntimeError('This helper requires the v1.1.2 schema13 cleanup manifest')
    if identity(project) != ('1.1.2', 13):
        raise RuntimeError('Apply the original v1.1.2 patch before this cleanup')
    all_entries = manifest['required'] + manifest['optional']
    all_names = [e['path'] for e in all_entries] + [e['path'] for e in manifest['retained']]
    if len(all_names) != len(set(all_names)):
        raise RuntimeError('Duplicate cleanup/retained path')
    for entry in manifest['retained']:
        target = checked_path(project, entry['path'])
        if (not target.is_file() or (digest(target) != entry['sha256']
                and text_digest(target) != entry['text_sha256'])):
            raise RuntimeError(f"Retained rollback source differs: {entry['path']}; no files changed")
    pending = []
    entries = manifest['required'] + (manifest['optional'] if include_optional else [])
    for entry in entries:
        target = checked_path(project, entry['path'])
        if not target.exists():
            continue
        if (digest(target) not in entry['accepted_sha256']
                and text_digest(target) not in entry['accepted_text_sha256']):
            raise RuntimeError(f"Retired file has local changes: {entry['path']}; no files changed")
        pending.append(entry['path'])
    return pending


def atomic_copy(source: Path, target: Path) -> None:
    fd, temporary = tempfile.mkstemp(prefix='.archive-scout-cleanup-', dir=target.parent)
    os.close(fd)
    try:
        shutil.copy2(source, temporary)
        os.replace(temporary, target)
    finally:
        Path(temporary).unlink(missing_ok=True)


def apply(project: Path, pending: list[str]) -> Path | None:
    if not pending:
        return None
    # Resolve and validate every final target before the first deletion.
    targets = [(name, checked_path(project, name)) for name in pending]
    backup = Path(tempfile.mkdtemp(prefix='ArchiveScout-v1.1.2-cleanup-backup-', dir=project.parent))
    for name, target in targets:
        saved = backup / name
        saved.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(target, saved)
    deleted = []
    try:
        for name, target in targets:
            target.unlink()
            deleted.append((name, target))
    except Exception as exc:
        try:
            for name, target in reversed(deleted):
                atomic_copy(backup / name, target)
        except Exception as rollback:
            raise RuntimeError(f'Cleanup and rollback failed; restore source from {backup}: {rollback}') from exc
        raise RuntimeError(f'Cleanup failed; deleted source was restored. Backup: {backup}') from exc
    return backup


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--project', required=True, type=Path)
    action = parser.add_mutually_exclusive_group()
    action.add_argument('--apply', action='store_true')
    action.add_argument('--check', action='store_true', help='Exit with an error if retired files remain, before running tests')
    parser.add_argument('--include-optional', action='store_true', help='Also remove the listed historical delivery evidence and notes')
    args = parser.parse_args()
    try:
        project = args.project.expanduser().resolve(strict=True)
        pending = preflight(project, include_optional=args.include_optional)
        if args.check and pending:
            raise RuntimeError(f'{len(pending)} retired files remain; apply this cleanup before running tests')
        backup = apply(project, pending) if args.apply else None
    except (OSError, ValueError, KeyError, RuntimeError, SyntaxError) as exc:
        parser.exit(1, f'Cleanup refused: {exc}\n')
    print(json.dumps({'release': '1.1.2', 'status': 'applied' if args.apply else 'preview',
        'deleted' if args.apply else 'would_delete': pending, 'file_count': len(pending),
        'source_backup': str(backup) if backup else None}, indent=2))


if __name__ == '__main__':
    main()
