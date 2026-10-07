from __future__ import annotations

from ..database.lease import guard_project
from ..database.connection import live_project_writer_pids

import gzip
import shutil
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

from ..database.connection import DATABASE_NAME
from ..constants import SCHEMA_VERSION
from ..utils import utc_now
from ..events import ProgressEvent


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")


def create_project_backup(root: Path, reason: str = "manual", keep: int = 5, max_mb: float = 1024.0, *, callback=None) -> Path:
    """Create a compressed SQLite backup without copying capture/media payloads."""
    root = Path(root)
    source = root / DATABASE_NAME
    if not source.exists():
        raise FileNotFoundError(source)
    backup_dir = root / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    stem = f"archive_scout_{_timestamp()}_{reason.replace(' ', '_')}"
    raw = backup_dir / f"{stem}.sqlite3.tmp"
    destination = backup_dir / f"{stem}.sqlite3.gz"
    source_db = sqlite3.connect(source)
    destination_db = sqlite3.connect(raw)
    last_emit = 0.0

    def report(stage: str, current: int, total: int) -> None:
        nonlocal last_emit
        now = time.monotonic()
        if callback and (current >= total or now - last_emit >= 0.5):
            last_emit = now
            callback(ProgressEvent(stage, "Creating project backup", current, total))

    try:
        if callback:
            source_db.backup(destination_db, pages=256,
                             progress=lambda status, remaining, total: report("backup_copy", total - remaining, total))
        else:
            source_db.backup(destination_db)
    finally:
        destination_db.close()
        source_db.close()
    with raw.open("rb") as src, gzip.open(destination, "wb", compresslevel=6) as dst:
        if callback:
            size = raw.stat().st_size
            current = 0
            last_emit = 0.0
            while chunk := src.read(1024 * 1024):
                dst.write(chunk)
                current += len(chunk)
                report("backup_compress", current, size)
        else:
            shutil.copyfileobj(src, dst, length=1024 * 1024)
    raw.unlink(missing_ok=True)
    _record_backup(root, destination, reason)
    prune_backups(root, keep, max_mb=max_mb)
    return destination


def _record_backup(root: Path, path: Path, reason: str) -> None:
    database_path = root / DATABASE_NAME
    if not database_path.exists():
        return
    database = sqlite3.connect(database_path)
    try:
        has_table = database.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='project_backups'"
        ).fetchone()
        if has_table:
            database.execute(
                "INSERT OR IGNORE INTO project_backups(path,reason,size_bytes,created_at) VALUES(?,?,?,?)",
                (str(path), reason, path.stat().st_size if path.exists() else 0, utc_now()),
            )
            database.commit()
    except Exception:
        pass
    finally:
        database.close()


def list_project_backups(root: Path) -> list[Path]:
    backup_dir = Path(root) / "backups"
    if not backup_dir.exists():
        return []
    paths = list(backup_dir.glob("archive_scout_*.sqlite3")) + list(backup_dir.glob("archive_scout_*.sqlite3.gz"))
    return sorted(paths, key=lambda p: p.stat().st_mtime, reverse=True)


def prune_backups(root: Path, keep: int = 5, max_mb: float = 1024.0) -> None:
    keep = max(1, int(keep))
    budget = max(64 * 1024 * 1024, int(float(max_mb) * 1024 * 1024))
    paths = list_project_backups(root)
    total = 0
    for index, path in enumerate(paths):
        size = path.stat().st_size if path.exists() else 0
        # Always retain the newest backup. Thereafter enforce both count and
        # aggregate disk budget.
        if index >= keep or (index > 0 and total + size > budget):
            path.unlink(missing_ok=True)
            continue
        total += size


def _materialize_backup(backup_path: Path, destination: Path) -> None:
    if backup_path.suffix == ".gz":
        with gzip.open(backup_path, "rb") as src, destination.open("wb") as dst:
            shutil.copyfileobj(src, dst, length=1024 * 1024)
    else:
        shutil.copy2(backup_path, destination)


@guard_project
def restore_project_backup(root: Path, backup_path: Path) -> Path:
    root = Path(root)
    if live_project_writer_pids(root):
        raise RuntimeError("Pause the active project writer before restoring a database backup")
    backup_path = Path(backup_path)
    if not backup_path.exists():
        raise FileNotFoundError(backup_path)
    target = root / DATABASE_NAME
    safety = None
    if target.exists():
        safety = root / "backups" / f"archive_scout_{_timestamp()}_before_restore.sqlite3"
        safety.parent.mkdir(parents=True, exist_ok=True)
        source_db = sqlite3.connect(target)
        safety_db = sqlite3.connect(safety)
        try:
            source_db.backup(safety_db)
        finally:
            safety_db.close()
            source_db.close()
    temp = target.with_suffix(".restore.tmp")
    _materialize_backup(backup_path, temp)
    check = sqlite3.connect(temp)
    try:
        result = check.execute("PRAGMA integrity_check").fetchone()
        if not result or str(result[0]).lower() != "ok":
            raise RuntimeError(f"backup failed SQLite integrity check: {result}")
        schema = check.execute("SELECT version FROM schema_info LIMIT 1").fetchone()
        if not schema:
            raise RuntimeError("backup does not contain Archive Scout schema metadata")
        version = int(schema[0])
        if version > SCHEMA_VERSION:
            raise RuntimeError(f"backup schema {version} is newer than supported schema {SCHEMA_VERSION}")
    finally:
        check.close()
    temp.replace(target)
    for suffix in ("-wal", "-shm"):
        Path(str(target) + suffix).unlink(missing_ok=True)
    return safety or target
