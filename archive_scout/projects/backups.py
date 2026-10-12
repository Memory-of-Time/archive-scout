from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from ..database.connection import DATABASE_NAME
from ..database.lease import project_lease
from ..utils import utc_now


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def create_project_backup(root: Path, reason: str = "manual", keep: int = 5) -> Path:
    root = Path(root)
    source = root / DATABASE_NAME
    if not source.exists():
        raise FileNotFoundError(source)
    backup_dir = root / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    destination = backup_dir / f"archive_scout_{_timestamp()}_{reason.replace(' ', '_')}.sqlite3"
    source_db = sqlite3.connect(source)
    destination_db = sqlite3.connect(destination)
    try:
        source_db.backup(destination_db)
    finally:
        destination_db.close()
        source_db.close()
    _record_backup(root, destination, reason)
    prune_backups(root, keep)
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
    return sorted(backup_dir.glob("archive_scout_*.sqlite3"), key=lambda p: p.stat().st_mtime, reverse=True)


def prune_backups(root: Path, keep: int = 5) -> None:
    keep = max(1, int(keep))
    for path in list_project_backups(root)[keep:]:
        path.unlink(missing_ok=True)


def restore_project_backup(root: Path, backup_path: Path) -> Path:
    """Restore a verified SQLite snapshot through SQLite's online backup API.

    Replacing database files and deleting -wal/-shm sidecars while connections
    are open is unsafe, and Windows explicitly prevents it.  SQLite's backup
    API coordinates locks and restores a complete snapshot transactionally.
    The project lease excludes simultaneous Scout operations. This maintenance
    path is never entered by normal acquisition or indexing.
    """
    root = Path(root).expanduser().resolve()
    backup_path = Path(backup_path).expanduser().resolve()
    if not backup_path.is_file():
        raise FileNotFoundError(backup_path)
    target = root / DATABASE_NAME

    with project_lease(root):
        # Reject damaged snapshots before changing the live database. Use an
        # escaped URI so spaces, '#' and '?' in backup filenames remain valid.
        source = sqlite3.connect(f"{backup_path.as_uri()}?mode=ro", uri=True)
        try:
            check = source.execute("PRAGMA integrity_check").fetchone()
            if not check or str(check[0]).lower() != "ok":
                raise RuntimeError(f"backup failed SQLite integrity check: {check}")

            # A consistent online safety snapshot includes committed WAL pages.
            # This is intentionally done only for explicit restore operations.
            safety = create_project_backup(root, reason="before_restore", keep=999999) if target.exists() else None

            destination = sqlite3.connect(target, timeout=10)
            try:
                # Never unlink WAL/SHM sidecars or replace the database inode:
                # an existing connection may still have those files open.
                source.backup(destination)
            finally:
                destination.close()
            return safety or target
        finally:
            source.close()
