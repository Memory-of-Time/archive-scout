from __future__ import annotations

from ..database.lease import guard_project
from ..database.connection import live_project_writer_pids

import gzip
import hashlib
import os
import shutil
import sqlite3
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

from ..database.connection import DATABASE_NAME
from ..constants import SCHEMA_VERSION
from ..utils import utc_now
from ..events import ProgressEvent, Stopped


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")


def create_project_backup(root: Path, reason: str = "manual", keep: int = 5, max_mb: float = 1024.0, *, callback=None, stop_event=None) -> Path:
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
    last_emit = 0.0

    def report(stage: str, current: int, total: int) -> None:
        nonlocal last_emit
        if stop_event is not None and stop_event.is_set():
            raise Stopped
        now = time.monotonic()
        if callback and (current >= total or now - last_emit >= 0.5):
            last_emit = now
            callback(ProgressEvent(stage, "Creating project backup", current, total))

    compressed = backup_dir / f"{stem}.sqlite3.gz.tmp"
    try:
        report('backup_copy', 0, 1)
        source_db = sqlite3.connect(source)
        try:
            destination_db = sqlite3.connect(raw)
            try:
                source_db.backup(destination_db, pages=256,
                                 progress=lambda status, remaining, total: report("backup_copy", total - remaining, total))
            finally:
                destination_db.close()
        finally:
            source_db.close()
        # Validate the online snapshot before publishing any restorable filename.
        check = sqlite3.connect(raw)
        try:
            if stop_event is not None:
                check.set_progress_handler(lambda: int(stop_event.is_set()), 1000)
            if check.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                raise RuntimeError("Backup snapshot failed SQLite validation")
        except sqlite3.OperationalError:
            if stop_event is not None and stop_event.is_set():
                raise Stopped from None
            raise
        finally:
            check.close()
        _compress_backup(raw, compressed, callback, report)
        # Reading to EOF verifies gzip CRC/trailer. Match the source digest too.
        original_digest = hashlib.sha256()
        verified, total_verify = 0, raw.stat().st_size * 2
        report('backup_verify', 0, total_verify)
        with raw.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                original_digest.update(chunk)
                verified += len(chunk)
                report('backup_verify', verified, total_verify)
        restored_digest = hashlib.sha256()
        with gzip.open(compressed, "rb") as handle:
            while chunk := handle.read(1024 * 1024):
                restored_digest.update(chunk)
                verified += len(chunk)
                report('backup_verify', verified, total_verify)
        if original_digest.digest() != restored_digest.digest():
            raise RuntimeError("Compressed backup verification failed")
        with compressed.open("rb") as handle:
            os.fsync(handle.fileno())
        os.replace(compressed, destination)
    finally:
        compressed.unlink(missing_ok=True)
        raw.unlink(missing_ok=True)
    _record_backup(root, destination, reason)
    prune_backups(root, keep, max_mb=max_mb)
    return destination


def _compress_backup(raw: Path, destination: Path, callback, report) -> None:
    with raw.open("rb") as src, gzip.open(destination, "wb", compresslevel=6) as dst:
        size = raw.stat().st_size
        current = 0
        report('backup_compress', 0, size)
        while chunk := src.read(1024 * 1024):
            dst.write(chunk)
            current += len(chunk)
            report("backup_compress", current, size)


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
        source = sqlite3.connect(backup_path.resolve().as_uri() + "?mode=ro", uri=True)
        try:
            # Pin one complete committed view so a live WAL source cannot keep
            # restarting the copy as another connection appends newer pages.
            source.execute("BEGIN")
            source.execute("PRAGMA schema_version").fetchone()
            copy = sqlite3.connect(destination)
            try:
                _copy_database(source, copy)
            finally:
                copy.close()
        finally:
            source.close()


def _copy_database(source: sqlite3.Connection, destination: sqlite3.Connection) -> None:
    """Copy under SQLite ownership; bound lock waits without limiting copy size."""
    blocked_since = None

    def progress(status, remaining, total):
        nonlocal blocked_since
        if status in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}:
            now = time.monotonic()
            if blocked_since is None:
                blocked_since = now
            if now - blocked_since >= 10.0:
                raise RuntimeError("Database remained busy; close other writers/read transactions and retry restore")
        else:
            blocked_since = None

    source.backup(destination, pages=256, progress=progress, sleep=0.05)


def _validate_restore_snapshot(path: Path, *, project_schema: bool = True) -> int:
    check = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
    try:
        findings = check.execute("PRAGMA integrity_check").fetchall()
        if findings != [("ok",)]:
            raise RuntimeError(f"backup failed SQLite integrity check: {findings[:3]}")
        if project_schema:
            schema = check.execute("SELECT version FROM schema_info LIMIT 1").fetchone()
            if not schema:
                raise RuntimeError("backup does not contain Archive Scout schema metadata")
            version = int(schema[0])
            if version > SCHEMA_VERSION:
                raise RuntimeError(f"backup schema {version} is newer than supported schema {SCHEMA_VERSION}")
        return int(check.execute("PRAGMA page_size").fetchone()[0])
    finally:
        check.close()


@guard_project
def restore_project_backup(root: Path, backup_path: Path) -> Path:
    root = Path(root).expanduser().resolve()
    if live_project_writer_pids(root):
        raise RuntimeError("Pause the active project writer before restoring a database backup")
    backup_path = Path(backup_path)
    if not backup_path.exists():
        raise FileNotFoundError(backup_path)
    target = root / DATABASE_NAME
    safety = None
    existed = target.exists()
    # An isolated snapshot also includes committed source WAL pages for a raw
    # SQLite selection. Validate it before making an unnecessary safety copy.
    with tempfile.TemporaryDirectory(prefix="archive-scout-restore-", dir=root) as temporary:
        snapshot = Path(temporary) / "snapshot.sqlite3"
        _materialize_backup(backup_path, snapshot)
        page_size = _validate_restore_snapshot(snapshot)
        destination = sqlite3.connect(target, timeout=0.25)
        try:
            destination.execute("PRAGMA mmap_size=0")
            destination.execute("PRAGMA synchronous=FULL")
            if existed:
                safety = root / "backups" / f"archive_scout_{_timestamp()}_before_restore.sqlite3"
                safety.parent.mkdir(parents=True, exist_ok=True)
                safety_temp = Path(temporary) / "safety.sqlite3"
                safety_db = sqlite3.connect(safety_temp)
                try:
                    _copy_database(destination, safety_db)
                finally:
                    safety_db.close()
                _validate_restore_snapshot(safety_temp, project_schema=False)
                with safety_temp.open("rb") as handle:
                    os.fsync(handle.fileno())
                os.replace(safety_temp, safety)
            current_page_size = destination.execute("PRAGMA page_size").fetchone()[0]
            journal = destination.execute("PRAGMA journal_mode").fetchone()[0]
            if current_page_size != page_size and journal == "wal":
                # SQLite cannot change page size in WAL mode. Let SQLite obtain
                # the required exclusive lock, or fail without replacing bytes.
                destination.execute("PRAGMA journal_mode=DELETE")
            source = sqlite3.connect(snapshot.resolve().as_uri() + "?mode=ro", uri=True)
            try:
                _copy_database(source, destination)
            finally:
                source.close()
            if journal == "wal":
                destination.execute("PRAGMA journal_mode=WAL")
        finally:
            destination.close()
    # Do not replace a live inode or unlink WAL/SHM: existing readers may keep
    # their current snapshot and see restored content in their next transaction.
    return safety or target
