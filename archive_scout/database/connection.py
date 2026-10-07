from __future__ import annotations

import errno
import os
import sqlite3
from pathlib import Path

from ..constants import SCHEMA_VERSION
from .schema import initialize_schema

DATABASE_NAME = "archive_scout.sqlite3"


def _windows_pid_is_alive(pid: int) -> bool:
    """Check a Windows PID without sending it a signal.

    ``os.kill(pid, 0)`` is a safe existence probe on POSIX, but not on Windows:
    Python implements non-console signals there with ``TerminateProcess``.  Use
    a read-only process handle instead so checking project ownership can never
    terminate the process that owns the project.
    """
    import ctypes
    from ctypes import wintypes

    process_query_limited_information = 0x1000
    still_active = 259
    error_access_denied = 5

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    open_process = kernel32.OpenProcess
    open_process.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
    open_process.restype = wintypes.HANDLE
    get_exit_code = kernel32.GetExitCodeProcess
    get_exit_code.argtypes = (wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD))
    get_exit_code.restype = wintypes.BOOL
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = (wintypes.HANDLE,)
    close_handle.restype = wintypes.BOOL

    ctypes.set_last_error(0)
    handle = open_process(process_query_limited_information, False, int(pid))
    if not handle:
        # Access denied means a protected process exists but cannot be queried;
        # fail closed and treat it as live rather than stealing its project.
        return ctypes.get_last_error() == error_access_denied
    try:
        exit_code = wintypes.DWORD()
        if not get_exit_code(handle, ctypes.byref(exit_code)):
            # If Windows granted the handle but the state cannot be queried,
            # conservatively preserve ownership instead of declaring it stale.
            return True
        return int(exit_code.value) == still_active
    finally:
        close_handle(handle)


def _pid_is_alive(pid: int | None) -> bool:
    """Best-effort cross-platform liveness check for operation ownership."""
    try:
        value = int(pid or 0)
    except (TypeError, ValueError):
        return False
    if value <= 0:
        return False
    if value == os.getpid():
        return True
    if os.name == "nt":
        return _windows_pid_is_alive(value)
    try:
        os.kill(value, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError as exc:
        if getattr(exc, "errno", None) == errno.ESRCH:
            return False
        return True
    return True



def live_project_writer_pids(root: Path) -> list[int]:
    """Return live foreign/current operation owners without mutating project state."""
    path = Path(root).expanduser().resolve() / DATABASE_NAME
    if not path.exists():
        return []
    db = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=0.25)
    db.row_factory = sqlite3.Row
    try:
        rows = db.execute(
            "SELECT DISTINCT process_id FROM operation_runs WHERE status IN ('running','waiting_archive') AND process_id IS NOT NULL"
        ).fetchall()
        return sorted({int(row[0]) for row in rows if _pid_is_alive(int(row[0]))})
    except sqlite3.DatabaseError:
        return []
    finally:
        db.close()

def database_version(path: Path) -> int | None:
    if not path.exists():
        return None
    database: sqlite3.Connection | None = None
    try:
        database = sqlite3.connect(path)
        row = database.execute("SELECT version FROM schema_info LIMIT 1").fetchone()
        return int(row[0]) if row else None
    except Exception:
        return None
    finally:
        if database is not None:
            database.close()


def is_modern_database(path: Path) -> bool:
    return database_version(path) in {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, SCHEMA_VERSION}


def open_database_readonly(root: Path, *, timeout: float = 0.5) -> sqlite3.Connection:
    """Open an existing project for bounded, query-only GUI reads.

    Unlike :func:`open_database`, this helper never creates folders, migrates
    schemas, recovers stale worker state, checkpoints WAL, or claims writer
    ownership.  It is therefore safe for background views while another
    Archive Scout process owns the project writer.
    """
    root = Path(root).expanduser().resolve()
    path = root / DATABASE_NAME
    if not path.exists():
        raise FileNotFoundError(f"Archive Scout database does not exist: {path}")
    uri = path.as_uri() + "?mode=ro"
    database = sqlite3.connect(uri, uri=True, timeout=max(0.05, float(timeout)))
    try:
        database.row_factory = sqlite3.Row
        database.execute("PRAGMA query_only=ON")
        database.execute(f"PRAGMA busy_timeout={max(50, int(float(timeout) * 1000))}")
        row = database.execute("SELECT version FROM schema_info LIMIT 1").fetchone()
        version = int(row[0]) if row else None
        if version is None:
            raise RuntimeError("Project database does not contain Archive Scout schema metadata")
        if version > SCHEMA_VERSION:
            raise RuntimeError(
                f"Project schema {version} is newer than supported schema {SCHEMA_VERSION}; update Archive Scout before opening it"
            )
        if version != SCHEMA_VERSION:
            raise RuntimeError(
                f"Project schema {version} requires migration before it can be viewed; open the project for a normal operation first"
            )
        return database
    except Exception:
        database.close()
        raise


def open_database(root: Path, migrate: bool = True) -> sqlite3.Connection:
    root.mkdir(parents=True, exist_ok=True)
    path = root / DATABASE_NAME
    version = database_version(path) if path.exists() else None
    if version is not None and version > SCHEMA_VERSION:
        raise RuntimeError(f"Project schema {version} is newer than supported schema {SCHEMA_VERSION}; update Archive Scout before opening it")
    if migrate and path.exists() and version not in {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, SCHEMA_VERSION}:
        from ..projects.migration import migrate_legacy_project
        migrate_legacy_project(root)
        version = database_version(path)
    if migrate and version in {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}:
        try:
            from ..projects.backups import create_project_backup
            create_project_backup(root, reason=f"before_schema_{SCHEMA_VERSION}", keep=5)
        except Exception as exc:
            raise RuntimeError("Could not back up the project before migration; no schema changes were made") from exc
    database = sqlite3.connect(path, timeout=60)
    try:
        database.row_factory = sqlite3.Row
        database.execute("PRAGMA journal_mode=WAL")
        database.execute("PRAGMA synchronous=NORMAL")
        database.execute("PRAGMA foreign_keys=ON")
        database.execute("PRAGMA temp_store=MEMORY")
        database.execute("PRAGMA cache_size=-65536")
        # Bound mapped working sets on long runs while retaining the 64 MiB page
        # cache that protects write-heavy scans from unnecessary disk churn.
        database.execute("PRAGMA mmap_size=67108864")
        database.execute("PRAGMA wal_autocheckpoint=10000")
        database.execute("PRAGMA journal_size_limit=67108864")
        database.execute("PRAGMA busy_timeout=60000")
        initialize_schema(database)
        # Recover only operations whose owning process is actually gone. A GUI,
        # CLI/bot, or second process must never silently steal an active project.
        current_pid = os.getpid()
        running_rows = database.execute(
            "SELECT id,process_id FROM operation_runs WHERE status='running' ORDER BY id"
        ).fetchall()
        live_foreign = [
            int(row["process_id"]) for row in running_rows
            if row["process_id"] is not None
            and int(row["process_id"]) != current_pid
            and _pid_is_alive(int(row["process_id"]))
        ]
        if live_foreign:
            owners = ", ".join(str(value) for value in sorted(set(live_foreign)))
            raise RuntimeError(
                f"This Archive Scout project is already active in process {owners}. "
                "Close that operation before opening the project for write access."
            )

        active_current_process = any(
            row["process_id"] is not None and int(row["process_id"]) == current_pid
            for row in running_rows
        )
        if not active_current_process:
            database.execute("UPDATE captures SET state='pending' WHERE state='downloading'")
            database.execute("UPDATE captures SET state='downloaded_unscanned' WHERE state='scanning'")
            database.execute("UPDATE media_captures SET state='pending' WHERE state='downloading'")
            database.execute("UPDATE scan_runs SET status='interrupted' WHERE status='running'")
            database.execute("UPDATE quick_search_runs SET status='interrupted',updated_at=datetime('now') WHERE status='running'")

        stale_ids = [
            int(row["id"]) for row in running_rows
            if row["process_id"] is None
            or (int(row["process_id"]) != current_pid and not _pid_is_alive(int(row["process_id"])))
        ]
        if stale_ids:
            placeholders = ",".join("?" for _ in stale_ids)
            database.execute(
                f"""UPDATE operation_runs
                    SET status='interrupted',completed_at=datetime('now'),updated_at=datetime('now'),
                        message=COALESCE(message,'Recovered after an unclean shutdown')
                    WHERE id IN ({placeholders})""",
                stale_ids,
            )
        database.commit()
        return database
    except Exception:
        database.close()
        raise
