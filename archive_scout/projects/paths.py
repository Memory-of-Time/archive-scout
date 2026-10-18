from __future__ import annotations

import sqlite3
from pathlib import Path


MARKER = "scout_current_project_root"


def _old_project_suffix(value: str) -> str | None:
    """Recover a formerly absolute project-owned path after its folder moves.

    Only recognized internal capture/media paths are eligible. Unrelated
    external paths are never rebased or overwritten.
    """
    normalized = value.replace("\\", "/")
    for component in ("captures", "media"):
        pivot = "/" + component + "/"
        pos = normalized.find(pivot)
        if pos >= 0:
            return normalized[pos+1:]
    return None


def _owned_project_suffix(value: str, previous_root: str) -> str | None:
    """Find a stored project file despite aliases in the old absolute root.

    pathlib can resolve /var vs /private/var on macOS and 8.3 vs long-path
    spellings on Windows, even after the final project directory was moved.
    Only project-owned captures/ and media/ paths are eligible for rebasing.
    """
    suffix = _old_project_suffix(value)
    if not suffix:
        return None
    try:
        source = Path(value).expanduser().resolve(strict=False)
        origin = Path(previous_root).expanduser().resolve(strict=False)
        relative = source.relative_to(origin).as_posix()
        if relative.startswith(("captures/", "media/")):
            return relative
    except (OSError, ValueError, RuntimeError):
        pass

    # Alias resolution can fail after a move if the *project* path no longer
    # exists. Require several identical trailing directory components before
    # the storage folder; this must never rewrite an unrelated external file.
    normalized = value.replace("\\", "/")
    prefix = normalized[: -(len(suffix) + 1)]
    stored_parts = [part.casefold() for part in prefix.split("/") if part]
    old_parts = [part.casefold() for part in previous_root.replace("\\", "/").split("/") if part]
    if len(stored_parts) >= 3 and len(old_parts) >= 3 and stored_parts[-3:] == old_parts[-3:]:
        return suffix
    return None


def rebase_project_files(root: Path, database: sqlite3.Connection) -> int:
    """One-time DB maintenance when a project is relocated, never per capture."""
    root = root.expanduser().resolve()
    previous = database.execute("SELECT value FROM project_meta WHERE key=?", (MARKER,)).fetchone()
    previous_root = str(previous[0]) if previous else None
    if previous_root == str(root):
        return 0
    # A pre-v1.2.1 database has no root marker. Check a small sample first;
    # avoid an O(project size) scan when already in the right directory.
    if previous_root is None:
        samples = database.execute("SELECT path FROM documents WHERE path<>'' LIMIT 12").fetchall()
        samples += database.execute("SELECT path FROM media_captures WHERE path IS NOT NULL AND path<>'' LIMIT 12").fetchall()
        found_move = False
        for row in samples:
            raw = str(row[0])
            suffix = _old_project_suffix(raw)
            if suffix and not Path(raw).is_file() and (root / suffix).is_file():
                found_move = True
                break
        if not found_move:
            database.execute("INSERT OR REPLACE INTO project_meta(key,value) VALUES(?,?)", (MARKER,str(root)))
            return 0
    changed = 0
    for table in ("documents", "media_captures"):
        last_id = 0
        while True:
            rows = database.execute(
                f"SELECT id,path FROM {table} WHERE id>? AND path IS NOT NULL AND path<>'' ORDER BY id LIMIT 1000",
                (last_id,),
            ).fetchall()
            if not rows:
                break
            last_id = int(rows[-1][0])
            updates = []
            for row in rows:
                raw = str(row["path"])
                suffix = (
                    _owned_project_suffix(raw, previous_root)
                    if previous_root else _old_project_suffix(raw)
                )
                if not suffix or not suffix.startswith(("captures/", "media/")):
                    continue
                destination = root / suffix
                # Never discard old references when the corresponding file is
                # absent in the new project. Integrity/repair should report it.
                if destination.is_file():
                    updates.append((str(destination), int(row["id"])))
            if updates:
                database.executemany(f"UPDATE {table} SET path=? WHERE id=?", updates)
                changed += len(updates)
    database.execute("INSERT OR REPLACE INTO project_meta(key,value) VALUES(?,?)", (MARKER,str(root)))
    return changed
