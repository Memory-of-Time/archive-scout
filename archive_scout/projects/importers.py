from __future__ import annotations

import sqlite3
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from ..content import parse_page
from ..database.repositories import get_or_create_target, upsert_document
from ..events import ProgressEvent, Stopped
from ..utils import atomic_write_bytes, hash_text, normalize_search, utc_now


def import_text_folder(
    root: Path,
    source_folder: Path,
    database: sqlite3.Connection,
    stop_event,
    callback: Callable[[ProgressEvent], None] | None = None,
) -> int:
    files = sorted(path for path in source_folder.rglob("*") if path.is_file() and path.suffix.lower() in {".txt", ".html", ".htm"})
    target_id = get_or_create_target(database, f"local-import:{source_folder}")
    imported = 0
    for index, path in enumerate(files, 1):
        if stop_event.is_set():
            raise Stopped
        data = path.read_bytes()
        raw = data.decode("utf-8", errors="replace")
        original = path.resolve().as_uri()
        timestamp = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).strftime("%Y%m%d%H%M%S")
        stable_id = hashlib.sha256((original + "|" + timestamp + "|" + hashlib.sha256(data).hexdigest()).encode("utf-8", "replace")).hexdigest()[:24]
        destination = root / "captures" / "imported" / f"{stable_id}.txt"
        # Local-import file I/O occurs only during explicit import, never in
        # the Wayback download hot path. Avoid rewriting already identical files.
        if not destination.exists() or destination.read_bytes() != data:
            atomic_write_bytes(destination, data)
        now = utc_now()
        cursor = database.execute(
            """
            INSERT OR IGNORE INTO captures(
                original_url,timestamp,target_id,query_signature,mimetype,state,created_at,updated_at
            ) VALUES(?,?,?,?,?,'downloaded',?,?)
            """,
            (original, timestamp, target_id, "local-import", "text/plain", now, now),
        )
        row = database.execute(
            "SELECT id FROM captures WHERE original_url=? AND timestamp=? AND query_signature='local-import'",
            (original, timestamp),
        ).fetchone()
        capture_id = int(row["id"])
        title, visible, links = parse_page(raw, original)
        upsert_document(
            database,
            capture_id,
            destination,
            title,
            visible,
            links,
            hashlib.sha256(data).hexdigest(),
            hash_text(normalize_search(visible)),
            len(data),
        )
        imported += int(cursor.rowcount > 0)
        if callback:
            callback(ProgressEvent("import", f"Imported {index:,}/{len(files):,}", index, len(files)))
    return imported
