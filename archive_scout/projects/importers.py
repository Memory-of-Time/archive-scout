from __future__ import annotations

import sqlite3
import hashlib
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from ..content import decode_bytes, looks_textual_bytes, parse_text_representation
from ..database.repositories import get_or_create_target, upsert_document
from ..classification import path_extension
from ..events import ProgressEvent, Stopped
from ..utils import atomic_write_bytes, hash_text, normalize_search, utc_now


def import_text_folder(
    root: Path,
    source_folder: Path,
    database: sqlite3.Connection,
    stop_event,
    callback: Callable[[ProgressEvent], None] | None = None,
) -> int:
    # Stream traversal by directory; never materialize a project-sized Python
    # list. Preserve all nine historical text suffixes from the scan policy.
    supported = {".txt", ".html", ".htm", ".shtm", ".dhtm", ".xhtm", ".phtm", ".php", ".dat", ".cgi"}
    def iter_files():
        for base, folders, filenames in os.walk(source_folder):
            folders.sort()
            for filename in sorted(filenames):
                file_path = Path(base) / filename
                if file_path.is_file() and path_extension(file_path.resolve().as_uri()) in supported:
                    yield file_path
    files = iter_files()
    target_id = get_or_create_target(database, f"local-import:{source_folder}")
    imported = 0
    for index, path in enumerate(files, 1):
        if stop_event.is_set():
            raise Stopped
        data = path.read_bytes()
        # .dat may be binary even when explicitly imported; do not transcode
        # or certify bytes which do not pass the text-preservation predicate.
        mime = "text/html" if path.suffix.casefold() in {
            ".html", ".htm", ".shtm", ".dhtm", ".xhtm", ".phtm", ".php", ".cgi"
        } else "text/plain"
        if not looks_textual_bytes(data[:16384], mime):
            continue
        raw = decode_bytes(data, mime)
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
            (original, timestamp, target_id, "local-import", mime, now, now),
        )
        row = database.execute(
            "SELECT id FROM captures WHERE original_url=? AND timestamp=? AND query_signature='local-import'",
            (original, timestamp),
        ).fetchone()
        capture_id = int(row["id"])
        title, visible, links = parse_text_representation(raw, original, mime)
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
        if callback and (index == 1 or index % 100 == 0):
            callback(ProgressEvent("import", f"Imported {index:,} inspected local files", index, 0))
    return imported
