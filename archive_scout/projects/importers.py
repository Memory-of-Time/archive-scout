from __future__ import annotations

import hashlib
import os
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterator

from ..content import decode_bytes_with_encoding, parse_page
from ..database.repositories import get_or_create_target, upsert_document
from ..events import ProgressEvent, Stopped
from ..utils import hash_text, normalize_search, utc_now


_ALLOWED_SUFFIXES = {".txt", ".html", ".htm"}


def _iter_import_files(source_folder: Path) -> Iterator[Path]:
    # Stream directory traversal instead of materializing/sorting a huge tree.
    for folder, _dirs, names in os.walk(source_folder):
        base = Path(folder)
        for name in names:
            path = base / name
            if path.suffix.casefold() in _ALLOWED_SUFFIXES and path.is_file():
                yield path


def _ingest_file(root: Path, source: Path, data: bytes) -> Path:
    destination_dir = root / "captures" / "imported"
    destination_dir.mkdir(parents=True, exist_ok=True)
    identity = hashlib.sha256(str(source.resolve()).encode("utf-8", "surrogatepass")).hexdigest()[:12]
    suffix = source.suffix.casefold() or ".txt"
    destination = destination_dir / f"{identity}_{source.name}"
    if destination.suffix.casefold() not in _ALLOWED_SUFFIXES:
        destination = destination.with_suffix(suffix)
    fd, temp_name = tempfile.mkstemp(prefix=destination.name + ".", suffix=".tmp", dir=destination_dir)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        Path(temp_name).replace(destination)
    except Exception:
        try:
            os.close(fd)
        except OSError:
            pass
        Path(temp_name).unlink(missing_ok=True)
        raise
    return destination


def import_text_folder(
    root: Path,
    source_folder: Path,
    database: sqlite3.Connection,
    stop_event,
    callback: Callable[[ProgressEvent], None] | None = None,
) -> int:
    root = Path(root).resolve()
    source_folder = Path(source_folder).resolve()
    target_id = get_or_create_target(database, f"local-import:{source_folder}")
    imported = 0
    seen = 0
    for path in _iter_import_files(source_folder):
        if stop_event.is_set():
            raise Stopped
        seen += 1
        data = path.read_bytes()
        mimetype = "text/html" if path.suffix.casefold() in {".html", ".htm"} else "text/plain"
        raw, encoding = decode_bytes_with_encoding(data, mimetype)
        destination = _ingest_file(root, path, data)
        original = f"file://{path.resolve()}"
        stat = path.stat()
        local_mtime = datetime.fromtimestamp(stat.st_mtime, timezone.utc)
        # Local imports are explicitly tagged as local provenance. The timestamp
        # remains a valid sortable local-file mtime, never an invalid nanosecond
        # pseudo-Wayback timestamp.
        timestamp = local_mtime.strftime("%Y%m%d%H%M%S")
        now = utc_now()
        classification_reason = f"local_import; encoding={encoding}; mtime={local_mtime.isoformat()}"
        cursor = database.execute(
            """
            INSERT OR IGNORE INTO captures(
                original_url,timestamp,target_id,query_signature,mimetype,state,local_path,bytes_saved,
                content_hash,detected_encoding,resource_class,classification_reason,resource_classifier_revision,
                payload_availability,payload_origin,payload_retention,created_at,updated_at
            ) VALUES(?,?,?,?,?,'downloaded',?,?,?,?,? ,?,1,'retained','local_import','keep',?,?)
            """,
            (
                original, timestamp, target_id, "local-import", mimetype, str(destination), len(data),
                hashlib.sha256(data).hexdigest(), encoding, "text", classification_reason, now, now,
            ),
        )
        row = database.execute(
            "SELECT id FROM captures WHERE original_url=? AND timestamp=? AND query_signature='local-import'",
            (original, timestamp),
        ).fetchone()
        capture_id = int(row["id"])
        # Ensure repeated imports still point at the project-owned payload.
        database.execute(
            """UPDATE captures SET local_path=?,payload_availability='retained',payload_origin='local_import',
                      detected_encoding=?,resource_class='text',classification_reason=?,bytes_saved=?,content_hash=?,updated_at=?
               WHERE id=?""",
            (str(destination), encoding, classification_reason, len(data), hashlib.sha256(data).hexdigest(), now, capture_id),
        )
        title, visible, links = parse_page(raw, original)
        upsert_document(
            database,
            capture_id,
            destination,
            title,
            visible,
            links,
            hash_text(raw),
            hash_text(normalize_search(visible)),
            len(data),
        )
        imported += int(cursor.rowcount > 0)
        if callback and (seen == 1 or seen % 25 == 0):
            callback(ProgressEvent("import", f"Imported {seen:,} local file(s)", seen, None))
    if callback:
        callback(ProgressEvent("import", f"Import complete: {seen:,} file(s) processed", seen, seen))
    return imported
