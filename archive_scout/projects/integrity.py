from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Callable

from ..events import ProgressEvent
from ..utils import atomic_write_text, utc_now


def check_project_integrity(
    root: Path,
    database: sqlite3.Connection,
    callback: Callable[[ProgressEvent], None] | None = None,
) -> Path:
    """Check database links and manifested payload availability without redefining intentional lifecycle states."""
    root = Path(root)
    issues: list[str] = []
    referenced: set[Path] = set()

    captures = database.execute(
        """SELECT c.id,c.original_url,c.state,c.document_id,c.local_path,c.payload_availability,c.cleanup_pending,
                  c.bytes_saved,d.id AS joined_document_id,d.path AS document_path,d.size_bytes
           FROM captures c LEFT JOIN documents d ON d.id=c.document_id ORDER BY c.id"""
    ).fetchall()
    total = len(captures)
    for index, row in enumerate(captures, 1):
        availability = str(row["payload_availability"] or "not_acquired")
        cleanup_pending = int(row["cleanup_pending"] or 0)
        local_value = str(row["local_path"] or row["document_path"] or "").strip()
        path = Path(local_value) if local_value else None
        if path is not None:
            try:
                referenced.add(path.resolve())
            except OSError:
                referenced.add(path.absolute())

        # Intentional discard is a valid state: the missing raw payload must not
        # be reported as filesystem corruption. Cleanup-pending likewise has its
        # own idempotent recovery contract.
        if availability not in {"discarded"} and not cleanup_pending:
            # Older projects can have a downloaded state/local path while the newer
            # payload_availability field still has its migration default. Infer the
            # manifest expectation from that durable state rather than silently
            # treating the path as irrelevant.
            expects_complete = availability in {"retained", "retained_unscanned", "spooled_unscanned"} or (
                availability == "not_acquired" and bool(local_value) and str(row["state"] or "") in {"downloaded", "downloaded_unscanned", "scanning"}
            )
            if expects_complete:
                if path is None or not path.exists():
                    issues.append(f"MISSING_FILE\tcapture={row['id']}\t{row['original_url']}\t{path or ''}")
                elif not path.is_file():
                    issues.append(f"NOT_A_FILE\tcapture={row['id']}\t{path}")
                elif path.stat().st_size == 0 and int(row["bytes_saved"] or row["size_bytes"] or 0) > 0:
                    issues.append(f"EMPTY_RETAINED_PAYLOAD\tcapture={row['id']}\t{row['original_url']}\t{path}")
                else:
                    expected = int(row["bytes_saved"] or row["size_bytes"] or 0)
                    if expected and path.stat().st_size != expected:
                        issues.append(
                            f"SIZE_MISMATCH\tcapture={row['id']}\tdatabase={expected}\tdisk={path.stat().st_size}\t{path}"
                        )
            elif availability == "partial" and path is None:
                issues.append(f"MISSING_PARTIAL_PATH\tcapture={row['id']}\t{row['original_url']}")

        if row["document_id"] is not None and row["joined_document_id"] is None:
            issues.append(
                f"BROKEN_DATABASE_LINK\tcapture={row['id']}\tstate={row['state']}\tdocument={row['document_id']}\t{row['original_url']}"
            )
        if callback and (index % 250 == 0 or index == total):
            callback(ProgressEvent("integrity", f"Checked {index:,}/{total:,} capture manifests", index, total))

    # Documents can exist with capture.document_id unset in older projects. They
    # still constitute a legitimate reference and should prevent orphan reports.
    for row in database.execute("SELECT path FROM documents WHERE COALESCE(path,'')<>''"):
        path = Path(str(row[0]))
        try:
            referenced.add(path.resolve())
        except OSError:
            referenced.add(path.absolute())
    for row in database.execute("SELECT path FROM media_captures WHERE COALESCE(path,'')<>''"):
        path = Path(str(row[0]))
        try:
            referenced.add(path.resolve())
        except OSError:
            referenced.add(path.absolute())

    capture_root = root / "captures"
    if capture_root.exists():
        for path in capture_root.rglob("*"):
            if path.is_file() and path.suffix != ".part":
                try:
                    resolved = path.resolve()
                except OSError:
                    resolved = path.absolute()
                if resolved not in referenced:
                    issues.append(f"ORPHAN_FILE\t{path}")

    unresolved = database.execute(
        """SELECT operation,category,COUNT(*) AS count FROM errors
           WHERE resolved=0 AND ignored=0 GROUP BY operation,category ORDER BY operation,category"""
    ).fetchall()
    documents_total = int(database.execute("SELECT COUNT(*) FROM documents").fetchone()[0])
    lines = [
        "Archive Scout project integrity report",
        f"Generated: {utc_now()}",
        f"Project: {root}",
        f"Capture manifests checked: {total:,}",
        f"Documents recorded: {documents_total:,}",
        f"Issues found: {len(issues):,}",
        "",
        "UNRESOLVED, UNIGNORED ERROR COUNTS",
    ]
    lines.extend(f"{row['operation']}\t{row['category']}\t{row['count']}" for row in unresolved)
    lines.extend(["", "INTEGRITY ISSUES"])
    lines.extend(issues or ["None"])
    path = root / "reports" / "integrity.txt"
    atomic_write_text(path, "\n".join(lines) + "\n")
    return path
