"""Bounded, restartable, purely local scanning of retained archive bodies."""
from __future__ import annotations

import concurrent.futures
import hashlib
import os
import sqlite3
import threading
from collections import deque
from pathlib import Path
from typing import Callable, Iterator

from ..cdx.parameters import cdx_query_signature
from ..content import decode_bytes, parse_page
from ..database.repositories import record_error, resolve_errors, save_match, upsert_document
from ..events import ProgressEvent, Stopped
from ..utils import hash_text, utc_now
from .jobs import ScanJob
from .scoring import analyze_content, prepare_analysis_fields

SCAN_BYTE_BUDGET = 64 * 1024 * 1024


def _scan_one(row: dict, jobs: list[ScanJob]) -> dict:
    path = Path(row["path"])
    data = path.read_bytes()
    raw = decode_bytes(data, row.get("mimetype") or "")
    title, visible, links = parse_page(raw, row["original_url"])
    fields, normalized = prepare_analysis_fields(row["original_url"], title, visible, raw, links)
    analyses = {
        job.scan_run_id: analyze_content(
            row["original_url"], title, visible, raw, links,
            job.patterns, job.prefilter, fields, normalized,
        )
        for job in jobs
    }
    return {
        "capture_id": row["id"], "path": path, "title": title, "visible": visible,
        "links": links, "bytes_saved": len(data),
        "content_hash": hashlib.sha256(data).hexdigest(),
        "normalized_hash": hash_text(normalized["body"]), "analyses": analyses,
    }


def _pending_rows(database: sqlite3.Connection, config) -> Iterator[dict]:
    """Keyset scan over the active inventory; cursor memory is bounded."""
    signature = cdx_query_signature(config)
    targets = list(config.targets)
    if not targets:
        return
    cursor_id = 0
    target_slots = ",".join("?" for _ in targets)
    while True:
        batch = database.execute(
            """SELECT c.id,c.timestamp,c.original_url,c.mimetype,c.length,d.path
               FROM captures c JOIN targets t ON t.id=c.target_id
               LEFT JOIN documents d ON d.capture_id=c.id
               WHERE (c.state='downloaded_unscanned' OR
                     (c.state='downloaded' AND d.id IS NULL))
               AND c.query_signature=? AND t.pattern IN (""" + target_slots + """)
               AND c.id>? ORDER BY c.id LIMIT 500""",
            (signature, *targets, cursor_id),
        ).fetchall()
        if not batch:
            return
        for row in batch:
            entry = dict(row)
            cursor_id = entry["id"]
            if not entry.get("path"):
                from ..downloads.downloader import capture_path
                entry["path"] = str(capture_path(config.output_dir, entry["id"], entry["timestamp"], entry["original_url"]))
            yield entry


def scan_pending_saved(
    database: sqlite3.Connection,
    config,
    jobs: list[ScanJob],
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None = None,
    workers: int | None = None,
) -> int:
    """Scan after acquisition; unaffected by Wayback's request admission clock.

    Bounded futures and approximate in-flight bytes. Exceptionally large files
    are still accepted one at a time rather than truncated or discarded.
    """
    signature = cdx_query_signature(config)
    targets = list(config.targets)
    if not targets:
        return 0
    target_slots = ",".join("?" for _ in targets)
    count = int(database.execute(
        """SELECT COUNT(*) FROM captures c JOIN targets t ON t.id=c.target_id
           LEFT JOIN documents d ON d.capture_id=c.id
           WHERE (c.state='downloaded_unscanned' OR (c.state='downloaded' AND d.id IS NULL))
             AND c.query_signature=? AND t.pattern IN (""" + target_slots + ")",
        (signature, *targets),
    ).fetchone()[0])
    if not count:
        return 0
    worker_count = max(1, min(32, int(workers or min(8, os.cpu_count() or 4))))
    max_inflight = worker_count * 2
    estimated_budget = SCAN_BYTE_BUDGET
    rows = iter(_pending_rows(database, config))
    next_row = next(rows, None)
    completed = 0

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=worker_count, thread_name_prefix="archive-local-scan"
    ) as pool:
        futures: dict[concurrent.futures.Future, tuple[dict, int]] = {}
        in_flight_bytes = 0
        while futures or next_row is not None:
            if stop_event.is_set():
                for future in futures:
                    future.cancel()
                raise Stopped
            while next_row is not None and len(futures) < max_inflight:
                # File bytes are already local; archive CDX lengths can be
                # missing, compressed, or inaccurate. Use actual disk size and
                # reserve decoding/parsing overhead inside the memory budget.
                try:
                    length = Path(next_row["path"]).stat().st_size
                except OSError:
                    length = min(config.max_file_bytes, 8 * 1024 * 1024)
                estimate = max(1, length) * 3
                if futures and in_flight_bytes + estimate > estimated_budget:
                    break
                row = next_row
                # Oversized single bodies are processed without dropping evidence.
                futures[pool.submit(_scan_one, row, jobs)] = (row, estimate)
                in_flight_bytes += estimate
                next_row = next(rows, None)
            if not futures:
                continue
            done, _ = concurrent.futures.wait(
                futures, timeout=0.5, return_when=concurrent.futures.FIRST_COMPLETED,
            )
            if not done:
                continue
            with database:
                for future in done:
                    row, estimate = futures.pop(future)
                    in_flight_bytes -= estimate
                    # A scan and every match row are one atomic unit.  A failed
                    # save_match must never commit upsert_document's state change,
                    # which would otherwise strand a downloaded body as scanned.
                    database.execute("SAVEPOINT scout_capture_scan")
                    try:
                        result = future.result()
                        document_id = upsert_document(
                            database, result["capture_id"], result["path"], result["title"],
                            result["visible"], result["links"], result["content_hash"],
                            result["normalized_hash"], result["bytes_saved"],
                        )
                        for run_id, analysis in result["analyses"].items():
                            save_match(database, int(run_id), document_id, analysis)
                        database.execute(
                            "UPDATE captures SET state='downloaded',updated_at=? WHERE id=?",
                            (utc_now(), result["capture_id"]),
                        )
                        database.execute(
                            "UPDATE capture_routing SET routing='downloaded_scanned',updated_at=? WHERE capture_id=?",
                            (utc_now(), result["capture_id"]),
                        )
                        resolve_errors(database, capture_id=result["capture_id"], document_id=document_id)
                    except Exception as exc:
                        database.execute("ROLLBACK TO SAVEPOINT scout_capture_scan")
                        # Missing retained bytes need reacquisition; a transient
                        # SQLite or parser failure does NOT need network access.
                        missing = isinstance(exc, FileNotFoundError)
                        if missing:
                            database.execute(
                                "UPDATE captures SET state='error',updated_at=? WHERE id=?",
                                (utc_now(), row["id"]),
                            )
                        record_error(
                            database, "scan", "missing_local_file" if missing else "scan_failure",
                            repr(exc), capture_id=row["id"], retryable=True,
                        )
                    finally:
                        database.execute("RELEASE SAVEPOINT scout_capture_scan")
                    completed += 1
            if callback and (completed % 100 == 0 or completed == count):
                callback(ProgressEvent(
                    "scan", f"Locally scanned {completed:,}/{count:,} saved bodies",
                    completed, count,
                ))
    return completed
