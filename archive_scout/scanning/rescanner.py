from __future__ import annotations

import concurrent.futures
import hashlib
import json
import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Callable, Iterator

from ..content import decode_bytes, parse_page
from ..database.repositories import record_error, resolve_errors, save_match, upsert_document
from ..events import ProgressEvent, Stopped
from ..document_store import decompress_text, document_links
from ..utils import hash_text
from .jobs import ScanJob
from .workers import scanner_workers
from .batches import BoundedResultWriter
from .executor import ScanExecutor, ScanByteBudget
from .scoring import analyze_content, prepare_analysis_fields


def _analyze_saved_document(row: dict[str, object], jobs: list[ScanJob], report_config=None) -> dict[str, object]:
    if str(row.get("payload_availability") or "") == "discarded":
        return {"kind": "discarded", "row": row}
    path = Path(str(row["path"]))
    if not path.exists():
        return {"kind": "missing", "row": row, "path": path}
    try:
        data = path.read_bytes()
        content_hash = hashlib.sha256(data).hexdigest()
        size_bytes = len(data)
        document_changed = content_hash != str(row.get("content_hash") or "")
        content_type = str(row.get("mimetype") or "")
        if row.get("detected_encoding"):
            content_type = content_type.split(';', 1)[0] + "; charset=" + str(row["detected_encoding"])
        raw = decode_bytes(data, content_type)
        # Avoid retaining both raw bytes and decoded text during the expensive
        # parse/normalization/scoring phase. Hashing the in-memory bytes also
        # removes the old second full disk read of every rescanned capture.
        del data
        if not document_changed:
            try:
                title = str(row.get("title") or "")
                visible = str(row.get("body_text") or "") or decompress_text(row.get("body_zlib"))
                if not visible:
                    # Current projects store the body in the capture, not in
                    # SQLite. Reuse the bytes already decoded above instead of
                    # document_body reopening and decoding the entire file.
                    _parsed_title, visible, _parsed_links = parse_page(raw, str(row["original_url"]))
                links = document_links(row)
            except (TypeError, ValueError, json.JSONDecodeError):
                document_changed = True
        if document_changed:
            title, visible, links = parse_page(raw, str(row["original_url"]))
        prepared_fields, prepared_normalized_fields = prepare_analysis_fields(
            str(row["original_url"]), title, visible, raw, links
        )
        analyses = [
            (
                job.scan_run_id,
                analyze_content(
                    str(row["original_url"]), title, visible, raw, links, job.patterns, job.prefilter,
                    prepared_fields, prepared_normalized_fields,
                    include_hit_fields=(report_config is None or report_config.store_keyword_fields),
                    include_snippets=(report_config is None or report_config.store_snippets),
                    include_interesting_links=(report_config is None or report_config.store_interesting_links),
                ),
            )
            for job in jobs
        ]
        return {
            "kind": "success",
            "row": row,
            "path": path,
            "title": title,
            "visible": visible,
            "links": links,
            "content_hash": content_hash,
            "normalized_hash": (
                hash_text(prepared_normalized_fields["body"])
                if document_changed
                else str(row.get("normalized_hash") or hash_text(prepared_normalized_fields["body"]))
            ),
            "size_bytes": size_bytes,
            "document_changed": document_changed,
            "analyses": analyses,
        }
    except Exception as exc:
        return {"kind": "error", "row": row, "error": repr(exc)}


def _document_rows(
    database: sqlite3.Connection,
    clauses: list[str],
    params: list[object],
    batch_size: int = 1000,
) -> Iterator[dict[str, object]]:
    """Stream both processed documents and retained download-only captures."""
    last_id = 0
    while True:
        page_clauses = [*clauses, "d.id>?"]
        batch = database.execute(
            """
            SELECT d.*,c.original_url,c.mimetype,c.detected_encoding,c.id AS capture_id,
                   c.payload_availability,c.local_path AS capture_local_path
            FROM documents d JOIN captures c ON c.id=d.capture_id
            WHERE """ + " AND ".join(page_clauses) + " ORDER BY d.id LIMIT ?",
            [*params, last_id, max(1, int(batch_size))],
        ).fetchall()
        if not batch:
            break
        for row in batch:
            last_id = int(row["id"])
            yield dict(row)

    # Download-only deliberately has no document rows. The capture manifest is
    # still a complete local-text inventory, so make those saved bodies eligible
    # for the first local scan without any Wayback request.
    if clauses:
        return
    last_capture_id = 0
    while True:
        batch = database.execute(
            """SELECT c.id AS capture_id,c.original_url,c.mimetype,c.detected_encoding,c.local_path AS path,
                      c.payload_availability,'' AS title,'' AS body_text,NULL AS body_zlib,'[]' AS links_json,
                      COALESCE(c.content_hash,'') AS content_hash,'' AS normalized_hash,0 AS size_bytes,
                      0 AS id
               FROM captures c
               WHERE c.id>? AND c.document_id IS NULL
                 AND c.resource_class IN ('text','unknown')
                 AND c.payload_availability IN ('retained','retained_unscanned','spooled_unscanned','cleanup_pending')
                 AND COALESCE(c.local_path,'')<>''
               ORDER BY c.id LIMIT ?""",
            (last_capture_id, max(1, int(batch_size))),
        ).fetchall()
        if not batch:
            return
        for row in batch:
            last_capture_id = int(row["capture_id"])
            yield dict(row)


def rescan_keyword_sets(
    database: sqlite3.Connection,
    jobs: list[ScanJob],
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None = None,
    document_ids: list[int] | None = None,
    workers: int | None = None,
    report_config=None,
    *, scan_backend="auto", scan_memory_mb=256,
) -> None:
    if not jobs or any(not job.patterns for job in jobs):
        raise ValueError("at least one keyword rule is required in every selected keyword set")
    clauses: list[str] = []
    params: list[object] = []
    database.execute("DROP TABLE IF EXISTS temp.archive_scout_document_selection")
    if document_ids:
        database.execute(
            "CREATE TEMP TABLE archive_scout_document_selection(id INTEGER PRIMARY KEY) WITHOUT ROWID"
        )
        database.executemany(
            "INSERT OR IGNORE INTO archive_scout_document_selection(id) VALUES(?)",
            ((int(value),) for value in document_ids),
        )
        clauses.append(
            "EXISTS (SELECT 1 FROM archive_scout_document_selection s WHERE s.id=d.id)"
        )
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    total = int(
        database.execute(
            "SELECT COUNT(*) FROM documents d JOIN captures c ON c.id=d.capture_id" + where, params,
        ).fetchone()[0]
    )
    if not document_ids:
        total += int(database.execute(
            """SELECT COUNT(*) FROM captures c WHERE c.document_id IS NULL
               AND c.resource_class IN ('text','unknown')
               AND c.payload_availability IN ('retained','retained_unscanned','spooled_unscanned','cleanup_pending')
               AND COALESCE(c.local_path,'')<>''"""
        ).fetchone()[0])
    if not total:
        if callback:
            callback(ProgressEvent("rescan", "No saved documents to rescan.", 0, 0))
        return

    worker_count = scanner_workers(workers)
    max_inflight = max(worker_count, worker_count * 3)
    rows = _document_rows(database, clauses, params)
    completed = 0
    budget = ScanByteBudget(scan_memory_mb)
    reservations = {}
    deferred_row = None
    last_emit = 0.0

    def persist_results(results: list[dict[str, object]]) -> None:
        nonlocal completed, last_emit
        with database:
            for result in results:
                row = result["row"]
                assert isinstance(row, dict)
                capture_id = int(row["capture_id"])
                document_id = int(row.get("id") or 0)
                kind = str(result["kind"])
                if kind == "discarded":
                    # Explicitly unavailable by retention policy; this is
                    # coverage information, not a retryable acquisition error.
                    pass
                elif kind == "missing":
                    path = Path(result["path"])
                    record_error(
                        database,
                        "scan",
                        "missing_local_file",
                        f"saved file is missing: {path}",
                        capture_id=capture_id,
                        document_id=(document_id or None),
                        retryable=True,
                    )
                    database.execute(
                        """UPDATE captures SET state='pending',payload_availability='not_acquired',
                                  local_path=NULL WHERE id=?""", (capture_id,)
                    )
                elif kind == "error":
                    record_error(
                        database,
                        "scan",
                        "scan_failure",
                        str(result["error"]),
                        capture_id=capture_id,
                        document_id=document_id,
                        retryable=True,
                    )
                else:
                    if document_id == 0 or bool(result.get("document_changed")):
                        saved_document_id = upsert_document(
                            database,
                            capture_id,
                            Path(result["path"]),
                            str(result["title"]),
                            str(result["visible"]),
                            list(result["links"]),
                            str(result["content_hash"]),
                            str(result["normalized_hash"]),
                            int(result["size_bytes"]),
                        )
                    else:
                        saved_document_id = document_id
                    for scan_run_id, analysis in result["analyses"]:
                        save_match(database, int(scan_run_id), saved_document_id, analysis, report_config)
                    resolve_errors(
                        database,
                        capture_id=capture_id,
                        document_id=saved_document_id,
                        operations=("scan", "parse"),
                    )

        completed += len(results)
        now = time.monotonic()
        if callback and (completed >= total or now - last_emit >= 0.5):
            last_emit = now
            callback(ProgressEvent(
                "rescan", f"Rescanned {completed:,}/{total:,} against {len(jobs):,} keyword set(s)",
                completed, total, {"workers": worker_count, **pool.metrics_snapshot()},
            ))

    writer = BoundedResultWriter(persist_results)
    futures: dict[concurrent.futures.Future[dict[str, object]], dict[str, object]] = {}
    try:
        with ScanExecutor(worker_count, jobs, report=report_config, total=total, backend=scan_backend) as pool:
            def submit_available() -> None:
                nonlocal deferred_row
                while len(futures) < max_inflight:
                    if stop_event.is_set():
                        raise Stopped
                    try:
                        row = deferred_row if deferred_row is not None else next(rows)
                        deferred_row = None
                    except StopIteration:
                        return
                    size = budget.estimate(row)
                    if not budget.accepts(size):
                        deferred_row = row
                        return
                    budget.reserve(size)
                    reservations[int(row["capture_id"])] = size
                    futures[pool.submit(_analyze_saved_document, row, jobs, report_config)] = row

            submit_available()
            while futures:
                if stop_event.is_set():
                    raise Stopped
                done, _pending = concurrent.futures.wait(
                    futures, timeout=0.05, return_when=concurrent.futures.FIRST_COMPLETED
                )
                for future in done:
                    row = futures.pop(future)
                    budget.release(reservations.pop(int(row["capture_id"])))
                    writer.add(future.result())
                writer.flush_if_due()
                submit_available()
            writer.flush()
    except BaseException:
        for pending in futures:
            pending.cancel()
        writer.flush()
        raise


def rescan_documents(
    database: sqlite3.Connection,
    scan_run_id: int,
    keywords: list[str],
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None = None,
    document_ids: list[int] | None = None,
    workers: int | None = None,
    report_config=None,
    *, scan_backend="auto", scan_memory_mb=256,
) -> None:
    rescan_keyword_sets(
        database,
        [ScanJob.create(scan_run_id, "Current keywords", keywords)],
        stop_event,
        callback,
        document_ids,
        workers,
        report_config,
        scan_backend=scan_backend, scan_memory_mb=scan_memory_mb,
    )
