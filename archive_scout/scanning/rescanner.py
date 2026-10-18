from __future__ import annotations

import concurrent.futures
import json
import hashlib
import os
import sqlite3
import threading
from pathlib import Path
from typing import Callable, Iterator
from collections import deque

from ..content import decode_bytes, parse_page
from ..database.repositories import record_error, resolve_errors, save_match, upsert_document
from ..events import ProgressEvent, Stopped
from ..utils import hash_text, normalize_search
from .jobs import ScanJob
from .scoring import analyze_content, prepare_analysis_fields

# Bounded local working set, independent of how many archive captures exist.
RESCAN_BYTE_BUDGET = 64 * 1024 * 1024


def _analyze_saved_document(row: dict[str, object], jobs: list[ScanJob]) -> dict[str, object]:
    path = Path(str(row["path"]))
    if not path.exists():
        return {"kind": "missing", "row": row, "path": path}
    try:
        saved_bytes = path.read_bytes()
        content_hash = hashlib.sha256(saved_bytes).hexdigest()
        is_import = str(row.get("query_signature") or "") == "local-import"
        authoritative = str(row.get("expected_hash") or "")
        if not is_import and authoritative and (
            content_hash != authoritative or len(saved_bytes) != int(row.get("expected_size") or 0)
        ):
            return {"kind": "integrity", "row": row,
                    "error": "retained archive body differs from committed acquisition evidence"}
        raw = decode_bytes(saved_bytes, str(row.get("response_content_type") or row.get("mimetype") or ""))
        document_changed = content_hash != str(row.get("content_hash") or "")
        if not document_changed:
            try:
                title = str(row.get("title") or "")
                visible = str(row.get("body_text") or "")
                links_payload = json.loads(str(row.get("links_json") or "[]"))
                links = [str(value) for value in links_payload] if isinstance(links_payload, list) else []
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
                    str(row["original_url"]),
                    title,
                    visible,
                    raw,
                    links,
                    job.patterns,
                    job.prefilter,
                    prepared_fields,
                    prepared_normalized_fields,
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
                hash_text(normalize_search(visible))
                if document_changed
                else str(row.get("normalized_hash") or hash_text(normalize_search(visible)))
            ),
            "size_bytes": path.stat().st_size,
            "document_changed": document_changed,
            "analyses": analyses,
        }
    except Exception as exc:
        return {"kind": "error", "row": row, "error": repr(exc)}


def _document_rows(
    database: sqlite3.Connection,
    clauses: list[str],
    params: list[object],
    batch_size: int = 16,
) -> Iterator[dict[str, object]]:
    last_id = 0
    while True:
        page_clauses = [*clauses, "d.id>?"]
        batch = database.execute(
            """
            SELECT d.*,c.original_url,c.id AS capture_id,c.query_signature,c.mimetype,
                v.content_hash AS expected_hash,v.bytes_saved AS expected_size,
                v.response_content_type AS response_content_type
            FROM documents d
            JOIN captures c ON c.id=d.capture_id
            LEFT JOIN capture_payload_evidence v ON v.capture_id=c.id
            WHERE """ + " AND ".join(page_clauses) + " ORDER BY d.id LIMIT ?",
            [*params, last_id, max(1, int(batch_size))],
        ).fetchall()
        if not batch:
            return
        for row in batch:
            last_id = int(row["id"])
            yield dict(row)


def rescan_keyword_sets(
    database: sqlite3.Connection,
    jobs: list[ScanJob],
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None = None,
    document_ids: list[int] | None = None,
    workers: int | None = None,
    after_document_id: int = 0,
    checkpoint: Callable[[int], None] | None = None,
    missing_only: bool = False,
    scope_pairs: list[tuple[str, str]] | None = None,
    empty_message: str = "No saved documents to rescan.",
) -> None:
    if not jobs or any(not job.patterns for job in jobs):
        raise ValueError("at least one keyword rule is required in every selected keyword set")
    clauses: list[str] = ["d.id>?"] if after_document_id else []
    params: list[object] = [int(after_document_id)] if after_document_id else []
    if missing_only:
        # Reacquisition-pending documents (including those flagged by Repair)
        # do not have validated local bytes. Do not reread and overwrite their
        # pending state during the pre-download local backfill.
        clauses.append("c.state IN ('downloaded','downloaded_unscanned')")
        # A score=0 is still a persisted coverage marker; no need to reload
        # already-scored bodies into Python merely to rediscover no hits.
        placeholders = ",".join("?" for _ in jobs)
        clauses.append(f"(SELECT COUNT(DISTINCT m.scan_run_id) FROM document_matches m WHERE m.document_id=d.id AND m.scan_run_id IN ({placeholders}))<?")
        params.extend(job.scan_run_id for job in jobs)
        params.append(len(jobs))
    if scope_pairs is not None:
        if not scope_pairs:
            return
        clauses.append("EXISTS (SELECT 1 FROM targets t WHERE t.id=c.target_id AND (" +
                       " OR ".join("(t.pattern=? AND c.query_signature=?)" for _ in scope_pairs) + "))")
        for pair in scope_pairs:
            params.extend(pair)
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
            "SELECT COUNT(*) FROM documents d JOIN captures c ON c.id=d.capture_id" + where,
            params,
        ).fetchone()[0]
    )
    if not total:
        if callback:
            callback(ProgressEvent("rescan", empty_message, 0, 0))
        return

    worker_count = max(1, min(32, int(workers or min(8, os.cpu_count() or 4))))
    max_inflight = max(worker_count, worker_count * 3)
    rows = _document_rows(database, clauses, params)
    completed = 0
    submitted_order: deque[int] = deque()
    finished_ids: set[int] = set()
    in_flight_bytes = 0
    deferred_row: dict[str, object] | None = None

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=worker_count, thread_name_prefix="archive-rescan"
    ) as pool:
        futures: dict[concurrent.futures.Future[dict[str, object]], tuple[dict[str, object], int]] = {}

        def submit_available() -> None:
            nonlocal deferred_row, in_flight_bytes
            while len(futures) < max_inflight:
                if stop_event.is_set():
                    raise Stopped
                if deferred_row is not None:
                    row = deferred_row
                    deferred_row = None
                else:
                    try:
                        row = next(rows)
                    except StopIteration:
                        return
                # Real byte size, not stale CDX metadata; allow one oversized
                # document at a time without silently excluding it.
                try:
                    disk_bytes = Path(str(row["path"])).stat().st_size
                except OSError:
                    disk_bytes = max(1, int(row.get("size_bytes") or 1))
                estimate = max(1, disk_bytes) * 3 + 2 * len(str(row.get("body_text") or ""))
                if futures and in_flight_bytes + estimate > RESCAN_BYTE_BUDGET:
                    deferred_row = row
                    return
                future = pool.submit(_analyze_saved_document, row, jobs)
                futures[future] = (row, estimate)
                in_flight_bytes += estimate
                submitted_order.append(int(row["id"]))

        submit_available()
        while futures:
            if stop_event.is_set():
                for pending in futures:
                    pending.cancel()
                raise Stopped
            done, _pending = concurrent.futures.wait(
                futures, return_when=concurrent.futures.FIRST_COMPLETED
            )
            results: list[dict[str, object]] = []
            for future in done:
                _row, estimate = futures.pop(future)
                in_flight_bytes -= estimate
                results.append(future.result())

            # SQLite remains owned by this thread. Every completed worker group is
            # persisted in one transaction instead of one transaction per file.
            with database:
                for result in results:
                    row = result["row"]
                    assert isinstance(row, dict)
                    capture_id = int(row["capture_id"])
                    document_id = int(row["id"])
                    kind = str(result["kind"])
                    if kind == "missing":
                        path = Path(result["path"])
                        record_error(
                            database,
                            "scan",
                            "missing_local_file",
                            f"saved file is missing: {path}",
                            capture_id=capture_id,
                            document_id=document_id,
                            retryable=True,
                        )
                        database.execute(
                            "UPDATE captures SET state='error' WHERE id=?", (capture_id,)
                        )
                    elif kind == "integrity":
                        record_error(database, "scan", "local_integrity_failure",
                            str(result["error"]), capture_id=capture_id,
                            document_id=document_id, retryable=False)
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
                        if bool(result.get("document_changed")):
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
                            save_match(database, int(scan_run_id), saved_document_id, analysis)
                        resolve_errors(
                            database,
                            capture_id=capture_id,
                            document_id=saved_document_id,
                            operations=("scan", "parse"),
                        )
                    finished_ids.add(document_id)
                # Only persist a contiguous prefix, never the highest ID that
                # happened to finish first. This survives worker reordering and
                # commits the scan results and checkpoint atomically.
                contiguous = None
                while submitted_order and submitted_order[0] in finished_ids:
                    contiguous = submitted_order.popleft()
                    finished_ids.remove(contiguous)
                if contiguous is not None and checkpoint is not None:
                    checkpoint(contiguous)

            for result in results:
                completed += 1
                if callback:
                    prefix = "Missing local file" if result["kind"] == "missing" else "Rescanned"
                    callback(
                        ProgressEvent(
                            "rescan",
                            f"{prefix} {completed:,}/{total:,} against {len(jobs):,} keyword set(s)",
                            completed,
                            total,
                            {"workers": worker_count},
                        )
                    )
            submit_available()


def rescan_documents(
    database: sqlite3.Connection,
    scan_run_id: int,
    keywords: list[str],
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None = None,
    document_ids: list[int] | None = None,
    workers: int | None = None,
) -> None:
    rescan_keyword_sets(
        database,
        [ScanJob.create(scan_run_id, "Current keywords", keywords)],
        stop_event,
        callback,
        document_ids,
        workers,
    )
