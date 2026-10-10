from __future__ import annotations

import concurrent.futures
import hashlib
import json
import re
import sqlite3
import threading
import time
from dataclasses import replace
from typing import Callable
from urllib.parse import urlsplit

from ..cdx.client import CDXRow, HttpClient, PermanentRequestError, RateLimitDeferred, TransientRequestError, request_cdx_rows
from ..cdx.indexer import (
    PendingWindow,
    PagedBatch,
    PAGED_PIPELINE_PAGES,
    _select_page_batch,
    cdx_response_budget,
    decode_plan,
    encode_plan,
    index_windows,
    split_window,
    window_label,
)
from ..cdx.parallel import PageFetchResult, effective_page_workers, iter_cdx_pages
from ..cdx.parameters import (
    cdx_endpoints,
    cdx_paged_endpoints,
    cdx_query_signature,
    cdx_query_signatures,
    cdx_target_value,
    cdx_year_window,
    parse_num_pages,
    preferred_index_strategy,
)
from ..config import ProjectConfig
from ..database.repositories import (
    blocked_site_hosts,
    cdx_row_to_dict,
    get_or_create_media_target,
    iter_media_discovery_rows,
    mark_media_discovery_document,
    mark_media_discovery_lookup,
    media_discovery_counts,
    queue_media_discovery_candidates,
    record_error,
    record_site_issue,
    upsert_media_captures,
)
from ..downloads.rate_limit import SharedFixedRateLimiter, shared_host_gate
from ..downloads.validation import classify_exception
from ..events import ProgressEvent, Stopped
from ..utils import json_value, parse_cdx_parameter_lines, utc_now
from ..site_status import host_from_url, should_surface_site_issue, site_issue_message
from .discovery import discover_media, hosts_related, safe_document_text, target_hosts
from .extensions import allowed_media_url, selected_extensions

ALL_EXTENSIONS_STATE = "__all__"


def media_query_signature(config: ProjectConfig, page_size: int | None = None) -> str:
    media = config.media.normalized()
    payload = {
        "from": config.from_date,
        "to": config.to_date,
        "filters": config.cdx_filters,
        "collapses": config.cdx_collapses,
        "extra": config.cdx_extra_params,
        "page_size": int(config.page_size if page_size is None else page_size),
        "targets": media.targets or config.targets,
        "extensions": selected_extensions(media),
        "strategy": media.snapshot_strategy,
        "embedded": media.discover_embedded,
        "external": media.allow_external_embeds,
        "single_query_per_target": True,
        "network_strategy": config.network.normalized().index_strategy,
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]


def media_query_signatures(config: ProjectConfig) -> tuple[str, ...]:
    sizes = [config.page_size, 5000, 25000, 1000, 10000, 50000, 100000, 150000]
    return tuple(dict.fromkeys(media_query_signature(config, size) for size in sizes))


def media_index_state_signature(config: ProjectConfig, *, page_size: int | None = None, revision: int = 3) -> str:
    payload = {"media_query_signature": media_query_signature(config, page_size), "index_revision": int(revision)}
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]


def media_signature_candidates(config: ProjectConfig) -> tuple[tuple[str, str], ...]:
    values: list[tuple[str, str]] = []
    for size in [config.page_size, 5000, 25000, 1000, 10000, 50000, 100000, 150000]:
        media_signature = media_query_signature(config, size)
        for revision in (3, 2):
            item = (media_signature, media_index_state_signature(config, page_size=size, revision=revision))
            if item not in values:
                values.append(item)
    return tuple(values)


def media_target_pattern(target: str, extension: str = "") -> str:
    return target if not extension else target.rstrip("*") + "*" + extension


def extension_filter_regex(extensions: list[str]) -> str:
    values = sorted({value.casefold().lstrip(".") for value in extensions if value.strip(".")})
    if not values:
        return r"(?!)"
    escaped = "|".join(re.escape(value) for value in values)
    # Historical sites frequently append broken tracking fragments such as
    # image.jpg&ref=thumb without a question mark. Accept those separators too.
    return rf"(?i).*\.(?:{escaped})(?:$|[?&#;].*)"


def build_media_params(
    config: ProjectConfig,
    pattern: str,
    start: str,
    end: str,
    resume: str | None = None,
    exact: bool = False,
    page_size: int | None = None,
    extensions: list[str] | None = None,
):
    query_target = pattern if exact else cdx_target_value(pattern, config.cdx_match_type)
    params = [
        ("url", query_target),
        ("from", start),
        ("to", end),
        ("output", "json"),
        ("fl", "urlkey,timestamp,original,mimetype,statuscode,digest,length"),
    ]
    if exact:
        params.append(("matchType", "exact"))
    elif config.cdx_match_type:
        params.append(("matchType", config.cdx_match_type))
    params.extend(("filter", value) for value in config.cdx_filters)
    params.extend(("collapse", value) for value in config.cdx_collapses)
    if not exact and extensions:
        # One regex filter covers every selected extension. Scout never
        # performs an extension-by-extension CDX index.
        params.append(("filter", "original:" + extension_filter_regex(extensions)))
    params.extend(parse_cdx_parameter_lines(config.cdx_extra_params))
    params.extend([("limit", str(page_size or config.page_size)), ("showResumeKey", "true")])
    if resume:
        params.append(("resumeKey", resume))
    return params


def build_media_num_pages_params(
    config: ProjectConfig,
    pattern: str,
    start: str,
    end: str,
    extensions: list[str],
    page_blocks: int,
):
    params = build_media_params(config, pattern, start, end, extensions=extensions)
    params = [(key, value) for key, value in params if key not in {"limit", "showResumeKey", "resumeKey"}]
    params.append(("showNumPages", "true"))
    if int(page_blocks) > 0:
        params.append(("pageSize", str(int(page_blocks))))
    return params


def build_media_paged_params(
    config: ProjectConfig,
    pattern: str,
    start: str,
    end: str,
    extensions: list[str],
    page: int,
    page_blocks: int,
):
    params = build_media_params(config, pattern, start, end, extensions=extensions)
    params = [(key, value) for key, value in params if key not in {"limit", "showResumeKey", "resumeKey"}]
    params.append(("page", str(max(0, page))))
    if int(page_blocks) > 0:
        params.append(("pageSize", str(int(page_blocks))))
    return params


def _apply_snapshot_strategy(database: sqlite3.Connection, signature: str, strategy: str) -> None:
    if strategy == "all":
        return
    direction = "ASC" if strategy == "earliest" else "DESC"
    now = utc_now()
    with database:
        database.execute("DROP TABLE IF EXISTS temp.archive_scout_media_keep")
        database.execute(
            "CREATE TEMP TABLE archive_scout_media_keep(id INTEGER PRIMARY KEY) WITHOUT ROWID"
        )
        database.execute(
            f"""
            INSERT INTO archive_scout_media_keep(id)
            SELECT id FROM (
                SELECT id,ROW_NUMBER() OVER(
                    PARTITION BY original_url ORDER BY timestamp {direction},id {direction}
                ) AS position
                FROM media_captures WHERE query_signature=?
            ) WHERE position=1
            """,
            (signature,),
        )
        database.execute(
            """
            UPDATE media_captures SET state='pending',updated_at=?
            WHERE query_signature=? AND state='skipped_strategy'
              AND id IN (SELECT id FROM archive_scout_media_keep)
            """,
            (now, signature),
        )
        database.execute(
            """
            UPDATE media_captures SET state='skipped_strategy',updated_at=?
            WHERE query_signature=? AND state!='downloaded'
              AND id NOT IN (SELECT id FROM archive_scout_media_keep)
            """,
            (now, signature),
        )
        database.execute("DROP TABLE archive_scout_media_keep")


def _save_media_state(
    database: sqlite3.Connection,
    target_id: int,
    year: int,
    signature: str,
    resume_key: str | None,
    complete: bool,
    seen: int,
    error_id: int | None,
) -> None:
    database.execute(
        """
        INSERT INTO media_index_state(
            target_id,extension,year,query_signature,resume_key,complete,seen,error_id,updated_at
        ) VALUES(?,?,?,?,?,?,?,?,?)
        ON CONFLICT(target_id,extension,year,query_signature) DO UPDATE SET
            resume_key=excluded.resume_key,
            complete=excluded.complete,
            seen=excluded.seen,
            error_id=excluded.error_id,
            updated_at=excluded.updated_at
        """,
        (target_id, ALL_EXTENSIONS_STATE, year, signature, resume_key, int(complete), seen, error_id, utc_now()),
    )


def _adopt_compatible_media_state(
    database: sqlite3.Connection,
    target_id: int,
    year: int,
    config: ProjectConfig,
    signature: str,
    state_signature: str,
) -> None:
    existing = database.execute(
        "SELECT 1 FROM media_index_state WHERE target_id=? AND extension=? AND year=? AND query_signature=?",
        (target_id, ALL_EXTENSIONS_STATE, year, state_signature),
    ).fetchone()
    if existing:
        return
    start, end = cdx_year_window(config, year) or (f"{year:04d}0101000000", f"{year:04d}1231235959")
    for candidate_signature, candidate_state in media_signature_candidates(config):
        if candidate_state == state_signature:
            continue
        state = database.execute(
            "SELECT resume_key,complete,seen,error_id,updated_at FROM media_index_state "
            "WHERE target_id=? AND extension=? AND year=? AND query_signature=?",
            (target_id, ALL_EXTENSIONS_STATE, year, candidate_state),
        ).fetchone()
        if not state:
            continue
        if candidate_signature != signature:
            database.execute(
                "UPDATE OR IGNORE media_captures SET query_signature=? "
                "WHERE target_id=? AND query_signature=? AND timestamp BETWEEN ? AND ?",
                (signature, target_id, candidate_signature, start, end),
            )
            database.execute(
                "DELETE FROM media_captures WHERE target_id=? AND query_signature=? AND timestamp BETWEEN ? AND ?",
                (target_id, candidate_signature, start, end),
            )
        database.execute(
            "INSERT INTO media_index_state(target_id,extension,year,query_signature,resume_key,complete,seen,error_id,updated_at) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (target_id, ALL_EXTENSIONS_STATE, year, state_signature, state["resume_key"], state["complete"], state["seen"], state["error_id"], state["updated_at"]),
        )
        database.execute(
            "DELETE FROM media_index_state WHERE target_id=? AND extension=? AND year=? AND query_signature=?",
            (target_id, ALL_EXTENSIONS_STATE, year, candidate_state),
        )
        return


def _reuse_completed_main_index(
    config: ProjectConfig,
    database: sqlite3.Connection,
    target: str,
    media_target_id: int,
    year: int,
    media_signature: str,
    state_signature: str,
) -> tuple[bool, int, int]:
    """Populate media rows from a completed normal index without another CDX pass."""
    target_row = database.execute("SELECT id FROM targets WHERE pattern=?", (target,)).fetchone()
    if not target_row:
        return False, 0, 0
    normal_target_id = int(target_row["id"])
    reusable_signature = None
    for candidate in cdx_query_signatures(config):
        row = database.execute(
            "SELECT complete FROM index_state WHERE target_id=? AND year=? AND query_signature=?",
            (normal_target_id, year, candidate),
        ).fetchone()
        if row and row["complete"]:
            reusable_signature = candidate
            break
    if reusable_signature is None:
        return False, 0, 0

    start, end = cdx_year_window(config, year) or (f"{year:04d}0101000000", f"{year:04d}1231235959")
    media = config.media.normalized()
    seen = 0
    changed = 0
    cursor = database.execute(
        "SELECT original_url,timestamp,mimetype,statuscode,digest,length FROM captures "
        "WHERE target_id=? AND query_signature=? AND timestamp BETWEEN ? AND ? ORDER BY id",
        (normal_target_id, reusable_signature, start, end),
    )
    while True:
        batch = cursor.fetchmany(10000)
        if not batch:
            break
        accepted: list[tuple[CDXRow, str, str]] = []
        seen += len(batch)
        for item in batch:
            row: CDXRow = (
                str(item["timestamp"]),
                str(item["original_url"]),
                str(item["mimetype"] or ""),
                str(item["statuscode"] or ""),
                str(item["digest"] or ""),
                str(item["length"] or 0),
            )
            allowed, kind, extension = allowed_media_url(row[1], media, row[2])
            if allowed and kind:
                accepted.append((row, kind, extension))
        changed += upsert_media_captures(
            database, accepted, media_target_id, media_signature, source_type="main_index"
        )
    _save_media_state(database, media_target_id, year, state_signature, None, True, seen, None)
    return True, seen, changed


def _wait_seconds(config: ProjectConfig, failures: int) -> float:
    """Fixed interruptible wait after failed media CDX, not adaptive pacing."""
    return min(5.0, config.network.normalized().retry_base_seconds)


def _defer_media_window(
    config: ProjectConfig,
    database: sqlite3.Connection,
    plan,
    current: PendingWindow,
    target_id: int,
    year: int,
    signature: str,
    seen: int,
    error_id: int | None,
    target: str,
    label: str,
    exc: BaseException,
    completed: int,
    total: int,
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None,
) -> int:
    current.failures += 1
    if current.strategy == "paged":
        # Keep server-selected paging (page_blocks=0) server-selected. Turning
        # an automatic page into pageSize=1 recreates the thousands-of-pages
        # failure mode this release is intended to eliminate.
        if current.page_blocks > 1:
            current.page_blocks = max(1, current.page_blocks // 2)
    else:
        current.page_size = max(25, (current.page_size or config.page_size) // 2)
    with database:
        error_id = record_error(
            database,
            "media_index",
            "transient_media_index_delay",
            f"{target} {label}: {type(exc).__name__}: {exc}",
            retryable=True,
        )
        if len(plan.pending) > 1:
            plan.pending.append(plan.pending.pop(0))
        _save_media_state(database, target_id, year, signature, encode_plan(plan), False, seen, error_id)
    # Failed windows remain saved, and other windows stay eligible. Do not
    # require a manual Resume when a remote connection is temporarily down.
    if len(plan.pending) > 1:
        if callback:
            callback(ProgressEvent("media_index", "Deferred one unresponsive combined-media window behind the remaining queue; it will retry automatically.", completed, total))
        return error_id
    wait = _wait_seconds(config, current.failures)
    if callback:
        callback(ProgressEvent("media_index", f"Wayback is temporarily unavailable. Media indexing remains active and retries in {wait:.1f}s.", completed, total))
    stop_event.wait(wait)
    if stop_event.is_set():
        raise Stopped
    return error_id


def _resolve_media_strategy(current: PendingWindow, config: ProjectConfig, target: str) -> None:
    desired = preferred_index_strategy(config, target)
    if current.strategy not in {"paged", "resume"}:
        current.strategy = desired
    # An unfinished numbered-page queue is durable progress.  An *automatic*
    # preference change must never discard its cursor or completed pages.
    # Explicit user-selected strategies apply to new windows; preserve the
    # in-flight page plan until its checkpoint can be completed safely.
    if not current.pagination_supported and current.strategy == "paged":
        current.strategy = "resume"
    network = config.network.normalized()
    if network.index_strategy == "auto" and current.strategy == "paged":
        # Preserve the original grouping of an unfinished media page plan.
        current.page_blocks = max(1, current.page_blocks or 9)
    elif current.page_blocks <= 0:
        current.page_blocks = network.page_blocks


def _request_media_paged_batch(
    config: ProjectConfig,
    client: HttpClient,
    target: str,
    current: PendingWindow,
    extensions: list[str],
    stop_event: threading.Event,
    consume_success: Callable[[PageFetchResult], None] | None = None,
) -> PagedBatch:
    endpoints = cdx_paged_endpoints(config)
    network = config.network.normalized()
    if current.page_count < 0:
        payload = client.get_cdx_any(
            endpoints,
            build_media_num_pages_params(config, target, current.start, current.end, extensions, current.page_blocks),
            max_bytes=1024 * 1024,
            prefer_text=False,
        )
        current.page_count = parse_num_pages(payload)
        current.page = min(current.page, current.page_count)
        current.retry_pages = [page for page in current.retry_pages if page < current.page_count]
    if current.page >= current.page_count and not current.retry_pages:
        return PagedBatch([], [], True)

    page_workers = effective_page_workers(network.cdx_workers, current.page_blocks)
    pages, next_page = _select_page_batch(current, max(page_workers, PAGED_PIPELINE_PAGES))
    if not pages:
        return PagedBatch([], [], True)
    results: list[PageFetchResult] = []
    for result in iter_cdx_pages(
        client,
        endpoints,
        pages,
        lambda page: build_media_paged_params(
            config, target, current.start, current.end, extensions, page, current.page_blocks
        ),
        stop_event,
        workers=page_workers,
        max_bytes=(192 * 1024 * 1024 if current.page_blocks <= 0 else max(64 * 1024 * 1024, current.page_blocks * 12 * 1024 * 1024)),
        prefer_text=False,
    ):
        if result.succeeded and consume_success is not None:
            consume_success(result)
        results.append(result)
    current.page = next_page
    retry_set = set(current.retry_pages)
    for result in results:
        if result.succeeded:
            retry_set.discard(result.page)
            current.page_failures.pop(result.page, None)
        else:
            retry_set.add(result.page)
            current.page_failures[result.page] = current.page_failures.get(result.page, 0) + 1
    current.retry_pages = sorted(retry_set)
    return PagedBatch(results, pages, current.page >= current.page_count and not current.retry_pages)


def _request_media_resume(
    config: ProjectConfig,
    client: HttpClient,
    target: str,
    current: PendingWindow,
    extensions: list[str],
) -> tuple[list[dict[str, str]], bool]:
    page_size = current.page_size or config.page_size
    result = request_cdx_rows(
        client,
        cdx_endpoints(config),
        build_media_params(
            config,
            target,
            current.start,
            current.end,
            current.resume_key,
            page_size=page_size,
            extensions=extensions,
        ),
        max_bytes=cdx_response_budget(page_size),
        prefer_text=True,
    )
    rows, next_resume = result.rows, result.resume_key
    if next_resume:
        if next_resume == current.resume_key:
            raise TransientRequestError("CDX returned the same media resume key twice", splittable=True)
        current.resume_key = next_resume
        return rows, False
    return rows, True


def _media_failure_error(failures: list[PageFetchResult]) -> BaseException:
    if not failures:
        return TransientRequestError("unknown paged media CDX failure", splittable=False)
    return failures[0].error or TransientRequestError("unknown paged media CDX failure", splittable=False)


def _pagination_unavailable(exc: BaseException) -> bool:
    message = str(exc)
    return isinstance(exc, RuntimeError) and ("HTTP 400" in message or "page-count" in message)


def _permanent_media_page_error(exc: BaseException) -> bool:
    if isinstance(exc, (TransientRequestError, RateLimitDeferred)):
        return False
    if _pagination_unavailable(exc):
        return False
    return isinstance(exc, RuntimeError)


def _accept_media_rows(
    rows: list[CDXRow] | list[dict[str, str]],
    media,
) -> list[tuple[CDXRow | dict[str, str], str, str]]:
    """Filter media rows without expanding every compact CDX tuple to a dict."""
    accepted: list[tuple[CDXRow | dict[str, str], str, str]] = []
    for row in rows:
        if isinstance(row, dict):
            original = str(row.get("original") or "")
            mimetype = str(row.get("mimetype") or "")
        else:
            original = str(row[1] if len(row) > 1 else "")
            mimetype = str(row[2] if len(row) > 2 else "")
        allowed, kind, actual_extension = allowed_media_url(original, media, mimetype)
        if allowed and kind:
            accepted.append((row, kind, actual_extension))
    return accepted


def index_direct_media(
    config: ProjectConfig,
    database: sqlite3.Connection,
    client: HttpClient,
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None,
    signature: str,
    state_signature: str,
) -> None:
    media = config.media.normalized()
    targets = media.targets or config.targets
    extensions = selected_extensions(media)
    tasks: list[tuple[str, ProjectConfig, int, list[tuple[str, str]]]] = []
    for target in targets:
        target_config = config.for_target(target) if target in config.targets else config
        for year in range(target_config.from_year, target_config.to_year + 1):
            windows = index_windows(target_config, target, year)
            if windows:
                tasks.append((target, target_config, year, windows))
    total = sum(len(windows) for _, _, _, windows in tasks)
    completed = 0
    connection_failure_streak = 0

    for target, target_config, year, default_windows in tasks:
        if stop_event.is_set():
            raise Stopped
        target_id = get_or_create_media_target(database, target)
        with database:
            _adopt_compatible_media_state(
                database, target_id, year, target_config, signature, state_signature
            )
        state = database.execute(
            """
            SELECT resume_key,complete,seen,error_id FROM media_index_state
            WHERE target_id=? AND extension=? AND year=? AND query_signature=?
            """,
            (target_id, ALL_EXTENSIONS_STATE, year, state_signature),
        ).fetchone()
        if state and state["complete"]:
            completed += len(default_windows)
            if callback:
                callback(ProgressEvent("media_index", f"Already indexed selected media for {target} in {year}", completed, total))
            continue

        # If the normal site index is already complete, media discovery is a
        # local SQLite filter rather than another network-wide CDX traversal.
        if target in config.targets:
            with database:
                reused, reused_seen, reused_changed = _reuse_completed_main_index(
                    target_config,
                    database,
                    target,
                    target_id,
                    year,
                    signature,
                    state_signature,
                )
            if reused:
                completed += len(default_windows)
                if callback:
                    callback(
                        ProgressEvent(
                            "media_index",
                            f"Reused the completed site index for {target} {year}: checked {reused_seen:,}, stored {reused_changed:,} media captures without another CDX request.",
                            completed,
                            total,
                        )
                    )
                continue

        seen = int(state["seen"] or 0) if state else 0
        error_id = int(state["error_id"]) if state and state["error_id"] else None
        plan = decode_plan(state["resume_key"] if state else None, default_windows)
        completed += plan.completed
        total += max(0, plan.planned - len(default_windows))

        while plan.pending:
            if stop_event.is_set():
                with database:
                    _save_media_state(database, target_id, year, state_signature, encode_plan(plan), False, seen, error_id)
                raise Stopped
            current = plan.pending[0]
            _resolve_media_strategy(current, target_config, target)
            label = window_label(current.start, current.end)
            detail = (
                f"pages {current.page:,}/{current.page_count:,}; {len(current.retry_pages)} retry"
                if current.strategy == "paged" and current.page_count >= 0
                else current.strategy
            )
            if callback:
                callback(ProgressEvent("media_index", f"Indexing all selected media for {target} during {label} ({detail})", completed, total))
            request_started = time.monotonic()
            try:
                if current.strategy == "paged":
                    received = 0
                    accepted_count = 0
                    changed = 0
                    write_seconds = 0.0

                    def store_completed_media_page(result: PageFetchResult) -> None:
                        nonlocal received, accepted_count, changed, write_seconds
                        page_received = len(result.rows)
                        accepted = _accept_media_rows(result.rows, media)
                        write_started = time.monotonic()
                        with database:
                            changed += upsert_media_captures(database, accepted, target_id, signature)
                        write_seconds += time.monotonic() - write_started
                        received += page_received
                        accepted_count += len(accepted)
                        result.rows.clear()
                        accepted.clear()

                    batch = _request_media_paged_batch(
                        target_config, client, target, current, extensions, stop_event,
                        store_completed_media_page,
                    )
                    request_seconds = max(0.0, time.monotonic() - request_started - write_seconds)
                    successes = batch.successful
                    failures = batch.failed
                    with database:
                        seen += received
                        if successes:
                            current.failures = 0
                            connection_failure_streak = 0
                            transient_failure_streak = 0
                        if batch.finished:
                            plan.pending.pop(0)
                            plan.completed += 1
                            completed += 1
                        complete = not plan.pending
                        _save_media_state(
                            database,
                            target_id,
                            year,
                            state_signature,
                            encode_plan(plan),
                            complete,
                            seen,
                            None if complete else error_id,
                        )
                        if error_id and not failures:
                            database.execute("UPDATE errors SET resolved=1,last_seen=? WHERE id=?", (utc_now(), error_id))
                            error_id = None
                    if callback:
                        callback(
                            ProgressEvent(
                                "media_index",
                                f"{target} {label}: {len(successes)}/{len(batch.requested_pages)} pages, received {received:,}, accepted {accepted_count:,}, stored {changed:,} — network {request_seconds:.1f}s, database {write_seconds:.2f}s",
                                completed,
                                total,
                            )
                        )
                    if not failures:
                        continue

                    failure_exc = _media_failure_error(failures)
                    if not successes and all(
                        isinstance(item.error, TransientRequestError) and item.error.connection_failed
                        for item in failures
                    ):
                        raise failure_exc
                    if max(current.page_failures.values(), default=0) >= 2:
                        current.strategy = "resume"
                        current.pagination_supported = False
                        current.page = 0
                        current.page_count = -1
                        current.resume_key = None
                        current.retry_pages.clear()
                        current.page_failures.clear()
                        current.failures = 0
                        current.page_size = max(100, target_config.page_size // 2)
                        parts = split_window(current)
                        if parts:
                            for part in parts:
                                part.strategy = "resume"
                                part.pagination_supported = False
                            plan.pending[0:1] = parts
                            added = len(parts) - 1
                            plan.planned += added
                            total += added
                        with database:
                            error_id = record_error(
                                database,
                                "media_index",
                                "slow_media_page_fallback",
                                f"{target} {label}: one combined-media CDX page failed repeatedly; switching the saved window to smaller resume-key work.",
                                retryable=True,
                            )
                            _save_media_state(database, target_id, year, state_signature, encode_plan(plan), False, seen, error_id)
                        if callback:
                            callback(ProgressEvent("media_index", f"One combined-media CDX page remained slow for {target} {label}; successful pages were kept and the remaining range was converted to smaller resumable windows.", completed, total))
                        continue
                    if not successes and _pagination_unavailable(failure_exc):
                        current.pagination_supported = False
                        current.strategy = "resume"
                        current.page = 0
                        current.page_count = -1
                        current.retry_pages.clear()
                        current.page_failures.clear()
                        with database:
                            _save_media_state(database, target_id, year, state_signature, encode_plan(plan), False, seen, error_id)
                        continue
                    permanent = next(
                        (item.error for item in failures if item.error and _permanent_media_page_error(item.error)),
                        None,
                    )
                    if permanent is not None:
                        raise permanent
                    with database:
                        error_id = record_error(
                            database,
                            "media_index",
                            "transient_media_page_retry",
                            f"{target} {label}: {len(failures)} media CDX page(s) requeued: {failure_exc}",
                            retryable=True,
                        )
                        _save_media_state(database, target_id, year, state_signature, encode_plan(plan), False, seen, error_id)
                    if successes:
                        if callback:
                            callback(ProgressEvent("media_index", f"Requeued {len(failures)} slow media page(s) while continuing with untouched pages.", completed, total))
                        continue
                    error_id = _defer_media_window(
                        target_config, database, plan, current, target_id, year, state_signature,
                        seen, error_id, target, label, failure_exc, completed, total,
                        stop_event, callback,
                    )
                    continue

                rows, finished = _request_media_resume(
                    target_config, client, target, current, extensions
                )
                connection_failure_streak = 0
                transient_failure_streak = 0
                request_seconds = time.monotonic() - request_started
                received = len(rows)
                accepted = _accept_media_rows(rows, media)
                accepted_count = len(accepted)
                write_started = time.monotonic()
                with database:
                    changed = upsert_media_captures(database, accepted, target_id, signature)
                    seen += received
                    current.failures = 0
                    if finished:
                        plan.pending.pop(0)
                        plan.completed += 1
                        completed += 1
                    complete = not plan.pending
                    _save_media_state(database, target_id, year, state_signature, encode_plan(plan), complete, seen, None if complete else error_id)
                    if error_id:
                        database.execute("UPDATE errors SET resolved=1,last_seen=? WHERE id=?", (utc_now(), error_id))
                        error_id = None
                write_seconds = time.monotonic() - write_started
                if callback:
                    callback(
                        ProgressEvent(
                            "media_index",
                            f"{target} {label}: received {received:,}, accepted {accepted_count:,}, stored {changed:,} — network {request_seconds:.1f}s, database {write_seconds:.2f}s",
                            completed,
                            total,
                        )
                    )
                accepted.clear()
                rows.clear()
            except Stopped:
                with database:
                    _save_media_state(database, target_id, year, state_signature, encode_plan(plan), False, seen, error_id)
                raise
            except (RateLimitDeferred, TransientRequestError) as exc:
                if isinstance(exc, TransientRequestError) and exc.connection_failed:
                    connection_failure_streak += 1
                    current.failures += 1
                    network = target_config.network.normalized()
                    with database:
                        error_id = record_error(
                            database,
                            "media_index",
                            "wayback_connection_unavailable",
                            f"{target} {label}: {exc}",
                            retryable=True,
                        )
                        _save_media_state(database, target_id, year, state_signature, encode_plan(plan), False, seen, error_id)
                    # The transport already tried the available backends;
                    # preserve the queue and retry automatically until Stop.
                    wait = min(5.0, network.connection_retry_seconds)
                    if callback:
                        callback(
                            ProgressEvent(
                                "network",
                                f"Wayback connection setup failed. Retrying the same saved media request in {wait:.1f}s "
                                f"(failed attempts: {connection_failure_streak})…",
                                completed,
                                total,
                            )
                        )
                    stop_event.wait(wait)
                    if stop_event.is_set():
                        raise Stopped
                    continue
                if isinstance(exc, TransientRequestError):
                    transient_failure_streak += 1
                    network = target_config.network.normalized()
                if current.strategy == "paged" and current.page_count < 0:
                    current.strategy = "resume"
                    current.pagination_supported = False
                    current.page = 0
                    current.page_count = -1
                    current.retry_pages.clear()
                    current.page_failures.clear()
                    parts = split_window(current) if getattr(exc, "splittable", False) else []
                    if parts:
                        for part in parts:
                            part.strategy = "resume"
                            part.pagination_supported = False
                        plan.pending[0:1] = parts
                        added = len(parts) - 1
                        plan.planned += added
                        total += added
                    with database:
                        _save_media_state(database, target_id, year, state_signature, encode_plan(plan), False, seen, error_id)
                    if callback:
                        callback(ProgressEvent("media_index", f"Paged CDX could not count media for {target} {label}; continuing with resumable smaller windows.", completed, total))
                    continue
                if current.strategy != "paged" and current.pagination_supported:
                    parts = split_window(current) if getattr(exc, "splittable", False) else []
                    if parts:
                        plan.pending[0:1] = parts
                        added = len(parts) - 1
                        plan.planned += added
                        total += added
                        with database:
                            _save_media_state(database, target_id, year, state_signature, encode_plan(plan), False, seen, error_id)
                        if callback:
                            callback(ProgressEvent("media_index", f"Combined media CDX timed out for {target} {label}; split into {len(parts)} smaller windows.", completed, total))
                        continue
                    if preferred_index_strategy(target_config, target) == "paged":
                        current.strategy = "paged"
                        current.page = 0
                        current.page_count = -1
                        current.resume_key = None
                        current.retry_pages.clear()
                        current.page_failures.clear()
                        with database:
                            _save_media_state(database, target_id, year, state_signature, encode_plan(plan), False, seen, error_id)
                        if callback:
                            callback(ProgressEvent("media_index", f"Switching the combined media window to paged CDX indexing for {target} {label}.", completed, total))
                        continue
                error_id = _defer_media_window(
                    target_config, database, plan, current, target_id, year, state_signature,
                    seen, error_id, target, label, exc, completed, total, stop_event, callback,
                )
            except RuntimeError as exc:
                if isinstance(exc, PermanentRequestError) and exc.category in {"wayback_excluded", "wayback_forbidden"}:
                    category = exc.category
                    message = site_issue_message(category, target, "media CDX indexing", exc.status)
                    remaining = len(plan.pending)
                    plan.pending.clear()
                    plan.completed += remaining
                    completed += remaining
                    with database:
                        error_id = record_error(
                            database, "media_index", category, f"{target} {label}: {exc}",
                            http_status=exc.status, retryable=False,
                        )
                        record_site_issue(
                            database, host_from_url(target), "media_cdx_index", category, message,
                            target=target, http_status=exc.status,
                        )
                        _save_media_state(database, target_id, year, state_signature, None, True, seen, error_id)
                    if callback:
                        callback(ProgressEvent("site_issue", message, completed, total))
                    continue
                if current.strategy == "paged" and _pagination_unavailable(exc):
                    current.pagination_supported = False
                    current.strategy = "resume"
                    current.page = 0
                    current.page_count = -1
                    current.retry_pages.clear()
                    current.page_failures.clear()
                    with database:
                        _save_media_state(database, target_id, year, state_signature, encode_plan(plan), False, seen, error_id)
                    continue
                with database:
                    error_id = record_error(database, "media_index", "index_failure", f"{target} {label}: {type(exc).__name__}: {exc}", retryable=False)
                    _save_media_state(database, target_id, year, state_signature, encode_plan(plan), False, seen, error_id)
                raise
            except Exception as exc:
                with database:
                    error_id = record_error(
                        database,
                        "media_index",
                        "unexpected_media_index_error",
                        f"{target} {label}: {type(exc).__name__}: {exc}",
                        retryable=False,
                    )
                    _save_media_state(database, target_id, year, state_signature, encode_plan(plan), False, seen, error_id)
                raise


def _discover_document_media(
    output_dir,
    max_file_bytes: int,
    media,
    target_host_set: set[str],
    external_only: bool,
    row: dict,
) -> tuple[int, str, list[tuple[str, int | None, str, str]]]:
    """Pure local worker used by embedded-media discovery."""
    document_id = int(row["id"])
    content_hash = str(row.get("content_hash") or "")
    known_links = [str(value) for value in json_value(row.get("links_json"), []) if str(value).strip()]
    raw = safe_document_text(output_dir, str(row.get("path") or ""), max_file_bytes)
    found = discover_media(raw, str(row.get("original_url") or ""), media, known_links)
    batch: list[tuple[str, int | None, str, str]] = []
    for item in found:
        host = host_from_url(item.url)
        if not host or host in {"unknown", "web.archive.org"}:
            continue
        is_external = not hosts_related(host, target_host_set)
        if external_only and not is_external:
            continue
        if is_external and not media.allow_external_embeds:
            continue
        batch.append((
            item.url,
            document_id,
            "external_embedded" if is_external else "embedded",
            item.kind_hint,
        ))
    return document_id, content_hash, batch


def _discover_embedded_queue(
    config: ProjectConfig,
    database: sqlite3.Connection,
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None,
    signature: str,
    *,
    external_only: bool,
) -> int:
    """Discover media from saved documents with bounded local parallelism.

    SQLite access remains on the operation thread. File reads and HTML/player
    parsing run in a small worker pool, so large projects no longer serialize
    every saved-page parse before external media indexing can begin.
    """
    media = config.media.normalized()
    if not media.discover_embedded:
        return 0
    # In the dedicated external-after-scan workflow, "external" is defined
    # relative to the site(s) that were actually indexed and scanned. Stale or
    # optional direct-media targets must not change that boundary.
    discovery_targets = config.targets if external_only else (media.targets or config.targets)
    targets = target_hosts(discovery_targets)
    total = int(database.execute("SELECT COUNT(*) FROM documents").fetchone()[0])
    scanned = queued = 0
    cursor = database.execute(
        """
        SELECT d.id,d.path,d.links_json,d.content_hash,c.original_url,
               mdd.content_hash AS discovery_content_hash
        FROM documents d
        JOIN captures c ON c.id=d.capture_id
        LEFT JOIN media_discovery_documents mdd
          ON mdd.document_id=d.id AND mdd.query_signature=?
        ORDER BY d.id
        """,
        (signature,),
    )
    workers = min(8, max(1, int(config.workers)))
    batch_size = max(64, workers * 16)
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers, thread_name_prefix="archive-discovery") as pool:
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            futures: list[concurrent.futures.Future] = []
            for row in rows:
                if stop_event.is_set():
                    raise Stopped
                document_id = int(row["id"])
                content_hash = str(row["content_hash"] or "")
                if str(row["discovery_content_hash"] or "") == content_hash and content_hash:
                    scanned += 1
                    continue
                futures.append(pool.submit(
                    _discover_document_media,
                    config.output_dir,
                    config.max_file_bytes,
                    media,
                    targets,
                    external_only,
                    dict(row),
                ))
            completed_results: list[tuple[int, str, list[tuple[str, int | None, str, str]]]] = []
            for future in concurrent.futures.as_completed(futures):
                if stop_event.is_set():
                    for pending in futures:
                        pending.cancel()
                    raise Stopped
                completed_results.append(future.result())
            # One transaction per local discovery batch instead of one fsync/
            # transaction boundary per document. Worker threads never touch SQLite.
            if completed_results:
                with database:
                    for document_id, content_hash, discovered in completed_results:
                        queued += queue_media_discovery_candidates(database, signature, discovered)
                        mark_media_discovery_document(database, signature, document_id, content_hash, len(discovered))
                        scanned += 1
            if callback and (scanned == total or scanned % 100 == 0):
                callback(ProgressEvent(
                    "media_embed",
                    f"Discovering embedded media in saved pages {scanned:,}/{total:,}; {queued:,} new URLs queued",
                    scanned,
                    total,
                ))
    return queued


def _embedded_lookup(
    config: ProjectConfig,
    client: HttpClient,
    row: sqlite3.Row,
    snapshot_strategy: str,
    endpoints: tuple[str, ...],
) -> tuple[sqlite3.Row, list[CDXRow], BaseException | None]:
    """Resolve one exact embedded URL using the cheapest valid CDX query.

    The default earliest/latest policies need only one capture. Earlier builds
    fetched as many as 50k/100k captures for every embedded URL and then selected
    one locally, which made media discovery unnecessarily expensive.
    """
    try:
        url = str(row["original_url"])
        strategy = snapshot_strategy
        if strategy == "earliest":
            params = build_media_params(config, url, config.from_date, config.to_date, exact=True, page_size=1)
            params = [(key, value) for key, value in params if key != "showResumeKey"]
            result = request_cdx_rows(
                client, endpoints, params, max_bytes=8 * 1024 * 1024, prefer_text=True
            )
            return row, result.rows[:1], None
        if strategy == "latest":
            params = build_media_params(config, url, config.from_date, config.to_date, exact=True, page_size=1)
            params = [(key, value) for key, value in params if key not in {"limit", "showResumeKey", "resumeKey", "fastLatest"}]
            params.extend([("limit", "-1"), ("fastLatest", "true")])
            result = request_cdx_rows(
                client, endpoints, params, max_bytes=8 * 1024 * 1024, prefer_text=True
            )
            return row, result.rows[-1:] if result.rows else [], None

        rows: list[CDXRow] = []
        resume: str | None = None
        while True:
            result = request_cdx_rows(
                client,
                endpoints,
                build_media_params(
                    config, url, config.from_date, config.to_date, resume, exact=True, page_size=config.page_size
                ),
                max_bytes=cdx_response_budget(config.page_size),
                prefer_text=True,
            )
            rows.extend(result.rows)
            if not result.resume_key:
                break
            if result.resume_key == resume:
                raise TransientRequestError("CDX returned the same embedded-media resume key twice", splittable=True)
            resume = result.resume_key
        return row, rows, None
    except BaseException as exc:
        return row, [], exc


def _embedded_row_kind(row: dict[str, str], kind_hint: str, media) -> tuple[bool, str | None, str]:
    allowed, kind, extension = allowed_media_url(row["original"], media, row.get("mimetype", ""))
    if allowed and kind:
        return allowed, kind, extension
    mime = str(row.get("mimetype") or "").split(";", 1)[0].casefold()
    if kind_hint in {"image", "video"} and not extension and mime in {"", "application/octet-stream", "binary/octet-stream"}:
        if kind_hint == "image" and media.include_images:
            return True, "image", ""
        if kind_hint == "video" and media.include_videos:
            return True, "video", ""
    return False, kind, extension


def index_embedded_media(
    config: ProjectConfig,
    database: sqlite3.Connection,
    client: HttpClient,
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None,
    signature: str,
    *,
    external_only: bool = False,
) -> None:
    """Discover and index embedded media with a persistent, bounded lookup queue.

    The old implementation accumulated every unique URL in memory and then
    performed one exact CDX request at a time. This version scans documents
    incrementally, remembers which document hashes were already inspected, and
    overlaps slow exact lookups while the existing shared request-start limiter
    preserves the configured Wayback request rate.
    """
    media = config.media.normalized()
    if not media.discover_embedded:
        return
    _discover_embedded_queue(config, database, stop_event, callback, signature, external_only=external_only)
    pending_total = int(database.execute(
        """SELECT COUNT(*) FROM media_discovery_queue
           WHERE query_signature=? AND state IN ('pending','error') AND lookup_attempts<?""",
        (signature, config.max_attempts),
    ).fetchone()[0])
    if not pending_total:
        counts = media_discovery_counts(database, signature)
        if callback:
            callback(ProgressEvent(
                "media_embed",
                f"Embedded-media discovery is current: {counts.get('indexed',0):,} indexed, {counts.get('unavailable',0):,} unavailable.",
                0,
                0,
            ))
        return

    workers = min(max(1, config.network.normalized().cdx_workers), 10)
    max_inflight = max(workers, workers * 2)
    endpoints = cdx_endpoints(config)
    row_iter = iter_media_discovery_rows(database, signature, config.max_attempts)
    completed = indexed = unavailable = errors = 0
    blocked_hosts = blocked_site_hosts(database)

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers, thread_name_prefix="archive-embed") as pool:
        futures: dict[concurrent.futures.Future, sqlite3.Row] = {}

        def submit_available() -> None:
            nonlocal completed, unavailable
            while len(futures) < max_inflight:
                try:
                    queue_row = next(row_iter)
                except StopIteration:
                    return
                if stop_event.is_set():
                    raise Stopped
                link = str(queue_row["original_url"] or "")
                host = host_from_url(link)
                if host in blocked_hosts:
                    with database:
                        mark_media_discovery_lookup(
                            database, int(queue_row["id"]), "unavailable",
                            error="Skipped because this host is already known to be excluded or robots-blocked in Wayback.",
                            increment_attempt=False,
                        )
                    completed += 1
                    unavailable += 1
                    if callback and (completed == pending_total or completed % 100 == 0):
                        callback(ProgressEvent(
                            "media_embed",
                            f"Embedded media lookup {completed:,}/{pending_total:,}; indexed {indexed:,}; unavailable {unavailable:,}; errors {errors:,}",
                            completed, pending_total,
                            {"indexed": indexed, "unavailable": unavailable, "errors": errors},
                        ))
                    continue
                future = pool.submit(_embedded_lookup, config, client, queue_row, media.snapshot_strategy, endpoints)
                futures[future] = queue_row

        submit_available()
        while futures:
            if stop_event.is_set():
                for future in futures:
                    future.cancel()
                raise Stopped
            done, _ = concurrent.futures.wait(tuple(futures), timeout=0.25, return_when=concurrent.futures.FIRST_COMPLETED)
            if not done:
                continue
            for future in done:
                original_queue_row = futures.pop(future)
                queue_row, rows, error = future.result()
                link = str(queue_row["original_url"])
                document_id = int(queue_row["source_document_id"]) if queue_row["source_document_id"] is not None else None
                source_type = str(queue_row["source_type"] or "external_embedded")
                hint = str(queue_row["kind_hint"] or "")
                accepted_count = 0
                if error is None:
                    if media.snapshot_strategy == "earliest" and rows:
                        chosen = [min(rows, key=lambda value: value[0])]
                    elif media.snapshot_strategy == "latest" and rows:
                        chosen = [max(rows, key=lambda value: value[0])]
                    else:
                        chosen = rows
                    accepted_items: list[tuple[dict[str, str], str, str]] = []
                    for compact in chosen:
                        value = cdx_row_to_dict(compact)
                        allowed, kind, extension = _embedded_row_kind(value, hint, media)
                        if allowed and kind:
                            accepted_items.append((value, kind, extension))
                    accepted_count = len(accepted_items)
                    with database:
                        if accepted_items:
                            upsert_media_captures(
                                database, accepted_items, None, signature, document_id, source_type
                            )
                        mark_media_discovery_lookup(
                            database,
                            int(queue_row["id"]),
                            "indexed" if accepted_count else "unavailable",
                            result_count=accepted_count,
                        )
                    if accepted_count:
                        indexed += 1
                    else:
                        unavailable += 1
                else:
                    if isinstance(error, (RateLimitDeferred, Stopped)):
                        for pending in futures:
                            pending.cancel()
                        raise error
                    category, status, retryable = classify_exception(error if isinstance(error, Exception) else Exception(str(error)))
                    host = host_from_url(link)
                    permanent = not retryable
                    issue_message = site_issue_message(category, link, "embedded-media indexing", status)
                    if category in {"wayback_excluded", "robots_blocked"}:
                        blocked_hosts.add(host)
                    with database:
                        mark_media_discovery_lookup(
                            database,
                            int(queue_row["id"]),
                            "unavailable" if permanent else "error",
                            error=str(error),
                        )
                        record_error(
                            database,
                            "media_embed",
                            category if category != "unknown" else "embedded_lookup_error",
                            f"{link}: {error}",
                            document_id=document_id,
                            http_status=status,
                            retryable=retryable,
                        )
                        if should_surface_site_issue(category):
                            record_site_issue(
                                database,
                                host,
                                "embedded_media_index",
                                category,
                                issue_message,
                                target=link,
                                http_status=status,
                            )
                    errors += int(retryable)
                    unavailable += int(permanent)
                    if callback and should_surface_site_issue(category):
                        callback(ProgressEvent("site_issue", issue_message))
                completed += 1
                if callback:
                    callback(ProgressEvent(
                        "media_embed",
                        f"Embedded media lookup {completed:,}/{pending_total:,}; indexed {indexed:,}; unavailable {unavailable:,}; errors {errors:,}",
                        completed,
                        pending_total,
                        {"indexed": indexed, "unavailable": unavailable, "errors": errors},
                    ))
                submit_available()

def index_external_embedded_media(
    config: ProjectConfig,
    database: sqlite3.Connection,
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None = None,
) -> str:
    """Index only external media URLs found after saved text pages have been scanned."""
    config = config.normalized()
    config.media = replace(
        config.media.normalized(),
        enabled=True,
        discover_embedded=True,
        allow_external_embeds=True,
    )
    if not selected_extensions(config.media):
        raise ValueError("no image or video extensions remain after include/exclude filtering")
    signature = media_query_signature(config)
    limiter = SharedFixedRateLimiter(config.cdx_delay)
    host_gate = shared_host_gate(config.rate_limit_base_pause, config.rate_limit_max_pause)

    def on_retry(attempt: int, total: int, reason: str, wait_seconds: float) -> None:
        if callback:
            stage = "rate_limit" if "all Wayback requests paused" in reason else "media_embed"
            if wait_seconds > 0:
                message = f"{reason}. Retry {attempt}/{total} in {wait_seconds:.1f}s…"
            else:
                message = reason
            callback(ProgressEvent(stage, message))

    client = HttpClient(
        limiter,
        1,
        min(max(config.read_timeout, 30.0), 120.0),
        config.user_agent,
        stop_event,
        retry_callback=on_retry,
        connect_timeout=min(max(config.connect_timeout, 5.0), 15.0),
        read_timeout=min(max(config.read_timeout, 30.0), 120.0),
        pool_size=config.network.normalized().cdx_workers,
        host_gate=host_gate,
        rate_limit_attempts=config.rate_limit_attempts,
        rate_limit_max_wait=config.rate_limit_max_wait,
        network_backend=config.network.normalized().backend,
        trust_environment=config.network.normalized().trust_environment,
        network_callback=(lambda message: callback(ProgressEvent("network", message)) if callback else None),
    )
    try:
        index_embedded_media(
            config, database, client, stop_event, callback, signature, external_only=True
        )
        _apply_snapshot_strategy(database, signature, config.media.snapshot_strategy)
        return signature
    finally:
        client.close()


def index_media(
    config: ProjectConfig,
    database: sqlite3.Connection,
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None = None,
) -> str:
    config = config.normalized()
    media = config.media.normalized()
    if not (media.targets or config.targets):
        raise ValueError("add at least one media target or site target")
    if not selected_extensions(media):
        raise ValueError("no image or video extensions remain after include/exclude filtering")
    signature = media_query_signature(config)
    state_signature = media_index_state_signature(config)
    limiter = SharedFixedRateLimiter(config.cdx_delay)
    host_gate = shared_host_gate(config.rate_limit_base_pause, config.rate_limit_max_pause)

    def on_retry(attempt: int, total: int, reason: str, wait_seconds: float) -> None:
        if callback:
            if "all Wayback requests paused" in reason:
                limit = f"/{total}" if total else ""
                message = f"{reason}. Shared pause {attempt}{limit} for {wait_seconds:.1f}s; one recovery probe will run next…"
                stage = "rate_limit"
            elif wait_seconds <= 0:
                message = reason
                stage = "network"
            else:
                message = f"CDX media request failed ({reason}). Retrying attempt {attempt}/{total} in {wait_seconds:.1f}s…"
                stage = "media_index"
            callback(ProgressEvent(stage, message))

    client = HttpClient(
        limiter,
        1,
        min(max(config.read_timeout, 30.0), 120.0),
        config.user_agent,
        stop_event,
        retry_callback=on_retry,
        connect_timeout=min(max(config.connect_timeout, 5.0), 15.0),
        read_timeout=min(max(config.read_timeout, 30.0), 120.0),
        pool_size=config.network.normalized().cdx_workers,
        host_gate=host_gate,
        rate_limit_attempts=config.rate_limit_attempts,
        rate_limit_max_wait=config.rate_limit_max_wait,
        network_backend=config.network.normalized().backend,
        trust_environment=config.network.normalized().trust_environment,
        network_callback=(lambda message: callback(ProgressEvent("network", message)) if callback else None),
    )
    try:
        index_direct_media(config, database, client, stop_event, callback, signature, state_signature)
        index_embedded_media(config, database, client, stop_event, callback, signature)
        _apply_snapshot_strategy(database, signature, media.snapshot_strategy)
        return signature
    finally:
        client.close()
