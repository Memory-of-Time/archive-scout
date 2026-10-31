from __future__ import annotations

from ..retry_budget import NoProgressBudget, CursorHistory, incomplete_retry_message

import concurrent.futures
from collections import deque
import hashlib
import json
import re
import sqlite3
import threading
import time
from dataclasses import replace
from pathlib import Path
from typing import Callable
from urllib.parse import urlsplit

from ..cdx.client import CDXRow, HttpClient, MalformedCDXResponse, PermanentRequestError, RateLimitDeferred, TransientRequestError, request_cdx_rows, request_cdx_json_payload, summarize_cdx_error
from ..cdx.indexer import (
    PendingWindow,
    PagedBatch,
    PAGED_PIPELINE_PAGES,
    _select_page_batch,
    cdx_response_budget,
    decode_plan,
    encode_plan,
    index_windows,
    validate_cdx_scope,
    record_recovery_event,
    uncovered_index_ranges,
    _reconcile_paged_completion,
    _query_is_oversized,
    split_window,
    window_label,
)
from ..cdx.parallel import PageFetchResult, effective_page_workers, memory_limited_page_workers, iter_cdx_pages
from ..cdx.parameters import (
    cdx_endpoints,
    cdx_paged_endpoints,
    cdx_query_signature,
    cdx_query_signatures,
    cdx_target_value,
    cdx_year_window,
    cdx_layout_signature,
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
    resolve_errors,
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
MEDIA_INDEX_REVISION = 7


def media_query_signature(config: ProjectConfig, page_size: int | None = None, *, discovery_version: int = 2) -> str:
    media = config.media.normalized()
    payload = {
        "from": config.from_date,
        "to": config.to_date,
        "match_type": config.cdx_match_type,
        "effective_targets": [
            (target, {
                "from": effective.from_date, "to": effective.to_date,
                "filters": effective.cdx_filters, "collapses": effective.cdx_collapses,
                "match_type": effective.cdx_match_type, "extra": effective.cdx_extra_params,
                "collapse_scope": effective.text_collapse_scope,
            })
            for target in (media.targets or config.targets)
            for effective in [config.for_target(target) if target in config.targets else config]
        ],
        "filters": config.cdx_filters,
        "collapses": config.cdx_collapses,
        "extra": config.cdx_extra_params,
        "page_size": int(config.page_size if page_size is None else page_size),
        "targets": media.targets or config.targets,
        "extensions": selected_extensions(media),
        "strategy": media.snapshot_strategy,
        "selection_proof": "unfiltered_timestamps_v1" if media.snapshot_strategy in {"earliest", "latest"} else "configured_collapse",
        "embedded": media.discover_embedded,
        "external": media.allow_external_embeds,
        "single_query_per_target": True,
        "network_strategy": config.network.normalized().index_strategy,
    }
    if media.discover_embedded and discovery_version >= 2:
        from ..database.scope import scope_rows, discovery_scope_config
        payload['saved_page_scope'] = scope_rows(discovery_scope_config(config))
        payload['discovery_proof'] = 'selected_full_body_v2'
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]


def media_query_signatures(config: ProjectConfig) -> tuple[str, ...]:
    sizes = [config.page_size, 5000, 25000, 1000, 10000, 50000, 100000, 150000]
    return tuple(dict.fromkeys(media_query_signature(config, size) for size in sizes))


def _extra_collapse_affects_extreme(config: ProjectConfig) -> bool:
    return config.media.snapshot_strategy in {'earliest', 'latest'} and any(
        key == 'collapse' for key, _ in parse_cdx_parameter_lines(config.cdx_extra_params))


def media_index_state_signature(config: ProjectConfig, *, page_size: int | None = None, revision: int | None = None) -> str:
    if revision is None:
        revision = MEDIA_INDEX_REVISION
    payload = {"media_query_signature": media_query_signature(config, page_size), "index_revision": int(revision)}
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]


def media_signature_candidates(config: ProjectConfig) -> tuple[tuple[str, str], ...]:
    values: list[tuple[str, str]] = []
    for size in [config.page_size, 5000, 25000, 1000, 10000, 50000, 100000, 150000]:
        for discovery_version in (2, 1):
            media_signature = media_query_signature(config, size, discovery_version=discovery_version)
            for revision in (MEDIA_INDEX_REVISION, 6, 5, 4, 3, 2):
                raw = json.dumps({'media_query_signature': media_signature, 'index_revision': revision},
                                 sort_keys=True, separators=(',', ':'))
                item = (media_signature, hashlib.sha256(raw.encode()).hexdigest()[:20])
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
    return rf"(?i).*(?:\.|%2e)(?:{escaped})(?:$|[?&#;].*)"


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
    # Earliest/latest cannot be established from an arbitrary collapsed
    # representative. Keep the full timestamp universe until local selection.
    if config.media.normalized().snapshot_strategy == "all":
        params.extend(("collapse", value) for value in config.cdx_collapses)
    # Wayback rejects the previous fieldless full-record media expression as
    # an invalid field spec. Separate original/mimetype filters would be AND,
    # but local media policy needs suffix OR MIME. Enumerate this CDX scope
    # without an automatic media predicate and apply _accept_media_rows locally.
    # Retain the extensions argument for all existing query-builder callers.
    extras = parse_cdx_parameter_lines(config.cdx_extra_params)
    managed = {"page", "pagesize", "shownumpages", "showpagedindex", "offset"}
    for key, _ in extras:
        if key.casefold() in managed:
            raise ValueError(f"{key} is controlled by media indexing and cannot be added as an advanced parameter")
    if config.media.normalized().snapshot_strategy != 'all':
        extras = [(key, value) for key, value in extras if key != 'collapse']
    params.extend(extras)
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
    scope_config: ProjectConfig | None = None,
) -> None:
    existing = database.execute(
        "SELECT 1 FROM media_index_state WHERE target_id=? AND extension=? AND year=? AND query_signature=?",
        (target_id, ALL_EXTENSIONS_STATE, year, state_signature),
    ).fetchone()
    if existing:
        return
    start, end = ((config.from_date, config.to_date) if year == 0 else
                  cdx_year_window(config, year) or (f"{year:04d}0101000000", f"{year:04d}1231235959"))
    identity_config = scope_config or config
    for candidate_signature, candidate_state in media_signature_candidates(identity_config):
        if candidate_state == state_signature:
            continue
        state = database.execute(
            "SELECT resume_key,complete,seen,error_id,updated_at FROM media_index_state "
            "WHERE target_id=? AND extension=? AND year=? AND query_signature=?",
            (target_id, ALL_EXTENSIONS_STATE, year, candidate_state),
        ).fetchone()
        if not state:
            continue
        # A changed remote predicate changes the cursor universe. Only adopt
        # checkpoints from this revision; retain older states and saved bodies.
        current_proof = json.dumps({'media_query_signature': candidate_signature, 'index_revision': MEDIA_INDEX_REVISION},
                                   sort_keys=True, separators=(',', ':'))
        if candidate_state != hashlib.sha256(current_proof.encode()).hexdigest()[:20]:
            continue
        if candidate_signature != signature:
            database.execute(
                "UPDATE OR IGNORE media_captures SET query_signature=? "
                "WHERE target_id=? AND query_signature=? AND timestamp BETWEEN ? AND ?",
                (signature, target_id, candidate_signature, start, end),
            )
            # Conflicting historical identities remain available with their
            # files and errors. Never delete them merely to adopt a page size.
        database.execute(
            "INSERT INTO media_index_state(target_id,extension,year,query_signature,resume_key,complete,seen,error_id,updated_at) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (target_id, ALL_EXTENSIONS_STATE, year, state_signature, state["resume_key"], state["complete"], state["seen"], state["error_id"], state["updated_at"]),
        )
        database.execute("""INSERT OR IGNORE INTO media_index_pages
            (target_id,year,state_signature,window_start,window_end,layout_signature,page,row_count,updated_at)
            SELECT target_id,year,?,window_start,window_end,layout_signature,page,row_count,updated_at
            FROM media_index_pages WHERE target_id=? AND year=? AND state_signature=?""",
            (state_signature, target_id, year, candidate_state))
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
    """Text checkpoints cannot prove the expanded standalone media query."""
    return False, 0, 0


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
    yield_to_other_tasks: bool = False,
) -> int:
    current.failures += 1
    if current.strategy == 'resume' and _query_is_oversized(exc):
        # Page length is transport layout, not membership. Preserve the exact
        # cursor and date/collapse scope while reducing an explicitly oversized
        # response. Repeating the same over-budget media page cannot recover.
        current.page_size = max(100, (current.page_size or config.page_size) // 2)
    current.next_eligible_at = time.time() + _wait_seconds(config,current.failures)
    # HTTP 5xx/429/refusal is not evidence that a previously selected
    # pagination grouping or a collapsed resume window is too large.
    # Preserve the live cursor and layout exactly across transient failures.
    with database:
        error_id = record_error(
            database,
            "media_index",
            "transient_media_index_delay",
            f"{target} {label}: {type(exc).__name__}: {exc}",
            http_status=getattr(exc, 'status', None),
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
    if yield_to_other_tasks:
        return error_id
    if callback:
        callback(ProgressEvent("media_index", f"Media CDX request failed: {summarize_cdx_error(exc)}. Retrying the saved request in {wait:.1f}s.", completed, total))
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


class MediaPaginationUnavailable(MalformedCDXResponse):
    pass


def _media_page_layout(config, current):
    layout = cdx_layout_signature(config, current.start, current.end, "paged", current.page_blocks)
    if current.page_endpoint:
        layout += ":" + hashlib.sha256(current.page_endpoint.encode()).hexdigest()[:12]
    return layout


def _media_page_count(client, endpoints, params):
    try:
        payload = request_cdx_json_payload(client, endpoints, params, max_bytes=1024 * 1024)
    except PermanentRequestError as exc:
        if exc.status in {400, 404, 405}:
            raise MediaPaginationUnavailable(f"Media page-count helper rejected the request (HTTP {exc.status})", splittable=False) from exc
        raise
    if isinstance(payload, bool) or (isinstance(payload, list) and payload and isinstance(payload[0], list)
            and {"timestamp", "original", "urlkey"}.intersection(str(value) for value in payload[0])):
        raise MediaPaginationUnavailable("Media page-count request returned capture rows or invalid metadata; no page layout verified", splittable=False)
    try:
        return parse_num_pages(payload)
    except RuntimeError as exc:
        raise MediaPaginationUnavailable("Media page-count metadata is unsupported: " + str(exc)[:300], splittable=False) from exc


def _request_media_paged_batch(
    config: ProjectConfig,
    client: HttpClient,
    target: str,
    current: PendingWindow,
    extensions: list[str],
    stop_event: threading.Event,
    consume_success: Callable[[PageFetchResult], None] | None = None,
    completed_pages: set[int] | None = None,
    deadline: float | None = None,
) -> PagedBatch:
    endpoints = cdx_paged_endpoints(config)
    from ..constants import CDX_URL
    allowed_endpoints = set(endpoints) | ({CDX_URL} if config.network.endpoint_mode == "auto" else set())
    if current.page_endpoint:
        if current.page_endpoint not in allowed_endpoints:
            raise MediaPaginationUnavailable("Saved media page endpoint differs from the configured endpoint scope", splittable=False)
        endpoints = (current.page_endpoint,)
    network = config.network.normalized()
    layout = _media_page_layout(config, current)
    if current.page_count >= 0 and current.page_layout != layout:
        current.page_count = -1
        current.page = 0
        current.retry_pages.clear()
        current.page_failures.clear()
    if current.page_count < 0:
        params = build_media_num_pages_params(config, target, current.start, current.end, extensions, current.page_blocks)
        try:
            page_count = _media_page_count(client, endpoints, params)
        except (MediaPaginationUnavailable, PermanentRequestError) as exc:
            if isinstance(exc, PermanentRequestError) and exc.status != 400:
                raise
            if current.page_endpoint or config.network.endpoint_mode != "auto" or CDX_URL in endpoints:
                raise
            page_count = _media_page_count(client, (CDX_URL,), params)
            current.page_endpoint = CDX_URL
            current.page = 0
            current.retry_pages.clear()
            current.page_failures.clear()
            if completed_pages is not None:
                completed_pages.clear()
            endpoints = (CDX_URL,)
            layout = _media_page_layout(config, current)
        current.page_count = page_count
        current.page_layout = layout
        current.page = min(current.page, current.page_count)
        current.retry_pages = [page for page in current.retry_pages if page < current.page_count]
    if current.page >= current.page_count and not current.retry_pages:
        return PagedBatch([], [], True)

    body_limit = (192 * 1024 * 1024 if current.page_blocks <= 0
                  else max(64 * 1024 * 1024, current.page_blocks * 12 * 1024 * 1024))
    page_workers = memory_limited_page_workers(effective_page_workers(network.cdx_workers, current.page_blocks),
                                               body_limit, network.cdx_memory_budget_mb)
    pages, next_page = _select_page_batch(current, min(PAGED_PIPELINE_PAGES, max(page_workers, page_workers * 2)))
    completed_pages = completed_pages or set()
    requested_pages = [page for page in pages if page not in completed_pages]
    if not requested_pages:
        current.page = next_page
        current.retry_pages = [page for page in current.retry_pages if page not in completed_pages]
        return PagedBatch([], pages, current.page >= current.page_count and not current.retry_pages)
    if not pages:
        return PagedBatch([], [], True)
    results: list[PageFetchResult] = []
    for result in iter_cdx_pages(
        client,
        endpoints,
        requested_pages,
        lambda page: build_media_paged_params(
            config, target, current.start, current.end, extensions, page, current.page_blocks
        ),
        stop_event,
        workers=page_workers,
        max_bytes=body_limit,
        prefer_text=False,
        deadline=deadline,
    ):
        if result.succeeded and consume_success is not None:
            consume_success(result)
        results.append(result)
    # Selected but unsubmitted pages must never be skipped by the cursor.
    # An expiry prevents new admission but permits committed successes to land.
    admitted = {result.page for result in results}
    not_admitted = [page for page in requested_pages if page not in admitted]
    current.page = min(next_page, min(not_admitted)) if not_admitted else next_page
    retry_set = set(current.retry_pages)
    retry_set.update(page for page in not_admitted if page < next_page)
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
    *, exact: bool = False,
) -> tuple[list[CDXRow], bool, str | None]:
    # Cursor advancement must be atomic with inserted media inventory rows.
    current.verify_paged_next = False
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
            exact=exact,
        ),
        max_bytes=cdx_response_budget(page_size),
        prefer_text=True,
        verify_terminal=True,
    )
    rows, next_resume = result.rows, result.resume_key
    if next_resume:
        if next_resume == current.resume_key:
            raise TransientRequestError("CDX returned the same media resume key twice", splittable=True)
        return rows, False, next_resume
    if len(rows) >= page_size and not result.terminal_verified and isinstance(client, HttpClient):
        confirmation = request_cdx_rows(
            client, cdx_endpoints(config),
            build_media_params(config, target, current.start, current.end, current.resume_key,
                               page_size=page_size + 1, extensions=extensions, exact=exact),
            max_bytes=cdx_response_budget(page_size + 1), prefer_text=False)
        if confirmation.resume_key and confirmation.resume_key != current.resume_key:
            return confirmation.rows, False, confirmation.resume_key
        if confirmation.resume_key or confirmation.rows != rows:
            from ..cdx.client import MalformedCDXResponse
            raise MalformedCDXResponse(
                "Full-sized media CDX page has unconfirmed terminal membership", splittable=False)
    if result.needs_paged_check and current.page_count < 0:
        current.verify_paged_next = True
        return rows, False, None
    return rows, True, None


def _media_failure_error(failures: list[PageFetchResult]) -> BaseException:
    if not failures:
        return TransientRequestError("unknown paged media CDX failure", splittable=False)
    return failures[0].error or TransientRequestError("unknown paged media CDX failure", splittable=False)


def _pagination_unavailable(exc: BaseException) -> bool:
    message = str(exc)
    return isinstance(exc, MediaPaginationUnavailable) or (isinstance(exc, RuntimeError) and ("HTTP 400" in message or "page-count" in message))


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
) -> bool:
    media = config.media.normalized()
    targets = media.targets or config.targets
    extensions = selected_extensions(media)
    tasks: list[tuple[str, ProjectConfig, int, list[tuple[str, str]]]] = []
    for target in targets:
        target_config = config.for_target(target) if target in config.targets else config
        existing_target = database.execute("SELECT id FROM media_targets WHERE pattern=?", (target,)).fetchone()
        legacy_progress = False
        empty_annual_states = []
        range_state = bool(existing_target and database.execute(
            "SELECT 1 FROM media_index_state WHERE target_id=? AND year=0 AND query_signature=?",
            (existing_target[0], state_signature),
        ).fetchone())
        if existing_target and not range_state:
            candidates = dict.fromkeys([(signature, state_signature),
                *media_signature_candidates(config), *media_signature_candidates(target_config)])
            for candidate_signature, old_state in candidates:
                if database.execute("SELECT 1 FROM media_captures WHERE target_id=? AND query_signature=? LIMIT 1",
                                    (existing_target[0], candidate_signature)).fetchone():
                    legacy_progress = True
                    break
                for old in database.execute(
                    "SELECT year,complete,seen,resume_key FROM media_index_state "
                    "WHERE target_id=? AND year>0 AND query_signature=?",
                    (existing_target[0], old_state),
                ):
                    saved = decode_plan(old["resume_key"], index_windows(target_config, target, old["year"]))
                    if (old["complete"] or old["seen"] or saved.completed or any(
                        w.resume_key or w.page or w.retry_pages or w.page_layout for w in saved.pending
                    ) or database.execute("SELECT 1 FROM media_index_pages WHERE target_id=? AND year>0 AND state_signature=? LIMIT 1",
                                          (existing_target[0], old_state)).fetchone()):
                        legacy_progress = True
                        break
                    empty_annual_states.append((existing_target[0], old["year"], old_state))
                if legacy_progress:
                    break
        if target_config.text_collapse_scope == "range" and target_config.from_year != target_config.to_year:
            # Retire only empty failed plans; no successful page/cursor/capture
            # is discarded or relabeled complete. Errors remain in history.
            # Range selection has a different collapse scope from annual selection.
            # Do not replay legacy years indefinitely or relabel their cursors as
            # range progress. Keep all old annual states and bodies untouched;
            # only the separate year=0 range state can prove range coverage.
            if not legacy_progress and empty_annual_states:
                with database:
                    database.executemany(
                        "DELETE FROM media_index_state WHERE target_id=? AND year=? AND query_signature=?",
                        empty_annual_states,
                    )
            tasks.append((target, target_config, 0, [(target_config.from_date, target_config.to_date)]))
        else:
            for year in range(target_config.from_year, target_config.to_year + 1):
                windows = index_windows(target_config, target, year)
                if windows:
                    tasks.append((target, target_config, year, windows))
    total = sum(len(windows) for _, _, _, windows in tasks)
    completed = 0
    connection_failure_streak = 0
    transient_failure_streak = 0
    task_queue = deque(tasks)
    due: dict[tuple[str,int],float] = {}
    spent: dict[str,float] = {}
    accounted = {}
    retry_budgets = {}
    cursor_histories = {}
    retry_deferred = set()
    blocked_tasks = set()
    budget_deferred: set[str] = set()
    while task_queue:
        if stop_event.is_set():
            raise Stopped
        task = task_queue.popleft()
        target, target_config, year, default_windows = task
        if target in budget_deferred:
            continue
        task_key = (target,year)
        if task_key in retry_deferred or task_key in blocked_tasks:
            continue
        now = time.monotonic()
        if due.get(task_key,0) > now:
            task_queue.append(task)
            if not any(due.get((t[0],t[2]),0) <= now for t in task_queue):
                stop_event.wait(min(1.0, due[task_key]-now))
            continue
        turn_started = time.monotonic()
        requests_this_turn = 0
        budget_limit = target_config.index_target_minutes * 60 if target_config.skip_slow_index_targets else None
        target_id = get_or_create_media_target(database, target)
        with database:
            _adopt_compatible_media_state(
                database, target_id, year, target_config, signature, state_signature,
                scope_config=config,
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
                callback(ProgressEvent("media_index", f"Already indexed selected media for {target} during {window_label(*default_windows[0]) if year == 0 else year}", completed, total))
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
        if task_key not in accounted:
            for window in plan.pending:
                if window.merged_verified and not window.pagination_supported:
                    window.pagination_supported = True
                    window.strategy = "paged"
        previous_complete, previous_extra = accounted.get(task_key,(0,0))
        completed += plan.completed - previous_complete
        extra = max(0, plan.planned - len(default_windows))
        total += extra - previous_extra

        retry_budget = retry_budgets.setdefault(task_key, NoProgressBudget(target_config))
        retry_budget.resume()
        while plan.pending:
            blocked_window = plan.pending[0]
            if blocked_window.merged_verified and not blocked_window.pagination_supported:
                with database:
                    error_id = record_error(database, "media_index", "coverage_verification_unavailable",
                        f"{target}: merged media rows retained, but numbered verification is unavailable; coverage remains incomplete",
                        retryable=True)
                    _save_media_state(database, target_id, year, state_signature, encode_plan(plan), False, seen, error_id)
                blocked_tasks.add(task_key)
                if callback:
                    callback(ProgressEvent("media_index_incomplete", f"{target}: numbered verification is unavailable; discovered media and unfinished coverage retained.",
                        completed, total, {"retain_notice": True}))
                break
            if retry_budget.exhausted:
                with database:
                    previous_error = database.execute('SELECT http_status,message FROM errors WHERE id=?',
                                                      (error_id,)).fetchone() if error_id else None
                    detail = retry_budget.details()
                    error_id = record_error(database, 'media_index', 'retry_budget_exhausted',
                        f"{target}: {detail['attempts_without_progress']}/{detail['attempt_limit']} attempts "
                        f"without progress; {detail['active_no_progress_seconds']:.1f}s active recovery time "
                        "(time queued behind other targets excluded). Last failure: "
                        + (str(previous_error[1])[:1200] if previous_error else 'no verified response'),
                        http_status=previous_error[0] if previous_error else None, retryable=True)
                    _save_media_state(database,target_id,year,state_signature,encode_plan(plan),False,seen,error_id)
                retry_deferred.add(task_key)
                if callback:
                    callback(ProgressEvent("media_index_incomplete", incomplete_retry_message(target),
                             completed, total, {**detail, "retain_notice": True}))
                break
            active = spent.get(target,0.0) + time.monotonic() - turn_started
            if budget_limit is not None and active >= budget_limit:
                with database:
                    error_id = record_error(database, "media_index", "deferred_time_budget",
                        f"{target}: indexing budget {target_config.index_target_minutes:g} minutes reached; cursor retained",
                        retryable=True)
                    _save_media_state(database,target_id,year,state_signature,encode_plan(plan),False,seen,error_id)
                budget_deferred.add(target)
                if callback:
                    callback(ProgressEvent("media_index",f"Deferred media target {target}: time budget reached; saved for Resume"))
                break
            if requests_this_turn >= 8 and task_queue:
                with database:
                    _save_media_state(database,target_id,year,state_signature,encode_plan(plan),False,seen,error_id)
                task_queue.append(task)
                break
            requests_this_turn += 1
            if stop_event.is_set():
                with database:
                    _save_media_state(database, target_id, year, state_signature, encode_plan(plan), False, seen, error_id)
                raise Stopped
            current = plan.pending[0]
            remaining = current.next_eligible_at - time.time()
            if remaining > 0:
                if task_queue:
                    due[task_key] = time.monotonic() + remaining
                    task_queue.append(task)
                    break
                stop_event.wait(min(1.0,remaining))
                if stop_event.is_set():
                    raise Stopped
                continue
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
            retry_budget.attempted()
            try:
                if current.strategy == "paged":
                    received = 0
                    accepted_count = 0
                    changed = 0
                    write_seconds = 0.0

                    def store_completed_media_page(result: PageFetchResult) -> None:
                        nonlocal received, accepted_count, changed, write_seconds, seen
                        if result.resume_key is not None:
                            raise TransientRequestError("numbered media page advertised unresolved continuation", splittable=False)
                        page_received = len(result.rows)
                        excluded_hosts = validate_cdx_scope(
                            result.rows,target,current.start,current.end,
                            target_config.cdx_filters,target_config.cdx_match_type,
                            discard_adjacent_surt_hosts=True)
                        accepted = _accept_media_rows(result.rows, media)
                        write_started = time.monotonic()
                        with database:
                            changed += upsert_media_captures(database, accepted, target_id, signature)
                            if excluded_hosts:
                                record_recovery_event(database, "media_index", "cdx_adjacent_host_excluded",
                                    f"{target} {label}: discarded {excluded_hosts} adjacent out-of-scope host rows from media page {result.page}",
                                    details={"excluded":excluded_hosts,"page":result.page,"raw_rows":page_received})
                            # No page is accepted without its durable ledger.
                            database.execute("""INSERT OR REPLACE INTO media_index_pages
                                (target_id,year,state_signature,window_start,window_end,layout_signature,page,row_count,updated_at)
                                VALUES(?,?,?,?,?,?,?,?,?)""",
                                (target_id,year,state_signature,current.start,current.end,
                                 _media_page_layout(target_config, current),
                                 int(result.page),page_received,utc_now()))
                            seen += page_received
                            _save_media_state(database,target_id,year,state_signature,
                                              encode_plan(plan),False,seen,error_id)
                        retry_budget.progress()
                        completed_pages.add(int(result.page))
                        write_seconds += time.monotonic() - write_started
                        received += page_received
                        accepted_count += len(accepted)
                        result.rows.clear()
                        accepted.clear()

                    # Read only one bounded ledger for the current window.
                    completed_pages = {int(row[0]) for row in database.execute(
                        """SELECT page FROM media_index_pages WHERE target_id=? AND year=?
                           AND state_signature=? AND window_start=? AND window_end=?
                           AND layout_signature=?""",
                        (target_id,year,state_signature,current.start,current.end,
                         _media_page_layout(target_config, current)))}
                    batch = _request_media_paged_batch(
                        target_config, client, target, current, extensions, stop_event,
                        store_completed_media_page, completed_pages,
                        deadline=(turn_started + budget_limit - spent.get(target, 0.0))
                            if budget_limit is not None else None,
                    )
                    request_seconds = max(0.0, time.monotonic() - request_started - write_seconds)
                    successes = batch.successful
                    failures = batch.failed
                    with database:
                        if successes:
                            current.failures = 0
                            connection_failure_streak = 0
                            transient_failure_streak = 0
                        if batch.finished and _reconcile_paged_completion(current, completed_pages):
                            retry_budget.progress()
                            if current.merged_verified:
                                plan.pending.pop(0)
                                plan.completed += 1
                                completed += 1
                            else:
                                current.strategy = "resume"
                                current.resume_key = None
                                current.failures = 0
                                if callback:
                                    callback(ProgressEvent("media_index", f"{target} {label}: numbered media pages are durable; reconciling the merged CDX index before declaring full coverage."))
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
                        stop_event, callback, yield_to_other_tasks=bool(task_queue) or retry_budget.exhausted,
                    )
                    if task_queue:
                        due[task_key] = time.monotonic() + min(30.0,_wait_seconds(target_config,current.failures))
                        task_queue.append(task)
                        break
                    continue

                rows, finished, next_cursor = _request_media_resume(
                    target_config, client, target, current, extensions
                )
                history_key = (task_key,current.start,current.end)
                history = cursor_histories.setdefault(history_key, CursorHistory())
                history.check(next_cursor)
                raw_received = len(rows)
                excluded_hosts = validate_cdx_scope(
                    rows,target,current.start,current.end,
                    target_config.cdx_filters,target_config.cdx_match_type,
                    discard_adjacent_surt_hosts=True)
                connection_failure_streak = 0
                transient_failure_streak = 0
                request_seconds = time.monotonic() - request_started
                received = raw_received
                accepted = _accept_media_rows(rows, media)
                accepted_count = len(accepted)
                write_started = time.monotonic()
                old_seen, old_cursor = seen, current.resume_key
                old_strategy, old_merged = current.strategy, current.merged_verified
                old_failures, old_completed = current.failures, plan.completed
                old_completed_windows, old_error_id = completed, error_id
                try:
                    with database:
                        changed = upsert_media_captures(database, accepted, target_id, signature)
                        if excluded_hosts:
                            record_recovery_event(database, "media_index", "cdx_adjacent_host_excluded",
                                f"{target} {label}: discarded {excluded_hosts} adjacent out-of-scope host rows from media continuation",
                                details={"excluded":excluded_hosts,"raw_rows":received})
                        seen += received
                        if current.verify_paged_next:
                            current.strategy = "paged"
                            current.merged_verified = True
                            current.resume_key = None
                        current.failures = 0
                        if next_cursor is not None:
                            current.resume_key = next_cursor
                        if finished:
                            plan.pending.pop(0)
                            plan.completed += 1
                            completed += 1
                        complete = not plan.pending
                        _save_media_state(database, target_id, year, state_signature, encode_plan(plan), complete, seen, None if complete else error_id)
                        if error_id:
                            database.execute("UPDATE errors SET resolved=1,last_seen=? WHERE id=?", (utc_now(), error_id))
                            error_id = None
                except BaseException:
                    seen, current.resume_key = old_seen, old_cursor
                    current.strategy, current.merged_verified = old_strategy, old_merged
                    current.verify_paged_next = False
                    current.failures, plan.completed = old_failures, old_completed
                    completed, error_id = old_completed_windows, old_error_id
                    if finished and (not plan.pending or plan.pending[0] is not current):
                        plan.pending.insert(0, current)
                    raise
                history.committed(next_cursor)
                if finished:
                    cursor_histories.pop(history_key, None)
                retry_budget.progress()
                write_seconds = time.monotonic() - write_started
                if callback:
                    callback(
                        ProgressEvent(
                            "media_index",
                            f"{target} {label}: received {received:,}, accepted {accepted_count:,}, stored {changed:,} — network {request_seconds:.1f}s, database {write_seconds:.2f}s"
                            + (f"; excluded {excluded_hosts:,} adjacent out-of-scope host rows" if excluded_hosts else ""),
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
                if current.strategy == "paged" and _pagination_unavailable(exc):
                    current.pagination_supported = False
                    current.strategy = "resume"
                    with database:
                        error_id = record_error(database, "media_index", "coverage_verification_unavailable",
                            f"{target} {label}: {summarize_cdx_error(exc)}; inventory retained", retryable=True)
                        _save_media_state(database, target_id, year, state_signature, encode_plan(plan), False, seen, error_id)
                    if callback:
                        callback(ProgressEvent("media_index_incomplete",
                            f"{target}: numbered verification unavailable: {summarize_cdx_error(exc)}; discovered media retained.",
                            completed, total, {"retain_notice": True}))
                    continue
                if isinstance(exc, TransientRequestError) and exc.connection_failed:
                    connection_failure_streak += 1
                    current.failures += 1
                    network = target_config.network.normalized()
                    current.next_eligible_at = time.time() + min(5.0,network.connection_retry_seconds)
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
                    if task_queue:
                        due[task_key] = time.monotonic() + max(1.0, wait)
                        task_queue.append(task)
                        break
                    stop_event.wait(retry_budget.wait_seconds(wait) if not retry_budget.exhausted else 0)
                    if stop_event.is_set():
                        raise Stopped
                    current.next_eligible_at = 0.0
                    continue
                if isinstance(exc, TransientRequestError):
                    transient_failure_streak += 1
                    network = target_config.network.normalized()
                # Transient HTTP failures cannot justify switching an existing
                # page layout or dropping its saved cursor and page evidence.
                error_id = _defer_media_window(
                    target_config, database, plan, current, target_id, year, state_signature,
                    seen, error_id, target, label, exc, completed, total, stop_event, callback,
                    yield_to_other_tasks=bool(task_queue) or retry_budget.exhausted,
                )
                if task_queue:
                    due[task_key] = time.monotonic() + min(30.0,_wait_seconds(target_config,current.failures))
                    task_queue.append(task)
                    break
            except RuntimeError as exc:
                if isinstance(exc, PermanentRequestError) and exc.status == 403:
                    category = exc.category
                    message = site_issue_message(category, target, "media CDX indexing", exc.status)
                    # Forbidden/excluded responses are coverage blockers, not
                    # proof that the indexed inventory is empty. Preserve the
                    # exact unfinished cursor for a later explicit attempt.
                    with database:
                        error_id = record_error(
                            database, "media_index", category, f"{target} {label}: {exc}",
                            http_status=exc.status, retryable=False,
                        )
                        record_site_issue(
                            database, host_from_url(target), "media_cdx_index", category, message,
                            target=target, http_status=exc.status,
                        )
                        _save_media_state(database, target_id, year, state_signature, encode_plan(plan), False, seen, error_id)
                    blocked_tasks.add(task_key)
                    if callback:
                        callback(ProgressEvent("site_issue", message +
                            "; this scope remains unfinished; continuing other media indexing scopes.",
                            completed, total, {"target": target, "http_status": exc.status, "retain_notice": True}))
                    break
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
        accounted[task_key] = (plan.completed,max(0,plan.planned-len(default_windows)))
        retry_budget.pause()
        spent[target] = spent.get(target,0.0) + time.monotonic()-turn_started
    return bool(budget_deferred or retry_deferred or blocked_tasks)


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
    path = Path(str(row.get("path") or ""))
    if not path.is_absolute():
        path = output_dir / path
    if not path.is_file():
        raise FileNotFoundError(f"Saved document required for media discovery is missing: {document_id}")
    known_links = [str(value) for value in json_value(row.get("links_json"), []) if str(value).strip()]
    raw = safe_document_text(output_dir, str(row.get("path") or ""), max_file_bytes,
                             expected_hash=content_hash)
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
    from ..database.scope import discovery_scope_config
    source_config = discovery_scope_config(config)
    discovery_targets = source_config.targets
    targets = target_hosts(discovery_targets)
    from ..database.scope import prepare_saved_scope, saved_scope_predicate
    prepare_saved_scope(database, source_config)
    scope = saved_scope_predicate()
    total = int(database.execute('SELECT COUNT(*) FROM documents d JOIN captures c ON c.id=d.capture_id WHERE ' + scope).fetchone()[0])
    scanned = queued = 0
    cursor = database.execute(
        """
        SELECT d.id,d.path,d.links_json,d.content_hash,c.original_url,
               mdd.content_hash AS discovery_content_hash
        FROM documents d
        JOIN captures c ON c.id=d.capture_id
        LEFT JOIN media_discovery_documents mdd
          ON mdd.document_id=d.id AND mdd.query_signature=?
        WHERE """ + scope + " ORDER BY d.id",
        (signature,),
    )
    workers = min(8, max(1, int(config.workers)))
    byte_budget = 64 * 1024 * 1024
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers, thread_name_prefix="archive-discovery") as pool:
        while True:
            rows = cursor.fetchmany(64)
            if not rows:
                break
            remaining = iter(rows)
            next_row = next(remaining, None)
            futures = {}
            inflight_bytes = 0
            while next_row is not None or futures:
                if stop_event.is_set():
                    for future in futures:
                        future.cancel()
                    raise Stopped
                while next_row is not None and len(futures) < workers:
                    row = next_row
                    content_hash = str(row['content_hash'] or '')
                    if content_hash and str(row['discovery_content_hash'] or '') == content_hash:
                        scanned += 1
                        next_row = next(remaining, None)
                        continue
                    path = Path(str(row['path']))
                    if not path.is_absolute():
                        path = config.output_dir / path
                    try:
                        estimate = min(path.stat().st_size, config.max_file_bytes + 1) * 3
                    except OSError:
                        estimate = 1
                    estimate = max(1, estimate)
                    if futures and inflight_bytes + estimate > byte_budget:
                        break
                    future = pool.submit(_discover_document_media, config.output_dir,
                        config.max_file_bytes, media, targets, external_only, dict(row))
                    futures[future] = (int(row['id']), estimate)
                    inflight_bytes += estimate
                    next_row = next(remaining, None)
                if not futures:
                    continue
                done, _ = concurrent.futures.wait(futures, timeout=0.25,
                    return_when=concurrent.futures.FIRST_COMPLETED)
                with database:
                    for future in done:
                        document_id, estimate = futures.pop(future)
                        inflight_bytes -= estimate
                        try:
                            document_id, content_hash, discovered = future.result()
                        except OSError as exc:
                            record_error(database, 'media_discovery', 'local_source_unavailable',
                                str(exc), document_id=document_id, retryable=True)
                            if callback:
                                callback(ProgressEvent('media_embed',
                                    f'Document {document_id}: complete media discovery remains unresolved: {exc}'))
                            continue
                        queued += queue_media_discovery_candidates(database, signature, discovered)
                        mark_media_discovery_document(database, signature, document_id, content_hash, len(discovered))
                        resolve_errors(database, document_id=document_id, operations=('media_discovery',))
                        scanned += 1
            if callback:
                callback(ProgressEvent('media_embed',
                    f'Discovering embedded media in selected saved pages {scanned:,}/{total:,}; {queued:,} new URLs queued',
                    scanned, total))

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
            validate_cdx_scope(result.rows,url,config.from_date,config.to_date,config.cdx_filters,'exact')
            return row, result.rows, None
        if strategy == "latest":
            params = build_media_params(config, url, config.from_date, config.to_date, exact=True, page_size=1)
            params = [(key, value) for key, value in params if key not in {"limit", "showResumeKey", "resumeKey", "fastLatest"}]
            params.extend([("limit", "-1"), ("fastLatest", "true")])
            result = request_cdx_rows(
                client, endpoints, params, max_bytes=8 * 1024 * 1024, prefer_text=True
            )
            validate_cdx_scope(result.rows,url,config.from_date,config.to_date,config.cdx_filters,'exact')
            return row, result.rows, None

        raise ValueError("All-snapshot embedded indexing requires the persistent page queue")
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
    discover_documents: bool = True,
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
    if discover_documents:
        _discover_embedded_queue(config, database, stop_event, callback, signature, external_only=external_only)
    if media.snapshot_strategy == 'all':
        from .embedded_pages import index_all_embedded_pages
        index_all_embedded_pages(config, database, client, stop_event, callback, signature)
        return
    attempt_limit = (2**63-1 if config.network.persistent_retries else config.max_attempts)
    pending_total = int(database.execute(
        """SELECT COUNT(*) FROM media_discovery_queue
           WHERE query_signature=? AND state IN ('pending','error') AND lookup_attempts<?""",
        (signature, attempt_limit),
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
    row_iter = iter_media_discovery_rows(database, signature, attempt_limit)
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
                        if document_id is not None and not database.execute(
                            "SELECT 1 FROM media_discovery_queue WHERE source_document_id=? "
                            "AND (state IN ('pending','error','resolving','blocked') "
                            "OR (state='unavailable' AND COALESCE(last_error,'')!='')) LIMIT 1",
                            (document_id,)).fetchone():
                            resolve_errors(database, document_id=document_id, operations=("media_embed",))
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

def _complete_embedded_media(config, database, client, stop_event, callback, signature, *, external_only=False, acquire_ready=False):
    """Retry lookup turns without repeating saved-document discovery each turn."""
    turn = 0
    budget = NoProgressBudget(config)
    def progress_token():
        # Attempt counters and error timestamps are not successful progress.
        row = database.execute("SELECT SUM(state='indexed'),"
            "SUM(state='unavailable' AND COALESCE(last_error,'')='') FROM media_discovery_queue "
            "WHERE query_signature=?", (signature,)).fetchone()
        captures = database.execute('SELECT COUNT(*) FROM media_captures WHERE query_signature=?',
                                    (signature,)).fetchone()[0]
        return (int(captures), *tuple(row))
    previous = progress_token()
    while True:
        if stop_event.is_set():
            raise Stopped
        budget.attempted()
        try:
            index_embedded_media(config, database, client, stop_event, callback, signature,
                                 external_only=external_only, discover_documents=turn == 0)
        except RateLimitDeferred:
            if not config.network.persistent_retries:
                break
        current = progress_token()
        if current != previous:
            budget.progress()
        previous = current
        remaining = database.execute(
            "SELECT 1 FROM media_discovery_queue WHERE query_signature=? AND state IN ('pending','error') LIMIT 1",
            (signature,)).fetchone()
        if remaining and acquire_ready:
            from .downloader import download_media
            download_media(config, database, stop_event, callback,
                media_signature=signature, allow_partial_inventory=True)
        if not remaining or not config.network.persistent_retries:
            return
        if budget.exhausted:
            if callback:
                callback(ProgressEvent('media_index_incomplete', incomplete_retry_message('Embedded lookups'),
                                       detail={'retain_notice': True}))
            return
        wait = budget.wait_seconds(min(config.network.retry_max_seconds,
                   config.network.retry_base_seconds * 2**min(turn, 12)))
        if callback:
            callback(ProgressEvent("media_embed", f"Embedded lookup checkpoints retained; retry in {wait:.1f}s "
                f"(no-progress budget {budget.attempts}/{budget.limit}). Ready media is independent of unresolved URLs."))
        if stop_event.wait(wait):
            raise Stopped
        turn += 1


def index_external_embedded_media(
    config: ProjectConfig,
    database: sqlite3.Connection,
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None = None,
    acquire_ready: bool = False,
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
        _complete_embedded_media(
            config, database, client, stop_event, callback, signature, external_only=True, acquire_ready=acquire_ready
        )
        from ..coverage import require_embedded_coverage, IndexCoveragePending
        try:
            require_embedded_coverage(database, config, signature)
        except IndexCoveragePending:
            return signature
        _apply_snapshot_strategy(database, signature, config.media.snapshot_strategy)
        return signature
    finally:
        client.close()


def index_media(
    config: ProjectConfig,
    database: sqlite3.Connection,
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None = None,
    acquire_ready: bool = False,
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
        deferred = index_direct_media(config, database, client, stop_event, callback, signature, state_signature)
        _complete_embedded_media(config, database, client, stop_event, callback, signature, acquire_ready=acquire_ready)
        from ..coverage import media_coverage_gaps, require_embedded_coverage, IndexCoveragePending
        pending = bool(media_coverage_gaps(database, config))
        try:
            require_embedded_coverage(database, config, signature)
        except IndexCoveragePending:
            pending = True
        if not deferred and not pending:
            _apply_snapshot_strategy(database, signature, media.snapshot_strategy)
        elif callback:
            callback(ProgressEvent("media_index_incomplete",
                "Media index remains incomplete; leaving all previously acquired snapshots intact until full coverage is proven."))
        return signature
    finally:
        client.close()
