from __future__ import annotations

from ..retry_budget import NoProgressBudget, CursorHistory, incomplete_retry_message

import calendar
import re
import fnmatch
from urllib.parse import urlsplit
from collections import Counter, deque
import json
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable

from ..config import ProjectConfig
from ..database.repositories import get_or_create_target, record_error, record_site_issue, upsert_captures
from ..downloads.rate_limit import SharedFixedRateLimiter, shared_host_gate
from ..events import ProgressEvent, Stopped
from ..site_status import host_from_url, site_issue_message
from ..utils import utc_now
from .client import HttpClient, MalformedCDXResponse, PermanentRequestError, RateLimitDeferred, TransientRequestError, is_timeout_error, request_cdx_rows, request_cdx_json_payload, summarize_cdx_error
from .parallel import PageFetchResult, effective_page_workers, memory_limited_page_workers, iter_cdx_pages
from .parameters import (
    build_cdx_params,
    build_num_pages_params,
    build_paged_cdx_params,
    cdx_endpoints,
    cdx_paged_endpoints,
    cdx_query_signature,
    cdx_query_signatures,
    cdx_layout_signature,
    cdx_year_window,
    parse_num_pages,
    preferred_index_strategy,
)


PAGED_PIPELINE_PAGES = 24
PAGED_REQUEST_ATTEMPTS = 5
PAGED_PAGE_FAILURE_LIMIT = 5
# Only persistent HTTP 504 on a *broad, not-yet-paginated* range qualifies.
# Cap exploratory splits across the whole saved task until a CDX response
# succeeds. A host-wide outage must not explode one range into thousands.
GATEWAY_504_SPLIT_AFTER = 2
GATEWAY_504_MAX_SPLITS_WITHOUT_RESPONSE = 5
GATEWAY_504_MIN_WINDOW_DAYS = 28


@dataclass(slots=True)
class PendingWindow:
    start: str
    end: str
    resume_key: str | None = None
    failures: int = 0
    page_size: int = 0
    strategy: str = "auto"
    page: int = 0
    page_count: int = -1
    page_blocks: int = 0
    page_layout: str = ""
    next_eligible_at: float = 0.0
    pagination_supported: bool = True
    retry_pages: list[int] = field(default_factory=list)
    page_failures: dict[int, int] = field(default_factory=dict)
    gateway_504_failures: int = 0


@dataclass(slots=True)
class IndexPlan:
    pending: list[PendingWindow]
    completed: int
    planned: int
    gateway_splits_without_response: int = 0


@dataclass(slots=True)
class PagedBatch:
    results: list[PageFetchResult]
    requested_pages: list[int]
    finished: bool

    @property
    def successful(self) -> list[PageFetchResult]:
        return [item for item in self.results if item.succeeded]

    @property
    def failed(self) -> list[PageFetchResult]:
        return [item for item in self.results if not item.succeeded]


def emit(callback: Callable[[ProgressEvent], None] | None, event: ProgressEvent) -> None:
    if callback:
        callback(event)


def month_windows(config: ProjectConfig, year: int) -> list[tuple[str, str]]:
    windows: list[tuple[str, str]] = []
    for month in range(1, 13):
        last_day = calendar.monthrange(year, month)[1]
        start = max(config.from_date, f"{year:04d}{month:02d}01000000")
        end = min(config.to_date, f"{year:04d}{month:02d}{last_day:02d}235959")
        if start <= end:
            windows.append((start, end))
    return windows


def index_windows(config: ProjectConfig, target: str, year: int) -> list[tuple[str, str]]:
    """Start each target/year as one large resumable window.

    The timeout recovery engine already subdivides only the ranges that actually
    fail. Starting with twelve unconditional monthly windows multiplied request
    overhead on healthy archives, especially when resume-key batches are large.
    """
    window = cdx_year_window(config, year)
    return [window] if window else []


def encode_resume(start: str, end: str, resume: str | None) -> str:
    return json.dumps(
        {"version": 1, "window_start": start, "window_end": end, "resume_key": resume},
        separators=(",", ":"),
        sort_keys=True,
    )


def decode_resume(value: str | None) -> tuple[str, str, str | None] | None:
    if not value or not value.lstrip().startswith("{"):
        return None
    try:
        payload = json.loads(value)
        if int(payload.get("version", 1)) != 1:
            return None
        start = str(payload["window_start"])
        end = str(payload["window_end"])
        resume = payload.get("resume_key")
        return start, end, str(resume) if resume else None
    except Exception:
        return None


def encode_plan(plan: IndexPlan) -> str | None:
    if not plan.pending:
        return None
    payload = {
        "version": 5,
        "completed": int(plan.completed),
        "planned": int(plan.planned),
        "gateway_splits_without_response": int(plan.gateway_splits_without_response),
        "pending": [
            {
                "start": item.start,
                "end": item.end,
                "resume_key": item.resume_key,
                "failures": int(item.failures),
                "gateway_504_failures": int(item.gateway_504_failures),
                "page_size": int(item.page_size),
                "strategy": item.strategy,
                "page": int(item.page),
                "page_count": int(item.page_count),
                "page_blocks": int(item.page_blocks),
                "page_layout": item.page_layout,
                "next_eligible_at": float(item.next_eligible_at),
                "pagination_supported": bool(item.pagination_supported),
                "retry_pages": sorted({int(page) for page in item.retry_pages if int(page) >= 0}),
                "page_failures": {
                    str(int(page)): max(0, int(count))
                    for page, count in item.page_failures.items()
                    if int(page) >= 0 and int(count) > 0
                },
            }
            for item in plan.pending
        ],
    }
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def decode_plan(value: str | None, default_windows: list[tuple[str, str]]) -> IndexPlan:
    if value and value.lstrip().startswith("{"):
        try:
            payload = json.loads(value)
            version = int(payload.get("version", 1))
            if version in {2, 3, 4, 5}:
                pending: list[PendingWindow] = []
                for raw in payload.get("pending") or []:
                    start = str(raw["start"])
                    end = str(raw["end"])
                    if start > end:
                        continue
                    resume = raw.get("resume_key")
                    retry_pages = sorted({max(0, int(page)) for page in raw.get("retry_pages") or []})
                    raw_failures = raw.get("page_failures") or {}
                    page_failures = {
                        max(0, int(page)): max(0, int(count))
                        for page, count in raw_failures.items()
                        if int(count) > 0
                    }
                    pending.append(
                        PendingWindow(
                            start=start,
                            end=end,
                            resume_key=str(resume) if resume else None,
                            failures=max(0, int(raw.get("failures", 0))),
                            gateway_504_failures=max(0, int(raw.get("gateway_504_failures", 0))),
                            page_size=max(0, int(raw.get("page_size", 0))),
                            strategy=str(raw.get("strategy") or "auto"),
                            page=max(0, int(raw.get("page", 0))),
                            page_count=int(raw.get("page_count", -1)),
                            page_blocks=max(0, int(raw.get("page_blocks", 0))),
                            page_layout=str(raw.get("page_layout") or ""),
                            next_eligible_at=max(0.0,float(raw.get("next_eligible_at") or 0.0)),
                            pagination_supported=bool(raw.get("pagination_supported", True)),
                            retry_pages=retry_pages,
                            page_failures=page_failures,
                        )
                    )
                completed = max(0, int(payload.get("completed", 0)))
                planned = max(completed + len(pending), int(payload.get("planned", 0)))
                if pending:
                    return IndexPlan(pending, completed, planned,
                                     max(0, int(payload.get("gateway_splits_without_response", 0))))
        except Exception:
            pass

    legacy = decode_resume(value)
    if legacy:
        saved_start, saved_end, saved_resume = legacy
        later = [(start, end) for start, end in default_windows if start > saved_end]
        completed = sum(1 for _, end in default_windows if end < saved_start)
        pending = [PendingWindow(saved_start, saved_end, saved_resume)]
        pending.extend(PendingWindow(start, end) for start, end in later)
        return IndexPlan(pending, completed, completed + len(pending))

    pending = [PendingWindow(start, end) for start, end in default_windows]
    return IndexPlan(pending, 0, len(pending))


def split_window(window: PendingWindow) -> list[PendingWindow]:
    start_dt = datetime.strptime(window.start, "%Y%m%d%H%M%S")
    end_dt = datetime.strptime(window.end, "%Y%m%d%H%M%S")
    duration = end_dt - start_dt

    if duration >= timedelta(days=60):
        chunk = timedelta(days=30)
    elif duration >= timedelta(days=8):
        chunk = timedelta(days=7)
    elif duration >= timedelta(days=2):
        chunk = timedelta(days=1)
    elif duration >= timedelta(hours=12):
        chunk = timedelta(hours=6)
    elif duration >= timedelta(hours=2):
        chunk = timedelta(hours=1)
    elif duration >= timedelta(minutes=30):
        chunk = timedelta(minutes=15)
    elif duration >= timedelta(minutes=10):
        chunk = timedelta(minutes=5)
    elif duration >= timedelta(minutes=2):
        chunk = timedelta(minutes=1)
    elif duration >= timedelta(seconds=30):
        chunk = timedelta(seconds=15)
    elif duration >= timedelta(seconds=10):
        chunk = timedelta(seconds=5)
    elif duration >= timedelta(seconds=2):
        chunk = timedelta(seconds=1)
    else:
        return []

    parts: list[PendingWindow] = []
    cursor = start_dt
    while cursor <= end_dt:
        part_end = min(end_dt, cursor + chunk - timedelta(seconds=1))
        parts.append(
            PendingWindow(
                start=cursor.strftime("%Y%m%d%H%M%S"),
                end=part_end.strftime("%Y%m%d%H%M%S"),
                page_size=max(100, window.page_size // 2) if window.page_size else 0,
                strategy=window.strategy,
                page_blocks=max(0, window.page_blocks),
                pagination_supported=window.pagination_supported,
            )
        )
        cursor = part_end + timedelta(seconds=1)
    return parts if len(parts) > 1 else []



def coalesce_unstarted_micro_windows(
    plan: IndexPlan, *, seen: int, collapse_enabled: bool,
    minimum_run: int = 8, maximum_seconds: int = 3600,
) -> int:
    """Repair inherited second-sized plans without losing committed cursors.

    A collapsed query may only be reassembled if *nothing* from this scope has
    been committed, because collapse:urlkey is not compositionally equivalent
    across date ranges. Non-collapsed windows have stable member semantics.
    No cursor, numbered-page ledger, or window with failure state is touched.
    Returns the number of redundant windows removed, for checkpoint reporting.
    """
    if collapse_enabled and (seen or plan.completed):
        return 0
    if len(plan.pending) < minimum_run:
        return 0

    def eligible(window: PendingWindow) -> bool:
        if (window.resume_key is not None or window.page or window.page_count >= 0
                or window.retry_pages or window.page_failures or window.failures
                or window.gateway_504_failures):
            return False
        try:
            width = (datetime.strptime(window.end, "%Y%m%d%H%M%S") -
                     datetime.strptime(window.start, "%Y%m%d%H%M%S")).total_seconds()
        except ValueError:
            return False
        return 0 <= width <= 5

    pending = plan.pending
    repaired: list[PendingWindow] = []
    position = 0
    while position < len(pending):
        current = pending[position]
        if not eligible(current):
            repaired.append(current)
            position += 1
            continue
        next_pos = position + 1
        while next_pos < len(pending):
            next_window = pending[next_pos]
            if not eligible(next_window):
                break
            if (current.strategy != next_window.strategy or
                current.page_size != next_window.page_size or
                current.page_blocks != next_window.page_blocks or
                current.pagination_supported != next_window.pagination_supported):
                break
            try:
                left = datetime.strptime(pending[next_pos - 1].end, "%Y%m%d%H%M%S")
                right = datetime.strptime(next_window.start, "%Y%m%d%H%M%S")
                start = datetime.strptime(current.start, "%Y%m%d%H%M%S")
                finish = datetime.strptime(next_window.end, "%Y%m%d%H%M%S")
            except ValueError:
                break
            if right - left != timedelta(seconds=1) or (finish - start).total_seconds() > maximum_seconds:
                break
            next_pos += 1
        if next_pos - position < minimum_run:
            repaired.append(current)
            position += 1
            continue
        # Reconstruction retains original scope, page size, and strategy.
        repaired.append(PendingWindow(current.start, pending[next_pos - 1].end,
                                       page_size=current.page_size, strategy=current.strategy,
                                       page_blocks=current.page_blocks,
                                       pagination_supported=current.pagination_supported))
        position = next_pos
    removed = len(pending) - len(repaired)
    if removed:
        plan.pending = repaired
        plan.planned = max(plan.completed + len(repaired), plan.planned - removed)
    return removed


def _bisect_persistent_gateway_504(window: PendingWindow) -> list[PendingWindow]:
    """Conservatively split a not-yet-started CDX range after repeated 504s.

    In contrast to the response-size splitter, use exactly two inclusive,
    contiguous halves. Never discard a live resume key or numbered-page ledger.
    This helper is only reached from the failure path.
    """
    if window.resume_key or window.page or window.page_count >= 0:
        return []
    try:
        start = datetime.strptime(window.start, "%Y%m%d%H%M%S")
        end = datetime.strptime(window.end, "%Y%m%d%H%M%S")
    except ValueError:
        return []
    if end - start < timedelta(days=GATEWAY_504_MIN_WINDOW_DAYS):
        return []
    midpoint = start + timedelta(seconds=int((end - start).total_seconds() // 2))
    left_end = midpoint.strftime("%Y%m%d%H%M%S")
    right_start = (midpoint + timedelta(seconds=1)).strftime("%Y%m%d%H%M%S")
    return [
        PendingWindow(window.start, left_end,
                      page_size=window.page_size, strategy=window.strategy,
                      page_blocks=window.page_blocks,
                      pagination_supported=window.pagination_supported),
        PendingWindow(right_start, window.end,
                      page_size=window.page_size, strategy=window.strategy,
                      page_blocks=window.page_blocks,
                      pagination_supported=window.pagination_supported),
    ]


def _maybe_subdivide_persistent_gateway_504(
    database: sqlite3.Connection,
    plan: IndexPlan,
    current: PendingWindow,
    exc: TransientRequestError,
    persist_task_state: Callable[[str | None, bool, int, int | None], None],
    seen: int,
    error_id: int | None,
    callback: Callable[[ProgressEvent], None] | None,
    completed_windows: int,
    total_windows: int,
    collapse_enabled: bool = False,
) -> bool:
    """Durably replace a stuck broad range with two halves, never drop work.

    This is deliberately distinct from the ordinary split predicate: a single
    HTTP 504 is insufficient, and 429/503/connection failures never trigger it.
    A task-wide persisted budget stops an outage from generating limitless work.
    """
    details = str(exc).casefold()
    mixed_endpoint_outage = ("all cdx endpoints failed" in details and any(
        f"http {status}" in details for status in (408, 425, 429, 500, 502, 503)
    ))
    if (collapse_enabled or exc.status != 504 or exc.connection_failed or isinstance(exc, RateLimitDeferred)
            or mixed_endpoint_outage):
        current.gateway_504_failures = 0
        return False
    current.gateway_504_failures += 1
    if (current.gateway_504_failures < GATEWAY_504_SPLIT_AFTER
            or plan.gateway_splits_without_response >= GATEWAY_504_MAX_SPLITS_WITHOUT_RESPONSE):
        return False
    children = _bisect_persistent_gateway_504(current)
    if not children or not plan.pending or plan.pending[0] is not current:
        return False

    original_planned = plan.planned
    original_budget = plan.gateway_splits_without_response
    try:
        with database:
            plan.pending[0:1] = children
            plan.planned += 1
            plan.gateway_splits_without_response += 1
            persist_task_state(encode_plan(plan), False, seen, error_id)
    except BaseException:
        # The SQLite transaction rolled back: also restore the live cursor,
        # so the caller cannot accidentally persist work that wasn't committed.
        plan.pending[0:2] = [current]
        plan.planned = original_planned
        plan.gateway_splits_without_response = original_budget
        raise
    emit(callback, ProgressEvent(
        "index",
        f"Repeated HTTP 504 on {window_label(current.start, current.end)}. "
        f"Saved both nonoverlapping halves for retry; no captures marked complete "
        f"({plan.gateway_splits_without_response}/"
        f"{GATEWAY_504_MAX_SPLITS_WITHOUT_RESPONSE} exploratory splits since last response).",
        completed_windows, total_windows + 1,
    ))
    return True


def window_label(start: str, end: str) -> str:
    start_date = datetime.strptime(start, "%Y%m%d%H%M%S")
    end_date = datetime.strptime(end, "%Y%m%d%H%M%S")
    if start_date.date() == end_date.date():
        if start_date.hour == 0 and start_date.minute == 0 and end_date.hour == 23 and end_date.minute == 59:
            return start_date.strftime("%Y-%m-%d")
        if start_date.hour == end_date.hour and start_date.minute == end_date.minute:
            return f"{start_date:%Y-%m-%d %H:%M:%S}–{end_date:%H:%M:%S}"
        return f"{start_date:%Y-%m-%d %H:%M}–{end_date:%H:%M}"
    if start_date.year == end_date.year and start_date.day == 1 and end_date.month == start_date.month:
        last_day = calendar.monthrange(start_date.year, start_date.month)[1]
        if end_date.day == last_day:
            return start_date.strftime("%Y-%m")
    if start_date.year == end_date.year and start_date.month == 1 and start_date.day == 1 and end_date.month == 12 and end_date.day == 31:
        return start_date.strftime("%Y")
    return f"{start_date:%Y-%m-%d}–{end_date:%Y-%m-%d}"



def _query_is_oversized(exc: BaseException) -> bool:
    """Only explicit response-size / memory pressure justifies aggressive reduction.

    Transport errors and HTTP 5xx may be flagged `splittable` by the client for
    callers that can retry smaller work, but don't establish query-size pressure.
    """
    description = str(exc).casefold()
    return any(token in description for token in (
        "safe in-memory budget", "response exceeds", "response was larger",
        "parsing exceeded available memory", "json parsing exceeded available memory",
    ))


def _can_split_cdx_window(current: PendingWindow, exc: TransientRequestError) -> bool:
    """Split query-specific failures, never a transient Wayback outage.

    Read timeouts and malformed responses can be query-related, but are also
    caused by temporary service unavailability. A 30-minute minimum parent
    window prevents a stalled service from producing thousands of second-sized
    CDX requests. Explicit response-size pressure can still split more finely.
    """
    if not exc.splittable or exc.connection_failed:
        return False
    if current.resume_key and (
        current.failures < 3
        or (_query_is_oversized(exc) and current.page_size > 100)
    ):
        # A CDX cursor follows URL-key order, not date order. Splitting a
        # progressed date range discards the cursor and repeats earlier work.
        # Retry it first; explicit size pressure shrinks its page before any
        # restart. Keep the existing eligible fallback after repeated failures
        # so an unusable cursor cannot strand the remaining inventory forever.
        return False
    if getattr(exc, "status", None) in {408, 425, 429, 500, 502, 503, 504}:
        return False
    # Some client aggregators wrap multiple endpoint errors in a single error.
    # Do not mistake an HTML/gateway outage for a malformed oversized query.
    description = str(exc).casefold()
    if "all cdx endpoints failed" in description and any(
        f"http {status}" in description for status in (408, 425, 429, 500, 502, 503, 504)
    ):
        return False
    if _query_is_oversized(exc):
        return True
    if not (exc.timed_out or isinstance(exc, MalformedCDXResponse)
            or "same resume key twice" in description):
        return False
    try:
        duration = (datetime.strptime(current.end, "%Y%m%d%H%M%S")
                    - datetime.strptime(current.start, "%Y%m%d%H%M%S"))
    except ValueError:
        return False
    return duration >= timedelta(minutes=30)


def transient_backoff(config: ProjectConfig, failures: int) -> float:
    """Per-query no-progress backoff, not a delay on healthy CDX traffic.

    Persisted window failures survive process restart. Repeating 60-second
    gateway failures at a fixed five-second interval produces no evidence and
    unnecessarily revisits every year. Respect the configured retry ceiling.
    """
    network = config.network.normalized()
    base = max(1.0, float(network.retry_base_seconds))
    ceiling = max(base, float(network.retry_max_seconds))
    return min(ceiling, base * (2.0 ** min(12, max(0, int(failures) - 1))))


def save_state(
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
        INSERT INTO index_state(target_id,year,query_signature,resume_key,complete,seen,error_id,updated_at)
        VALUES(?,?,?,?,?,?,?,?)
        ON CONFLICT(target_id,year,query_signature) DO UPDATE SET
            resume_key=excluded.resume_key,
            complete=excluded.complete,
            seen=excluded.seen,
            error_id=excluded.error_id,
            updated_at=excluded.updated_at
        """,
        (target_id, year, signature, resume_key, int(complete), seen, error_id, utc_now()),
    )


def _adopt_compatible_index_state(
    database: sqlite3.Connection,
    target_id: int,
    year: int,
    config: ProjectConfig,
    signature: str,
) -> None:
    """Adopt earlier state whose only signature difference is page size."""
    existing = database.execute(
        "SELECT 1 FROM index_state WHERE target_id=? AND year=? AND query_signature=?",
        (target_id, year, signature),
    ).fetchone()
    if existing:
        return
    start, end = cdx_year_window(config, year) or (f"{year:04d}0101000000", f"{year:04d}1231235959")
    for candidate in cdx_query_signatures(config):
        if candidate == signature:
            continue
        state = database.execute(
            "SELECT resume_key,complete,seen,error_id,updated_at FROM index_state "
            "WHERE target_id=? AND year=? AND query_signature=?",
            (target_id, year, candidate),
        ).fetchone()
        if not state:
            continue
        # There is no current state row, so most updates are conflict-free.
        # UPDATE OR IGNORE protects projects that already contain a handful of
        # rows under both signatures after an interrupted upgrade.
        database.execute(
            "UPDATE OR IGNORE captures SET query_signature=? "
            "WHERE target_id=? AND query_signature=? AND timestamp BETWEEN ? AND ?",
            (signature, target_id, candidate, start, end),
        )
        database.execute(
            "INSERT INTO index_state(target_id,year,query_signature,resume_key,complete,seen,error_id,updated_at) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (target_id, year, signature, state["resume_key"], state["complete"], state["seen"], state["error_id"], state["updated_at"]),
        )
        database.execute(
            "DELETE FROM index_state WHERE target_id=? AND year=? AND query_signature=?",
            (target_id, year, candidate),
        )
        return


def _adopt_compatible_range_coverage(
    database: sqlite3.Connection, target_id: int, config: ProjectConfig,
    signature: str,
) -> None:
    """Adopt v1.2.4 range checkpoints before planning uncovered intervals.

    Work in SQL, not a per-snapshot Python list. Preserve old evidence and
    partial plans. Only page-layout-identical checkpoints are transferred.
    Repeated calls do nothing once the canonical range is already present.
    """
    for old in cdx_query_signatures(config):
        if old == signature:
            continue
        rows = database.execute(
            """SELECT range_start,range_end,plan_json,strategy,layout_signature,
                      complete,seen,error_id,updated_at
                 FROM index_coverage WHERE target_id=? AND query_signature=?
                 AND range_start>=? AND range_end<=?""",
            (target_id, old, config.from_date, config.to_date),
        ).fetchall()
        for item in rows:
            left, right = str(item["range_start"]), str(item["range_end"])
            present = database.execute(
                """SELECT 1 FROM index_coverage WHERE target_id=? AND
                   query_signature=? AND range_start=? AND range_end=?""",
                (target_id, signature, left, right),
            ).fetchone()
            if present:
                continue
            # Existing v1.2.4 layout signatures remain valid: changing the
            # semantic query identifier doesn't alter the numbered page layout.
            database.execute(
                """INSERT OR IGNORE INTO index_coverage
                   (target_id,query_signature,range_start,range_end,plan_json,
                    strategy,layout_signature,complete,seen,error_id,updated_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (target_id, signature, left, right, item["plan_json"],
                 item["strategy"], item["layout_signature"], item["complete"],
                 item["seen"], item["error_id"], item["updated_at"]),
            )
            database.execute(
                """INSERT OR IGNORE INTO index_pages
                   (query_signature,target_id,window_start,window_end,
                    layout_signature,page,row_count,status,updated_at)
                   SELECT ?,target_id,window_start,window_end,
                    layout_signature,page,row_count,status,updated_at
                   FROM index_pages WHERE target_id=? AND query_signature=?
                    AND window_start>=? AND window_end<=?""",
                (signature, target_id, old, left, right),
            )
            # One indexed UPDATE, not a fetchall of large capture payloads.
            # Collisions remain under their original signature, preserving
            # existing documents and evidence instead of deleting them.
            database.execute(
                """UPDATE OR IGNORE captures SET query_signature=?
                   WHERE target_id=? AND query_signature=?
                     AND timestamp BETWEEN ? AND ?""",
                (signature, target_id, old, left, right),
            )


def _record_network_event(database: sqlite3.Connection, stage: str, message: str, details: dict | None = None) -> None:
    """Bound detailed incident storage but retain durable aggregate history."""
    import hashlib
    now = utc_now()
    # Host-specific error strings often vary per URL. Group conservatively by
    # stage and message prefix, not by throwing away capture evidence.
    identity = hashlib.sha256((stage + "\0" + message[:200]).encode("utf-8", "replace")).hexdigest()[:20]
    try:
        database.execute("""INSERT INTO network_event_summaries
            (date,stage,message_fingerprint,sample_message,occurrence_count,first_seen,last_seen)
            VALUES(?,?,?,?,1,?,?) ON CONFLICT(date,stage,message_fingerprint) DO UPDATE SET
            occurrence_count=occurrence_count+1,last_seen=excluded.last_seen""",
            (now[:10],stage,identity,message[:500],now,now))
        database.execute(
            "INSERT INTO network_events(stage,message,details_json,created_at) VALUES(?,?,?,?)",
            (stage, message, json.dumps(details or {}, ensure_ascii=False, sort_keys=True), now),
        )
        # Every 256th event, trim a bounded span by indexed primary key; this
        # does not affect preserved capture errors, checkpoints or source files.
        if database.execute("SELECT last_insert_rowid()").fetchone()[0] % 256 == 0:
            database.execute("""DELETE FROM network_events WHERE id <
                (SELECT COALESCE(MAX(id),0)-10000 FROM network_events)""")
    except sqlite3.OperationalError:
        pass


def _defer_transient_window(
    config: ProjectConfig,
    database: sqlite3.Connection,
    plan: IndexPlan,
    current: PendingWindow,
    persist_task_state: Callable[[str | None, bool, int, int | None], None],
    seen: int,
    error_id: int | None,
    exc: BaseException,
    callback: Callable[[ProgressEvent], None] | None,
    completed_windows: int,
    total_windows: int,
    stop_event: threading.Event,
    yield_to_other_tasks: bool = False,
) -> int:
    current.failures += 1
    # Durable eligibility survives Stop, process exit, and project relocation.
    current.next_eligible_at = time.time() + transient_backoff(config,current.failures)
    # A busy/unreachable service does not imply a query is oversized. Preserve
    # the user's page size and ZipNum grouping during outages, so retrying does
    # not progressively multiply the requests needed for the same inventory.
    if _query_is_oversized(exc):
        current.page_size = max(100, (current.page_size or config.page_size) // 2)
        if current.strategy != "paged" and current.page_blocks > 0:
            current.page_blocks = max(1, current.page_blocks // 2)
    message = f"{type(exc).__name__}: {exc}"
    network = config.network.normalized()
    with database:
        error_id = record_error(
            database,
            "index",
            "invalid_index_response" if isinstance(exc, MalformedCDXResponse) else "transient_index_delay",
            f"{window_label(current.start, current.end)}: {message}",
            http_status=getattr(exc, 'status', None),
            retryable=True,
        )
        _record_network_event(
            database,
            "index",
            message,
            {
                "window_start": current.start,
                "window_end": current.end,
                "strategy": current.strategy,
                "failures": current.failures,
                "page": current.page,
                "page_count": current.page_count,
                "retry_pages": current.retry_pages[:100],
            },
        )
        # A host outage affects every saved window. Walking to the next
        # one-second window after each 503 creates the false appearance of
        # progress and burns requests without gaining any CDX records.
        # Only rotate for failures plausibly isolated to the query itself.
        if len(plan.pending) > 1 and _can_split_cdx_window(current, exc):
            plan.pending.append(plan.pending.pop(0))
        persist_task_state(encode_plan(plan), False, seen, error_id)

    threshold = network.failure_pause_threshold
    # Transient outages are not a terminal operation state. Every window remains
    # in the checkpoint and will be revisited automatically until stopped.

    if len(plan.pending) > 1 and _can_split_cdx_window(current, exc):
        emit(
            callback,
            ProgressEvent(
                "index",
                f"Wayback did not answer {window_label(current.start, current.end)}. Progress was saved and this query-specific failure moved behind remaining work.",
                completed_windows,
                total_windows,
            ),
        )
        return error_id

    # Do not require a manual Resume after an intermittent outage.

    wait_seconds = transient_backoff(config, current.failures)
    if yield_to_other_tasks:
        emit(callback, ProgressEvent("index", "Deferred one failed target; other unfinished targets will run while this exact cursor cools down. "
             + summarize_cdx_error(exc), completed_windows, total_windows, {"retain_notice": True}))
        return error_id
    emit(
        callback,
        ProgressEvent(
            "index",
            ("CDX response remains unverified; checkpoint retained" if isinstance(exc, MalformedCDXResponse)
             else "Wayback request remains unavailable; checkpoint retained")
            + f". {summarize_cdx_error(exc)}. Retrying in {wait_seconds:.1f}s (attempt {current.failures + 1}).",
            completed_windows,
            total_windows,
            {"retain_notice": True, "failures": current.failures},
        ),
    )
    stop_event.wait(wait_seconds)
    if stop_event.is_set():
        raise Stopped
    # This process already completed the cooldown. Do not wait twice.
    current.next_eligible_at = 0.0
    return error_id


def _client_for_config(
    config: ProjectConfig,
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None,
) -> HttpClient:
    network = config.network.normalized()
    limiter = SharedFixedRateLimiter(config.cdx_delay)
    host_gate = shared_host_gate(config.rate_limit_base_pause, config.rate_limit_max_pause)

    def on_retry(attempt: int, total: int, reason: str, wait_seconds: float) -> None:
        if "all Wayback requests paused" in reason:
            limit = f"/{total}" if total else ""
            message = f"{reason}. Shared pause {attempt}{limit} for {wait_seconds:.1f}s; one recovery probe will run next…"
            stage = "rate_limit"
        elif wait_seconds <= 0:
            message = reason
            stage = "network"
        else:
            message = f"CDX request failed ({reason}). Retrying attempt {attempt}/{total} in {wait_seconds:.1f}s…"
            stage = "index"
        emit(callback, ProgressEvent(stage, message, detail={"retain_notice": True}))

    def on_network(message: str) -> None:
        emit(callback, ProgressEvent("network", message))

    return HttpClient(
        limiter,
        1,
        min(max(config.read_timeout, 30.0), 120.0),
        config.user_agent,
        stop_event,
        retry_callback=on_retry,
        connect_timeout=min(max(config.connect_timeout, 5.0), 15.0),
        read_timeout=min(max(config.read_timeout, 30.0), 120.0),
        pool_size=network.cdx_workers,
        host_gate=host_gate,
        rate_limit_attempts=config.rate_limit_attempts or 3,
        rate_limit_max_wait=config.rate_limit_max_wait,
        network_backend=network.backend,
        trust_environment=network.trust_environment,
        network_callback=on_network,
    )


def _resolve_strategy(current: PendingWindow, config: ProjectConfig, target: str) -> None:
    desired = preferred_index_strategy(config, target)
    if current.strategy not in {"resume", "paged"}:
        current.strategy = desired
    if not current.pagination_supported and current.strategy == "paged":
        current.strategy = "resume"
    network = config.network.normalized()
    if network.index_strategy == "auto" and current.strategy == "paged" and current.page_count < 0:
        # Auto mode is the fixed reference-downloader profile.  Do not let a
        # legacy/custom numbered-page block value silently turn the ten-worker
        # Timemap pipeline back into large, low-concurrency page bodies.
        current.page_blocks = 9
    elif current.page_blocks <= 0:
        current.page_blocks = network.page_blocks


def _is_adjacent_surt_host(hostname: str, suffix: str) -> bool:
    """Recognize only adjacent URL-key prefix spill, not arbitrary other sites.

    Certain CDX wildcard searches cross a SURT prefix boundary: querying
    *.liveleak.com can yield www.liveleak+quran.com. Those records must not
    be inserted or allowed to poison an otherwise verified continuation page.
    """
    label, dot, remainder = suffix.partition(".")
    if not (dot and label and remainder and hostname.endswith("." + remainder)):
        return False
    adjacent_label = hostname[:-(len(remainder) + 1)].rsplit(".", 1)[-1]
    return adjacent_label.startswith(label) and adjacent_label != label


def validate_cdx_scope(
    rows: list, target: str, start: str, end: str,
    filters: list[str], match_type: str = "",
    *, discard_adjacent_surt_hosts: bool = False,
) -> int | None:
    """Reject *provable* CDX scope contradictions, not valid IA canonicalization.

    The service implements host/domain/prefix/exact and URL-key canonicalization.
    Local shell matching against ``netloc`` mistakenly rejected default ports,
    root hosts and genuine subdomain results after successful HTTP 200 replies.
    Only enforce inexpensive, demonstrable hostname boundaries and explicitly
    unambiguous literal paths. Server regexes remain server-owned semantics.

    By default this is strictly rejecting. An indexing caller may opt into
    discarding only a recognized adjacent SURT host spill from a successfully
    parsed CDX response. This does not change the continuation or terminal
    validation, and date/status/path/other-host contradictions remain fatal.
    The supplied list is compacted in place (no second 100k-row allocation).
    Returns the number of excluded adjacent-host rows.
    """
    require_200 = any(str(v).casefold() == "statuscode:200" for v in filters)
    raw = str(target).strip()
    raw_without_scheme = raw.split("://", 1)[-1]
    host_pattern = raw_without_scheme.split("/", 1)[0].casefold().rstrip(".")
    # A wildcard host's root domain is eligible as well as its subdomains.
    target_port = None
    if ":" in host_pattern and not host_pattern.startswith("["):
        possible_host, _, possible_port = host_pattern.rpartition(":")
        if possible_port.isdigit():
            host_pattern, target_port = possible_host, int(possible_port)
    # A trailing URL wildcard can occur before any slash (e.g. *.site.org*).
    # Such a glob is not a literal DNS suffix. Treating 'site.org*' as one
    # rejects every valid row and causes an endless unverified-response loop.
    suffix = host_pattern[2:] if (host_pattern.startswith("*.")
                                 and not any(ch in host_pattern[2:] for ch in "*?[]")) else ""
    literal_host = not any(ch in host_pattern for ch in "*?[]")
    mode = str(match_type or "").casefold()
    # Never assume Java's default SURT matching equals a Python glob.
    path = raw_without_scheme.partition("/")[2]
    simple_path = (mode in {"prefix", "exact"} and path and not any(ch in path for ch in "*?[]%"))
    write_at = 0
    discarded = 0
    for row in rows:
        timestamp, url, _mime, status, _digest, _length = row
        if (len(timestamp) != 14 or not timestamp.isascii() or not timestamp.isdigit()
                or not (start <= timestamp <= end)):
            raise MalformedCDXResponse("CDX returned a timestamp outside the requested range", splittable=False)
        if require_200 and str(status) != "200":
            raise MalformedCDXResponse("CDX returned a capture outside statuscode:200", splittable=False)
        try:
            parsed = urlsplit(str(url))
            hostname = (parsed.hostname or "").casefold().rstrip(".")
            port = parsed.port
        except ValueError as exc:
            raise MalformedCDXResponse("CDX capture contains an invalid host or port", splittable=False) from exc
        if not hostname:
            raise MalformedCDXResponse("CDX returned a capture without a valid host", splittable=False)
        # URL's :80/:443 is not a different hostname. For a configured
        # *non-default* target port, compare it only when explicitly specified.
        if target_port is not None:
            actual_port = port if port is not None else (443 if parsed.scheme == "https" else 80)
            if actual_port != target_port:
                raise MalformedCDXResponse("CDX returned a capture outside the selected target port", splittable=False)
        if suffix:
            host_ok = hostname == suffix or hostname.endswith("." + suffix)
        elif mode == "domain" and literal_host:
            host_ok = hostname == host_pattern or hostname.endswith("." + host_pattern)
        elif literal_host:
            # IA's canonical URL keys can merge www and the bare host. A
            # returned spelling of that same key is not an unrelated domain.
            host_ok = hostname.removeprefix("www.") == host_pattern.removeprefix("www.")
        else:
            host_ok = fnmatch.fnmatchcase(hostname, host_pattern) or (
                host_pattern.startswith("*.") and fnmatch.fnmatchcase(hostname, host_pattern[2:]))
        if not host_ok:
            if (discard_adjacent_surt_hosts and suffix and target_port is None
                    and _is_adjacent_surt_host(hostname, suffix)):
                discarded += 1
                continue
            raise MalformedCDXResponse(
                f"CDX local scope check rejected unrelated host {hostname!r} for target {target!r} "
                f"(matchType={mode or 'implicit'}); this is not evidence of an IA outage",
                splittable=False,
            )
        if simple_path and mode in {"prefix", "exact"} and "%" not in parsed.path:
            candidate = parsed.path.lstrip("/").casefold()
            requested = path.casefold()
            if not (candidate.startswith(requested) if mode == "prefix" else candidate == requested):
                raise MalformedCDXResponse("CDX returned a capture outside the selected literal path", splittable=False)
        if discarded:
            rows[write_at] = row
        write_at += 1
    if discarded:
        if write_at == 0:
            # A wholly foreign nonempty response could indicate a misrouted
            # request, not merely harmless prefix spill. Do not checkpoint it
            # as terminal or resume progress without any target evidence.
            raise MalformedCDXResponse(
                "CDX returned only adjacent out-of-scope hosts; no in-scope "
                "capture confirmed this continuation", splittable=False)
        del rows[write_at:]
    return discarded if discard_adjacent_surt_hosts else None


def cdx_response_budget(page_size: int) -> int:
    """Bound a large CDX response without penalizing normal 100k batches."""
    size = max(100, int(page_size))
    return min(256 * 1024 * 1024, max(64 * 1024 * 1024, size * 1536))


def _reconcile_paged_completion(current: PendingWindow, completed_pages: set[int]) -> bool:
    """End-of-window safeguard; one O(page-count) pass only on completion.

    Page counters and the cursor are a scheduling hint, not durable evidence of
    complete coverage. Every page must have a committed row-ledger entry.
    """
    if current.page_count < 0:
        return False
    missing = [page for page in range(current.page_count) if page not in completed_pages]
    if missing:
        current.retry_pages = sorted(set(current.retry_pages).union(missing))
        return False
    return True


def _select_page_batch(current: PendingWindow, workers: int) -> tuple[list[int], int]:
    workers = max(1, int(workers))
    retry_pages = sorted({page for page in current.retry_pages if 0 <= page < current.page_count})
    pages: list[int] = []
    next_page = max(0, current.page)
    new_pages_remain = next_page < current.page_count
    retry_quota = workers if not new_pages_remain else min(len(retry_pages), max(1, workers // 2))
    pages.extend(retry_pages[:retry_quota])
    while len(pages) < workers and next_page < current.page_count:
        if next_page not in retry_pages:
            pages.append(next_page)
        next_page += 1
    if len(pages) < workers:
        for page in retry_pages[retry_quota:]:
            if page not in pages:
                pages.append(page)
            if len(pages) >= workers:
                break
    return pages, next_page


def _request_paged_batch(
    client: HttpClient,
    config: ProjectConfig,
    target: str,
    current: PendingWindow,
    stop_event: threading.Event,
    consume_success: Callable[[PageFetchResult], None] | None = None,
    completed_pages: set[int] | None = None,
    deadline: float | None = None,
) -> PagedBatch:
    endpoints = cdx_paged_endpoints(config)
    network = config.network.normalized()
    effective_layout = cdx_layout_signature(config, current.start, current.end, "paged", current.page_blocks)
    if current.page_count >= 0 and current.page_layout != effective_layout:
        # Legacy plans without a bound count, and plans whose endpoint/layout
        # changed, must revalidate page numbering before any completion.
        current.page_count = -1
        current.page = 0
        current.retry_pages.clear()
        current.page_failures.clear()
    if current.page_count < 0:
        current.page_count = _request_paged_count(
            client,
            endpoints,
            build_num_pages_params(config, target, current.start, current.end, current.page_blocks),
            config,
            stop_event,
        )
        current.page_layout = effective_layout
        current.page = min(current.page, current.page_count)
        current.retry_pages = [page for page in current.retry_pages if page < current.page_count]
    if current.page >= current.page_count and not current.retry_pages:
        return PagedBatch([], [], True)

    body_limit = (192 * 1024 * 1024 if current.page_blocks <= 0
                  else max(64 * 1024 * 1024, current.page_blocks * 12 * 1024 * 1024))
    page_workers = memory_limited_page_workers(
        effective_page_workers(network.cdx_workers, current.page_blocks),
        body_limit, network.cdx_memory_budget_mb,
    )
    # Bound each turn to at most two worker waves (and 24 pages). A rolling
    # worker pool remains saturated, but target time budgets and fairness
    # cannot be trapped behind 1,000 unrelated pages.
    pages, next_page = _select_page_batch(current, min(PAGED_PIPELINE_PAGES, max(page_workers, page_workers * 2)))
    completed_pages = completed_pages or set()
    requested_pages = [page for page in pages if page not in completed_pages]
    if not requested_pages:
        current.page = next_page
        current.retry_pages = [page for page in current.retry_pages if page not in completed_pages]
        finished = current.page >= current.page_count and not current.retry_pages
        return PagedBatch([], pages, finished)
    results: list[PageFetchResult] = []
    for result in iter_cdx_pages(
        client,
        endpoints,
        requested_pages,
        lambda page: build_paged_cdx_params(config, target, current.start, current.end, page, current.page_blocks),
        stop_event,
        workers=page_workers,
        # Server-sized pages can contain far more ZipNum blocks than the old
        # pageSize=9 transport. Keep a generous per-response safety ceiling while
        # concurrency is independently capped by effective_page_workers().
        max_bytes=body_limit,
        prefer_text=False,
        json_only=True,
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
    finished = current.page >= current.page_count and not current.retry_pages
    return PagedBatch(results, pages, finished)


def _request_resume(
    client: HttpClient,
    config: ProjectConfig,
    target: str,
    current: PendingWindow,
) -> tuple[list[tuple[str, str, str, str, str, str]], bool, str | None]:
    # Read the next cursor, but do not advance it before the rows are durable.
    page_size = current.page_size or config.page_size
    result = request_cdx_rows(
        client,
        cdx_endpoints(config),
        build_cdx_params(config, target, current.start, current.end, current.resume_key, page_size=page_size),
        max_bytes=cdx_response_budget(page_size),
        prefer_text=True,
    )
    rows, next_resume = result.rows, result.resume_key
    if next_resume:
        if next_resume == current.resume_key:
            raise TransientRequestError("CDX returned the same resume key twice", splittable=True)
        return rows, False, next_resume
    # If the service returned exactly a full requested page but omitted the
    # resume token, exhaustion and a clean-prefix truncation look identical.
    # Confirm once through alternate serialization, never silently advance a
    # checkpoint whose continuation evidence is contradictory.
    if len(rows) >= page_size and isinstance(client, HttpClient):
        confirmation = request_cdx_rows(
            client, cdx_endpoints(config),
            build_cdx_params(config, target, current.start, current.end,
                             current.resume_key, page_size=page_size + 1),
            max_bytes=cdx_response_budget(page_size + 1), prefer_text=False,
        )
        if confirmation.resume_key and confirmation.resume_key != current.resume_key:
            # The alternate response supplied the missing continuation. Accept
            # its complete rows and cursor together, rather than rejecting the
            # very evidence needed to leave this page.
            return confirmation.rows, False, confirmation.resume_key
        if confirmation.resume_key or confirmation.rows != rows:
            raise MalformedCDXResponse(
                "Full-sized CDX page had no continuation token and alternate "
                "serialization did not confirm terminal membership", splittable=False)
    return rows, True, None


def _paged_failure_error(failures: list[PageFetchResult]) -> BaseException:
    if not failures:
        return TransientRequestError("unknown paged CDX failure", splittable=False)
    return failures[0].error or TransientRequestError("unknown paged CDX failure", splittable=False)


def _is_pagination_unavailable(exc: BaseException) -> bool:
    message = str(exc)
    return isinstance(exc, RuntimeError) and ("HTTP 400" in message or "page-count" in message)


def _is_permanent_page_error(exc: BaseException) -> bool:
    if isinstance(exc, (TransientRequestError, RateLimitDeferred)):
        return False
    if _is_pagination_unavailable(exc):
        return False
    return isinstance(exc, RuntimeError)


def coalesce_untouched_range_plan(plan: IndexPlan, start: str, end: str, seen: int) -> bool:
    """Reassemble an empty inherited range, not a progressed traversal."""
    if seen or plan.completed or plan.gateway_splits_without_response or len(plan.pending) < 2:
        return False
    windows = sorted(plan.pending, key=lambda w: w.start)
    if windows[0].start != start or windows[-1].end != end:
        return False
    for i, window in enumerate(windows):
        if (window.resume_key or window.page or window.page_count >= 0 or window.page_layout
                or window.retry_pages or window.strategy not in {"auto", "resume"}):
            return False
        if i and _shift_timestamp(windows[i - 1].end, 1) != window.start:
            return False
    plan.pending = [PendingWindow(start, end, failures=max(w.failures for w in windows),
                                  page_size=min((w.page_size for w in windows if w.page_size), default=0),
                                  next_eligible_at=max(w.next_eligible_at for w in windows))]
    plan.planned = 1
    return True


def index_archive(
    config: ProjectConfig,
    database: sqlite3.Connection,
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None = None,
) -> bool:
    config = config.normalized()
    client = _client_for_config(config, stop_event, callback)
    if not config.cdx_collapses:
        emit(callback, ProgressEvent("index", "Warning: no CDX collapse is selected. Every archived snapshot may be returned."))

    # state_kind is either the legacy per-year checkpoint model or Audit3's
    # explicit range coverage model. Older projects retain year scope so their
    # server-side collapse boundaries do not silently change.
    tasks: list[tuple[str, ProjectConfig, int, str, int | None, str, str, list[tuple[str, str]], str]] = []
    for target in config.targets:
        target_config = config.for_target(target)
        signature = cdx_query_signature(target_config)
        target_id = get_or_create_target(database, target, target_config.settings_for_target(target))
        if target_config.text_collapse_scope == "range":
            with database:
                _adopt_compatible_range_coverage(database, target_id, target_config, signature)
        if target_config.text_collapse_scope == "year":
            emit(callback, ProgressEvent("index", f"Legacy yearly scope is active for {target}; existing selection/checkpoints are preserved. "
                "Settings → Connection and indexing → Index date scope controls range versus year."))
            for year in range(target_config.from_year, target_config.to_year + 1):
                windows = index_windows(target_config, target, year)
                if windows:
                    tasks.append((target, target_config, target_id, "year", year, windows[0][0], windows[-1][1], windows, signature))
        else:
            for gap_start, gap_end in uncovered_index_ranges(
                database, target_id, signature, target_config.from_date, target_config.to_date
            ):
                tasks.append((target, target_config, target_id, "range", None, gap_start, gap_end, [(gap_start, gap_end)], signature))

    total_windows = sum(len(windows) for *_prefix, windows, _signature in tasks)
    completed_windows = 0
    connection_failure_streak = 0
    transient_failure_streak = 0
    # A target gets bounded work turns. Stalled queries retain their exact
    # durable state, but may no longer monopolize the operation for hours.
    task_queue = deque(tasks)
    due: dict[tuple[str, str, int | None, str, str], float] = {}
    spent: dict[str, float] = {}
    retry_budgets = {}
    cursor_histories = {}
    retry_deferred = set()
    budget_deferred: set[str] = set()
    accounted: dict[tuple, tuple[int, int]] = {}
    remaining_tasks = Counter(task[0] for task in tasks)

    def queue_status():
        deferred_targets = budget_deferred | {key[0] for key in retry_deferred}
        cooling_targets = {key[0] for key, ready_at in due.items()
                           if ready_at > time.monotonic() and key[0] in remaining_tasks}
        retrying_targets = {key[0] for key, budget in retry_budgets.items()
                            if budget.attempts and key[0] in remaining_tasks} - deferred_targets
        emit(callback, ProgressEvent('index_status',
            f"Indexing queue: {len(remaining_tasks):,} target(s) remain for completion checks; "
            f"{len(retrying_targets):,} retrying, {len(cooling_targets):,} cooling, "
            f"{len(deferred_targets):,} deferred with checkpoints retained. "
            "Capture downloads are a separate phase.",
            detail={'unfinished_targets': len(remaining_tasks), 'retrying_targets': len(retrying_targets),
                    'cooling_targets': len(cooling_targets), 'deferred_targets': len(deferred_targets)}))

    def task_complete(target):
        remaining_tasks[target] -= 1
        if remaining_tasks[target] <= 0:
            del remaining_tasks[target]

    queue_status()
    try:
        while task_queue:
            if stop_event.is_set():
                raise Stopped
            task = task_queue.popleft()
            target, target_config, target_id, state_kind, year, task_start, task_end, default_windows, signature = task
            task_key = (target, state_kind, year, task_start, task_end)
            if target in budget_deferred or task_key in retry_deferred:
                continue
            ready = due.get(task_key, 0.0)
            now = time.monotonic()
            if ready > now:
                task_queue.append(task)
                # Never spin through an all-cooling queue; the Stop event
                # interrupts this small bounded wait immediately.
                future_times = [due.get((t[0], t[3], t[4], t[5], t[6]), 0.0)
                                for t in task_queue]
                if future_times and min(future_times) > now:
                    # Remain almost idle during shared outages, but permit
                    # immediate user Stop and prompt recovery at eligibility.
                    stop_event.wait(min(5.0, max(0.05, min(future_times) - now)))
                continue
            due.pop(task_key, None)
            turn_started = time.monotonic()
            requests_this_turn = 0
            yielded = False
            budget_limit = target_config.index_target_minutes * 60.0 if target_config.skip_slow_index_targets else None

            def persist_task_state(plan_json: str | None, complete: bool, seen_value: int, error_value: int | None) -> None:
                if state_kind == "year":
                    assert year is not None
                    save_state(database, target_id, year, signature, plan_json, complete, seen_value, error_value)
                else:
                    try:
                        active = plan.pending[0] if plan.pending else None
                    except (NameError, UnboundLocalError):
                        active = None
                    strategy = active.strategy if active is not None else "resume"
                    layout = cdx_layout_signature(
                        target_config, task_start, task_end, strategy, active.page_blocks if active is not None else 0
                    )
                    save_coverage_state(database, target_id, signature, task_start, task_end, plan_json, complete,
                                        seen_value, error_value, strategy, layout)

            scope_label = str(year) if state_kind == "year" else window_label(task_start, task_end)
            if state_kind == "year":
                assert year is not None
                with database:
                    _adopt_compatible_index_state(database, target_id, year, target_config, signature)
                state = database.execute(
                    "SELECT resume_key,complete,seen,error_id FROM index_state WHERE target_id=? AND year=? AND query_signature=?",
                    (target_id, year, signature),
                ).fetchone()
                state_plan = state["resume_key"] if state else None
            else:
                state = database.execute(
                    """SELECT plan_json,complete,seen,error_id FROM index_coverage
                       WHERE target_id=? AND query_signature=? AND range_start=? AND range_end=?""",
                    (target_id, signature, task_start, task_end),
                ).fetchone()
                state_plan = state["plan_json"] if state else None
            if state and state["complete"]:
                task_complete(target)
                completed_windows += len(default_windows)
                emit(callback, ProgressEvent("index", f"Already indexed {target} for {scope_label}", completed_windows, total_windows))
                continue

            seen = int(state["seen"] or 0) if state else 0
            error_id = int(state["error_id"]) if state and state["error_id"] else None
            plan = decode_plan(state_plan, default_windows)
            if state_kind == "range" and not seen and not plan.completed:
                durable = database.execute(
                    "SELECT 1 FROM captures WHERE target_id=? AND query_signature=? LIMIT 1",
                    (target_id, signature),
                ).fetchone() or database.execute(
                    "SELECT 1 FROM index_pages WHERE target_id=? AND query_signature=? LIMIT 1",
                    (target_id, signature),
                ).fetchone()
                if not durable and coalesce_untouched_range_plan(plan, task_start, task_end, seen):
                    with database:
                        persist_task_state(encode_plan(plan), False, seen, error_id)
                    emit(callback, ProgressEvent("index", "Reassembled untouched legacy windows into the full selected date range; no committed work changed."))
            old_pending, old_planned = plan.pending, plan.planned
            removed = coalesce_unstarted_micro_windows(
                plan, seen=seen, collapse_enabled=bool(target_config.cdx_collapses))
            if removed:
                try:
                    with database:
                        persist_task_state(encode_plan(plan), False, seen, error_id)
                        record_recovery_event(database, "index", "inherited_micro_window_repair",
                            f"Combined {removed:,} untouched one-second CDX windows without altering committed checkpoints.",
                            details={"target": target, "remaining_windows": len(plan.pending)})
                except BaseException:
                    plan.pending, plan.planned = old_pending, old_planned
                    raise
                emit(callback, ProgressEvent("index", f"Repaired {removed:,} inherited tiny CDX windows; saved checkpoint before new requests."))
            elif target_config.cdx_collapses and seen and len(plan.pending) >= 8:
                emit(callback, ProgressEvent("index", "Existing fragmented collapsed CDX checkpoint retained: "
                    "automatic reassembly could change which timestamp represents a URL."))
            counted_complete, counted_extra = accounted.get(task_key, (0, 0))
            completed_windows += plan.completed - counted_complete
            extra = plan.planned - len(default_windows)
            total_windows += extra - counted_extra
            accounted[task_key] = (plan.completed, extra)

            retry_budget = retry_budgets.setdefault(task_key, NoProgressBudget(target_config))
            retry_budget.resume()
            while plan.pending:
                if retry_budget.exhausted:
                    with database:
                        previous_error = database.execute('SELECT http_status,message FROM errors WHERE id=?',
                                                          (error_id,)).fetchone() if error_id else None
                        detail = retry_budget.details()
                        error_id = record_error(database, 'index', 'retry_budget_exhausted',
                            f"{target}: {detail['attempts_without_progress']}/{detail['attempt_limit']} attempts "
                            f"without progress; {detail['active_no_progress_seconds']:.1f}s active recovery time "
                            "(time queued behind other targets excluded). Last failure: "
                            + (str(previous_error[1])[:1200] if previous_error else 'no verified response'),
                            http_status=previous_error[0] if previous_error else None, retryable=True)
                        persist_task_state(encode_plan(plan), False, seen, error_id)
                    retry_deferred.add(task_key)
                    emit(callback, ProgressEvent("index_deferred", incomplete_retry_message(target),
                         completed_windows, total_windows, {**detail, "retain_notice": True}))
                    break
                active = spent.get(target, 0.0) + (time.monotonic() - turn_started)
                if budget_limit is not None and active >= budget_limit:
                    with database:
                        error_id = record_error(database, "index", "deferred_time_budget",
                            f"{target}: indexing time budget {target_config.index_target_minutes:g} minutes reached; unfinished cursor retained",
                            retryable=True)
                        persist_task_state(encode_plan(plan), False, seen, error_id)
                    budget_deferred.add(target)
                    emit(callback, ProgressEvent("index_deferred", f"Deferred {target}: indexing budget reached; saved unfinished work for a later Resume."))
                    break
                # Keep healthy large inventories moving without paying the
                # plan reconstruction cost after every 100k-row response.
                if requests_this_turn >= 8 and task_queue:
                    with database:
                        persist_task_state(encode_plan(plan), False, seen, error_id)
                    task_queue.append(task)
                    yielded = True
                    break
                if stop_event.is_set():
                    with database:
                        persist_task_state(encode_plan(plan), False, seen, error_id)
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
                requests_this_turn += 1
                _resolve_strategy(current, target_config, target)
                label = window_label(current.start, current.end)
                if current.strategy == "paged":
                    if current.page_count < 0:
                        phase = "Timemap page-count setup"
                    else:
                        phase = (
                            f"parallel Timemap page queue; next {current.page:,}/{current.page_count:,}; "
                            f"{len(current.retry_pages)} retry"
                        )
                elif current.resume_key:
                    phase = "resume-key continuation"
                else:
                    phase = ("legacy yearly resume traversal" if state_kind == "year" else
                             "full-range resume traversal" if (current.start, current.end) ==
                             (target_config.from_date, target_config.to_date) else "saved-window resume traversal")
                emit(callback, ProgressEvent(
                    "index",
                    f"{target} • {label} • {phase}; seen {seen:,}",
                    completed_windows, total_windows,
                    {
                        "phase": phase, "target": target, "scope": label,
                        "strategy": current.strategy, "seen": seen,
                        "work_unit": completed_windows, "work_units": total_windows,
                    },
                ))
                request_started = time.monotonic()
                retry_budget.attempted()
                try:
                    if current.strategy == "paged":
                        received = 0
                        changed = 0
                        write_seconds = 0.0
                        batch_pages_done = 0
                        last_page_progress = time.monotonic()

                        completed_pages = {
                            int(row[0]) for row in database.execute(
                                """SELECT page FROM index_pages WHERE query_signature=? AND target_id=?
                                   AND window_start=? AND window_end=? AND status='complete'
                                   AND layout_signature=?""",
                                (signature, target_id, current.start, current.end,
                                 cdx_layout_signature(target_config, current.start, current.end, 'paged', current.page_blocks)),
                            )
                        }

                        def store_completed_page(result: PageFetchResult) -> None:
                            nonlocal received, changed, write_seconds, batch_pages_done, last_page_progress, seen
                            if result.resume_key is not None:
                                # A numbered page that advertises additional work
                                # cannot be committed as a terminal page.  Do not
                                # silently discard the server's continuation key.
                                raise TransientRequestError(
                                    "numbered CDX page returned an unexpected continuation; preserving incomplete coverage",
                                    splittable=False,
                                )
                            page_received = len(result.rows)
                            excluded_hosts = validate_cdx_scope(
                                result.rows, target, current.start, current.end,
                                target_config.cdx_filters, target_config.cdx_match_type,
                                discard_adjacent_surt_hosts=True,
                            )
                            write_started = time.monotonic()
                            # Capture rows and their page checkpoint are one
                            # transaction: after a power loss the page is either
                            # wholly eligible for retry or already known complete.
                            with database:
                                changed += upsert_captures(database, result.rows, target_id, signature)
                                if excluded_hosts:
                                    record_recovery_event(database, "index", "cdx_adjacent_host_excluded",
                                        f"{target} {label}: discarded {excluded_hosts} adjacent out-of-scope host rows from CDX page {result.page}",
                                        details={"excluded": excluded_hosts, "page": result.page, "raw_rows": page_received})
                                database.execute(
                                    """INSERT INTO index_pages(query_signature,target_id,window_start,window_end,page,row_count,status,layout_signature,updated_at)
                                       VALUES(?,?,?,?,?,?,'complete',?,?)
                                       ON CONFLICT(query_signature,target_id,window_start,window_end,layout_signature,page) DO UPDATE SET
                                       row_count=excluded.row_count,status='complete',layout_signature=excluded.layout_signature,updated_at=excluded.updated_at""",
                                    (signature, target_id, current.start, current.end, int(result.page), page_received,
                                     cdx_layout_signature(target_config, current.start, current.end, 'paged', current.page_blocks), utc_now()),
                                )
                            retry_budget.progress()
                            completed_pages.add(int(result.page))
                            write_seconds += time.monotonic() - write_started
                            received += page_received
                            seen += page_received
                            batch_pages_done += 1
                            now = time.monotonic()
                            if (
                                now - last_page_progress >= 1.0
                                or len(completed_pages) >= current.page_count
                            ):
                                emit(
                                    callback,
                                    ProgressEvent(
                                        "index",
                                        f"{target} {label}: completed {len(completed_pages):,}/{current.page_count:,} Timemap pages; "
                                        f"this block finished {batch_pages_done:,} pages and received {received:,} captures",
                                        completed_windows,
                                        total_windows,
                                    ),
                                )
                                last_page_progress = now
                            # Release the largest object while sibling requests
                            # are still in flight instead of retaining a full
                            # worker batch in memory.
                            result.rows.clear()

                        batch = _request_paged_batch(
                            client, target_config, target, current, stop_event, store_completed_page, completed_pages,
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
                            if batch.finished:
                                # A crash after a successful page commit may
                                # leave the plan cursor behind its page ledger.
                                # Never mark a window complete on a cursor
                                # alone: every numbered page must be durable.
                                if not _reconcile_paged_completion(current, completed_pages):
                                    batch.finished = False
                                else:
                                    retry_budget.progress()
                                    plan.pending.pop(0)
                                    plan.completed += 1
                                    completed_windows += 1
                            complete = not plan.pending
                            persist_task_state(encode_plan(plan), complete, seen, None if complete else error_id)
                            if error_id and not failures:
                                database.execute("UPDATE errors SET resolved=1,last_seen=? WHERE id=?", (utc_now(), error_id))
                                error_id = None
                        pages_done = len(successes)
                        emit(
                            callback,
                            ProgressEvent(
                                "index",
                                f"{target} {label}: {pages_done}/{len(batch.requested_pages)} pages, received {received:,}, stored {changed:,}, seen {seen:,} — network {request_seconds:.1f}s, database {write_seconds:.2f}s",
                                completed_windows,
                                total_windows,
                            ),
                        )
                        if not failures:
                            continue

                        failure_exc = _paged_failure_error(failures)
                        if not successes and all(
                            isinstance(item.error, TransientRequestError) and item.error.connection_failed
                            for item in failures
                        ):
                            # Page workers return failures as results so successful
                            # siblings can still be committed. A complete connection
                            # failure must nevertheless enter the operation-wide
                            # connection circuit instead of being mistaken for one
                            # repeatedly slow CDX page.
                            raise failure_exc
                        if not successes and _is_pagination_unavailable(failure_exc):
                            current.pagination_supported = False
                            current.strategy = "resume"
                            current.page = 0
                            current.page_count = -1
                            current.retry_pages.clear()
                            current.page_failures.clear()
                            with database:
                                persist_task_state(encode_plan(plan), False, seen, error_id)
                            emit(callback, ProgressEvent("index", f"Paged CDX is unavailable for {target} {label}; continuing with resume keys.", completed_windows, total_windows))
                            continue
                        permanent = next((item.error for item in failures if item.error and _is_permanent_page_error(item.error)), None)
                        if permanent is not None:
                            raise permanent
                        highest_page_failures = max(current.page_failures.values(), default=0)
                        new_pages_remain = current.page < current.page_count
                        temporary_service_failure = any(
                            isinstance(item.error, TransientRequestError) and (
                                item.error.timed_out or item.error.connection_failed
                                or item.error.status in {408, 425, 429, 500, 502, 503, 504}
                            ) for item in failures
                        )
                        keep_recovering = target_config.network.persistent_retries and temporary_service_failure
                        if not keep_recovering and highest_page_failures >= PAGED_PAGE_FAILURE_LIMIT and (
                            not successes or not new_pages_remain
                        ):
                            # Keep the exact failed page numbers. Converting a
                            # nearly completed Timemap year into broad resume-key
                            # windows repeated successful work and was the main
                            # source of earlier development-build indexing stalls.
                            with database:
                                error_id = record_error(
                                    database,
                                    "index",
                                    "timemap_pages_unavailable",
                                    f"{target} {label}: {len(current.retry_pages)} Timemap page(s) remained unavailable "
                                    f"after {highest_page_failures} attempts: {failure_exc}",
                                    retryable=True,
                                )
                                record_recovery_event(
                                    database,
                                    "index",
                                    "timemap_page_queue_saved",
                                    f"{target} {label}: saved only the failed Timemap pages for Resume.",
                                    details={"pages": current.retry_pages[:100], "attempts": highest_page_failures},
                                )
                                persist_task_state(encode_plan(plan), False, seen, error_id)
                            retry_deferred.add(task_key)
                            emit(callback, ProgressEvent('index_deferred', incomplete_retry_message(target),
                                 completed_windows,total_windows,{'retain_notice':True}))
                            break
                        with database:
                            record_recovery_event(
                                database, "index", "transient_page_retry",
                                f"{target} {label}: {len(failures)} CDX page(s) requeued: {failure_exc}",
                                details={"pages": [item.page for item in failures]},
                            )
                            _record_network_event(
                                database,
                                "index_page",
                                str(failure_exc),
                                {"pages": [item.page for item in failures], "retry_pages": current.retry_pages[:100]},
                            )
                            persist_task_state(encode_plan(plan), False, seen, error_id)
                        if successes:
                            emit(callback, ProgressEvent("index", f"Requeued {len(failures)} slow page(s) while continuing with untouched pages.", completed_windows, total_windows))
                            continue
                        if completed_pages and not new_pages_remain:
                            emit(
                                callback,
                                ProgressEvent(
                                    "index",
                                    f"Retrying {len(current.retry_pages)} isolated Timemap page(s); all successful pages remain checkpointed.",
                                    completed_windows,
                                    total_windows,
                                ),
                            )
                            continue
                        error_id = _defer_transient_window(
                            target_config, database, plan, current, persist_task_state, seen, error_id,
                            failure_exc, callback, completed_windows, total_windows, stop_event,
                            yield_to_other_tasks=bool(task_queue) or retry_budget.exhausted,
                        )
                        if task_queue:
                            due[task_key] = time.monotonic() + transient_backoff(target_config, current.failures)
                            task_queue.append(task)
                            yielded = True
                            break
                        continue

                    rows, finished, next_cursor = _request_resume(client, target_config, target, current)
                    history_key = (task_key,current.start,current.end)
                    history = cursor_histories.setdefault(history_key, CursorHistory())
                    history.check(next_cursor)
                    raw_received = len(rows)
                    excluded_hosts = validate_cdx_scope(
                        rows, target, current.start, current.end,
                        target_config.cdx_filters, target_config.cdx_match_type,
                        discard_adjacent_surt_hosts=True,
                    )
                    connection_failure_streak = 0
                    transient_failure_streak = 0
                    request_seconds = time.monotonic() - request_started
                    received = raw_received
                    write_started = time.monotonic()
                    upsert_seconds = checkpoint_seconds = commit_seconds = 0.0
                    # Roll back in-memory checkpoint changes if the SQLite write
                    # or commit fails. Otherwise an error handler might persist a
                    # cursor that skips an uncommitted CDX response.
                    old_seen, old_cursor = seen, current.resume_key
                    old_failures, old_completed = current.failures, plan.completed
                    old_gateway_count = current.gateway_504_failures
                    old_gateway_budget = plan.gateway_splits_without_response
                    old_completed_windows, old_error_id = completed_windows, error_id
                    try:
                        with database:
                            upsert_started = time.monotonic()
                            changed = upsert_captures(database, rows, target_id, signature)
                            upsert_seconds = time.monotonic() - upsert_started
                            checkpoint_started = time.monotonic()
                            seen += received
                            current.failures = 0
                            current.gateway_504_failures = 0
                            plan.gateway_splits_without_response = 0
                            if next_cursor is not None:
                                current.resume_key = next_cursor
                            if finished:
                                plan.pending.pop(0)
                                plan.completed += 1
                                completed_windows += 1
                            complete = not plan.pending
                            persist_task_state(encode_plan(plan), complete, seen, None if complete else error_id)
                            if excluded_hosts:
                                record_recovery_event(database, "index", "cdx_adjacent_host_excluded",
                                    f"{target} {label}: discarded {excluded_hosts} adjacent out-of-scope host rows from CDX continuation",
                                    details={"excluded": excluded_hosts, "raw_rows": received})
                            if error_id:
                                database.execute("UPDATE errors SET resolved=1,last_seen=? WHERE id=?", (utc_now(), error_id))
                                error_id = None
                            checkpoint_seconds = time.monotonic() - checkpoint_started
                        commit_seconds = time.monotonic() - write_started - upsert_seconds - checkpoint_seconds
                    except BaseException:
                        seen, current.resume_key = old_seen, old_cursor
                        current.failures, plan.completed = old_failures, old_completed
                        current.gateway_504_failures = old_gateway_count
                        plan.gateway_splits_without_response = old_gateway_budget
                        completed_windows, error_id = old_completed_windows, old_error_id
                        if finished and (not plan.pending or plan.pending[0] is not current):
                            plan.pending.insert(0, current)
                        raise
                    history.committed(next_cursor)
                    if finished:
                        cursor_histories.pop(history_key, None)
                    retry_budget.progress()
                    write_seconds = time.monotonic() - write_started
                    emit(
                        callback,
                        ProgressEvent(
                            "index",
                            f"{target} {label}: received {received:,}, stored {changed:,}, seen {seen:,}; "
                            f"network/parse/scope {request_seconds:.1f}s, database {write_seconds:.2f}s "
                            f"(upsert {upsert_seconds:.2f}s, state {checkpoint_seconds:.2f}s, commit {commit_seconds:.2f}s)"
                            + (f"; excluded {excluded_hosts:,} adjacent out-of-scope host rows" if excluded_hosts else ""),
                            completed_windows,
                            total_windows,
                            {"retain_notice": True, "received": received, "stored": changed,
                             "seen": seen, "request_parse_scope_seconds": request_seconds,
                             "database_seconds": write_seconds, "upsert_seconds": upsert_seconds,
                             "checkpoint_seconds": checkpoint_seconds, "commit_seconds": commit_seconds,
                             "out_of_scope_excluded": excluded_hosts},
                        ),
                    )
                    # Do not retain the previous 50k-row page while the next
                    # response is being downloaded and parsed. Python evaluates
                    # the next assignment's right-hand side before releasing the
                    # old local value, which otherwise briefly doubles peak memory.
                    rows.clear()
                except Stopped:
                    with database:
                        persist_task_state(encode_plan(plan), False, seen, error_id)
                    raise
                except RateLimitDeferred as exc:
                    error_id = _defer_transient_window(
                        target_config, database, plan, current, persist_task_state, seen, error_id,
                        exc, callback, completed_windows, total_windows, stop_event,
                        yield_to_other_tasks=bool(task_queue) or retry_budget.exhausted,
                    )
                    if task_queue:
                        due[task_key] = time.monotonic() + transient_backoff(target_config, current.failures)
                        task_queue.append(task)
                        yielded = True
                        break
                except TransientRequestError as exc:
                    if exc.connection_failed:
                        current.gateway_504_failures = 0
                        connection_failure_streak += 1
                        current.failures += 1
                        network = target_config.network.normalized()
                        current.next_eligible_at = time.time() + min(60.0,max(2.0,network.connection_retry_seconds)
                            * (2 ** min(5,max(0,connection_failure_streak-1))))
                        with database:
                            error_id = record_error(
                                database,
                                "index",
                                "wayback_connection_unavailable",
                                f"{target} {label}: {exc}",
                                retryable=True,
                            )
                            _record_network_event(
                                database,
                                "connection",
                                str(exc),
                                {"streak": connection_failure_streak, "target": target, "window": label},
                            )
                            persist_task_state(encode_plan(plan), False, seen, error_id)
                        # Preserve the saved checkpoint and retry autonomously. A
                        # user Stop still interrupts waits immediately.
                        wait_seconds = min(60.0, max(2.0, network.connection_retry_seconds)
                                           * (2 ** min(5, max(0, connection_failure_streak - 1))))
                        emit(
                            callback,
                            ProgressEvent(
                                "network",
                                f"Wayback connection setup failed. Retrying the same saved request in {wait_seconds:.1f}s "
                                f"({connection_failure_streak}/{network.connection_failure_pause_threshold})…",
                                completed_windows,
                                total_windows,
                            ),
                        )
                        if task_queue:
                            due[task_key] = time.monotonic() + min(60.0, wait_seconds)
                            task_queue.append(task)
                            yielded = True
                            break
                        stop_event.wait(retry_budget.wait_seconds(wait_seconds) if not retry_budget.exhausted else 0)
                        if stop_event.is_set():
                            raise Stopped
                        current.next_eligible_at = 0.0
                        continue
                    if _maybe_subdivide_persistent_gateway_504(
                        database, plan, current, exc, persist_task_state, seen, error_id,
                        callback, completed_windows, total_windows,
                        collapse_enabled=bool(target_config.cdx_collapses),
                    ):
                        total_windows += 1
                        transient_failure_streak = 0
                        continue
                    transient_failure_streak += 1
                    network = target_config.network.normalized()
                    no_progress_limit = min(
                        network.failure_pause_threshold,
                        4 if exc.timed_out else 6,
                    )
                    if transient_failure_streak >= no_progress_limit:
                        # _defer_transient_window counts this failed turn once.
                        # The threshold notice must not escalate its cooldown.
                        with database:
                            error_id = record_error(database, "index", "transient_index_delay",
                                f"{target} {label}: {transient_failure_streak} temporary failures: {exc}", retryable=True)
                            persist_task_state(encode_plan(plan), False, seen, error_id)
                        transient_failure_streak = 0
                    if current.strategy == "paged" and current.page_count < 0:
                        # Service failures are *not* evidence that Timemap paging
                        # is unsupported. Preserve this saved page-count setup
                        # and retry after the shared outage backoff. Only an
                        # explicit unsupported-pagination response (handled in
                        # the permanent-error branch) may switch strategies.
                        error_id = _defer_transient_window(
                            target_config, database, plan, current, persist_task_state,
                            seen, error_id, exc, callback, completed_windows,
                            total_windows, stop_event, yield_to_other_tasks=bool(task_queue) or retry_budget.exhausted,
                        )
                        if task_queue:
                            due[task_key] = time.monotonic() + transient_backoff(target_config, current.failures)
                            task_queue.append(task)
                            yielded = True
                            break
                        continue
                    if current.strategy == "resume" and current.pagination_supported:
                        parts = split_window(current) if (not target_config.cdx_collapses and _can_split_cdx_window(current, exc)) else []
                        if parts:
                            plan.pending[0:1] = parts
                            added = len(parts) - 1
                            plan.planned += added
                            total_windows += added
                            with database:
                                persist_task_state(encode_plan(plan), False, seen, error_id)
                            reason = ("; last-resort restart after repeated failures at the saved cursor"
                                      if current.resume_key else "")
                            emit(callback, ProgressEvent("index", f"CDX query-size/timeout recovery: split {target} {label} into {len(parts)} smaller windows (unfinished ranges retained{reason}).", completed_windows, total_windows, {"retain_notice": True}))
                            continue
                        if not target_config.cdx_collapses and _can_split_cdx_window(current, exc) and preferred_index_strategy(target_config, target) == "paged":
                            current.strategy = "paged"
                            current.page = 0
                            current.page_count = -1
                            current.resume_key = None
                            current.retry_pages.clear()
                            current.page_failures.clear()
                            with database:
                                persist_task_state(encode_plan(plan), False, seen, error_id)
                            emit(callback, ProgressEvent("index", f"Resume-key indexing remained slow for {target} {label}; switching this window to paged CDX indexing.", completed_windows, total_windows))
                            continue
                    error_id = _defer_transient_window(
                        target_config, database, plan, current, persist_task_state, seen, error_id,
                        exc, callback, completed_windows, total_windows, stop_event,
                        yield_to_other_tasks=bool(task_queue) or retry_budget.exhausted,
                    )
                    if task_queue:
                        due[task_key] = time.monotonic() + transient_backoff(target_config, current.failures)
                        task_queue.append(task)
                        yielded = True
                        break
                except RuntimeError as exc:
                    if isinstance(exc, PermanentRequestError) and exc.category in {"wayback_excluded", "wayback_forbidden"}:
                        category = exc.category
                        message = site_issue_message(category, target, "CDX indexing", exc.status)
                        # A 403 is not evidence that every capture was indexed.
                        # Keep the exact window pending and report the blocking
                        # condition instead of inventing an empty complete year.
                        with database:
                            error_id = record_error(
                                database, "index", category, f"{target} {label}: {exc}",
                                http_status=exc.status, retryable=False,
                            )
                            record_site_issue(database, host_from_url(target), "cdx_index",
                                              category, message, target=target, http_status=exc.status)
                            persist_task_state(encode_plan(plan), False, seen, error_id)
                        emit(callback, ProgressEvent("site_issue", message + "; indexing remains incomplete.",
                                                     completed_windows, total_windows))
                        raise
                    if current.strategy == "paged" and _is_pagination_unavailable(exc):
                        current.pagination_supported = False
                        current.strategy = "resume"
                        current.page = 0
                        current.page_count = -1
                        current.retry_pages.clear()
                        current.page_failures.clear()
                        with database:
                            persist_task_state(encode_plan(plan), False, seen, error_id)
                        emit(callback, ProgressEvent("index", f"Paged CDX is unavailable for {target} {label}; continuing with resume keys.", completed_windows, total_windows))
                        continue
                    message = f"{target} {label}: {type(exc).__name__}: {exc}"
                    with database:
                        error_id = record_error(database, "index", "index_failure", message, retryable=False)
                        persist_task_state(encode_plan(plan), False, seen, error_id)
                    emit(callback, ProgressEvent("index", f"Indexing stopped on a permanent configuration or local-data error for {target} {label}. Progress was saved.", completed_windows, total_windows))
                    raise
                except Exception as exc:
                    # Programming, parsing, SQLite, and local filesystem errors
                    # are not network retries. Requeueing them forever hides the
                    # real defect and can make the interface appear stuck.
                    with database:
                        error_id = record_error(
                            database,
                            "index",
                            "unexpected_index_error",
                            f"{target} {label}: {type(exc).__name__}: {exc}",
                            retryable=False,
                        )
                        persist_task_state(encode_plan(plan), False, seen, error_id)
                    raise
            accounted[task_key] = (plan.completed, plan.planned - len(default_windows))
            retry_budget.pause()
            spent[target] = spent.get(target, 0.0) + (time.monotonic() - turn_started)
            if not plan.pending:
                task_complete(target)
                retry_budgets.pop(task_key, None)
                emit(callback, ProgressEvent("index", f"Finished indexed inventory for {target} • {scope_label}", completed_windows, total_windows, {"phase": "inventory complete", "target": target, "scope": scope_label}))
            queue_status()
    finally:
        client.close()
    from .status import text_index_status, format_index_status
    status = text_index_status(database, config)
    emit(callback, ProgressEvent('index_status', format_index_status(status),
         status['complete_targets'], status['selected_targets'],
         {**{key: value for key, value in status.items() if key != 'targets'}, 'retain_notice': True}))
    return bool(status['unfinished_targets'] or budget_deferred or retry_deferred)

def _request_paged_count(
    client: HttpClient,
    endpoints: tuple[str, ...],
    params: list[tuple[str, str]],
    config: ProjectConfig,
    stop_event: threading.Event,
) -> int:
    """Retry the small native-JSON page count before abandoning Timemap.

    The reference downloader gives every Timemap request five attempts.  A
    single count timeout previously pushed Archive Scout into its much slower
    resume-window fallback immediately.
    """
    attempts = max(PAGED_REQUEST_ATTEMPTS, int(config.retries))
    last_error: BaseException | None = None
    for attempt in range(1, attempts + 1):
        if stop_event.is_set():
            raise Stopped
        try:
            payload = request_cdx_json_payload(
                client,
                endpoints,
                params,
                max_bytes=1024 * 1024,
            )
            return parse_num_pages(payload)
        except (RateLimitDeferred, PermanentRequestError, Stopped):
            raise
        except TransientRequestError as exc:
            last_error = exc
            # One connection_failed result already represents a complete pass
            # through the configured independent transports. Hand it back to
            # the operation-wide connection circuit instead of multiplying a
            # DNS/proxy/TLS outage by five more multi-backend passes.
            if exc.connection_failed or exc.timed_out:
                raise
            if attempt >= attempts:
                raise
            retry_callback = getattr(client, "retry_callback", None)
            if retry_callback:
                retry_callback(
                    attempt + 1,
                    attempts,
                    "Timemap page-count request failed; retrying native JSON",
                    0.0,
                )
    if last_error is not None:
        raise last_error
    raise TransientRequestError("Timemap page-count request failed", splittable=True)

def save_coverage_state(
    database: sqlite3.Connection, target_id: int, signature: str, start: str, end: str,
    plan_json: str | None, complete: bool, seen: int, error_id: int | None,
    strategy: str = "resume", layout_signature: str = "",
) -> None:
    database.execute(
        """INSERT INTO index_coverage(
               target_id,query_signature,range_start,range_end,plan_json,strategy,layout_signature,complete,seen,error_id,updated_at
           ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(target_id,query_signature,range_start,range_end) DO UPDATE SET
               plan_json=excluded.plan_json,strategy=excluded.strategy,layout_signature=excluded.layout_signature,
               complete=excluded.complete,seen=excluded.seen,error_id=excluded.error_id,updated_at=excluded.updated_at""",
        (target_id, signature, start, end, plan_json, strategy, layout_signature, int(complete), seen, error_id, utc_now()),
    )

def _shift_timestamp(value: str, seconds: int) -> str:
    parsed = datetime.strptime(value, "%Y%m%d%H%M%S") + timedelta(seconds=seconds)
    return parsed.strftime("%Y%m%d%H%M%S")

def uncovered_index_ranges(
    database: sqlite3.Connection, target_id: int, signature: str, start: str, end: str
) -> list[tuple[str, str]]:
    """Return uncovered gaps inside one requested interval from proven complete coverage."""
    intervals: list[tuple[str, str]] = []
    for row in database.execute(
        """SELECT range_start,range_end FROM index_coverage
           WHERE target_id=? AND query_signature=? AND complete=1
             AND range_end>=? AND range_start<=? ORDER BY range_start,range_end""",
        (target_id, signature, start, end),
    ):
        left = max(start, str(row[0])); right = min(end, str(row[1]))
        if left <= right:
            intervals.append((left, right))
    if not intervals:
        return [(start, end)]
    merged: list[list[str]] = []
    for left, right in intervals:
        if not merged or left > _shift_timestamp(merged[-1][1], 1):
            merged.append([left, right])
        elif right > merged[-1][1]:
            merged[-1][1] = right
    gaps: list[tuple[str, str]] = []
    cursor = start
    for left, right in merged:
        if cursor < left:
            gaps.append((cursor, _shift_timestamp(left, -1)))
        if right >= end:
            cursor = _shift_timestamp(end, 1)
            break
        cursor = max(cursor, _shift_timestamp(right, 1))
    if cursor <= end:
        gaps.append((cursor, end))
    return gaps

def record_recovery_event(database: sqlite3.Connection, operation: str, category: str,
                          message: str, *, details: dict | None = None) -> None:
    _record_network_event(database, operation + "_recovery", message,
                          {"category":category, **(details or {})})
