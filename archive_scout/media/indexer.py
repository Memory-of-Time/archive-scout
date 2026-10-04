from __future__ import annotations

import concurrent.futures
import fnmatch
import hashlib
import json
import random
import re
import sqlite3
import threading
import time
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable
from urllib.parse import urlsplit

from ..cdx.client import CDXRow, HttpClient, PermanentRequestError, RateLimitDeferred, TransientRequestError, request_cdx_rows
from ..cdx.indexer import (
    PendingWindow,
    PagedBatch,
    PAGED_PAGE_FAILURE_LIMIT,
    PAGED_PIPELINE_PAGES,
    _request_paged_count,
    _select_page_batch,
    cdx_response_budget,
    decode_plan,
    encode_plan,
    index_windows,
    split_window,
    uncovered_index_ranges,
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
    record_recovery_event,
    record_site_issue,
    upsert_media_captures,
)
from ..downloads.rate_limit import (SharedFixedRateLimiter, WAYBACK_INDEX_RATE_KEY, shared_host_gate)
from ..downloads.validation import classify_exception
from ..events import ConnectivityPaused, ProgressEvent, Stopped
from ..utils import json_value, parse_cdx_parameter_lines, utc_now
from ..site_status import host_from_url, should_surface_site_issue, site_issue_message
from .discovery import discover_media, hosts_related, safe_document_text, target_hosts
from .extensions import allowed_media_url, selected_extensions

ALL_EXTENSIONS_STATE = "__all__"
MEDIA_DISCOVERY_REVISION = 2


def _media_signature_payload(config: ProjectConfig) -> dict:
    media = config.media.normalized()
    payload = {
        "filters": media.cdx_filters,
        "collapses": media.cdx_collapses,
        "extra": media.cdx_extra_params,
        "targets": media.targets or config.targets,
        "extensions": selected_extensions(media),
        "strategy": media.snapshot_strategy,
        "embedded": media.discover_embedded,
        "external": media.allow_external_embeds,
        "single_query_per_target": True,
        "network_strategy": config.network.normalized().index_strategy,
    }
    if config.text_collapse_scope == "range":
        payload["coverage_scope"] = "range"
        if payload["collapses"] or media.snapshot_strategy != "all":
            payload["from"] = config.from_date
            payload["to"] = config.to_date
    else:
        payload["from"] = config.from_date
        payload["to"] = config.to_date
    return payload


def _audit2_media_signature_payload(config: ProjectConfig) -> dict:
    media = config.media.normalized()
    return {
        "from": config.from_date,
        "to": config.to_date,
        "filters": media.cdx_filters,
        "collapses": media.cdx_collapses,
        "extra": media.cdx_extra_params,
        "targets": media.targets or config.targets,
        "extensions": selected_extensions(media),
        "strategy": media.snapshot_strategy,
        "embedded": media.discover_embedded,
        "external": media.allow_external_embeds,
        "single_query_per_target": True,
        "network_strategy": config.network.normalized().index_strategy,
    }



def media_signature_is_date_bound(config: ProjectConfig) -> bool:
    """Whether current media inventory identity already contains its date bounds."""
    media = config.media.normalized()
    if config.text_collapse_scope != "range":
        return True
    collapses = media.cdx_collapses
    return bool(collapses or media.snapshot_strategy != "all")

def media_query_signature(config: ProjectConfig, page_size: int | None = None) -> str:
    """Semantic media inventory signature, independent of transport page size."""
    del page_size
    raw = json.dumps(_media_signature_payload(config), sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]


def _legacy_media_query_signature(config: ProjectConfig, page_size: int | None = None) -> str:
    payload = _audit2_media_signature_payload(config)
    if page_size is not None:
        payload["page_size"] = int(page_size)
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]


def media_query_signatures(config: ProjectConfig) -> tuple[str, ...]:
    sizes = [config.page_size, 5000, 25000, 1000, 10000, 50000, 100000, 150000]
    values = [media_query_signature(config)]
    # Audit2 applied media collapse independently inside each calendar year. A
    # range-scoped Audit3 query must not inherit those rows as proof of complete
    # range coverage because earliest/latest survivors can differ.
    if config.text_collapse_scope == "year":
        values.append(_legacy_media_query_signature(config, None))
        values.extend(_legacy_media_query_signature(config, size) for size in sizes)
    return tuple(dict.fromkeys(values))

def media_index_state_signature(config: ProjectConfig, *, page_size: int | None = None, revision: int | None = None) -> str:
    if revision is None:
        revision = 5 if config.text_collapse_scope == "range" else 4
    payload = {"media_query_signature": media_query_signature(config, page_size), "index_revision": int(revision)}
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]


def media_signature_candidates(config: ProjectConfig) -> tuple[tuple[str, str], ...]:
    values: list[tuple[str, str]] = []
    current_signature = media_query_signature(config)
    current_revision = 5 if config.text_collapse_scope == "range" else 4
    for revision in range(current_revision, 1, -1):
        item = (current_signature, media_index_state_signature(config, revision=revision))
        if item not in values:
            values.append(item)
    if config.text_collapse_scope == "year":
        for size in [None, config.page_size, 5000, 25000, 1000, 10000, 50000, 100000, 150000]:
            media_signature = _legacy_media_query_signature(config, size)
            for revision in (4, 3, 2):
                state_payload = {"media_query_signature": media_signature, "index_revision": int(revision)}
                raw = json.dumps(state_payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
                state_signature = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]
                item = (media_signature, state_signature)
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
    media = config.media.normalized()
    params.extend(("filter", value) for value in media.cdx_filters)
    media_collapses = media.cdx_collapses
    params.extend(("collapse", value) for value in media_collapses)
    # Do not server-filter broad media inventories by URL extension. Dynamic
    # endpoints (for example image.php) and percent-encoded historical URLs can
    # serve eligible media. Metadata/payload classification applies the user's
    # exact format policy after the complete media inventory is available.
    params.extend(parse_cdx_parameter_lines(media.cdx_extra_params))
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
    params = [
        (key, value)
        for key, value in params
        if key not in {"limit", "showResumeKey", "resumeKey", "fl"}
    ]
    params.append(("showNumPages", "true"))
    blocks = int(page_blocks)
    if blocks <= 0:
        blocks = 9
    params.append(("pageSize", str(blocks)))
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
    params = [
        (key, "urlkey,timestamp,original,mimetype,statuscode,digest,length") if key == "fl" else (key, value)
        for key, value in params
    ]
    params.append(("page", str(max(0, page))))
    blocks = int(page_blocks)
    if blocks <= 0:
        blocks = 9
    params.append(("pageSize", str(blocks)))
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
                    PARTITION BY COALESCE(NULLIF(urlkey,''),original_url) ORDER BY timestamp {direction},id {direction}
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




def media_layout_signature(
    config: ProjectConfig, start: str, end: str, strategy: str, page_blocks: int = 0
) -> str:
    media = config.media.normalized()
    payload = {
        "start": start, "end": end, "strategy": strategy,
        "endpoint_mode": config.network.normalized().endpoint_mode,
        "page_blocks": int(page_blocks), "page_size": int(config.page_size),
        "filters": media.cdx_filters,
        "collapses": media.cdx_collapses,
        "extra": media.cdx_extra_params,
        "extensions": selected_extensions(media),
        "sort": "urlkey,timestamp",
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]


def _save_media_coverage_state(
    database: sqlite3.Connection, target_id: int, state_signature: str,
    start: str, end: str, plan_json: str | None, complete: bool, seen: int,
    error_id: int | None, strategy: str = "resume", layout_signature: str = "",
) -> None:
    database.execute(
        """INSERT INTO media_index_coverage(
               target_id,query_signature,range_start,range_end,plan_json,strategy,layout_signature,complete,seen,error_id,updated_at
           ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(target_id,query_signature,range_start,range_end) DO UPDATE SET
               plan_json=excluded.plan_json,strategy=excluded.strategy,layout_signature=excluded.layout_signature,
               complete=excluded.complete,seen=excluded.seen,error_id=excluded.error_id,updated_at=excluded.updated_at""",
        (target_id, state_signature, start, end, plan_json, strategy, layout_signature,
         int(complete), seen, error_id, utc_now()),
    )


def _shift_media_timestamp(value: str, seconds: int) -> str:
    parsed = datetime.strptime(value, "%Y%m%d%H%M%S") + timedelta(seconds=seconds)
    return parsed.strftime("%Y%m%d%H%M%S")


def uncovered_media_ranges(
    database: sqlite3.Connection, target_id: int, state_signature: str, start: str, end: str
) -> list[tuple[str, str]]:
    intervals: list[tuple[str, str]] = []
    for row in database.execute(
        """SELECT range_start,range_end FROM media_index_coverage
           WHERE target_id=? AND query_signature=? AND complete=1
             AND range_end>=? AND range_start<=? ORDER BY range_start,range_end""",
        (target_id, state_signature, start, end),
    ):
        left = max(start, str(row[0])); right = min(end, str(row[1]))
        if left <= right:
            intervals.append((left, right))
    if not intervals:
        return [(start, end)]
    merged: list[list[str]] = []
    for left, right in intervals:
        if not merged or left > _shift_media_timestamp(merged[-1][1], 1):
            merged.append([left, right])
        elif right > merged[-1][1]:
            merged[-1][1] = right
    gaps: list[tuple[str, str]] = []
    cursor = start
    for left, right in merged:
        if cursor < left:
            gaps.append((cursor, _shift_media_timestamp(left, -1)))
        if right >= end:
            cursor = _shift_media_timestamp(end, 1)
            break
        cursor = max(cursor, _shift_media_timestamp(right, 1))
    if cursor <= end:
        gaps.append((cursor, end))
    return gaps

def _merge_compatible_media_rows(
    database: sqlite3.Connection,
    target_id: int,
    old_signature: str,
    new_signature: str,
    start: str,
    end: str,
) -> None:
    rows = database.execute(
        """SELECT * FROM media_captures
           WHERE target_id=? AND query_signature=? AND timestamp BETWEEN ? AND ?
           ORDER BY id""",
        (target_id, old_signature, start, end),
    ).fetchall()
    for old in rows:
        current = database.execute(
            """SELECT * FROM media_captures
               WHERE original_url=? AND timestamp=? AND query_signature=? LIMIT 1""",
            (str(old["original_url"]), str(old["timestamp"]), new_signature),
        ).fetchone()
        if current is None:
            database.execute(
                "UPDATE media_captures SET query_signature=?,updated_at=? WHERE id=?",
                (new_signature, utc_now(), int(old["id"])),
            )
            continue
        # Identity collision: preserve both rows so errors/provenance/download
        # state linked to the legacy row cannot be cascaded away. Promote only
        # missing acquired-file state onto the semantic-signature row.
        old_path = str(old["path"] or "")
        promoted_state = str(current["state"] or "pending")
        if not str(current["path"] or "") and old_path and str(old["state"] or "") == "downloaded":
            promoted_state = "downloaded"
        database.execute(
            """UPDATE media_captures SET
                   path=COALESCE(NULLIF(path,''),NULLIF(?,'')),
                   content_hash=COALESCE(NULLIF(content_hash,''),NULLIF(?,'')),
                   http_status=COALESCE(http_status,?),
                   final_url=COALESCE(NULLIF(final_url,''),NULLIF(?,'')),
                   bytes_saved=MAX(COALESCE(bytes_saved,0),?),
                   download_attempts=MIN(download_attempts,?),
                   state=?,updated_at=?
               WHERE id=?""",
            (
                old_path, str(old["content_hash"] or ""), old["http_status"],
                str(old["final_url"] or ""), int(old["bytes_saved"] or 0),
                int(old["download_attempts"] or 0), promoted_state, utc_now(),
                int(current["id"]),
            ),
        )


def _copy_media_page_checkpoints(
    database: sqlite3.Connection,
    target_id: int,
    old_state_signature: str,
    new_state_signature: str,
    start: str,
    end: str,
) -> None:
    database.execute(
        """INSERT OR IGNORE INTO media_index_pages(
               query_signature,target_id,extension,window_start,window_end,page,row_count,status,updated_at
           )
           SELECT ?,target_id,extension,window_start,window_end,page,row_count,status,updated_at
           FROM media_index_pages
           WHERE query_signature=? AND target_id=?
             AND window_start<=? AND window_end>=?""",
        (new_state_signature, old_state_signature, target_id, end, start),
    )


def _adopt_compatible_media_state(
    database: sqlite3.Connection,
    target_id: int,
    year: int,
    config: ProjectConfig,
    signature: str,
    state_signature: str,
) -> None:
    start, end = cdx_year_window(config, year) or (
        f"{year:04d}0101000000", f"{year:04d}1231235959"
    )
    current = database.execute(
        """SELECT resume_key,complete,seen,error_id,updated_at FROM media_index_state
           WHERE target_id=? AND extension=? AND year=? AND query_signature=?""",
        (target_id, ALL_EXTENSIONS_STATE, year, state_signature),
    ).fetchone()

    for candidate_signature, candidate_state in media_signature_candidates(config):
        if candidate_state == state_signature:
            continue
        state = database.execute(
            """SELECT resume_key,complete,seen,error_id,updated_at FROM media_index_state
               WHERE target_id=? AND extension=? AND year=? AND query_signature=?""",
            (target_id, ALL_EXTENSIONS_STATE, year, candidate_state),
        ).fetchone()
        if not state:
            continue
        if candidate_signature != signature:
            _merge_compatible_media_rows(
                database, target_id, candidate_signature, signature, start, end
            )
        _copy_media_page_checkpoints(
            database, target_id, candidate_state, state_signature, start, end
        )

        if current is None:
            database.execute(
                """INSERT INTO media_index_state(
                       target_id,extension,year,query_signature,resume_key,complete,seen,error_id,updated_at
                   ) VALUES(?,?,?,?,?,?,?,?,?)""",
                (
                    target_id, ALL_EXTENSIONS_STATE, year, state_signature,
                    state["resume_key"], state["complete"], state["seen"],
                    state["error_id"], state["updated_at"],
                ),
            )
            current = state
        else:
            complete = max(int(current["complete"] or 0), int(state["complete"] or 0))
            seen = max(int(current["seen"] or 0), int(state["seen"] or 0))
            resume_key = current["resume_key"]
            error_id = current["error_id"]
            if complete and int(state["complete"] or 0):
                resume_key = state["resume_key"]
                error_id = state["error_id"] if error_id is None else error_id
            database.execute(
                """UPDATE media_index_state SET resume_key=?,complete=?,seen=?,error_id=?,updated_at=?
                   WHERE target_id=? AND extension=? AND year=? AND query_signature=?""",
                (
                    resume_key, complete, seen, error_id, utc_now(), target_id,
                    ALL_EXTENSIONS_STATE, year, state_signature,
                ),
            )
            current = database.execute(
                """SELECT resume_key,complete,seen,error_id,updated_at FROM media_index_state
                   WHERE target_id=? AND extension=? AND year=? AND query_signature=?""",
                (target_id, ALL_EXTENSIONS_STATE, year, state_signature),
            ).fetchone()


def _main_index_covers_media_query(config: ProjectConfig) -> bool:
    """Return whether the text inventory is provably no narrower than media.

    CDX filters/extra parameters are conjunctive restrictions and collapse rules
    can discard captures. Local reuse is therefore safe only when every text
    restriction is also present in the media request and every text collapse is
    also requested by media. Unknown/extra text semantics disable reuse rather
    than incorrectly declaring media coverage complete.
    """
    media = config.media.normalized()
    text_filters = set(config.cdx_filters)
    media_filters = set(media.cdx_filters)
    if not text_filters.issubset(media_filters):
        return False
    effective_media_collapses = set(
        media.cdx_collapses
    )
    if not set(config.cdx_collapses).issubset(effective_media_collapses):
        return False
    if not set(config.cdx_extra_params).issubset(set(media.cdx_extra_params)):
        return False
    return True


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
    if not _main_index_covers_media_query(config):
        return False, 0, 0
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


def _reuse_completed_main_index_range(
    config: ProjectConfig,
    database: sqlite3.Connection,
    target: str,
    media_target_id: int,
    start: str,
    end: str,
    media_signature: str,
    state_signature: str,
) -> tuple[bool, int, int]:
    """Populate media inventory from proven complete Audit3 text coverage."""
    if not _main_index_covers_media_query(config):
        return False, 0, 0
    target_row = database.execute("SELECT id FROM targets WHERE pattern=?", (target,)).fetchone()
    if not target_row:
        return False, 0, 0
    normal_target_id = int(target_row["id"])
    text_signature = cdx_query_signature(config)
    if uncovered_index_ranges(database, normal_target_id, text_signature, start, end):
        return False, 0, 0

    media = config.media.normalized()
    seen = changed = 0
    cursor = database.execute(
        """SELECT original_url,timestamp,mimetype,statuscode,digest,length FROM captures
           WHERE target_id=? AND query_signature=? AND timestamp BETWEEN ? AND ? ORDER BY id""",
        (normal_target_id, text_signature, start, end),
    )
    while True:
        batch = cursor.fetchmany(10000)
        if not batch:
            break
        accepted: list[tuple[CDXRow, str, str]] = []
        seen += len(batch)
        for item in batch:
            row: CDXRow = (
                str(item["timestamp"]), str(item["original_url"]), str(item["mimetype"] or ""),
                str(item["statuscode"] or ""), str(item["digest"] or ""), str(item["length"] or 0),
            )
            allowed, kind, extension = allowed_media_url(row[1], media, row[2])
            if allowed and kind:
                accepted.append((row, kind, extension))
        changed += upsert_media_captures(
            database, accepted, media_target_id, media_signature, source_type="main_index"
        )
    _save_media_coverage_state(
        database, media_target_id, state_signature, start, end, None, True, seen, None,
        strategy="reuse", layout_signature="main-index",
    )
    return True, seen, changed


def _wait_seconds(config: ProjectConfig, failures: int) -> float:
    network = config.network.normalized()
    base = min(network.retry_max_seconds, network.retry_base_seconds * 2 ** min(max(0, failures - 1), 6))
    return base * random.uniform(0.85, 1.15)


def _defer_media_window(
    config: ProjectConfig,
    database: sqlite3.Connection,
    plan,
    current: PendingWindow,
    persist_state: Callable[[str | None, bool, int, int | None], None],
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
        # Keep server-selected paging server-selected. Turning an automatic
        # page into pageSize=1 recreates the thousands-of-pages failure mode.
        if current.page_blocks > 1:
            current.page_blocks = max(1, current.page_blocks // 2)
    else:
        current.page_size = max(25, (current.page_size or config.page_size) // 2)
    with database:
        record_recovery_event(
            database,
            "media_index",
            "transient_media_index_delay",
            f"{target} {label}: {type(exc).__name__}: {exc}",
            details={"failures": current.failures, "strategy": current.strategy},
        )
        if len(plan.pending) > 1:
            plan.pending.append(plan.pending.pop(0))
        persist_state(encode_plan(plan), False, seen, error_id)
    network = config.network.normalized()
    threshold = network.failure_pause_threshold

    def pause_error(message: str) -> int:
        with database:
            new_error = record_error(
                database, "media_index", "transient_media_index_delay",
                f"{target} {label}: {message}", retryable=True,
            )
            persist_state(encode_plan(plan), False, seen, new_error)
        return new_error

    if plan.pending and all(item.failures >= threshold for item in plan.pending):
        error_id = pause_error(f"all remaining media index windows reached the retry threshold: {exc}")
        raise ConnectivityPaused(
            "Wayback could not answer any remaining combined-media index window. "
            "The exact media queue was saved and can be continued with Resume."
        ) from exc
    if len(plan.pending) > 1:
        if callback:
            callback(ProgressEvent("media_index", "Deferred one unresponsive combined-media window behind the remaining queue; it will retry automatically.", completed, total))
        return error_id
    if not network.persistent_retries and current.failures >= max(3, config.retries):
        error_id = pause_error(f"media indexing retry limit reached: {exc}")
        raise ConnectivityPaused(f"media indexing retry limit reached; progress was saved: {exc}") from exc
    if current.failures >= threshold:
        error_id = pause_error(f"media index window remained unreachable after {current.failures} recovery cycles: {exc}")
        raise ConnectivityPaused(
            f"Wayback remained unreachable for this media window after {current.failures} recovery cycles. Progress was saved."
        ) from exc
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
    # A persisted numbered-page queue is layout-specific durable work. Audit3's
    # fresh auto policy may prefer resume for unknown ranges, but it never
    # reinterprets an unfinished saved page queue as a resume-key offset.
    if not current.pagination_supported and current.strategy == "paged":
        current.strategy = "resume"
    network = config.network.normalized()
    if network.index_strategy == "auto" and current.strategy == "paged":
        # Direct-media auto indexing uses the same fixed fast Timemap profile as
        # main URL indexing, regardless of an older saved custom block count.
        current.page_blocks = 9
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
    completed_pages: set[int] | None = None,
) -> PagedBatch:
    endpoints = cdx_paged_endpoints(config)
    network = config.network.normalized()
    if current.page_count < 0:
        current.page_count = _request_paged_count(
            client,
            endpoints,
            build_media_num_pages_params(config, target, current.start, current.end, extensions, current.page_blocks),
            config,
            stop_event,
        )
        current.page = min(current.page, current.page_count)
        current.retry_pages = [page for page in current.retry_pages if page < current.page_count]
    if current.page >= current.page_count and not current.retry_pages:
        return PagedBatch([], [], True)

    page_workers = effective_page_workers(network.cdx_workers, current.page_blocks)
    pages, next_page = _select_page_batch(current, max(page_workers, PAGED_PIPELINE_PAGES))
    completed_pages = completed_pages or set()
    requested_pages = [page for page in pages if page not in completed_pages]
    if not requested_pages:
        current.page = next_page
        current.retry_pages = [page for page in current.retry_pages if page not in completed_pages]
        return PagedBatch([], pages, current.page >= current.page_count and not current.retry_pages)
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
        max_bytes=(192 * 1024 * 1024 if current.page_blocks <= 0 else max(64 * 1024 * 1024, current.page_blocks * 12 * 1024 * 1024)),
        prefer_text=False,
        json_only=True,
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
    for item in failures:
        if isinstance(item.error, RateLimitDeferred):
            return item.error
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
    # Audit2 projects keep their durable year-scoped media queues because
    # server-side collapse across a combined range can select different
    # snapshots. New Audit3 range projects use explicit coverage/gaps and start
    # unknown ranges with one data-bearing resume traversal.
    tasks: list[tuple[str, ProjectConfig, int, str, int | None, str, str, list[tuple[str, str]]]] = []
    for target in targets:
        target_config = config.for_target(target) if target in config.targets else config
        target_id = get_or_create_media_target(database, target)
        if target_config.text_collapse_scope == "year":
            for year in range(target_config.from_year, target_config.to_year + 1):
                windows = index_windows(target_config, target, year)
                if windows:
                    tasks.append((target, target_config, target_id, "year", year, windows[0][0], windows[-1][1], windows))
        else:
            for gap_start, gap_end in uncovered_media_ranges(
                database, target_id, state_signature, target_config.from_date, target_config.to_date
            ):
                tasks.append((target, target_config, target_id, "range", None, gap_start, gap_end, [(gap_start, gap_end)]))
    total = sum(len(windows) for *_prefix, windows in tasks)
    completed = 0
    connection_failure_streak = 0
    transient_failure_streak = 0

    for target, target_config, target_id, state_kind, year, task_start, task_end, default_windows in tasks:
        if stop_event.is_set():
            raise Stopped

        def persist_state(plan_json: str | None, complete: bool, seen_value: int, error_value: int | None) -> None:
            if state_kind == "year":
                assert year is not None
                _save_media_state(database, target_id, year, state_signature, plan_json, complete, seen_value, error_value)
            else:
                try:
                    active = plan.pending[0] if plan.pending else None
                except (NameError, UnboundLocalError):
                    active = None
                strategy = active.strategy if active is not None else "resume"
                layout = media_layout_signature(
                    target_config, task_start, task_end, strategy, active.page_blocks if active is not None else 0
                )
                _save_media_coverage_state(
                    database, target_id, state_signature, task_start, task_end, plan_json, complete,
                    seen_value, error_value, strategy=strategy, layout_signature=layout,
                )

        if state_kind == "year":
            assert year is not None
            with database:
                _adopt_compatible_media_state(
                    database, target_id, year, target_config, signature, state_signature
                )
            state = database.execute(
                """SELECT resume_key,complete,seen,error_id FROM media_index_state
                   WHERE target_id=? AND extension=? AND year=? AND query_signature=?""",
                (target_id, ALL_EXTENSIONS_STATE, year, state_signature),
            ).fetchone()
            state_plan = state["resume_key"] if state else None
            scope_label = str(year)
        else:
            state = database.execute(
                """SELECT plan_json,complete,seen,error_id FROM media_index_coverage
                   WHERE target_id=? AND query_signature=? AND range_start=? AND range_end=?""",
                (target_id, state_signature, task_start, task_end),
            ).fetchone()
            state_plan = state["plan_json"] if state else None
            scope_label = window_label(task_start, task_end)

        if state and state["complete"]:
            completed += len(default_windows)
            if callback:
                callback(ProgressEvent("media_index", f"Already indexed selected media for {target} during {scope_label}", completed, total))
            continue

        # If the normal site index is already complete and at least as broad as
        # the media query, filter it locally rather than issuing another CDX pass.
        if target in config.targets:
            with database:
                if state_kind == "year":
                    assert year is not None
                    reused, reused_seen, reused_changed = _reuse_completed_main_index(
                        target_config, database, target, target_id, year, signature, state_signature
                    )
                else:
                    reused, reused_seen, reused_changed = _reuse_completed_main_index_range(
                        target_config, database, target, target_id, task_start, task_end, signature, state_signature
                    )
            if reused:
                completed += len(default_windows)
                if callback:
                    callback(ProgressEvent(
                        "media_index",
                        f"Reused completed site-index coverage for {target} {scope_label}: checked {reused_seen:,}, "
                        f"stored {reused_changed:,} media captures without another CDX request.",
                        completed, total,
                    ))
                continue

        seen = int(state["seen"] or 0) if state else 0
        error_id = int(state["error_id"]) if state and state["error_id"] else None
        plan = decode_plan(state_plan, default_windows)
        completed += plan.completed
        total += max(0, plan.planned - len(default_windows))

        while plan.pending:
            if stop_event.is_set():
                with database:
                    persist_state(encode_plan(plan), False, seen, error_id)
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
                    batch_pages_done = 0
                    last_page_progress = time.monotonic()

                    completed_pages = {
                        int(row[0]) for row in database.execute(
                            """SELECT page FROM media_index_pages WHERE query_signature=? AND target_id=? AND extension=?
                               AND window_start=? AND window_end=? AND status='complete'
                               AND (layout_signature='' OR layout_signature=?)""",
                            (state_signature, target_id, ALL_EXTENSIONS_STATE, current.start, current.end,
                             media_layout_signature(target_config, current.start, current.end, 'paged', current.page_blocks)),
                        )
                    }

                    def store_completed_media_page(result: PageFetchResult) -> None:
                        nonlocal received, accepted_count, changed, write_seconds, batch_pages_done, last_page_progress, seen
                        page_received = len(result.rows)
                        accepted = _accept_media_rows(result.rows, media)
                        write_started = time.monotonic()
                        with database:
                            changed += upsert_media_captures(database, accepted, target_id, signature)
                            database.execute(
                                """INSERT INTO media_index_pages(query_signature,target_id,extension,window_start,window_end,page,row_count,status,layout_signature,updated_at)
                                   VALUES(?,?,?,?,?,?,?,'complete',?,?)
                                   ON CONFLICT(query_signature,target_id,extension,window_start,window_end,page) DO UPDATE SET
                                   row_count=excluded.row_count,status='complete',layout_signature=excluded.layout_signature,updated_at=excluded.updated_at""",
                                (state_signature, target_id, ALL_EXTENSIONS_STATE, current.start, current.end, int(result.page), page_received,
                                 media_layout_signature(target_config, current.start, current.end, 'paged', current.page_blocks), utc_now()),
                            )
                        completed_pages.add(int(result.page))
                        write_seconds += time.monotonic() - write_started
                        received += page_received
                        seen += page_received
                        accepted_count += len(accepted)
                        batch_pages_done += 1
                        now = time.monotonic()
                        if callback and (
                            now - last_page_progress >= 1.0
                            or len(completed_pages) >= current.page_count
                        ):
                            callback(
                                ProgressEvent(
                                    "media_index",
                                    f"{target} {label}: completed {len(completed_pages):,}/{current.page_count:,} Timemap pages; "
                                    f"this block finished {batch_pages_done:,} pages and accepted {accepted_count:,} media captures",
                                    completed,
                                    total,
                                )
                            )
                            last_page_progress = now
                        result.rows.clear()
                        accepted.clear()

                    batch = _request_media_paged_batch(
                        target_config, client, target, current, extensions, stop_event,
                        store_completed_media_page, completed_pages,
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
                            plan.pending.pop(0)
                            plan.completed += 1
                            completed += 1
                        complete = not plan.pending
                        persist_state(encode_plan(plan), complete, seen, None if complete else error_id)
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
                            persist_state(encode_plan(plan), False, seen, error_id)
                        continue
                    permanent = next(
                        (item.error for item in failures if item.error and _permanent_media_page_error(item.error)),
                        None,
                    )
                    if permanent is not None:
                        raise permanent
                    highest_page_failures = max(current.page_failures.values(), default=0)
                    new_pages_remain = current.page < current.page_count
                    if highest_page_failures >= PAGED_PAGE_FAILURE_LIMIT and (
                        not successes or not new_pages_remain
                    ):
                        with database:
                            error_id = record_error(
                                database,
                                "media_index",
                                "timemap_media_pages_unavailable",
                                f"{target} {label}: {len(current.retry_pages)} Timemap media page(s) remained unavailable "
                                f"after {highest_page_failures} attempts: {failure_exc}",
                                retryable=True,
                            )
                            record_recovery_event(
                                database,
                                "media_index",
                                "timemap_media_page_queue_saved",
                                f"{target} {label}: saved only the failed Timemap media pages for Resume.",
                                details={"pages": current.retry_pages[:100], "attempts": highest_page_failures},
                            )
                            persist_state(encode_plan(plan), False, seen, error_id)
                        raise ConnectivityPaused(
                            f"{len(current.retry_pages)} Timemap media page(s) remained unavailable after "
                            f"{highest_page_failures} attempts. Successful pages were preserved and only the exact "
                            "failed media-page queue was saved for Resume."
                        ) from failure_exc
                    with database:
                        record_recovery_event(
                            database,
                            "media_index",
                            "transient_media_page_retry",
                            f"{target} {label}: {len(failures)} media CDX page(s) requeued: {failure_exc}",
                            details={"pages": [item.page for item in failures]},
                        )
                        persist_state(encode_plan(plan), False, seen, error_id)
                    if successes:
                        if callback:
                            callback(ProgressEvent("media_index", f"Requeued {len(failures)} slow media page(s) while continuing with untouched pages.", completed, total))
                        continue
                    if completed_pages and not new_pages_remain:
                        if callback:
                            callback(
                                ProgressEvent(
                                    "media_index",
                                    f"Retrying {len(current.retry_pages)} isolated Timemap media page(s); all successful pages remain checkpointed.",
                                    completed,
                                    total,
                                )
                            )
                        continue
                    error_id = _defer_media_window(
                        target_config, database, plan, current, persist_state,
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
                    persist_state(encode_plan(plan), complete, seen, None if complete else error_id)
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
                    persist_state(encode_plan(plan), False, seen, error_id)
                raise
            except RateLimitDeferred as exc:
                with database:
                    record_recovery_event(
                        database,
                        "media_index",
                        "service_rate_limit_paused",
                        f"{target} {label}: {exc}",
                        details=exc.to_detail(),
                    )
                    persist_state(encode_plan(plan), False, seen, error_id)
                if callback:
                    callback(
                        ProgressEvent(
                            "rate_limit_paused",
                            f"{target} {label}: Wayback service pause saved exactly; Resume will continue without changing the media request plan.",
                            completed,
                            total,
                            exc.to_detail(),
                        )
                    )
                raise
            except TransientRequestError as exc:
                if exc.connection_failed:
                    connection_failure_streak += 1
                    current.failures += 1
                    network = target_config.network.normalized()
                    with database:
                        if connection_failure_streak >= network.connection_failure_pause_threshold:
                            error_id = record_error(
                                database, "media_index", "wayback_connection_unavailable",
                                f"{target} {label}: {exc}", retryable=True,
                            )
                        else:
                            record_recovery_event(
                                database, "media_index", "connection_retry",
                                f"{target} {label}: {exc}",
                                details={"streak": connection_failure_streak},
                            )
                        persist_state(encode_plan(plan), False, seen, error_id)
                    if connection_failure_streak >= network.connection_failure_pause_threshold:
                        raise ConnectivityPaused(
                            f"Archive Scout could not establish a Wayback connection for media indexing after "
                            f"{connection_failure_streak} complete multi-backend attempts. The exact queue was saved."
                        ) from exc
                    wait = min(15.0, network.connection_retry_seconds * 2 ** max(0, connection_failure_streak - 1))
                    if callback:
                        callback(
                            ProgressEvent(
                                "network",
                                f"Wayback connection setup failed. Retrying the same saved media request in {wait:.1f}s "
                                f"({connection_failure_streak}/{network.connection_failure_pause_threshold})…",
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
                    no_progress_limit = min(
                        network.failure_pause_threshold,
                        4 if exc.timed_out else 6,
                    )
                    if transient_failure_streak >= no_progress_limit:
                        current.failures += 1
                        with database:
                            error_id = record_error(
                                database,
                                "media_index",
                                "transient_media_index_delay",
                                f"{target} {label}: {transient_failure_streak} consecutive transient CDX failures without a successful response: {exc}",
                                retryable=True,
                            )
                            persist_state(encode_plan(plan), False, seen, error_id)
                        raise ConnectivityPaused(
                            f"Wayback returned no usable media CDX response after {transient_failure_streak} consecutive recovery attempts. "
                            "The exact media queue was saved instead of looping indefinitely."
                        ) from exc
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
                        persist_state(encode_plan(plan), False, seen, error_id)
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
                            persist_state(encode_plan(plan), False, seen, error_id)
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
                            persist_state(encode_plan(plan), False, seen, error_id)
                        if callback:
                            callback(ProgressEvent("media_index", f"Switching the combined media window to paged CDX indexing for {target} {label}.", completed, total))
                        continue
                error_id = _defer_media_window(
                    target_config, database, plan, current, persist_state,
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
                        persist_state(None, True, seen, error_id)
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
                        persist_state(encode_plan(plan), False, seen, error_id)
                    continue
                with database:
                    error_id = record_error(database, "media_index", "index_failure", f"{target} {label}: {type(exc).__name__}: {exc}", retryable=False)
                    persist_state(encode_plan(plan), False, seen, error_id)
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
                    persist_state(encode_plan(plan), False, seen, error_id)
                raise


def _discover_document_media(
    output_dir,
    max_file_bytes: int,
    media,
    target_host_set: set[str],
    external_only: bool,
    row: dict,
) -> tuple[int, int | None, str, int, int, list[tuple[str, int | None, str, str]]]:
    """Pure local worker used by saved-page embedded-media discovery."""
    page_id = int(row["id"])
    source_document_id_value = row.get("source_document_id", row.get("id"))
    source_document_id = int(source_document_id_value) if source_document_id_value is not None else None
    content_hash = str(row.get("content_hash") or "")
    path_text = str(row.get("path") or "")
    size_bytes = mtime_ns = 0
    try:
        path = Path(path_text)
        if not path.is_absolute():
            path = Path(output_dir) / path
        stat = path.stat()
        size_bytes = int(stat.st_size)
        mtime_ns = int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000)))
    except OSError:
        pass
    known_links = [str(value) for value in json_value(row.get("links_json"), []) if str(value).strip()]
    raw = safe_document_text(output_dir, path_text, max_file_bytes)
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
            source_document_id,
            "external_embedded" if is_external else "embedded",
            item.kind_hint,
        ))
    return page_id, source_document_id, content_hash, size_bytes, mtime_ns, batch


def _discover_embedded_queue(
    config: ProjectConfig,
    database: sqlite3.Connection,
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None,
    signature: str,
    *,
    external_only: bool,
) -> int:
    """Discover media from saved pages with persistent extraction checkpoints.

    Scanned documents are checkpointed by content hash. Download-only captures
    are checkpointed separately by capture identity, extraction revision, file
    size, and mtime, so Resume does not repeatedly parse an unchanged corpus.
    """
    media = config.media.normalized()
    if not media.discover_embedded:
        return 0
    discovery_targets = config.targets if external_only else (media.targets or config.targets)
    targets = target_hosts(discovery_targets)
    document_total = int(database.execute("SELECT COUNT(*) FROM documents").fetchone()[0])
    unscanned_total = int(database.execute(
        "SELECT COUNT(*) FROM captures WHERE state='downloaded_unscanned' AND local_path IS NOT NULL"
    ).fetchone()[0])
    total = document_total + unscanned_total
    scanned = queued = 0
    cursor = database.execute(
        """
        SELECT 'document' AS source_kind,d.id,d.id AS source_document_id,
               d.path,d.links_json,d.content_hash,c.original_url,
               mdd.content_hash AS discovery_content_hash,
               NULL AS discovery_size,NULL AS discovery_mtime,NULL AS discovery_revision
        FROM documents d
        JOIN captures c ON c.id=d.capture_id
        LEFT JOIN media_discovery_documents mdd
          ON mdd.document_id=d.id AND mdd.query_signature=?
        UNION ALL
        SELECT 'capture' AS source_kind,c.id,NULL AS source_document_id,
               c.local_path AS path,'[]' AS links_json,c.content_hash,c.original_url,
               NULL AS discovery_content_hash,
               mdc.size_bytes AS discovery_size,mdc.mtime_ns AS discovery_mtime,
               mdc.extraction_version AS discovery_revision
        FROM captures c
        LEFT JOIN media_discovery_captures mdc
          ON mdc.capture_id=c.id AND mdc.query_signature=? AND mdc.extraction_version=?
        WHERE c.state='downloaded_unscanned' AND c.local_path IS NOT NULL
        ORDER BY source_kind,id
        """,
        (signature, signature, MEDIA_DISCOVERY_REVISION),
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
                source_document_id = row["source_document_id"]
                content_hash = str(row["content_hash"] or "")
                if source_document_id is not None:
                    if str(row["discovery_content_hash"] or "") == content_hash and content_hash:
                        scanned += 1
                        continue
                else:
                    try:
                        local = Path(str(row["path"] or ""))
                        if not local.is_absolute():
                            local = config.output_dir / local
                        stat = local.stat()
                        size_bytes = int(stat.st_size)
                        mtime_ns = int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000)))
                    except OSError:
                        size_bytes = mtime_ns = -1
                    if (
                        int(row["discovery_revision"] or 0) == MEDIA_DISCOVERY_REVISION
                        and int(row["discovery_size"] or -2) == size_bytes
                        and int(row["discovery_mtime"] or -2) == mtime_ns
                    ):
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
            completed_results: list[
                tuple[int, int | None, str, int, int, list[tuple[str, int | None, str, str]]]
            ] = []
            for future in concurrent.futures.as_completed(futures):
                if stop_event.is_set():
                    for pending in futures:
                        pending.cancel()
                    raise Stopped
                completed_results.append(future.result())
            if completed_results:
                with database:
                    for page_id, source_document_id, content_hash, size_bytes, mtime_ns, discovered in completed_results:
                        queued += queue_media_discovery_candidates(database, signature, discovered)
                        if source_document_id is not None:
                            mark_media_discovery_document(
                                database, signature, source_document_id, content_hash, len(discovered)
                            )
                        else:
                            database.execute(
                                """INSERT INTO media_discovery_captures(
                                       query_signature,capture_id,extraction_version,size_bytes,mtime_ns,
                                       candidate_count,scanned_at)
                                   VALUES(?,?,?,?,?,?,?)
                                   ON CONFLICT(query_signature,capture_id,extraction_version) DO UPDATE SET
                                       size_bytes=excluded.size_bytes,mtime_ns=excluded.mtime_ns,
                                       candidate_count=excluded.candidate_count,scanned_at=excluded.scanned_at""",
                                (
                                    signature, page_id, MEDIA_DISCOVERY_REVISION,
                                    max(0, int(size_bytes)), max(0, int(mtime_ns)),
                                    len(discovered), utc_now(),
                                ),
                            )
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
    """Resolve an exact candidate without letting collapse choose eligibility first."""
    try:
        url = str(row["original_url"])
        media = config.media.normalized()
        kind_hint = str(row["kind_hint"] or "")
        rows: list[CDXRow] = []
        resume: str | None = None
        # A bounded exact traversal is normally tiny. It intentionally avoids
        # default collapse=urlkey so metadata eligibility can be applied before
        # earliest/latest selection. If metadata is weak, payload validation in
        # the media downloader remains authoritative.
        for _page in range(8):
            result = request_cdx_rows(
                client,
                endpoints,
                build_media_params(
                    config, url, config.from_date, config.to_date, resume,
                    exact=True, page_size=min(config.page_size, 512),
                ),
                max_bytes=16 * 1024 * 1024,
                prefer_text=True,
            )
            rows.extend(result.rows)
            if snapshot_strategy in {"earliest", "latest"}:
                accepted = []
                for compact in rows:
                    value = cdx_row_to_dict(compact)
                    allowed, kind, _extension = _embedded_row_kind(value, kind_hint, media)
                    if allowed and kind:
                        accepted.append(compact)
                if accepted:
                    return row, rows, None
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


def _target_pattern_covers_url(pattern: str, url: str) -> bool:
    parsed = urlsplit(url)
    if parsed.scheme and parsed.netloc:
        value = parsed.netloc + parsed.path
        if parsed.query:
            value += "?" + parsed.query
    else:
        value = url.split("://", 1)[-1]
    return fnmatch.fnmatchcase(value.casefold(), str(pattern or "").casefold())


def _cached_embedded_inventory(
    database: sqlite3.Connection,
    config: ProjectConfig,
    signature: str,
    original_url: str,
    kind_hint: str,
) -> tuple[list[sqlite3.Row], list[int]] | None:
    """Resolve an embedded URL only from a provably complete compatible index.

    A single cached media row is not proof of coverage. Reuse is allowed only
    when the same media query/state signature completed every requested year for
    a direct-media target that contains this URL. If no matching row exists we
    deliberately fall back to the exact CDX lookup because extension filters or
    historical URL oddities may have excluded it from the broad inventory.
    """
    state_signature = media_index_state_signature(config)
    covering_ids: list[int] = []
    for target in database.execute("SELECT id,pattern FROM media_targets ORDER BY id"):
        if not _target_pattern_covers_url(str(target["pattern"] or ""), original_url):
            continue
        target_id = int(target["id"])
        if config.text_collapse_scope == "range":
            complete = not uncovered_media_ranges(
                database, target_id, state_signature, config.from_date, config.to_date
            )
        else:
            complete = True
            for year in range(config.from_year, config.to_year + 1):
                window = cdx_year_window(config, year)
                if window is None:
                    continue
                state = database.execute(
                    """SELECT complete FROM media_index_state
                       WHERE target_id=? AND extension=? AND year=? AND query_signature=?""",
                    (target_id, ALL_EXTENSIONS_STATE, year, state_signature),
                ).fetchone()
                if not state or not int(state["complete"] or 0):
                    complete = False
                    break
        if complete:
            covering_ids.append(target_id)
    if not covering_ids:
        return None

    placeholders = ",".join("?" for _ in covering_ids)
    rows = database.execute(
        f"""SELECT id,timestamp,original_url,mimetype,statuscode,digest,length,target_id
            FROM media_captures
            WHERE query_signature=? AND original_url=?
              AND timestamp BETWEEN ? AND ? AND target_id IN ({placeholders})
            ORDER BY timestamp,id""",
        (signature, original_url, config.from_date, config.to_date, *covering_ids),
    ).fetchall()
    if not rows:
        return None

    media = config.media.normalized()
    accepted: list[sqlite3.Row] = []
    for row in rows:
        value = {
            "timestamp": str(row["timestamp"]),
            "original": str(row["original_url"]),
            "mimetype": str(row["mimetype"] or ""),
            "statuscode": str(row["statuscode"] or ""),
            "digest": str(row["digest"] or ""),
            "length": str(row["length"] or 0),
        }
        allowed, kind, _extension = _embedded_row_kind(value, kind_hint, media)
        if allowed and kind:
            accepted.append(row)
    if not accepted:
        return None

    if media.snapshot_strategy == "earliest":
        chosen = [min(accepted, key=lambda row: str(row["timestamp"]))]
    elif media.snapshot_strategy == "latest":
        chosen = [max(accepted, key=lambda row: str(row["timestamp"]))]
    else:
        chosen = accepted
    return chosen, [int(row["id"]) for row in chosen]


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
    # Text-prefix validation may queue deferred media even when ordinary embedded
    # discovery is disabled. Those candidates still belong to the standard media
    # phase and must be resolved here rather than completed inside text workers.
    if media.discover_embedded:
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
            nonlocal completed, indexed, unavailable
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

                cached = _cached_embedded_inventory(
                    database, config, signature, link, str(queue_row["kind_hint"] or "")
                )
                if cached is not None:
                    chosen_rows, chosen_ids = cached
                    document_id = (
                        int(queue_row["source_document_id"])
                        if queue_row["source_document_id"] is not None else None
                    )
                    with database:
                        if document_id is not None and chosen_ids:
                            placeholders = ",".join("?" for _ in chosen_ids)
                            database.execute(
                                f"UPDATE media_captures SET source_document_id=COALESCE(source_document_id,?),updated_at=? "
                                f"WHERE id IN ({placeholders})",
                                (document_id, utc_now(), *chosen_ids),
                            )
                        mark_media_discovery_lookup(
                            database, int(queue_row["id"]), "indexed",
                            result_count=len(chosen_rows), increment_attempt=False,
                        )
                    completed += 1
                    indexed += 1
                    if callback and (completed == pending_total or completed % 100 == 0):
                        callback(ProgressEvent(
                            "media_embed",
                            f"Embedded media lookup {completed:,}/{pending_total:,}; indexed {indexed:,}; unavailable {unavailable:,}; errors {errors:,}",
                            completed, pending_total,
                            {"indexed": indexed, "unavailable": unavailable, "errors": errors, "inventory_reused": True},
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
                    eligible_items: list[tuple[dict[str, str], str, str]] = []
                    for compact in rows:
                        value = cdx_row_to_dict(compact)
                        allowed, kind, extension = _embedded_row_kind(value, hint, media)
                        if allowed and kind:
                            eligible_items.append((value, kind, extension))
                    # Persist every metadata-eligible exact capture first, then
                    # apply earliest/latest deterministically over the complete
                    # local set. If payload validation later disproves the chosen
                    # format, the media downloader can promote the next eligible
                    # snapshot without downloading every snapshot.
                    accepted_items = eligible_items
                    accepted_count = len(accepted_items)
                    with database:
                        if accepted_items:
                            upsert_media_captures(
                                database, accepted_items, None, signature, document_id, source_type
                            )
                            _apply_snapshot_strategy(database, signature, media.snapshot_strategy)
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
    limiter = SharedFixedRateLimiter(config.cdx_delay, key=WAYBACK_INDEX_RATE_KEY)
    host_gate = shared_host_gate(config.rate_limit_base_pause, config.rate_limit_max_pause)

    def on_retry(attempt: int, total: int, reason: str, wait_seconds: float) -> None:
        if callback:
            if wait_seconds > 0:
                message = f"{reason}. Retry {attempt}/{total} in {wait_seconds:.1f}s…"
            else:
                message = reason
            callback(ProgressEvent("media_embed", message))

    def on_rate_event(detail: dict[str, object]) -> None:
        if not callback:
            return
        phase = str(detail.get("phase") or "cooldown")
        status = int(detail.get("http_status") or 429)
        wait_seconds = max(0.0, float(detail.get("wait_seconds") or 0.0))
        if phase == "paused":
            message = f"Wayback HTTP {status} recovery budget is exhausted; embedded-media progress was saved for Resume."
            stage = "rate_limit_paused"
        else:
            message = f"Wayback HTTP {status} service cooldown active for up to {wait_seconds:.1f}s; one recovery probe will run next."
            stage = "rate_limit_waiting"
        callback(ProgressEvent(stage, message, detail=dict(detail)))

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
        rate_event_callback=on_rate_event,
        connection_failure_pause_threshold=config.network.normalized().connection_failure_pause_threshold,
        connection_retry_seconds=config.network.normalized().connection_retry_seconds,
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
    limiter = SharedFixedRateLimiter(config.cdx_delay, key=WAYBACK_INDEX_RATE_KEY)
    host_gate = shared_host_gate(config.rate_limit_base_pause, config.rate_limit_max_pause)

    def on_retry(attempt: int, total: int, reason: str, wait_seconds: float) -> None:
        if callback:
            if wait_seconds <= 0:
                message = reason
                stage = "network"
            else:
                message = f"CDX media request failed ({reason}). Retrying attempt {attempt}/{total} in {wait_seconds:.1f}s…"
                stage = "media_index"
            callback(ProgressEvent(stage, message))

    def on_rate_event(detail: dict[str, object]) -> None:
        if not callback:
            return
        phase = str(detail.get("phase") or "cooldown")
        status = int(detail.get("http_status") or 429)
        wait_seconds = max(0.0, float(detail.get("wait_seconds") or 0.0))
        spacing = detail.get("effective_spacing_seconds")
        if phase == "paused":
            message = f"Wayback HTTP {status} recovery budget is exhausted; the exact media queue was saved for Resume."
            stage = "rate_limit_paused"
        else:
            spacing_text = f" Effective index spacing: {float(spacing):.3f}s." if spacing is not None else ""
            message = f"Wayback HTTP {status} service cooldown active for up to {wait_seconds:.1f}s; one recovery probe will run next.{spacing_text}"
            stage = "rate_limit_waiting"
        callback(ProgressEvent(stage, message, detail=dict(detail)))

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
        rate_event_callback=on_rate_event,
        connection_failure_pause_threshold=config.network.normalized().connection_failure_pause_threshold,
        connection_retry_seconds=config.network.normalized().connection_retry_seconds,
    )
    try:
        index_direct_media(config, database, client, stop_event, callback, signature, state_signature)
        index_embedded_media(config, database, client, stop_event, callback, signature)
        _apply_snapshot_strategy(database, signature, media.snapshot_strategy)
        return signature
    finally:
        client.close()
