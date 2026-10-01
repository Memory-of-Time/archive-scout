from __future__ import annotations

import hashlib
import sqlite3
import json
from datetime import datetime

from ..config import ProjectConfig
from ..utils import parse_cdx_parameter_lines, utc_now


def _cdx_signature_payload(config: ProjectConfig) -> dict:
    """Return semantic text-inventory identity.

    Audit2 projects are explicitly year-scoped and retain their historical
    date-bound identity. Audit3 range-scoped projects keep the requested date
    interval in durable coverage rows instead, so extending a project can reuse
    completed subranges without pretending pagination checkpoints from one
    layout certify another.
    """
    payload = {
        "filters": config.cdx_filters,
        "collapses": config.cdx_collapses,
        "match_type": config.cdx_match_type,
        "extra": config.cdx_extra_params,
    }
    if config.text_collapse_scope == "range":
        payload["coverage_scope"] = "range"
        # Collapsed results are range-dependent: extending/narrowing the server
        # query can change which adjacent snapshot survives. Keep bounds in the
        # selected-result identity so changed collapsed ranges are fully queried.
        if config.cdx_collapses:
            payload["from"] = config.from_date
            payload["to"] = config.to_date
    else:
        payload["from"] = config.from_date
        payload["to"] = config.to_date
    return payload



def cdx_signature_is_date_bound(config: ProjectConfig) -> bool:
    """Whether the semantic text inventory identity already contains its date bounds."""
    return config.text_collapse_scope != "range" or bool(config.cdx_collapses)

def cdx_query_signature(config: ProjectConfig, page_size: int | None = None) -> str:
    """Semantic inventory signature, independent of transport batch size."""
    del page_size
    raw = json.dumps(
        _cdx_signature_payload(config), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]


def _audit2_signature_payload(config: ProjectConfig) -> dict:
    # Audit2 always included the selected date bounds and had no coverage-scope
    # discriminator. Keep this byte-for-byte semantic shape for compatibility
    # adoption from existing projects.
    return {
        "from": config.from_date,
        "to": config.to_date,
        "filters": config.cdx_filters,
        "collapses": config.cdx_collapses,
        "match_type": config.cdx_match_type,
        "extra": config.cdx_extra_params,
    }


def _legacy_cdx_query_signature(config: ProjectConfig, page_size: int | None = None) -> str:
    payload = _audit2_signature_payload(config)
    if page_size is not None:
        payload["page_size"] = int(page_size)
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]


def cdx_query_signatures(config: ProjectConfig) -> tuple[str, ...]:
    """Return safe compatible inventory identities.

    Audit2 split server-side collapse by calendar year.  Audit3 range-scope
    queries must not adopt that inventory when collapse is enabled because the
    surviving captures can differ.  Year-scope projects and uncollapsed range
    queries can safely adopt the earlier identity.
    """
    sizes = [config.page_size, 5000, 25000, 1000, 10000, 50000, 100000, 150000]
    values = [cdx_query_signature(config)]
    if config.text_collapse_scope == "year" or not config.cdx_collapses:
        values.append(_legacy_cdx_query_signature(config, None))
        values.extend(_legacy_cdx_query_signature(config, size) for size in sizes)
    return tuple(dict.fromkeys(values))


def cdx_layout_signature(config: ProjectConfig, start: str, end: str, strategy: str, page_blocks: int = 0) -> str:
    payload = {
        "start": start, "end": end, "strategy": strategy,
        "endpoint_mode": config.network.normalized().endpoint_mode,
        "page_blocks": int(page_blocks), "page_size": int(config.page_size),
        "sort": "urlkey,timestamp",
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]


def _merge_compatible_capture_rows(
    database: sqlite3.Connection,
    target_id: int,
    old_signature: str,
    new_signature: str,
    start: str,
    end: str,
) -> None:
    """Move non-conflicting capture identity while preserving collision history.

    Query signatures describe membership, not the archived response itself. Most
    legacy rows can adopt the semantic signature in place. When the destination
    identity already exists, keep both rows so documents/reviews/research linked
    to either capture are never cascaded away, and copy only useful acquisition
    state onto the destination row when it is missing there.
    """
    rows = database.execute(
        """SELECT * FROM captures
           WHERE target_id=? AND query_signature=? AND timestamp BETWEEN ? AND ?
           ORDER BY id""",
        (target_id, old_signature, start, end),
    ).fetchall()
    for old in rows:
        current = database.execute(
            """SELECT * FROM captures
               WHERE original_url=? AND timestamp=? AND query_signature=? LIMIT 1""",
            (str(old["original_url"]), str(old["timestamp"]), new_signature),
        ).fetchone()
        if current is None:
            database.execute(
                "UPDATE captures SET query_signature=?,updated_at=? WHERE id=?",
                (new_signature, utc_now(), int(old["id"])),
            )
            continue

        old_path = str(old["local_path"] or "")
        current_path = str(current["local_path"] or "")
        promoted_state = str(current["state"] or "pending")
        if not current_path and old_path and str(old["state"] or "") in {
            "downloaded", "downloaded_unscanned", "scanning"
        }:
            # The destination capture can safely reuse the exact saved payload,
            # but it deliberately does not steal the old document_id. If a later
            # scan is requested it can rebuild derivatives from the same file.
            promoted_state = "downloaded_unscanned"

        database.execute(
            """UPDATE captures SET
                   mimetype=COALESCE(NULLIF(mimetype,''),?),
                   statuscode=COALESCE(NULLIF(statuscode,''),?),
                   digest=COALESCE(NULLIF(digest,''),?),
                   length=CASE WHEN COALESCE(length,0)<=0 THEN ? ELSE length END,
                   state=?,
                   local_path=COALESCE(NULLIF(local_path,''),NULLIF(?,'')),
                   content_hash=COALESCE(NULLIF(content_hash,''),NULLIF(?,'')),
                   detected_encoding=COALESCE(NULLIF(detected_encoding,''),NULLIF(?,'')),
                   http_status=COALESCE(http_status,?),
                   final_url=COALESCE(NULLIF(final_url,''),NULLIF(?,'')),
                   bytes_saved=MAX(COALESCE(bytes_saved,0),?),
                   download_attempts=MIN(download_attempts,?),
                   updated_at=?
               WHERE id=?""",
            (
                str(old["mimetype"] or ""), str(old["statuscode"] or ""),
                str(old["digest"] or ""), int(old["length"] or 0), promoted_state,
                old_path, str(old["content_hash"] or ""),
                str(old["detected_encoding"] or ""), old["http_status"],
                str(old["final_url"] or ""), int(old["bytes_saved"] or 0),
                int(old["download_attempts"] or 0), utc_now(), int(current["id"]),
            ),
        )


def _copy_index_page_checkpoints(
    database: sqlite3.Connection,
    target_id: int,
    old_signature: str,
    new_signature: str,
    start: str,
    end: str,
) -> None:
    database.execute(
        """INSERT OR IGNORE INTO index_pages(
               query_signature,target_id,window_start,window_end,page,row_count,status,updated_at
           )
           SELECT ?,target_id,window_start,window_end,page,row_count,status,updated_at
           FROM index_pages
           WHERE query_signature=? AND target_id=?
             AND window_start<=? AND window_end>=?""",
        (new_signature, old_signature, target_id, end, start),
    )


def adopt_compatible_index_state(
    database: sqlite3.Connection,
    target_id: int,
    year: int,
    config: ProjectConfig,
    signature: str,
) -> None:
    """Adopt page-size-bound legacy identity without losing resumable work."""
    start, end = cdx_year_window(config, year) or (
        f"{year:04d}0101000000", f"{year:04d}1231235959"
    )
    current = database.execute(
        """SELECT resume_key,complete,seen,error_id,updated_at
           FROM index_state WHERE target_id=? AND year=? AND query_signature=?""",
        (target_id, year, signature),
    ).fetchone()

    for candidate in cdx_query_signatures(config):
        if candidate == signature:
            continue
        state = database.execute(
            """SELECT resume_key,complete,seen,error_id,updated_at FROM index_state
               WHERE target_id=? AND year=? AND query_signature=?""",
            (target_id, year, candidate),
        ).fetchone()
        if not state:
            continue

        _merge_compatible_capture_rows(
            database, target_id, candidate, signature, start, end
        )
        _copy_index_page_checkpoints(
            database, target_id, candidate, signature, start, end
        )

        if current is None:
            database.execute(
                """INSERT INTO index_state(
                       target_id,year,query_signature,resume_key,complete,seen,error_id,updated_at
                   ) VALUES(?,?,?,?,?,?,?,?)""",
                (
                    target_id, year, signature, state["resume_key"],
                    state["complete"], state["seen"], state["error_id"],
                    state["updated_at"],
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
                """UPDATE index_state SET resume_key=?,complete=?,seen=?,error_id=?,updated_at=?
                   WHERE target_id=? AND year=? AND query_signature=?""",
                (
                    resume_key, complete, seen, error_id, utc_now(),
                    target_id, year, signature,
                ),
            )
            current = database.execute(
                """SELECT resume_key,complete,seen,error_id,updated_at FROM index_state
                   WHERE target_id=? AND year=? AND query_signature=?""",
                (target_id, year, signature),
            ).fetchone()


def adopt_compatible_index_identity(
    database: sqlite3.Connection,
    config: ProjectConfig,
) -> None:
    """Adopt legacy semantic-equivalent text inventory before any consumer.

    Direct Download does not run indexing first, so compatibility migration must
    not live only inside ``index_archive``. This lightweight pass touches only
    existing targets/state and is safe to call before replay selection.
    """
    normalized = config.normalized()
    for target in normalized.targets:
        target_config = normalized.for_target(target)
        target_row = database.execute(
            "SELECT id FROM targets WHERE pattern=?", (target,)
        ).fetchone()
        if not target_row:
            continue
        target_id = int(target_row[0])
        signature = cdx_query_signature(target_config)
        for year in range(target_config.from_year, target_config.to_year + 1):
            adopt_compatible_index_state(
                database, target_id, year, target_config, signature
            )

def cdx_year_window(config: ProjectConfig, year: int) -> tuple[str, str] | None:
    start = max(config.from_date, f"{year:04d}0101000000")
    end = min(config.to_date, f"{year:04d}1231235959")
    if start > end:
        return None
    return start, end


def cdx_target_value(target: str, match_type: str) -> str:
    if match_type in {"exact", "prefix", "host", "domain"}:
        target = target.rstrip("*")
    if match_type in {"host", "domain"}:
        target = target.rstrip("/")
    return target


def build_cdx_params(
    config: ProjectConfig,
    target: str,
    start: str,
    end: str,
    resume: str | None = None,
    page_size: int | None = None,
) -> list[tuple[str, str]]:
    params = [
        ("url", cdx_target_value(target, config.cdx_match_type)),
        ("from", start),
        ("to", end),
        ("output", "json"),
        ("fl", "urlkey,timestamp,original,mimetype,statuscode,digest,length"),
    ]
    if config.cdx_match_type:
        params.append(("matchType", config.cdx_match_type))
    params.extend(("filter", value) for value in config.cdx_filters)
    params.extend(("collapse", value) for value in config.cdx_collapses)
    params.extend(parse_cdx_parameter_lines(config.cdx_extra_params))
    params.extend([("limit", str(page_size or config.page_size)), ("showResumeKey", "true")])
    if resume:
        params.append(("resumeKey", resume))
    return params


def parse_cdx(payload: object) -> tuple[list[dict[str, str]], str | None]:
    # The extension/legacy API must enforce the same completeness contract as
    # the compact hot-path parser; otherwise malformed rows silently vanish.
    from .client import parse_cdx_rows_payload
    validated = parse_cdx_rows_payload(payload)
    if payload == [] or isinstance(payload, dict):
        return [], None
    header = payload[0]
    rows = [dict(zip(header, item)) for item in payload[1:1 + len(validated.rows)]]
    return rows, validated.resume_key


def cdx_endpoints(config: ProjectConfig) -> tuple[str, ...]:
    from ..constants import CDX_URL, CDX_TIMEMAP_JSON_URL, CDX_TIMEMAP_URL
    mode = config.network.normalized().endpoint_mode
    if mode == "cdx":
        return (CDX_URL,)
    if mode == "timemap":
        return (CDX_TIMEMAP_JSON_URL, CDX_TIMEMAP_URL)
    # Resume-key traversal is a CDX operation. Keep CDX first here and use the
    # Timemap-first endpoint order only for numbered-page acquisition below.
    return (CDX_URL, CDX_TIMEMAP_JSON_URL, CDX_TIMEMAP_URL)


def cdx_paged_endpoints(config: ProjectConfig) -> tuple[str, ...]:
    from ..constants import CDX_URL, CDX_TIMEMAP_JSON_URL
    mode = config.network.normalized().endpoint_mode
    if mode == "cdx":
        return (CDX_URL,)
    # Numbered automatic paging follows the reference downloader exactly: one
    # native Timemap JSON service. A failed page remains one durable page retry;
    # it is not silently reissued against different endpoint semantics.
    return (CDX_TIMEMAP_JSON_URL,)


def is_broad_cdx_query(config: ProjectConfig, target: str) -> bool:
    # An explicit exact match overrides the convenience wildcard appended by
    # normalize_target(). The outgoing request strips that wildcard, so strategy
    # selection must reason about the same effective query instead of treating a
    # normalized exact page URL as a broad prefix inventory.
    if config.cdx_match_type == "exact":
        return False
    if config.cdx_match_type in {"prefix", "host", "domain"}:
        return True
    value = target.strip()
    return value.endswith("/*") or value.startswith("*.") or value.endswith("*")


def preferred_index_strategy(config: ProjectConfig, target: str) -> str:
    strategy = config.network.normalized().index_strategy
    if strategy != "auto":
        return strategy
    # Audit3 starts unknown inventories with a data-bearing resume request.
    # This lets sparse multi-year ranges finish in one request instead of paying
    # a page-count + per-year Timemap tax. Existing saved paged plans remain
    # paged, and users can still explicitly choose the paged strategy for proven
    # dense inventories. Healthy runs do not switch strategies repeatedly.
    return "resume"


def build_num_pages_params(
    config: ProjectConfig,
    target: str,
    start: str,
    end: str,
    page_blocks: int | None = None,
) -> list[tuple[str, str]]:
    params = build_cdx_params(config, target, start, end, page_size=config.page_size)
    params = [
        (key, value)
        for key, value in params
        if key not in {"limit", "showResumeKey", "resumeKey", "fl"}
    ]
    params.append(("showNumPages", "true"))
    blocks = config.network.normalized().page_blocks if page_blocks is None else int(page_blocks)
    # The Settings-tab default is 0, meaning "automatic".  Archive Scout's
    # high-throughput automatic profile uses the proven pageSize=9 grouping
    # with ten parallel Timemap workers; explicit positive values remain custom.
    if blocks <= 0:
        blocks = 9
    params.append(("pageSize", str(blocks)))
    return params


def build_paged_cdx_params(
    config: ProjectConfig,
    target: str,
    start: str,
    end: str,
    page: int,
    page_blocks: int | None = None,
) -> list[tuple[str, str]]:
    params = build_cdx_params(config, target, start, end, page_size=config.page_size)
    params = [(key, value) for key, value in params if key not in {"limit", "showResumeKey", "resumeKey"}]
    params = [
        (key, "urlkey,timestamp,original,mimetype,statuscode,digest,length") if key == "fl" else (key, value)
        for key, value in params
    ]
    params.append(("page", str(max(0, int(page)))))
    blocks = config.network.normalized().page_blocks if page_blocks is None else int(page_blocks)
    if blocks <= 0:
        blocks = 9
    params.append(("pageSize", str(blocks)))
    return params


def parse_num_pages(payload: object) -> int:
    if isinstance(payload, int):
        return max(0, payload)
    if isinstance(payload, str) and payload.strip().isdigit():
        return max(0, int(payload.strip()))
    if isinstance(payload, list):
        # Timemap JSON's showNumPages response is normally a tiny two-row table
        # and the reference downloader reads payload[1][0].  Accept that shape
        # explicitly, while retaining the older scalar/nested compatibility.
        if len(payload) >= 2 and isinstance(payload[1], list) and payload[1]:
            value = payload[1][0]
            if str(value).strip().isdigit():
                return max(0, int(str(value).strip()))
        candidates = payload
        while isinstance(candidates, list) and len(candidates) == 1:
            candidates = candidates[0]
        if isinstance(candidates, int):
            return max(0, candidates)
        if isinstance(candidates, str) and candidates.strip().isdigit():
            return max(0, int(candidates.strip()))
    if isinstance(payload, dict):
        for key in ("pages", "numPages", "num_pages"):
            value = payload.get(key)
            if str(value).strip().isdigit():
                return max(0, int(str(value).strip()))
    raise RuntimeError(f"unexpected CDX page-count response: {payload!r}")
