from __future__ import annotations

import hashlib
import json
from datetime import datetime

from ..config import ProjectConfig
from ..utils import parse_cdx_parameter_lines


def _semantic_cdx_payload(config: ProjectConfig) -> dict:
    """v1.1.1-style identity: result membership, not HTTP page layout.

    A server-side collapse can select a *different* capture when the requested
    date range changes, so those ranges cannot reuse each other's inventory.
    """
    payload = {
        "filters": config.cdx_filters,
        "collapses": config.cdx_collapses,
        "match_type": config.cdx_match_type,
        "extra": config.cdx_extra_params,
    }
    if config.text_collapse_scope == "range":
        payload["coverage_scope"] = "range"
        if config.cdx_collapses:
            payload["from"] = config.from_date
            payload["to"] = config.to_date
    else:
        payload["from"] = config.from_date
        payload["to"] = config.to_date
    return payload


def cdx_query_signature(config: ProjectConfig, page_size: int | None = None) -> str:
    """Semantic inventory identity, independent of page size and worker count."""
    del page_size
    raw = json.dumps(_semantic_cdx_payload(config), ensure_ascii=False,
                     sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]


def _legacy_cdx_query_signature(config: ProjectConfig, page_size: int | None) -> str:
    """Pre-v1.2.4 identity, retained strictly for existing checkpoint adoption."""
    payload = {
        "from": config.from_date, "to": config.to_date,
        "filters": config.cdx_filters, "collapses": config.cdx_collapses,
        "match_type": config.cdx_match_type, "extra": config.cdx_extra_params,
    }
    if page_size is not None:
        payload["page_size"] = int(page_size)
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]


def cdx_query_signatures(config: ProjectConfig) -> tuple[str, ...]:
    """Canonical signature followed by compatible earlier transport identities.

    Legacy v1.2.3 signatures always incorporated *these exact date bounds*, so
    adopting them cannot mistake a different collapsed range for this one.
    """
    sizes = (config.page_size, 5000, 25000, 1000, 10000, 50000, 100000, 150000)
    values = [cdx_query_signature(config)]
    values.extend(_legacy_cdx_query_signature(config, size) for size in sizes)
    if config.text_collapse_scope == "year" or not config.cdx_collapses:
        values.append(_legacy_cdx_query_signature(config, None))
    return tuple(dict.fromkeys(values))


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
    if payload in (None, []):
        return [], None
    if isinstance(payload, dict):
        message = str(payload.get("message") or payload.get("error") or payload)
        lowered = message.lower()
        if "no capture" in lowered or "no result" in lowered or "not found" in lowered:
            return [], None
        raise RuntimeError(message)
    if not isinstance(payload, list) or not payload:
        return [], None
    header = payload[0]
    if not isinstance(header, list):
        raise RuntimeError("unexpected CDX response header")
    body = payload[1:]
    resume = None
    if len(body) >= 2 and body[-2] == [] and isinstance(body[-1], list) and len(body[-1]) == 1:
        resume = str(body[-1][0])
        body = body[:-2]
    if len(header) != len(set(map(str, header))) or not {"timestamp", "original"}.issubset(set(header)):
        raise RuntimeError("CDX response is missing required fields or has duplicate headers")
    rows: list[dict[str, str]] = []
    for item in body:
        if not isinstance(item, list) or len(item) != len(header):
            raise RuntimeError("incomplete CDX response row; refusing partial inventory")
        row = dict(zip(header, item))
        if not row.get("timestamp") or not row.get("original"):
            raise RuntimeError("CDX row has no capture timestamp or original URL")
        rows.append(row)
    return rows, resume


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
    if config.cdx_match_type in {"prefix", "host", "domain"}:
        return True
    value = target.strip()
    return value.endswith("/*") or value.startswith("*.") or value.endswith("*")


def preferred_index_strategy(config: ProjectConfig, target: str) -> str:
    strategy = config.network.normalized().index_strategy
    if strategy != "auto":
        return strategy
    # The v1.1.1 automatic strategy uses data-bearing resume traversal for
    # unknown inventories, avoiding page-count and sparse-year query overhead.
    # Explicit paged mode and persisted paged plans remain available for dense sites.
    return "resume"


def build_num_pages_params(
    config: ProjectConfig,
    target: str,
    start: str,
    end: str,
    page_blocks: int | None = None,
) -> list[tuple[str, str]]:
    params = build_cdx_params(config, target, start, end, page_size=config.page_size)
    params = [(key, value) for key, value in params if key not in {"limit", "showResumeKey", "resumeKey"}]
    params.append(("showNumPages", "true"))
    blocks = config.network.normalized().page_blocks if page_blocks is None else int(page_blocks)
    # 0 deliberately means "use Internet Archive's server-selected page size".
    # The server default groups substantially more ZipNum blocks than the old
    # hard-coded pageSize=9, which avoids turning broad sites into thousands of
    # tiny page requests while keeping the official pagination mechanism.
    if blocks > 0:
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
    params.append(("page", str(max(0, int(page)))))
    blocks = config.network.normalized().page_blocks if page_blocks is None else int(page_blocks)
    if blocks > 0:
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

def cdx_layout_signature(config: ProjectConfig, start: str, end: str, strategy: str, page_blocks: int = 0) -> str:
    payload = {
        "start": start, "end": end, "strategy": strategy,
        "endpoint_mode": config.network.normalized().endpoint_mode,
        "page_blocks": int(page_blocks), "page_size": int(config.page_size),
        "sort": "urlkey,timestamp",
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]
