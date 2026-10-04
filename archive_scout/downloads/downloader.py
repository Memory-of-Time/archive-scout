from __future__ import annotations

import concurrent.futures
import hashlib
import os
import re
import shutil
import sqlite3
import threading
import time
import urllib.parse
from collections import deque
from pathlib import Path
from typing import Callable, Iterator

from ..cdx.client import HttpClient, RateLimitDeferred
from ..cdx.parameters import adopt_compatible_index_identity, cdx_query_signature, cdx_signature_is_date_bound
from ..classification import (
    PREVIEW_BUDGET, RESOURCE_CLASSIFIER_REVISION, classify_capture_inventory,
    classify_payload_prefix,
)
from ..config import ProjectConfig
from ..constants import REPLAY_URL
from ..content import (
    CHARSET_PATTERN,
    classify_replay_content,
    classify_text_candidate,
    decode_bytes,
    decode_bytes_with_encoding,
    looks_textual_bytes,
    parse_page,
)
from ..database.repositories import (
    mark_capture_skipped,
    mark_media_discovery_document,
    queue_media_discovery_candidates,
    record_error,
    record_site_issue,
    requeue_reclassifiable_skips,
    resolve_errors,
    save_match,
    upsert_document,
)
from ..events import ConnectivityPaused, ProgressEvent, Stopped
from ..parsing.embeds import extract_embed_candidates_fast
from ..site_status import host_from_url, should_surface_site_issue, site_issue_message
from ..scanning.jobs import ScanJob
from ..scanning.keywords import compile_prefilter
from ..scanning.scoring import analyze_content, prepare_analysis_fields
from ..storage import capture_path as url_capture_path, sha256_file
from ..utils import hash_text, normalize_search, utc_now
from .rate_limit import (SharedFixedRateLimiter, WAYBACK_REPLAY_RATE_KEY, shared_host_gate)
from .validation import classify_exception
from ..network.transports import PreviewRejected, RedirectPolicyError, is_local_storage_error

CLASSIFIER_REVISION = 2


class _CombinedStopEvent:
    """Thread-event facade that is set when either source event is set."""

    def __init__(self, *events: threading.Event) -> None:
        self.events = tuple(events)

    def is_set(self) -> bool:
        return any(event.is_set() for event in self.events)

    def wait(self, timeout: float | None = None) -> bool:
        if self.is_set():
            return True
        if timeout is not None and timeout <= 0:
            return self.is_set()
        deadline = None if timeout is None else time.monotonic() + timeout
        while not self.is_set():
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return self.is_set()
                step = min(0.1, remaining)
            else:
                step = 0.1
            # Wait on the first real Event to avoid a pure polling sleep while
            # still observing the internal acquisition-cancel event promptly.
            self.events[0].wait(step)
        return True


def replay_url(timestamp: str, original: str, modifier: str = "id_") -> str:
    encoded = urllib.parse.quote(original, safe=":/?&=#%+;,[]@!$'()*")
    clean_modifier = modifier if modifier in {"id_", "if_", "oe_"} else "id_"
    return f"{REPLAY_URL}/{timestamp}{clean_modifier}/{encoded}"


def replay_original_url(url: str) -> str | None:
    """Return the embedded original URL from a Wayback replay URL."""
    try:
        parsed = urllib.parse.urlsplit(str(url))
    except ValueError:
        return None
    if parsed.hostname not in {"web.archive.org", "wwwb-app0.us.archive.org"}:
        return None
    match = re.match(r"^/web/[^/]+/(https?://.+)$", parsed.path + (("?" + parsed.query) if parsed.query else ""), re.I)
    if not match:
        return None
    return urllib.parse.unquote(match.group(1))


def _target_host_scope(config: ProjectConfig) -> list[tuple[str, bool]]:
    scopes: list[tuple[str, bool]] = []
    for target in config.targets:
        raw = str(target).strip().replace("*", "")
        if not raw:
            continue
        parsed = urllib.parse.urlsplit(raw if "://" in raw else "http://" + raw)
        host = (parsed.hostname or "").casefold().rstrip(".")
        if not host:
            continue
        settings = config.settings_for_target(target)
        match_type = str(settings.get("cdx_match_type") or config.cdx_match_type or "").casefold()
        scopes.append((host, match_type == "domain"))
    return scopes


def _host_in_project_scope(host: str, scopes: list[tuple[str, bool]]) -> bool:
    value = str(host or "").casefold().rstrip(".")
    for allowed, include_subdomains in scopes:
        if value == allowed or (include_subdomains and value.endswith("." + allowed)):
            return True
    return False


def make_replay_redirect_validator(config: ProjectConfig, source_original: str):
    """Return the shared archive-only redirect policy for one stored capture.

    The callback runs before the transport contacts each redirect destination.
    External is defined by the embedded original host, not by the common
    web.archive.org replay host. Live destinations are never silently attached
    to historical evidence in v1.0.4.
    """
    normalized = config.normalized()
    scopes = _target_host_scope(normalized)
    source_host = (urllib.parse.urlsplit(source_original).hostname or "").casefold().rstrip(".")

    def validate(source_url: str, destination_url: str) -> None:
        embedded = replay_original_url(destination_url)
        if embedded is None:
            parsed = urllib.parse.urlsplit(destination_url)
            if parsed.hostname and parsed.hostname.casefold() != "web.archive.org":
                raise RedirectPolicyError(source_url, destination_url, "live_redirect_blocked")
            # Archive-internal canonical/timestamp redirects without an embedded
            # original remain eligible; a later hop is checked again.
            return
        dest_host = (urllib.parse.urlsplit(embedded).hostname or "").casefold().rstrip(".")
        if dest_host == source_host or _host_in_project_scope(dest_host, scopes):
            return
        if normalized.download_external_redirects:
            return
        raise RedirectPolicyError(source_url, destination_url, "external_redirect_blocked")

    return validate


def capture_path(root: Path, capture_id: int, timestamp: str, original: str) -> Path:
    """Compatibility wrapper using the legacy URL-derived filename policy."""
    del capture_id
    return url_capture_path(root, timestamp, original)

def _inventory_scope_predicate(
    database: sqlite3.Connection,
    config: ProjectConfig,
    *,
    alias: str = "c",
) -> tuple[str, list[object]]:
    """Return the active text-inventory predicate for every configured target.

    Per-target CDX overrides can change the semantic query signature and date
    bounds.  Indexing has always stored those target-specific identities, but
    older replay/scan selectors compared every row with only the global project
    signature.  That made correctly indexed override rows disappear from later
    Simple-mode acquisition/scanning.  Keep target id, signature and date scope
    together all the way through the text pipeline.

    ``target_id IS NULL`` remains accepted for legacy/test rows created before
    target provenance became mandatory.
    """
    normalized = config.normalized()
    clauses: list[str] = []
    params: list[object] = []
    seen: set[tuple[object, ...]] = set()
    for target in normalized.targets:
        target_config = normalized.for_target(target)
        signature = cdx_query_signature(target_config)
        target_row = database.execute(
            "SELECT id FROM targets WHERE pattern=?", (target,)
        ).fetchone()
        target_id = int(target_row[0]) if target_row is not None else None
        date_bound = cdx_signature_is_date_bound(target_config)
        identity = (
            target_id, signature,
            "" if date_bound else target_config.from_date,
            "" if date_bound else target_config.to_date,
        )
        if identity in seen:
            continue
        seen.add(identity)
        parts: list[str] = []
        if target_id is not None:
            parts.append(f"({alias}.target_id=? OR {alias}.target_id IS NULL)")
            params.append(target_id)
        parts.append(f"{alias}.query_signature=?")
        params.append(signature)
        if not date_bound:
            parts.append(f"{alias}.timestamp BETWEEN ? AND ?")
            params.extend([target_config.from_date, target_config.to_date])
        clauses.append("(" + " AND ".join(parts) + ")")

    if not clauses:
        # Keep helpers useful for compatibility callers that supply an otherwise
        # valid config with no target provenance. Normal GUI acquisition already
        # rejects an empty target list before reaching this point.
        signature = cdx_query_signature(normalized)
        parts = [f"{alias}.query_signature=?"]
        params = [signature]
        if not cdx_signature_is_date_bound(normalized):
            parts.append(f"{alias}.timestamp BETWEEN ? AND ?")
            params.extend([normalized.from_date, normalized.to_date])
        return "(" + " AND ".join(parts) + ")", params
    return "(" + " OR ".join(clauses) + ")", params


def _active_query_signatures(config: ProjectConfig) -> tuple[str, ...]:
    normalized = config.normalized()
    if not normalized.targets:
        return (cdx_query_signature(normalized),)
    return tuple(dict.fromkeys(
        cdx_query_signature(normalized.for_target(target)) for target in normalized.targets
    ))


def _text_runtime_target_configs(
    config: ProjectConfig,
    capture_ids: list[int] | None = None,
) -> list[ProjectConfig]:
    """Split acquisition only when a target has runtime replay overrides.

    Query-defining overrides are handled by ``_inventory_scope_predicate`` and
    can therefore remain in one high-throughput queue.  Worker/replay-delay (and
    advanced scan-worker) overrides need their own executor/limiter instance, so
    those targets run as bounded per-target phases.  Explicit capture-id retries
    stay single-pass to avoid retrying the same row once per configured target.
    """
    normalized = config.normalized()
    if capture_ids or len(normalized.targets) <= 1:
        return [normalized]
    runtime_keys = {"workers", "download_delay", "scan_workers"}
    if not any(runtime_keys.intersection(normalized.settings_for_target(target)) for target in normalized.targets):
        return [normalized]
    return [normalized.for_target(target) for target in normalized.targets]


def _allocate_capture_path(database: sqlite3.Connection, root: Path, row: sqlite3.Row, reserved: set[str] | None = None) -> Path:
    existing = str(row["local_path"] or "") if "local_path" in row.keys() else ""
    if existing:
        return Path(existing)
    candidate = url_capture_path(root, str(row["timestamp"]), str(row["original_url"]))
    def occupied(path: Path) -> bool:
        return str(path) in (reserved or ()) or path.exists() or database.execute(
            "SELECT id FROM captures WHERE id<>? AND local_path=? LIMIT 1",
            (int(row["id"]), str(path)),
        ).fetchone() is not None

    if occupied(candidate):
        candidate = url_capture_path(root, str(row["timestamp"]), str(row["original_url"]), disambiguate=True)
        if occupied(candidate):
            # Escaping URL characters can collide with an already-percent-
            # escaped URL, even at the same timestamp. Never adopt another
            # capture's file just because the portable spelling is identical.
            base = url_capture_path(root, str(row["timestamp"]), str(row["original_url"]))
            counter = 0
            while True:
                suffix = f"~c{int(row['id'])}" + (f"-{counter}" if counter else "")
                candidate = base.with_name(base.stem + suffix + ".txt")
                if not occupied(candidate):
                    break
                counter += 1
    return candidate


def cumulative_download_progress(
    database: sqlite3.Connection,
    config: ProjectConfig,
    queued_total: int,
    capture_ids: list[int] | None = None,
) -> tuple[int, int]:
    if capture_ids:
        return 0, max(0, int(queued_total))
    scope_sql, scope_params = _inventory_scope_predicate(database, config, alias="captures")
    total = int(database.execute(
        "SELECT COUNT(*) FROM captures WHERE " + scope_sql, scope_params
    ).fetchone()[0])
    unfinished = int(database.execute(
        "SELECT COUNT(*) FROM captures WHERE " + scope_sql
        + " AND state IN ('pending','downloading','downloaded_unscanned','scanning')", scope_params,
    ).fetchone()[0])
    return max(0, total - unfinished), total

def prepare_download_rows(
    database: sqlite3.Connection,
    config: ProjectConfig,
    patterns,
    states: tuple[str, ...] = ("pending",),
    capture_ids: list[int] | None = None,
) -> tuple[int, Iterator[sqlite3.Row]]:
    """Classify indexed captures and create a bounded SQLite-backed replay queue.

    Intentional non-text/URL-filter decisions are auditable skip reasons, never
    Open Errors. Ambiguous metadata is downloaded and sniffed rather than lost.
    """
    requeue_reclassifiable_skips(database, config.download_scope, CLASSIFIER_REVISION)
    database.execute("DROP TABLE IF EXISTS temp.archive_scout_download_queue")
    database.execute(
        """CREATE TEMP TABLE archive_scout_download_queue(
               id INTEGER PRIMARY KEY,
               priority INTEGER NOT NULL,
               length INTEGER NOT NULL
           ) WITHOUT ROWID"""
    )
    database.execute(
        "CREATE INDEX archive_scout_download_queue_order ON archive_scout_download_queue(priority,length,id)"
    )
    database.execute("DROP TABLE IF EXISTS temp.archive_scout_capture_selection")

    source = "captures c"
    clauses: list[str] = []
    params: list[object] = []
    if capture_ids:
        database.execute(
            "CREATE TEMP TABLE archive_scout_capture_selection(id INTEGER PRIMARY KEY) WITHOUT ROWID"
        )
        database.executemany(
            "INSERT OR IGNORE INTO archive_scout_capture_selection(id) VALUES(?)",
            ((int(value),) for value in capture_ids),
        )
        source += " JOIN archive_scout_capture_selection s ON s.id=c.id"
    else:
        scope_sql, scope_params = _inventory_scope_predicate(database, config, alias="c")
        clauses.append(scope_sql)
        params.extend(scope_params)
        clauses.append("c.download_attempts<?")
        params.append(config.max_attempts)
    if states:
        clauses.append("c.state IN (" + ",".join("?" for _ in states) + ")")
        params.extend(states)

    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    cursor = database.execute(
        "SELECT c.id,c.original_url,c.mimetype,c.length FROM " + source + where + " ORDER BY c.id",
        params,
    )
    url_prefilter = compile_prefilter(patterns) if patterns else None
    while True:
        chunk = cursor.fetchmany(2000)
        if not chunk:
            break
        selected_ids: list[tuple[int, int, int]] = []
        with database:
            for row in chunk:
                capture_id = int(row["id"])
                classification = classify_text_candidate(
                    str(row["original_url"]), str(row["mimetype"] or "")
                )
                if classification == "binary":
                    mark_capture_skipped(database, capture_id, "known_non_text", CLASSIFIER_REVISION)
                    continue
                if config.download_scope == "keyword_urls" and url_prefilter is not None:
                    original_url = str(row["original_url"])
                    normalized_url = normalize_search(original_url)
                    if (
                        not url_prefilter.has_positive_rules
                        or not url_prefilter.matches(
                            {"url": original_url}, {"url": normalized_url}
                        )
                    ):
                        mark_capture_skipped(
                            database, capture_id, "url_keyword_filter", CLASSIFIER_REVISION
                        )
                        continue
                length = max(0, int(row["length"] or 0))
                selected_ids.append((capture_id, 1 if length <= 0 else 0, length))
            if selected_ids:
                database.executemany(
                    "INSERT OR IGNORE INTO archive_scout_download_queue(id,priority,length) VALUES(?,?,?)",
                    selected_ids,
                )
    total = int(database.execute(
        "SELECT COUNT(*) FROM archive_scout_download_queue"
    ).fetchone()[0])

    def iter_rows() -> Iterator[sqlite3.Row]:
        last_priority = -1
        last_length = -1
        last_id = 0
        while True:
            batch = database.execute(
                """
                SELECT c.* FROM captures c
                JOIN archive_scout_download_queue q ON q.id=c.id
                WHERE (q.priority,q.length,q.id)>(?,?,?)
                ORDER BY q.priority,q.length,q.id LIMIT 1000
                """,
                (last_priority, last_length, last_id),
            ).fetchall()
            if not batch:
                return
            for row in batch:
                length = max(0, int(row["length"] or 0))
                last_priority = 1 if length <= 0 else 0
                last_length = length
                last_id = int(row["id"])
                yield row

    return total, iter_rows()


def prepare_acquisition_rows(
    database: sqlite3.Connection,
    config: ProjectConfig,
    patterns=None,
    states: tuple[str, ...] = ("pending",),
    capture_ids: list[int] | None = None,
    *,
    stop_event: threading.Event | None = None,
    classification_callback=None,
) -> tuple[int, Iterator[sqlite3.Row], dict[str, int]]:
    """Stream replay candidates from the capture manifest without a project-sized queue.

    This is the single selection path used by both full scans and download-only
    acquisition. Metadata classification and optional URL-keyword gating are
    applied in bounded pages; intentional skips are persisted in batches.
    """
    # Direct download/resume consumers must see page-size-bound legacy rows even
    # when they do not run index_archive first. Adoption is idempotent and also
    # copies per-page completion checkpoints used by later indexing.
    with database:
        adopt_compatible_index_identity(database, config)
        requeue_reclassifiable_skips(database, config.download_scope, CLASSIFIER_REVISION)
    classification_counts = {
        name: 0 for name in ("text", "image", "video", "audio", "media_descriptor", "other_binary", "unknown")
    }
    for signature in _active_query_signatures(config):
        updated = classify_capture_inventory(
            database, signature,
            allow_media_descriptors_as_text=config.search_media_descriptors,
            stop_event=stop_event, progress_callback=classification_callback,
        )
        for name, value in updated.items():
            classification_counts[name] = classification_counts.get(name, 0) + int(value)
    database.execute("DROP TABLE IF EXISTS temp.archive_scout_capture_selection")
    source = "captures c"
    clauses: list[str] = []
    params: list[object] = []
    if capture_ids:
        database.execute(
            "CREATE TEMP TABLE archive_scout_capture_selection(id INTEGER PRIMARY KEY) WITHOUT ROWID"
        )
        database.executemany(
            "INSERT OR IGNORE INTO archive_scout_capture_selection(id) VALUES(?)",
            ((int(value),) for value in capture_ids),
        )
        source += " JOIN archive_scout_capture_selection s ON s.id=c.id"
    else:
        scope_sql, scope_params = _inventory_scope_predicate(database, config, alias="c")
        clauses.append(scope_sql)
        params.extend(scope_params)
        clauses.append("c.download_attempts<?")
        params.append(config.max_attempts)
    if states:
        clauses.append("c.state IN (" + ",".join("?" for _ in states) + ")")
        params.extend(states)
    base_where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    total = int(database.execute(
        "SELECT COUNT(*) FROM " + source + base_where, params
    ).fetchone()[0])
    stats = {"metadata_skipped": 0, "url_skipped": 0, **{f"class_{k}": v for k, v in classification_counts.items()}}
    url_prefilter = compile_prefilter(patterns) if patterns else None

    def classify_batch(rows: list[sqlite3.Row]) -> list[sqlite3.Row]:
        selected: list[sqlite3.Row] = []
        binary_ids: list[int] = []
        url_ids: list[int] = []
        for row in rows:
            capture_id = int(row["id"])
            resource_class = str(row["resource_class"] or "unknown") if "resource_class" in row.keys() else "unknown"
            if resource_class in {"image", "video", "audio", "other_binary"}:
                binary_ids.append(capture_id)
                continue
            if resource_class == "media_descriptor" and not config.search_media_descriptors:
                binary_ids.append(capture_id)
                continue
            if config.download_scope == "keyword_urls" and url_prefilter is not None:
                original_url = str(row["original_url"])
                normalized_url = normalize_search(original_url)
                if (
                    not url_prefilter.has_positive_rules
                    or not url_prefilter.matches(
                        {"url": original_url}, {"url": normalized_url}
                    )
                ):
                    url_ids.append(capture_id)
                    continue
            selected.append(row)
        if binary_ids or url_ids:
            now = utc_now()
            with database:
                if binary_ids:
                    database.executemany(
                        """UPDATE captures SET state='skipped',skip_reason=CASE WHEN resource_class IN ('image','video','audio') THEN 'classified_media' WHEN resource_class='media_descriptor' THEN 'classified_media_descriptor' ELSE 'unsupported_binary' END,
                           classifier_revision=?,updated_at=? WHERE id=?""",
                        ((CLASSIFIER_REVISION, now, capture_id) for capture_id in binary_ids),
                    )
                if url_ids:
                    database.executemany(
                        """UPDATE captures SET state='skipped',skip_reason='url_keyword_filter',
                           classifier_revision=?,updated_at=? WHERE id=?""",
                        ((CLASSIFIER_REVISION, now, capture_id) for capture_id in url_ids),
                    )
            stats["metadata_skipped"] += len(binary_ids)
            stats["url_skipped"] += len(url_ids)
        return selected

    def iter_rows() -> Iterator[sqlite3.Row]:
        if capture_ids:
            last_id = 0
            while True:
                where = base_where + (" AND " if base_where else " WHERE ") + "c.id>?"
                rows = database.execute(
                    "SELECT c.* FROM " + source + where + " ORDER BY c.id LIMIT 2000",
                    [*params, last_id],
                ).fetchall()
                if not rows:
                    return
                last_id = int(rows[-1]["id"])
                yield from classify_batch(rows)
            return

        # Existing composite indexes let the producer prefer known-size captures
        # without constructing a second project-sized SQLite queue.
        # Start just after every possible zero-length row so the row-value
        # keyset predicate can seek directly into positive lengths without a
        # separate `length>0` range that makes older SQLite versions rescan the
        # index prefix on every 2,000-row page.
        last_length = 0
        last_id = 9223372036854775807
        while True:
            where = base_where + (" AND " if base_where else " WHERE ")
            where += "(c.length,c.id)>(?,?)"
            rows = database.execute(
                "SELECT c.* FROM " + source + where + " ORDER BY c.length,c.id LIMIT 2000",
                [*params, last_length, last_id],
            ).fetchall()
            if not rows:
                break
            last_length = max(0, int(rows[-1]["length"] or 0))
            last_id = int(rows[-1]["id"])
            yield from classify_batch(rows)

        last_id = 0
        while True:
            where = base_where + (" AND " if base_where else " WHERE ")
            where += "c.length=0 AND c.id>?"
            rows = database.execute(
                "SELECT c.* FROM " + source + where + " ORDER BY c.id LIMIT 2000",
                [*params, last_id],
            ).fetchall()
            if not rows:
                break
            last_id = int(rows[-1]["id"])
            yield from classify_batch(rows)

    return total, iter_rows(), stats


def prepare_download_only_rows(
    database: sqlite3.Connection,
    config: ProjectConfig,
    states: tuple[str, ...] = ("pending",),
    capture_ids: list[int] | None = None,
) -> tuple[int, Iterator[sqlite3.Row], dict[str, int]]:
    """Compatibility wrapper around the unified acquisition selector."""
    return prepare_acquisition_rows(
        database, config, None, states=states, capture_ids=capture_ids
    )

def select_download_rows(
    database: sqlite3.Connection,
    config: ProjectConfig,
    patterns,
    states: tuple[str, ...] = ("pending",),
    capture_ids: list[int] | None = None,
) -> list[sqlite3.Row]:
    _total, rows = prepare_download_rows(
        database, config, patterns, states=states, capture_ids=capture_ids
    )
    return list(rows)


def _download_capture(
    row: dict[str, object],
    path: Path,
    config: ProjectConfig,
    client: HttpClient,
    *,
    verify_existing_hash: bool = True,
    compute_hash: bool = True,
    stop_event=None,
) -> dict:
    if stop_event is not None and stop_event.is_set():
        raise Stopped
    original = str(row["original_url"])
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_file():
        # Existing final paths have already crossed the atomic .part -> final
        # boundary, so they are complete captures. Read only the small sniff
        # prefix here instead of pulling the entire file into RAM merely to
        # slice the first 16 KiB. The hash is still verified in the normal
        # scan path when it is missing from the manifest.
        size = path.stat().st_size
        digest = str(row.get("content_hash") or "")
        if not digest and verify_existing_hash:
            _size, digest = sha256_file(path)
            size = _size
        with path.open("rb") as handle:
            preview = handle.read(16384)
        if not looks_textual_bytes(preview, str(row.get("mimetype") or "")):
            return {"kind": "non_text", "capture_id": int(row["id"])}
        return {
            "kind": "downloaded",
            "capture_id": int(row["id"]), "path": path, "bytes_saved": size,
            "content_hash": digest, "http_status": 200,
            "final_url": replay_url(str(row["timestamp"]), original),
            "content_type": str(row.get("mimetype") or ""), "preview": preview,
            "adopted_existing": True,
        }
    temp = path.with_name(path.name + ".part")
    # The old 25 MB setting must not silently discard a known larger text page.
    # For known CDX lengths, allow the advertised payload plus headroom while
    # retaining the configured budget for unknown-length responses.
    known_length = max(0, int(row.get("length") or 0))
    stream_limit = max(config.max_file_bytes, known_length + 1024 * 1024)
    replay = replay_url(str(row["timestamp"]), original)
    if stop_event is not None and stop_event.is_set():
        raise Stopped

    def preview_validator(headers: dict[str, str], prefix: bytes) -> str | None:
        content_type = headers.get("content-type") or headers.get("Content-Type") or str(row.get("mimetype") or "")
        decision = classify_payload_prefix(prefix[:PREVIEW_BUDGET], str(content_type), original)
        if decision.resource_class == "text":
            return None
        if decision.resource_class == "media_descriptor" and config.search_media_descriptors:
            return None
        # Audit5 phase boundary: a text worker never completes media. Once the
        # bounded prefix proves image/video identity, stop this GET and persist a
        # deferred candidate for the later standard media index/download phase.
        if decision.resource_class in {"image", "video", "audio", "other_binary"}:
            return decision.resource_class
        return None
    try:
        redirect_validator = make_replay_redirect_validator(config, original)
        if compute_hash:
            response = client.download_to_path(
                replay, temp, stream_limit,
                preview_validator=preview_validator,
                redirect_validator=redirect_validator,
            )
        else:
            response = client.download_to_path(
                replay, temp, stream_limit, compute_hash=False,
                preview_validator=preview_validator,
                redirect_validator=redirect_validator,
            )
    except TypeError as exc:
        # Preserve third-party/test adapters that predate Audit3's optional
        # prefix validator without accidentally re-enabling hashing for the
        # intentionally lean download-only/acquisition path.
        message = str(exc)
        if "preview_validator" in message or "redirect_validator" in message:
            if compute_hash:
                response = client.download_to_path(replay, temp, stream_limit)
            else:
                try:
                    response = client.download_to_path(replay, temp, stream_limit, compute_hash=False)
                except TypeError as nested:
                    if "compute_hash" not in str(nested):
                        raise
                    response = client.download_to_path(replay, temp, stream_limit)
        elif "compute_hash" in message:
            response = client.download_to_path(replay, temp, stream_limit)
        else:
            raise
    except PreviewRejected as exc:
        temp.unlink(missing_ok=True)
        resource_class = str(exc.classification)
        media_enabled = bool(
            config.media.enabled
            and ((resource_class == "image" and config.media.include_images)
                 or (resource_class == "video" and config.media.include_videos))
        )
        return {
            "kind": "deferred_media" if media_enabled else "non_text",
            "capture_id": int(row["id"]), "resource_class": resource_class,
            "early_rejected": True, "content_type": str(row.get("mimetype") or ""),
            "http_status": 200, "final_url": replay,
        }
    content_type = (
        response["headers"].get("content-type")
        or response["headers"].get("Content-Type")
        or row.get("mimetype")
        or ""
    )
    preview = bytes(response.get("preview") or b"")
    payload_decision = classify_payload_prefix(preview[:PREVIEW_BUDGET], str(content_type), original)
    if payload_decision.resource_class in {"image", "video"}:
        temp.unlink(missing_ok=True)
        media_enabled = bool(
            config.media.enabled
            and ((payload_decision.resource_class == "image" and config.media.include_images)
                 or (payload_decision.resource_class == "video" and config.media.include_videos))
        )
        return {
            "kind": "deferred_media" if media_enabled else "non_text",
            "capture_id": int(row["id"]),
            "resource_class": payload_decision.resource_class,
            "early_rejected": False, "content_type": str(content_type),
            "http_status": response["status"], "final_url": response["final_url"],
        }
    if not looks_textual_bytes(preview, str(content_type)):
        temp.unlink(missing_ok=True)
        return {
            "kind": "non_text", "capture_id": int(row["id"]),
            "content_type": str(content_type), "http_status": response["status"],
            "final_url": response["final_url"],
        }
    preview_text = decode_bytes(preview, str(content_type))
    replay_problem = classify_replay_content(preview_text, str(response["final_url"]))
    if replay_problem:
        temp.unlink(missing_ok=True)
        raise RuntimeError(replay_problem)
    os.replace(temp, path)
    charset = CHARSET_PATTERN.search(str(content_type))
    return {
        "kind": "downloaded",
        "capture_id": int(row["id"]), "path": path,
        "bytes_saved": int(response["bytes"]),
        "content_hash": str(response["content_hash"]),
        "http_status": response["status"], "final_url": response["final_url"],
        "content_type": str(content_type), "preview": preview,
        "encoding": charset.group(1) if charset else "",
    }


def _scan_saved_capture(
    row: dict[str, object], path: Path, config: ProjectConfig, jobs: list[ScanJob]
) -> dict:
    data = path.read_bytes()
    content_type = str(row.get("mimetype") or "")
    if row.get("detected_encoding"):
        content_type += "; charset=" + str(row["detected_encoding"])
    if not looks_textual_bytes(data[:16384], content_type):
        return {"kind": "non_text", "capture_id": int(row["id"]), "path": path}
    content_hash = str(row.get("content_hash") or "") or hashlib.sha256(data).hexdigest()
    raw, encoding = decode_bytes_with_encoding(data, content_type)
    # The decoded source is the canonical scan input from this point onward.
    # Releasing the byte buffer before DOM/normalization work avoids keeping
    # both a potentially huge bytes object and several Unicode views alive.
    del data
    replay_problem = classify_replay_content(raw, str(row.get("final_url") or replay_url(str(row["timestamp"]), str(row["original_url"]))))
    if replay_problem:
        raise RuntimeError(replay_problem)
    original = str(row["original_url"])
    title, visible, links = parse_page(raw, original)
    if config.media.enabled and config.media.discover_embedded:
        embed_urls = {candidate.url for candidate in extract_embed_candidates_fast(raw, original)}
        if embed_urls:
            links = sorted(set(links).union(embed_urls))
    prepared_fields, prepared_normalized_fields = prepare_analysis_fields(
        original, title, visible, raw, links
    )
    analyses = {
        job.scan_run_id: analyze_content(
            original, title, visible, raw, links, job.patterns, job.prefilter,
            prepared_fields, prepared_normalized_fields,
            include_hit_fields=config.report.store_keyword_fields,
            include_snippets=config.report.store_snippets,
            include_interesting_links=config.report.store_interesting_links,
        )
        for job in jobs
    }
    embedded_candidates: list[tuple[str, str]] = []
    if config.media.enabled and config.media.discover_embedded:
        from ..media.discovery import discover_media
        embedded_candidates = [(item.url, item.kind_hint) for item in discover_media(raw, original, config.media, links)]
    return {
        "kind": "scanned", "capture_id": int(row["id"]), "path": path,
        "title": title, "visible": visible, "links": links,
        "analyses": analyses, "content_hash": content_hash,
        "normalized_hash": hash_text(prepared_normalized_fields["body"]),
        "bytes_saved": path.stat().st_size, "encoding": encoding,
        "embedded_candidates": embedded_candidates,
    }


def fetch_parse_scan(row: sqlite3.Row, config: ProjectConfig, jobs: list[ScanJob], client: HttpClient) -> dict:
    """Legacy extension API; the main pipeline calls download and scan separately."""
    row_dict = dict(row)
    path = url_capture_path(config.output_dir, str(row["timestamp"]), str(row["original_url"]))
    downloaded = _download_capture(row_dict, path, config, client)
    if downloaded["kind"] != "downloaded":
        raise RuntimeError("downloaded response was not textual")
    row_dict.update(downloaded)
    return _scan_saved_capture(row_dict, path, config, jobs)


def save_success(
    database: sqlite3.Connection,
    result: dict,
    report_config=None,
    *,
    retain_payload: bool = True,
) -> int:
    document_id = upsert_document(
        database, result["capture_id"], result["path"], result["title"],
        result["visible"], result["links"], result["content_hash"],
        result["normalized_hash"], result["bytes_saved"],
        index_full_text=retain_payload,
    )
    availability = "retained" if retain_payload else "cleanup_pending"
    database.execute(
        "UPDATE captures SET state='downloaded',payload_availability=?,cleanup_pending=?,detected_encoding=?,updated_at=? WHERE id=?",
        (availability, 0 if retain_payload else 1, result.get("encoding") or "", utc_now(), result["capture_id"]),
    )
    for scan_run_id, analysis in result["analyses"].items():
        save_match(database, int(scan_run_id), document_id, analysis, report_config)
    resolve_errors(database, capture_id=result["capture_id"], document_id=document_id)
    return document_id



def _persist_embedded_candidates(
    database: sqlite3.Connection, config: ProjectConfig, document_id: int, result: dict
) -> None:
    candidates = list(result.get("embedded_candidates") or [])
    if not (config.media.enabled and config.media.discover_embedded):
        return
    # Import lazily to keep downloader/media package initialization acyclic.
    from ..media.indexer import media_query_signature
    from ..media.discovery import hosts_related, target_hosts
    signature = media_query_signature(config)
    target_host_set = target_hosts(config.targets)
    queued: list[tuple[str, int | None, str, str]] = []
    for url, kind_hint in candidates:
        host = host_from_url(str(url))
        if not host or host in {"unknown", "web.archive.org"}:
            continue
        external = not hosts_related(host, target_host_set)
        if external and not config.media.allow_external_embeds:
            continue
        queued.append((str(url), int(document_id), "external_embedded" if external else "embedded", str(kind_hint or "")))
    queue_media_discovery_candidates(database, signature, queued)
    mark_media_discovery_document(
        database, signature, int(document_id), str(result.get("content_hash") or ""), len(queued)
    )


def _owned_capture_path(root: Path, value: str | Path) -> Path | None:
    try:
        path = Path(value).resolve()
        capture_root = (Path(root) / "captures").resolve()
        if path.is_relative_to(capture_root):
            return path
    except (OSError, RuntimeError, ValueError):
        pass
    return None


def _commit_discard_evidence(
    database: sqlite3.Connection, config: ProjectConfig, outcomes: list[dict]
) -> list[tuple[int, Path]]:
    """FULL-sync scan evidence + cleanup intent before deleting canonical payloads."""
    if not outcomes:
        return []
    database.commit()
    previous_sync = int(database.execute("PRAGMA synchronous").fetchone()[0])
    database.execute("PRAGMA synchronous=FULL")
    cleanup: list[tuple[int, Path]] = []
    try:
        with database:
            for outcome in outcomes:
                document_id = save_success(
                    database, outcome, config.report, retain_payload=False
                )
                _persist_embedded_candidates(database, config, document_id, outcome)
                path = _owned_capture_path(config.output_dir, Path(outcome["path"]))
                if path is None:
                    # Never unlink imported/shared/out-of-project content. The scan
                    # result is valid, but this payload is intentionally retained.
                    database.execute(
                        "UPDATE captures SET payload_availability='retained',cleanup_pending=0 WHERE id=?",
                        (int(outcome["capture_id"]),),
                    )
                    continue
                cleanup.append((int(outcome["capture_id"]), path))
        # Context-manager commit above is the destructive-cleanup durability barrier.
    finally:
        database.execute(f"PRAGMA synchronous={previous_sync}")
    return cleanup


def _commit_non_text_discard(
    database: sqlite3.Connection, config: ProjectConfig, capture_id: int, path_value: str | Path
) -> list[tuple[int, Path]]:
    """Persist a non-text classification durably before deleting an Audit3 spool file."""
    database.commit()
    previous_sync = int(database.execute("PRAGMA synchronous").fetchone()[0])
    cleanup: list[tuple[int, Path]] = []
    try:
        database.execute("PRAGMA synchronous=FULL")
        with database:
            mark_capture_skipped(database, capture_id, "sniffed_non_text", CLASSIFIER_REVISION)
            path = _owned_capture_path(config.output_dir, path_value)
            if path is None:
                database.execute(
                    """UPDATE captures SET resource_class='other_binary',
                       classification_reason='payload_validation:non_text',
                       resource_classifier_revision=?,payload_availability='retained',
                       cleanup_pending=0,updated_at=? WHERE id=?""",
                    (RESOURCE_CLASSIFIER_REVISION, utc_now(), capture_id),
                )
            else:
                database.execute(
                    """UPDATE captures SET resource_class='other_binary',
                       classification_reason='payload_validation:non_text',
                       resource_classifier_revision=?,payload_availability='cleanup_pending',
                       cleanup_pending=1,updated_at=? WHERE id=?""",
                    (RESOURCE_CLASSIFIER_REVISION, utc_now(), capture_id),
                )
                cleanup.append((capture_id, path))
    finally:
        database.execute(f"PRAGMA synchronous={previous_sync}")
    return cleanup


def _finish_discard_cleanup(
    database: sqlite3.Connection, config: ProjectConfig, cleanup: list[tuple[int, Path]]
) -> set[int]:
    """Finish cleanup without deleting bytes still referenced by retained captures.

    Returned IDs are *settled* for spool accounting: either their owned bytes
    were deleted, or their retention policy was explicitly transitioned to
    keep because another capture still references the same path.
    """
    if not cleanup:
        return set()
    groups: dict[str, tuple[Path, set[int]]] = {}
    for capture_id, path in cleanup:
        key = str(path)
        if key not in groups:
            groups[key] = (path, set())
        groups[key][1].add(int(capture_id))

    deleted: set[int] = set()
    retained: set[int] = set()
    failed: list[tuple[int, BaseException]] = []
    for path, candidate_ids in groups.values():
        references = database.execute(
            "SELECT id,payload_availability FROM captures WHERE local_path=?",
            (str(path),),
        ).fetchall()
        foreign = [
            row for row in references
            if int(row["id"]) not in candidate_ids
            and str(row["payload_availability"] or "") != "discarded"
        ]
        if foreign:
            # Directory containment proves where the file is, not who owns the
            # only remaining reference. Transition this operation's captures to
            # retained recovery instead of unlinking shared bytes.
            retained.update(candidate_ids)
            continue
        try:
            if path.exists():
                path.unlink()
            deleted.update(candidate_ids)
        except OSError as exc:
            failed.extend((capture_id, exc) for capture_id in candidate_ids)

    now = utc_now()
    with database:
        if deleted:
            database.executemany(
                """UPDATE captures SET payload_availability='discarded',cleanup_pending=0,
                   local_path=NULL,discarded_at=?,updated_at=? WHERE id=?""",
                ((now, now, capture_id) for capture_id in deleted),
            )
        if retained:
            database.executemany(
                """UPDATE captures SET payload_availability='retained',payload_retention='keep',
                   cleanup_pending=0,updated_at=? WHERE id=?""",
                ((now, capture_id) for capture_id in retained),
            )
        for capture_id, exc in failed:
            database.execute(
                "UPDATE captures SET payload_availability='cleanup_pending',cleanup_pending=1,updated_at=? WHERE id=?",
                (now, capture_id),
            )
            record_error(
                database, "cleanup", "payload_cleanup_failed", repr(exc),
                capture_id=capture_id, retryable=True,
            )
    return deleted | retained

def recover_pending_discard_cleanup(
    database: sqlite3.Connection, root: Path, callback: Callable[[ProgressEvent], None] | None = None
) -> int:
    """Idempotently finish cleanup whose durable evidence barrier already committed."""
    rows = database.execute(
        "SELECT id,local_path FROM captures WHERE cleanup_pending=1 AND payload_availability='cleanup_pending'"
    ).fetchall()
    grouped: dict[str, tuple[Path | None, list[int], str]] = {}
    for row in rows:
        capture_id = int(row["id"])
        raw = str(row["local_path"] or "")
        path = _owned_capture_path(root, raw)
        key = str(path) if path is not None else f"unsafe:{capture_id}"
        if key not in grouped:
            grouped[key] = (path, [], raw)
        grouped[key][1].append(capture_id)

    settled = 0
    for path, capture_ids, raw in grouped.values():
        if path is None and raw:
            continue
        if path is not None:
            placeholders = ",".join("?" for _ in capture_ids)
            foreign = database.execute(
                f"SELECT 1 FROM captures WHERE local_path=? AND id NOT IN ({placeholders}) "
                "AND payload_availability!='discarded' LIMIT 1",
                (str(path), *capture_ids),
            ).fetchone()
            if foreign is not None:
                now = utc_now()
                with database:
                    database.executemany(
                        """UPDATE captures SET payload_availability='retained',payload_retention='keep',
                           cleanup_pending=0,updated_at=? WHERE id=?""",
                        ((now, capture_id) for capture_id in capture_ids),
                    )
                settled += len(capture_ids)
                continue
        try:
            if path is not None and path.exists():
                path.unlink()
            now = utc_now()
            with database:
                database.executemany(
                    """UPDATE captures SET payload_availability='discarded',cleanup_pending=0,
                       local_path=NULL,discarded_at=COALESCE(discarded_at,?),updated_at=? WHERE id=?""",
                    ((now, now, capture_id) for capture_id in capture_ids),
                )
                for capture_id in capture_ids:
                    resolve_errors(database, capture_id=capture_id)
            settled += len(capture_ids)
        except OSError:
            continue
    if settled and callback:
        callback(ProgressEvent("cleanup", f"Completed {settled:,} pending discard cleanup item(s)."))
    return settled

def _current_discard_spool_bytes(database: sqlite3.Connection, root: Path) -> int:
    """Count every discard-owned byte currently occupying project storage."""
    total = 0
    rows = database.execute(
        """SELECT local_path,payload_availability FROM captures
           WHERE payload_origin='acquired' AND payload_retention='discard_after_scan'
             AND payload_availability IN ('partial','spooled_unscanned','cleanup_pending')
             AND local_path IS NOT NULL"""
    ).fetchall()
    for row in rows:
        path = _owned_capture_path(root, str(row["local_path"] or ""))
        if path is None:
            continue
        candidates = [path]
        if str(row["payload_availability"] or "") == "partial":
            candidates.insert(0, path.with_name(path.name + ".part"))
        for candidate in candidates:
            try:
                if candidate.is_file():
                    total += candidate.stat().st_size
                    break
            except OSError:
                pass
    return total

def _reconcile_text_media_handoffs(database: sqlite3.Connection, config: ProjectConfig, signature: str) -> None:
    strategy = config.media.normalized().snapshot_strategy
    if strategy == "all":
        return
    rows = database.execute(
        """SELECT id,COALESCE(NULLIF(urlkey,''),original_url) AS selection_key,timestamp,path
           FROM media_captures WHERE query_signature=? AND source_type='text_validation_handoff'
             AND timestamp BETWEEN ? AND ? ORDER BY selection_key,timestamp,id""",
        (signature, config.from_date, config.to_date),
    ).fetchall()
    groups: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        groups.setdefault(str(row["selection_key"]), []).append(row)
    keep_ids: set[int] = set()
    for items in groups.values():
        chosen = min(items, key=lambda r: (str(r["timestamp"]), int(r["id"]))) if strategy == "earliest" else max(items, key=lambda r: (str(r["timestamp"]), int(r["id"])))
        keep_ids.add(int(chosen["id"]))
    now = utc_now()
    for row in rows:
        row_id = int(row["id"])
        if row_id in keep_ids:
            continue
        path_text = str(row["path"] or "")
        if path_text:
            try:
                path = Path(path_text).resolve()
                media_root = (config.output_dir / "media").resolve()
                if path.is_relative_to(media_root) and path.exists():
                    path.unlink()
            except (OSError, RuntimeError, ValueError):
                pass
        database.execute(
            """UPDATE media_captures SET state='skipped_strategy',skip_reason='snapshot_strategy',
               path=NULL,updated_at=? WHERE id=?""",
            (now, row_id),
        )

def _pending_scan_rows(
    database: sqlite3.Connection, config: ProjectConfig, capture_ids: list[int] | None = None,
    *, discard_only: bool = False,
) -> Iterator[sqlite3.Row]:
    clauses = ["c.state='downloaded_unscanned'", "c.local_path IS NOT NULL"]
    if discard_only:
        clauses.append("c.payload_availability='spooled_unscanned'")
    params: list[object] = []
    source = "captures c"
    if capture_ids:
        database.execute("DROP TABLE IF EXISTS temp.archive_scout_scan_selection")
        database.execute(
            "CREATE TEMP TABLE archive_scout_scan_selection(id INTEGER PRIMARY KEY) WITHOUT ROWID"
        )
        database.executemany(
            "INSERT OR IGNORE INTO archive_scout_scan_selection(id) VALUES(?)",
            ((int(value),) for value in capture_ids),
        )
        source += " JOIN temp.archive_scout_scan_selection s ON s.id=c.id"
    else:
        scope_sql, scope_params = _inventory_scope_predicate(database, config, alias="c")
        clauses.append(scope_sql)
        params.extend(scope_params)
    last = 0
    while True:
        rows = database.execute(
            "SELECT c.* FROM " + source + " WHERE " + " AND ".join(clauses)
            + " AND c.id>? ORDER BY c.id LIMIT 1000",
            [*params, last],
        ).fetchall()
        if not rows:
            return
        for row in rows:
            last = int(row["id"])
            yield row

def _acquire_archive(
    config: ProjectConfig,
    database: sqlite3.Connection,
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None,
    *,
    patterns=None,
    states: tuple[str, ...] = ("pending",),
    capture_ids: list[int] | None = None,
    progress_stage: str = "download",
    scan_jobs: list[ScanJob] | None = None,
) -> dict[str, int | float]:
    """Acquire text captures through one bounded, durable replay pipeline.

    Full scans and download-only operations intentionally share this exact path.
    No parsing, keyword scoring, media extraction, report work, or payload hashing
    is performed while replay workers are active. Final files are atomic and the
    manifest is updated in bounded batches so acquisition remains network-bound.
    """
    if config.download_scope == "index_only":
        if callback:
            callback(ProgressEvent(progress_stage, "Index-only scope selected; downloads skipped."))
        return {"queued": 0, "downloaded": 0, "skipped": 0, "errors": 0, "elapsed": 0.0}

    with database:
        database.execute("UPDATE captures SET state='pending' WHERE state='downloading'")

    total, row_iter, selection_stats = prepare_acquisition_rows(
        database, config, patterns, states=states, capture_ids=capture_ids,
        stop_event=stop_event,
        classification_callback=(
            (lambda processed: callback(ProgressEvent(
                "classification", f"Reclassified {processed:,} stale capture(s)…", processed, 0
            ))) if callback else None
        ),
    )
    discard_mode = bool(scan_jobs) and config.text_retention == "discard_after_scan"
    scan_jobs = list(scan_jobs or [])
    if callback:
        classes = ", ".join(
            f"{name} {int(selection_stats.get('class_' + name, 0)):,}"
            for name in ("text", "image", "video", "audio", "media_descriptor", "other_binary", "unknown")
            if int(selection_stats.get("class_" + name, 0))
        ) or "no stale rows required reclassification"
        callback(ProgressEvent("classification", f"Resource classification preparation complete: {classes}."))

    limiter = SharedFixedRateLimiter(config.download_delay, key=WAYBACK_REPLAY_RATE_KEY)
    host_gate = shared_host_gate(config.rate_limit_base_pause, config.rate_limit_max_pause)
    acquisition_cancel = threading.Event()
    worker_stop = _CombinedStopEvent(stop_event, acquisition_cancel)

    def on_retry(attempt: int, total_attempts: int, reason: str, wait_seconds: float) -> None:
        if callback:
            stage = "rate_limit" if "quota/overload cooldown" in reason or "all Wayback requests paused" in reason else "download_retry"
            callback(ProgressEvent(stage, f"{reason}. Retry {attempt}/{total_attempts} in {wait_seconds:.1f}s…"))

    client = HttpClient(
        limiter, config.retries, max(config.connect_timeout, config.read_timeout),
        config.user_agent, worker_stop, retry_callback=on_retry,
        connect_timeout=config.connect_timeout, read_timeout=config.read_timeout,
        pool_size=config.workers, host_gate=host_gate,
        rate_limit_attempts=config.rate_limit_attempts,
        rate_limit_max_wait=config.rate_limit_max_wait,
        network_backend=config.network.normalized().backend,
        trust_environment=config.network.normalized().trust_environment,
        network_callback=(lambda message: callback(ProgressEvent("network", message)) if callback else None),
        connection_failure_pause_threshold=config.network.normalized().connection_failure_pause_threshold,
        connection_retry_seconds=config.network.normalized().connection_retry_seconds,
    )

    inflight_limit = max(config.workers, config.workers * 3)
    stage_limit = max(64, min(512, config.workers * 16))
    ready_downloads: deque[tuple[dict[str, object], Path]] = deque()
    futures: dict[concurrent.futures.Future, dict[str, object]] = {}
    rows_exhausted = False
    submitted = downloaded = skipped = failures = 0
    started = time.monotonic()
    last_emit = 0.0
    last_flush = started
    flush_count = max(32, min(128, config.workers * 8))
    success_buffer: list[tuple[str, str, str, str, str, int, str, int, int, int, str]] = []
    skipped_buffer: list[tuple[int, str]] = []
    error_buffer: list[tuple[int, dict[str, object], BaseException]] = []

    scan_workers = 0
    scan_limit = 0
    scan_pool: concurrent.futures.ThreadPoolExecutor | None = None
    scan_futures: dict[concurrent.futures.Future, tuple[dict[str, object], int, bool]] = {}
    waiting_scan: deque[tuple[dict[str, object], int, bool]] = deque()
    spool_bytes = _current_discard_spool_bytes(database, config.output_dir) if discard_mode else 0
    spool_high = max(32 * 1024 * 1024, int(config.discard_spool_mb * 1024 * 1024))
    spool_low = int(spool_high * 0.70)

    def discard_reservation(item: dict[str, object]) -> int:
        if not discard_mode:
            return 0
        assigned = str(item.get("assigned_path") or "")
        if assigned and Path(assigned).is_file():
            return 0
        known = max(0, int(item.get("length") or 0))
        # Match _download_capture's maximum stream budget. CDX length is only a
        # hint and may understate decoded replay bytes, so it cannot be the hard
        # admission reservation by itself.
        return max(int(config.max_file_bytes), known + 1024 * 1024)
    backpressure_active = False
    scan_completed = scan_matched = scan_failures = 0
    retained_failure_bytes = 0
    if discard_mode:
        scan_workers = config.scan_workers or min(4, max(1, (os.cpu_count() or 4) // 2))
        scan_workers = max(1, min(8, int(scan_workers)))
        scan_limit = max(scan_workers * 3, scan_workers)
        scan_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=scan_workers, thread_name_prefix="archive-discard-scan"
        )

    def network_metrics() -> dict[str, float | int]:
        getter = getattr(client, "metrics_snapshot", None)
        if callable(getter):
            try:
                values = getter()
                if isinstance(values, dict):
                    return values
            except Exception:
                pass
        # Compatibility for third-party/test clients that predate metrics.
        return {
            "request_starts": submitted,
            "request_completions": downloaded + skipped + failures,
            "request_failures": failures,
            "network_bytes": 0,
            "retry_waits": 0,
            "rate_limit_events": 0,
            "pacing_wait_seconds": 0.0,
            "host_gate_wait_seconds": 0.0,
            "retry_wait_seconds": 0.0,
            "rate_limit_wait_seconds": 0.0,
            "network_seconds": 0.0,
        }

    def flush_results(force: bool = False) -> None:
        nonlocal last_flush
        pending_count = len(success_buffer) + len(skipped_buffer) + len(error_buffer)
        if not pending_count:
            return
        now_mono = time.monotonic()
        if not force and pending_count < flush_count and now_mono - last_flush < 1.0:
            return
        now = utc_now()
        with database:
            if success_buffer:
                database.executemany(
                    """UPDATE captures SET state='downloaded_unscanned',payload_availability=?,
                       payload_origin=?,payload_retention=?,local_path=?,
                       content_hash=COALESCE(NULLIF(?,''),content_hash),http_status=?,final_url=?,bytes_saved=?,
                       skip_reason=NULL,classifier_revision=?,
                       detected_encoding=COALESCE(NULLIF(?,''),detected_encoding),
                       download_attempts=download_attempts+1,updated_at=? WHERE id=?""",
                    (
                        (availability, origin, retention, path, content_hash, http_status, final_url, bytes_saved,
                         classifier_revision, encoding, now, capture_id)
                        for availability, origin, retention, path, content_hash, http_status, final_url, bytes_saved,
                            classifier_revision, capture_id, encoding in success_buffer
                    ),
                )
                success_ids = [row[-2] for row in success_buffer]
                placeholders = ",".join("?" for _ in success_ids)
                database.execute(
                    "UPDATE errors SET resolved=1,last_seen=? WHERE resolved=0 "
                    f"AND capture_id IN ({placeholders})",
                    (now, *success_ids),
                )
            if skipped_buffer:
                database.executemany(
                    """UPDATE captures SET state='skipped',
                       skip_reason=CASE
                           WHEN ? IN ('image','video','audio') THEN 'classified_media'
                           WHEN ?='media_descriptor' THEN 'classified_media_descriptor'
                           WHEN ?='other_binary' THEN 'unsupported_binary'
                           ELSE 'sniffed_non_text' END,
                       resource_class=CASE WHEN ?<>'' THEN ? ELSE resource_class END,
                       classification_reason=CASE WHEN ?<>'' THEN 'payload_validation:' || ? ELSE classification_reason END,
                       resource_classifier_revision=CASE WHEN ?<>'' THEN ? ELSE resource_classifier_revision END,
                       payload_availability='not_acquired',local_path=NULL,
                       classifier_revision=?,download_attempts=download_attempts+1,
                       updated_at=? WHERE id=?""",
                    ((kind, kind, kind, kind, kind, kind, kind, kind, RESOURCE_CLASSIFIER_REVISION,
                      CLASSIFIER_REVISION, now, capture_id)
                     for capture_id, kind in skipped_buffer),
                )
            for capture_id, item, exc in error_buffer:
                category, status, retryable = classify_exception(exc)
                database.execute(
                    """UPDATE captures SET state='error',payload_availability=CASE WHEN local_path IS NOT NULL THEN 'partial' ELSE payload_availability END,http_status=?,
                       download_attempts=download_attempts+1,updated_at=? WHERE id=?""",
                    (status, now, capture_id),
                )
                record_error(
                    database, "download", category, repr(exc), capture_id=capture_id,
                    http_status=status, retryable=retryable,
                )
                if should_surface_site_issue(category):
                    record_site_issue(
                        database, host_from_url(str(item["original_url"])),
                        "text_download", category,
                        site_issue_message(
                            category, str(item["original_url"]), "text download", status
                        ),
                        target=str(item["original_url"]), http_status=status,
                    )
        success_buffer.clear()
        skipped_buffer.clear()
        error_buffer.clear()
        last_flush = now_mono

    def stage_candidates() -> None:
        nonlocal rows_exhausted
        if rows_exhausted or len(ready_downloads) >= stage_limit:
            return
        staged: list[tuple[dict[str, object], Path]] = []
        reserved_paths: set[str] = set()
        while len(ready_downloads) + len(staged) < stage_limit:
            try:
                row = next(row_iter)
            except StopIteration:
                rows_exhausted = True
                break
            item = dict(row)
            path = _allocate_capture_path(database, config.output_dir, row, reserved_paths)
            reserved_paths.add(str(path))
            item["assigned_path"] = str(path)
            staged.append((item, path))
        if not staged:
            return
        now = utc_now()
        staged_updates = []
        for item, path in staged:
            existing_final = path.is_file()
            item["existing_final_before_operation"] = existing_final
            if existing_final:
                availability = str(item.get("payload_availability") or "retained_unscanned")
                if availability in {"not_acquired", "partial"}:
                    availability = "retained_unscanned"
                origin = str(item.get("payload_origin") or "adopted")
                retention = str(item.get("payload_retention") or "keep")
                if retention == "discard_after_scan" and origin != "acquired":
                    retention = "keep"
            else:
                availability = "partial"
                origin = "acquired"
                retention = config.text_retention
            item["payload_origin"] = origin
            item["payload_retention"] = retention
            staged_updates.append((str(path), availability, origin, retention, now, int(item["id"])))
        with database:
            database.executemany(
                """UPDATE captures SET local_path=?,payload_availability=?,payload_origin=?,
                   payload_retention=?,updated_at=? WHERE id=?""",
                staged_updates,
            )
        ready_downloads.extend(staged)

    def schedule_discard_scans() -> None:
        if not discard_mode or scan_pool is None:
            return
        scheduled: list[tuple[dict[str, object], int, bool]] = []
        while waiting_scan and len(scan_futures) + len(scheduled) < scan_limit:
            scheduled.append(waiting_scan.popleft())
        if scheduled:
            now = utc_now()
            with database:
                database.executemany(
                    "UPDATE captures SET state='scanning',updated_at=? WHERE id=?",
                    ((now, int(item["id"])) for item, _size, _eligible in scheduled),
                )
            for item, size, discard_eligible in scheduled:
                path = Path(str(item["local_path"]))
                future = scan_pool.submit(_scan_saved_capture, item, path, config, scan_jobs)
                scan_futures[future] = (item, size, discard_eligible)

    def process_discard_scans(timeout: float = 0.0) -> None:
        nonlocal spool_bytes, scan_completed, scan_matched, scan_failures, retained_failure_bytes
        if not discard_mode or not scan_futures:
            return
        done, _ = concurrent.futures.wait(
            tuple(scan_futures), timeout=timeout,
            return_when=concurrent.futures.FIRST_COMPLETED,
        )
        if not done:
            return
        successful: list[dict] = []
        successful_sizes: dict[int, int] = {}
        retained_successful: list[dict] = []
        with database:
            for future in done:
                item, size, discard_eligible = scan_futures.pop(future)
                capture_id = int(item["id"])
                try:
                    outcome = future.result()
                except Exception as exc:
                    scan_failures += 1
                    record_error(
                        database, "scan", "scan_failure", repr(exc),
                        capture_id=capture_id, retryable=True,
                    )
                    database.execute(
                        """UPDATE captures SET state='downloaded_unscanned',payload_availability='retained_unscanned',
                           payload_retention='keep',updated_at=? WHERE id=?""",
                        (utc_now(), capture_id),
                    )
                    # A scan failure keeps its source and explicitly transitions it
                    # out of the temporary discard spool into retained recovery
                    # storage. This is the only non-delete path that releases the
                    # spool reservation.
                    if discard_eligible:
                        spool_bytes = max(0, spool_bytes - size)
                        retained_failure_bytes += size
                    continue
                if outcome.get("kind") == "non_text":
                    if discard_eligible:
                        cleanup = _commit_non_text_discard(
                            database, config, capture_id, str(item.get("local_path") or "")
                        )
                        completed_ids = _finish_discard_cleanup(database, config, cleanup)
                        if capture_id in completed_ids:
                            spool_bytes = max(0, spool_bytes - size)
                    else:
                        mark_capture_skipped(database, capture_id, "sniffed_non_text", CLASSIFIER_REVISION)
                        database.execute(
                            "UPDATE captures SET payload_availability='retained',payload_retention='keep',updated_at=? WHERE id=?",
                            (utc_now(), capture_id),
                        )
                    continue
                scan_completed += 1
                scan_matched += int(any(
                    int(analysis.get("score") or 0) >= config.minimum_score
                    and not analysis.get("excluded") and not analysis.get("required_missing")
                    for analysis in outcome["analyses"].values()
                ))
                if discard_eligible:
                    successful.append(outcome)
                    successful_sizes[capture_id] = size
                else:
                    retained_successful.append(outcome)
            for outcome in retained_successful:
                document_id = save_success(database, outcome, config.report, retain_payload=True)
                _persist_embedded_candidates(database, config, document_id, outcome)
                database.execute(
                    "UPDATE captures SET payload_origin=COALESCE(NULLIF(payload_origin,''),'adopted'),payload_retention='keep' WHERE id=?",
                    (int(outcome["capture_id"]),),
                )
        if successful:
            cleanup = _commit_discard_evidence(database, config, successful)
            completed_ids = _finish_discard_cleanup(database, config, cleanup)
            for capture_id in completed_ids:
                spool_bytes = max(0, spool_bytes - successful_sizes.get(capture_id, 0))

    def emit_progress(force: bool = False) -> None:
        nonlocal last_emit
        if not callback:
            return
        now = time.monotonic()
        if not force and now - last_emit < 0.5:
            return
        last_emit = now
        elapsed = max(0.001, now - started)
        metadata_skipped = int(selection_stats["metadata_skipped"])
        url_skipped = int(selection_stats["url_skipped"])
        settled = downloaded + skipped + failures + metadata_skipped + url_skipped
        metrics = network_metrics()
        starts = int(metrics["request_starts"])
        completions = int(metrics["request_completions"])
        request_failures = int(metrics.get("request_failures", 0))
        retries = int(metrics["retry_waits"]) + int(metrics["rate_limit_events"])
        worker_wait_seconds = (
            float(metrics["pacing_wait_seconds"])
            + float(metrics["host_gate_wait_seconds"])
            + float(metrics["retry_wait_seconds"])
        )
        scheduled_rate_pause = float(metrics["rate_limit_wait_seconds"])
        network_seconds = float(metrics.get("network_seconds", 0.0))
        network_bytes = int(metrics.get("network_bytes", 0))
        label = "Download-only" if progress_stage == "download_only" else "Acquisition"
        discard_detail = (
            f"; scanned {scan_completed:,}; scan errors {scan_failures:,}; spool {spool_bytes / (1024*1024):.1f} MiB; "
            f"retained failed-scan bytes {retained_failure_bytes / (1024*1024):.1f} MiB"
            if discard_mode else ""
        )
        callback(ProgressEvent(
            progress_stage,
            f"{label}: wire request starts {starts:,} ({starts/elapsed:.1f}/s); "
            f"responses {completions:,}; transport failures {request_failures:,}; "
            f"saved {downloaded:,} ({downloaded/elapsed:.1f}/s); retries {retries:,}; "
            f"worker waits {worker_wait_seconds:.1f}s; scheduled rate pauses {scheduled_rate_pause:.1f}s; "
            f"skipped {skipped + metadata_skipped + url_skipped:,}; errors {failures:,}; {settled:,}/{total:,}" + discard_detail,
            min(settled, total), total,
            {
                "replay_submitted": submitted,
                "replay_started": starts,
                "replay_start_rate": starts / elapsed,
                "http_completions": completions,
                "network_retries": retries,
                "transport_failures": request_failures,
                "worker_wait_seconds": worker_wait_seconds,
                "scheduled_rate_pause_seconds": scheduled_rate_pause,
                "network_seconds": network_seconds,
                "network_bytes": network_bytes,
                "downloaded": downloaded,
                "download_rate": downloaded / elapsed,
                "skipped": skipped + metadata_skipped + url_skipped,
                "failures": failures,
                "pending": max(0, total - settled),
                "downloaded_unscanned": downloaded,
                "download_workers": config.workers,
                "scan_workers": scan_workers if discard_mode else 0,
                "scan_completed": scan_completed,
                "scan_matches": scan_matched,
                "scan_failures": scan_failures,
                "spool_bytes": spool_bytes,
                "retained_scan_failure_bytes": retained_failure_bytes,
            },
        ))

    deferred_error: RateLimitDeferred | ConnectivityPaused | None = None
    pool = concurrent.futures.ThreadPoolExecutor(
        max_workers=config.workers, thread_name_prefix="archive-acquire"
    )
    try:
        while True:
            if stop_event.is_set():
                raise Stopped

            process_discard_scans(0.0)
            schedule_discard_scans()
            if discard_mode:
                reserved_bytes = sum(discard_reservation(item) for item in futures.values())
                spool_pressure = spool_bytes + reserved_bytes
                try:
                    disk_free = shutil.disk_usage(config.output_dir).free
                except OSError:
                    disk_free = spool_high
                disk_low = disk_free < max(64 * 1024 * 1024, min(spool_high // 4, 512 * 1024 * 1024))
                if spool_pressure >= spool_high or disk_low:
                    backpressure_active = True
                elif spool_pressure <= spool_low and not disk_low:
                    backpressure_active = False
                if backpressure_active and callback:
                    callback(ProgressEvent(
                        "storage_backpressure",
                        f"Discard spool/reservations reached {spool_pressure / (1024*1024):.1f} MiB "
                        f"(free disk {disk_free / (1024*1024):.1f} MiB); pausing new replay admissions while scanners catch up.",
                    ))

            if not backpressure_active and len(ready_downloads) < max(config.workers, inflight_limit):
                stage_candidates()

            slots = 0 if backpressure_active else inflight_limit - len(futures)
            reserved_bytes = sum(discard_reservation(item) for item in futures.values()) if discard_mode else 0
            while slots > 0 and ready_downloads and deferred_error is None:
                item, path = ready_downloads[0]
                reservation = discard_reservation(item)
                if discard_mode and spool_bytes + reserved_bytes + reservation > spool_high:
                    # Permit one oversize object only when no other temporary
                    # work exists; otherwise wait for scan/cleanup to free room.
                    if futures or waiting_scan or scan_futures or spool_bytes > 0:
                        backpressure_active = True
                        break
                ready_downloads.popleft()
                futures[pool.submit(
                    _download_capture, item, path, config, client,
                    verify_existing_hash=False, compute_hash=False, stop_event=worker_stop,
                )] = item
                reserved_bytes += reservation
                submitted += 1
                slots -= 1

            if not futures:
                if discard_mode:
                    flush_results(force=True)
                    schedule_discard_scans()
                    process_discard_scans(0.05 if scan_futures else 0.0)
                    schedule_discard_scans()
                if deferred_error is not None:
                    flush_results(force=True)
                    break
                if rows_exhausted and not ready_downloads and not waiting_scan and not scan_futures:
                    flush_results(force=True)
                    break
                if discard_mode and spool_bytes >= spool_high and not ready_downloads and not waiting_scan and not scan_futures:
                    raise RuntimeError(
                        "discard spool cannot drain because cleanup is still pending; "
                        "free disk space or resolve cleanup errors, then Resume"
                    )
                flush_results()
                continue

            done, _ = concurrent.futures.wait(
                tuple(futures), timeout=0.05,
                return_when=concurrent.futures.FIRST_COMPLETED,
            )
            if not done:
                flush_results()
                emit_progress()
                continue

            for future in done:
                item = futures.pop(future)
                capture_id = int(item["id"])
                try:
                    result = future.result()
                    if result["kind"] == "non_text":
                        skipped_buffer.append((capture_id, str(result.get("resource_class") or "")))
                        skipped += 1
                        continue
                    if result["kind"] == "deferred_media":
                        # Persist only provenance/classification here. The standard
                        # media phase later resolves exact captures, applies format
                        # filters/snapshot policy, and performs the media replay GET.
                        from ..media.indexer import media_query_signature
                        media_signature = media_query_signature(config)
                        media_kind = str(result.get("resource_class") or "")
                        with database:
                            queue_media_discovery_candidates(
                                database, media_signature,
                                [(str(item["original_url"]), None, "text_validation_deferred", media_kind)],
                            )
                            database.execute(
                                """UPDATE captures SET state='skipped',skip_reason='deferred_to_media',
                                   resource_class=?,classification_reason='payload_validation_deferred',
                                   resource_classifier_revision=?,payload_availability='not_acquired',
                                   local_path=NULL,http_status=?,final_url=?,bytes_saved=0,
                                   download_attempts=download_attempts+1,updated_at=? WHERE id=?""",
                                (media_kind, RESOURCE_CLASSIFIER_REVISION, int(result.get("http_status") or 200),
                                 str(result.get("final_url") or ""), utc_now(), capture_id),
                            )
                        skipped += 1
                        continue
                    path = Path(result["path"])
                    adopted_existing = bool(result.get("adopted_existing"))
                    discard_eligible = bool(
                        discard_mode and not adopted_existing
                        and str(item.get("payload_origin") or "") == "acquired"
                        and str(item.get("payload_retention") or config.text_retention) == "discard_after_scan"
                    )
                    availability = "spooled_unscanned" if discard_eligible else "retained_unscanned"
                    origin = str(item.get("payload_origin") or ("adopted" if adopted_existing else "acquired"))
                    retention = "discard_after_scan" if discard_eligible else "keep"
                    success_buffer.append((
                        availability, origin, retention, str(path), str(result["content_hash"]),
                        int(result["http_status"]), str(result["final_url"]), int(result["bytes_saved"]),
                        CLASSIFIER_REVISION, capture_id, str(result.get("encoding") or ""),
                    ))
                    downloaded += 1
                    if discard_mode:
                        scan_item = dict(item)
                        scan_item.update(result)
                        scan_item["local_path"] = str(path)
                        size = int(result["bytes_saved"])
                        waiting_scan.append((scan_item, size, discard_eligible))
                        if discard_eligible:
                            spool_bytes += size
                except RateLimitDeferred as exc:
                    # Preserve the original pause reason, stop admitting queued
                    # work, and leave this capture retryable without consuming a
                    # normal download-attempt budget. Running workers observe the
                    # internal stop event; already-finalized files are adopted on
                    # Resume if their DB completion record was not yet flushed.
                    if deferred_error is None:
                        deferred_error = exc
                        acquisition_cancel.set()
                    with database:
                        database.execute(
                            "UPDATE captures SET state='pending',updated_at=? WHERE id=?",
                            (utc_now(), capture_id),
                        )
                except ConnectivityPaused as exc:
                    if deferred_error is None:
                        deferred_error = exc
                        acquisition_cancel.set()
                    with database:
                        database.execute(
                            "UPDATE captures SET state='pending',updated_at=? WHERE id=?",
                            (utc_now(), capture_id),
                        )
                except Stopped:
                    if deferred_error is None:
                        raise
                except Exception as exc:
                    if is_local_storage_error(exc):
                        acquisition_cancel.set()
                        with database:
                            database.execute(
                                "UPDATE captures SET state='pending',updated_at=? WHERE id=?",
                                (utc_now(), capture_id),
                            )
                        raise
                    error_buffer.append((capture_id, item, exc))
                    failures += 1

            flush_results(force=discard_mode and bool(waiting_scan))
            schedule_discard_scans()
            process_discard_scans(0.0)
            schedule_discard_scans()
            emit_progress()

            if deferred_error is not None:
                acquisition_cancel.set()
                for pending in futures:
                    pending.cancel()
                ready_downloads.clear()
                flush_results(force=True)
                break

        if deferred_error is not None:
            raise deferred_error
    except BaseException:
        acquisition_cancel.set()
        for future in futures:
            future.cancel()
        flush_results(force=True)
        pool.shutdown(wait=False, cancel_futures=True)
        if scan_pool is not None:
            for future in scan_futures:
                future.cancel()
            scan_pool.shutdown(wait=False, cancel_futures=True)
        raise
    else:
        pool.shutdown(wait=True)
        if scan_pool is not None:
            scan_pool.shutdown(wait=True)
        flush_results(force=True)
        emit_progress(force=True)
        metrics = network_metrics()
        return {
            "queued": total,
            "downloaded": downloaded,
            "skipped": skipped + int(selection_stats["metadata_skipped"]) + int(selection_stats["url_skipped"]),
            "errors": failures,
            "scan_errors": scan_failures,
            "elapsed": time.monotonic() - started,
            "http_starts": int(metrics["request_starts"]),
            "http_completions": int(metrics["request_completions"]),
        }
    finally:
        client.close()


def _scan_pending_captures(
    config: ProjectConfig,
    database: sqlite3.Connection,
    jobs: list[ScanJob],
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None,
    *,
    capture_ids: list[int] | None = None,
) -> dict[str, int | float]:
    """Consume durable downloaded_unscanned captures after acquisition finishes."""
    discard_mode = config.text_retention == "discard_after_scan"
    with database:
        database.execute("UPDATE captures SET state='downloaded_unscanned' WHERE state='scanning'")

    availability_clause = " AND c.payload_availability='spooled_unscanned'" if discard_mode else ""
    if capture_ids:
        database.execute("DROP TABLE IF EXISTS temp.archive_scout_scan_selection")
        database.execute(
            "CREATE TEMP TABLE archive_scout_scan_selection(id INTEGER PRIMARY KEY) WITHOUT ROWID"
        )
        database.executemany(
            "INSERT OR IGNORE INTO archive_scout_scan_selection(id) VALUES(?)",
            ((int(value),) for value in capture_ids),
        )
        total = int(database.execute(
            """SELECT COUNT(*) FROM captures c JOIN temp.archive_scout_scan_selection s ON s.id=c.id
               WHERE c.state='downloaded_unscanned' AND c.local_path IS NOT NULL""" + availability_clause
        ).fetchone()[0])
    else:
        scope_sql, scope_params = _inventory_scope_predicate(database, config, alias="c")
        total = int(database.execute(
            """SELECT COUNT(*) FROM captures c WHERE c.state='downloaded_unscanned'
               AND c.local_path IS NOT NULL AND """ + scope_sql + availability_clause,
            scope_params,
        ).fetchone()[0])

    if not total:
        if callback:
            callback(ProgressEvent("scan", "No downloaded captures are waiting for scanning.", 0, 0))
        return {"queued": 0, "scanned": 0, "matched": 0, "skipped": 0, "errors": 0, "elapsed": 0.0}

    scan_workers = config.scan_workers or min(8, max(1, (os.cpu_count() or 4) - 1))
    scan_workers = max(1, min(32, scan_workers))
    inflight_limit = max(scan_workers, scan_workers * 3)
    # Keep the long-standing helper/mocking contract for retain mode. Only the
    # destructive discard path needs the additional selector flag.
    row_iter = (
        _pending_scan_rows(database, config, capture_ids, discard_only=True)
        if discard_mode
        else _pending_scan_rows(database, config, capture_ids)
    )
    futures: dict[concurrent.futures.Future, dict[str, object]] = {}
    exhausted = False
    submitted = scanned = matched = skipped = failures = 0
    started = time.monotonic()
    last_emit = 0.0

    def emit_progress(force: bool = False) -> None:
        nonlocal last_emit
        if not callback:
            return
        now = time.monotonic()
        if not force and now - last_emit < 0.5:
            return
        last_emit = now
        elapsed = max(0.001, now - started)
        settled = scanned + skipped + failures
        callback(ProgressEvent(
            "scan",
            f"Scanning saved captures: {settled:,}/{total:,}; scanned {scanned:,} ({scanned/elapsed:.1f}/s); "
            f"matches {matched:,}; skipped {skipped:,}; errors {failures:,}",
            min(settled, total), total,
            {
                "scan_submitted": submitted,
                "scanned": scanned,
                "scan_rate": scanned / elapsed,
                "matched": matched,
                "skipped": skipped,
                "failures": failures,
                "scan_workers": scan_workers,
            },
        ))

    try:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=scan_workers, thread_name_prefix="archive-scan"
        ) as pool:
            while True:
                if stop_event.is_set():
                    raise Stopped

                slots = inflight_limit - len(futures)
                batch: list[dict[str, object]] = []
                while not exhausted and len(batch) < slots:
                    try:
                        row = next(row_iter)
                    except StopIteration:
                        exhausted = True
                        break
                    batch.append(dict(row))
                if batch:
                    now = utc_now()
                    with database:
                        database.executemany(
                            "UPDATE captures SET state='scanning',updated_at=? WHERE id=?",
                            ((now, int(item["id"])) for item in batch),
                        )
                    for item in batch:
                        path = Path(str(item["local_path"]))
                        futures[pool.submit(_scan_saved_capture, item, path, config, jobs)] = item
                        submitted += 1

                if not futures:
                    if exhausted:
                        break
                    continue

                done, _ = concurrent.futures.wait(
                    tuple(futures), timeout=0.05,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                if not done:
                    emit_progress()
                    continue

                results: list[tuple[dict[str, object], dict | BaseException]] = []
                for future in done:
                    item = futures.pop(future)
                    try:
                        results.append((item, future.result()))
                    except Exception as exc:
                        results.append((item, exc))

                successful: list[dict] = []
                with database:
                    for item, outcome in results:
                        capture_id = int(item["id"])
                        if isinstance(outcome, BaseException):
                            failures += 1
                            record_error(
                                database, "scan", "scan_failure", repr(outcome),
                                capture_id=capture_id, retryable=True,
                            )
                            database.execute(
                                "UPDATE captures SET state='downloaded_unscanned',updated_at=? WHERE id=?",
                                (utc_now(), capture_id),
                            )
                            continue
                        if outcome.get("kind") == "non_text":
                            mark_capture_skipped(
                                database, capture_id, "sniffed_non_text", CLASSIFIER_REVISION
                            )
                            skipped += 1
                            continue
                        if not discard_mode:
                            document_id = save_success(database, outcome, config.report, retain_payload=True)
                            _persist_embedded_candidates(database, config, document_id, outcome)
                        else:
                            successful.append(outcome)
                        scanned += 1
                        matched += int(any(
                            int(analysis.get("score") or 0) >= config.minimum_score
                            and not analysis.get("excluded") and not analysis.get("required_missing")
                            for analysis in outcome["analyses"].values()
                        ))
                if discard_mode and successful:
                    cleanup = _commit_discard_evidence(database, config, successful)
                    _finish_discard_cleanup(database, config, cleanup)
                emit_progress()

        emit_progress(force=True)
        return {
            "queued": total,
            "scanned": scanned,
            "matched": matched,
            "skipped": skipped,
            "errors": failures,
            "elapsed": time.monotonic() - started,
        }
    except Stopped:
        for future in futures:
            future.cancel()
        with database:
            database.execute("UPDATE captures SET state='downloaded_unscanned' WHERE state='scanning'")
        raise


def download_archive(
    config: ProjectConfig,
    database: sqlite3.Connection,
    scan_run_id: int,
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None,
    states: tuple[str, ...] = ("pending",),
    capture_ids: list[int] | None = None,
    scan_jobs: list[ScanJob] | None = None,
) -> dict[str, int | float]:
    """Acquire first through the shared replay engine, then scan local files.

    When a target has replay-runtime overrides, run bounded target phases so its
    worker count and replay delay are actually honored. Query-only overrides do
    not force a split; the unified selector keeps their target-specific CDX
    signatures/date bounds intact without sacrificing normal multi-target
    throughput.
    """
    if config.download_scope == "index_only":
        if callback:
            callback(ProgressEvent("download", "Index-only mode selected; downloads skipped."))
        return {"scan_errors": 0, "scanned": 0, "matched": 0}
    jobs = scan_jobs or [ScanJob.create(scan_run_id, config.keyword_set_name, config.keywords)]
    if not jobs or any(not job.patterns for job in jobs):
        raise ValueError("at least one keyword rule is required")
    combined_patterns = [item for job in jobs for item in job.patterns]
    runtime_configs = _text_runtime_target_configs(config, capture_ids)
    split_runtime = len(runtime_configs) > 1
    if split_runtime and callback:
        callback(ProgressEvent(
            "download",
            "Applying per-target replay worker/delay settings; targets will acquire in bounded phases.",
        ))

    aggregate: dict[str, int | float] = {"scan_errors": 0, "scanned": 0, "matched": 0}

    if config.text_retention == "discard_after_scan":
        # Retry only discard-spool files left by an earlier interrupted/failed
        # run. Pre-existing retained/imported captures are never swept into the
        # destructive policy merely because this operation selected discard.
        for runtime_config in runtime_configs:
            scan_stats = _scan_pending_captures(
                runtime_config, database, jobs, stop_event, callback, capture_ids=capture_ids
            )
            aggregate["scan_errors"] += int(scan_stats.get("errors", 0))
            aggregate["scanned"] += int(scan_stats.get("scanned", 0))
            aggregate["matched"] += int(scan_stats.get("matched", 0))
            acquire_stats = _acquire_archive(
                runtime_config, database, stop_event, callback,
                patterns=combined_patterns, states=states, capture_ids=capture_ids,
                progress_stage="download", scan_jobs=jobs,
            )
            aggregate["scan_errors"] += int(acquire_stats.get("scan_errors", 0))
    else:
        # Preserve the established acquisition-first contract even when runtime
        # settings require per-target replay pools: acquire every target first,
        # then drain the local scan backlog target by target.
        for runtime_config in runtime_configs:
            _acquire_archive(
                runtime_config, database, stop_event, callback,
                patterns=combined_patterns, states=states, capture_ids=capture_ids,
                progress_stage="download",
            )
        for runtime_config in runtime_configs:
            scan_stats = _scan_pending_captures(
                runtime_config, database, jobs, stop_event, callback, capture_ids=capture_ids
            )
            aggregate["scan_errors"] += int(scan_stats.get("errors", 0))
            aggregate["scanned"] += int(scan_stats.get("scanned", 0))
            aggregate["matched"] += int(scan_stats.get("matched", 0))
    return aggregate


def download_archive_only(
    config: ProjectConfig,
    database: sqlite3.Connection,
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None,
    states: tuple[str, ...] = ("pending",),
    capture_ids: list[int] | None = None,
) -> dict[str, int | float]:
    """Acquire text captures without creating scan/document/match work."""
    if config.text_retention == "discard_after_scan":
        raise ValueError("Scan and discard is unavailable for download-only because no scan completion exists")
    runtime_configs = _text_runtime_target_configs(config, capture_ids)
    if len(runtime_configs) == 1:
        return _acquire_archive(
            runtime_configs[0], database, stop_event, callback,
            patterns=None, states=states, capture_ids=capture_ids,
            progress_stage="download_only",
        )

    totals: dict[str, int | float] = {
        "queued": 0, "downloaded": 0, "skipped": 0, "errors": 0, "elapsed": 0.0,
    }
    for index, runtime_config in enumerate(runtime_configs, start=1):
        if callback:
            target = runtime_config.targets[0] if runtime_config.targets else f"target {index}"
            callback(ProgressEvent(
                "download_only",
                f"Applying per-target replay settings for {target} ({index}/{len(runtime_configs)}).",
            ))
        stats = _acquire_archive(
            runtime_config, database, stop_event, callback,
            patterns=None, states=states, capture_ids=capture_ids,
            progress_stage="download_only",
        )
        for name in totals:
            totals[name] += stats.get(name, 0)
    return totals
