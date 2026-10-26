from __future__ import annotations

import concurrent.futures
from collections import deque
import json
import hashlib
import errno
import os
import sqlite3
import threading
import time
import urllib.parse
from pathlib import Path
from typing import Callable, Iterator

from ..cdx.client import HttpClient, RateLimitDeferred
from ..cdx.parameters import cdx_query_signature
from ..database.classification import classify_indexed_captures
from ..classification import classify_indexed_resource, classify_payload_kind
from ..config import ProjectConfig
from ..constants import REPLAY_URL
from ..content import classify_replay_content, decode_bytes, is_text_candidate, looks_textual_bytes, parse_page, parse_text_representation
from ..database.repositories import record_error, record_site_issue, resolve_errors, save_match, upsert_document
from ..events import ProgressEvent, Stopped
from ..parsing.embeds import extract_embed_candidates_fast
from ..site_status import host_from_url, should_surface_site_issue, site_issue_message
from ..scanning.jobs import ScanJob
from ..scanning.keywords import compile_prefilter
from ..scanning.scoring import analyze_content, prepare_analysis_fields
from ..utils import atomic_write_bytes, hash_text, normalize_search, utc_now
from .rate_limit import SharedFixedRateLimiter, shared_host_gate
from .validation import classify_exception
from .schedule import record_failure, clear_retry, prepare_retry_queue, select_due_retry
from .checkpoint import write_receipt, discard_receipt, sync_directory
from .diagnostics import AcquisitionDiagnostics
from .redirects import make_replay_redirect_validator
from ..network.transports import RedirectPolicyError, ResilientTransport


class StorageFullError(OSError):
    """Hard operation blocker; never classify a full disk as a URL retry."""


def _storage_full(exc: BaseException) -> bool:
    visited = set()
    pending = [exc]
    while pending:
        exc = pending.pop()
        if not isinstance(exc, BaseException) or id(exc) in visited:
            continue
        visited.add(id(exc))
        if isinstance(exc, OSError) and (exc.errno in {errno.ENOSPC, getattr(errno, "EDQUOT", -1)}
                                            or getattr(exc, "winerror", None) in {39, 112}):
            return True
        if isinstance(exc, sqlite3.Error) and (getattr(exc, "sqlite_errorcode", None) == sqlite3.SQLITE_FULL
                                               or "database or disk is full" in str(exc).casefold()):
            return True
        pending.extend(error for _backend, error in getattr(exc, "failures", ()))
        pending.extend(x for x in (exc.__cause__, exc.__context__) if x is not None)
    return False


def replay_url(timestamp: str, original: str, modifier: str = "id_") -> str:
    encoded = urllib.parse.quote(original, safe=":/?&=#%+;,[]@!$'()*")
    clean_modifier = modifier if modifier in {"id_", "if_", "oe_"} else "id_"
    return f"{REPLAY_URL}/{timestamp}{clean_modifier}/{encoded}"


def capture_path(root: Path, capture_id: int, timestamp: str, original: str) -> Path:
    digest = hashlib.sha1(original.encode("utf-8", "surrogatepass")).hexdigest()
    return root / "captures" / timestamp[:4] / timestamp[4:6] / f"{capture_id}_{digest}.txt"


def _populate_download_scopes(database: sqlite3.Connection, config: ProjectConfig) -> None:
    """Keep target-specific inventory signatures in bounded SQLite temp storage."""
    database.execute("""CREATE TEMP TABLE IF NOT EXISTS archive_scout_download_scopes(
        pattern TEXT NOT NULL, signature TEXT NOT NULL, start TEXT NOT NULL,end TEXT NOT NULL,
        PRIMARY KEY(pattern,signature,start,end)) WITHOUT ROWID""")
    database.execute("DELETE FROM archive_scout_download_scopes")
    database.executemany(
        "INSERT OR IGNORE INTO archive_scout_download_scopes VALUES(?,?,?,?)",
        ((target, cdx_query_signature(scoped), scoped.from_date, scoped.to_date)
         for target in config.targets for scoped in [config.for_target(target)]),
    )



def _restore_changed_url_filter_skips(database: sqlite3.Connection, config: ProjectConfig) -> None:
    """A changed URL keyword selection must reconsider prior filter skips.

    Cache only the selection identity in project_meta; leave skipped rows alone
    on unchanged runs. The reset and identity update commit together so a crash
    cannot permanently hide rows when new keywords are chosen.
    """
    from ..database.scope import scope_rows
    selection = {
        "scope": config.download_scope,
        "targets": scope_rows(config),
        "rules": [item.rules for item in config.selected_keyword_sets()],
    }
    digest = hashlib.sha256(json.dumps(selection, ensure_ascii=False,
                        sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    key = "text_url_filter_selection_v1"
    old = database.execute("SELECT value FROM project_meta WHERE key=?", (key,)).fetchone()
    if old and old[0] == digest:
        return
    # Only current target/signature pairs, and only specifically URL-filtered
    # exclusions, are reopened. Media/policy skips remain permanently routed.
    with database:
        database.execute("""UPDATE captures SET state='pending',updated_at=?
            WHERE state='skipped' AND EXISTS (
                SELECT 1 FROM capture_routing r WHERE r.capture_id=captures.id
                AND r.routing='skipped_url_filter')
            AND EXISTS (SELECT 1 FROM targets t JOIN archive_scout_download_scopes s
                ON s.pattern=t.pattern AND s.signature=captures.query_signature AND captures.timestamp BETWEEN s.start AND s.end
                WHERE t.id=captures.target_id OR EXISTS
                    (SELECT 1 FROM capture_target_membership m WHERE m.capture_id=captures.id AND m.target_id=t.id))""", (utc_now(),))
        database.execute("""UPDATE capture_routing SET routing='pending',updated_at=?
            WHERE routing='skipped_url_filter' AND EXISTS (
                SELECT 1 FROM captures c JOIN targets t JOIN archive_scout_download_scopes s
                ON s.pattern=t.pattern AND s.signature=c.query_signature AND c.timestamp BETWEEN s.start AND s.end
                WHERE c.id=capture_routing.capture_id AND c.state='pending'
                AND (t.id=c.target_id OR EXISTS (SELECT 1 FROM capture_target_membership m
                    WHERE m.capture_id=c.id AND m.target_id=t.id)))""", (utc_now(),))
        database.execute("INSERT OR REPLACE INTO project_meta(key,value) VALUES(?,?)", (key,digest))


def _capture_progress_counts(database, config, capture_ids=None) -> dict[str, int]:
    """One inventory aggregation per batch, never a history scan per save."""
    _populate_download_scopes(database, config)
    source = "captures c"
    if capture_ids:
        database.execute("CREATE TEMP TABLE IF NOT EXISTS archive_scout_progress_selection(id INTEGER PRIMARY KEY) WITHOUT ROWID")
        database.execute("DELETE FROM archive_scout_progress_selection")
        database.executemany("INSERT OR IGNORE INTO archive_scout_progress_selection VALUES(?)",
                             ((int(value),) for value in capture_ids))
        # Drive explicit retries from selected IDs, not the entire history.
        source = "archive_scout_progress_selection p CROSS JOIN captures c ON c.id=p.id"
    rows = database.execute("SELECT c.state,COUNT(*) FROM " + source + """ WHERE EXISTS (
        SELECT 1 FROM targets t JOIN archive_scout_download_scopes s
        ON s.pattern=t.pattern AND s.signature=c.query_signature AND c.timestamp BETWEEN s.start AND s.end
        WHERE t.id=c.target_id OR EXISTS(SELECT 1 FROM capture_target_membership m
            WHERE m.capture_id=c.id AND m.target_id=t.id)) GROUP BY c.state""")
    return {str(row[0]): int(row[1]) for row in rows}


def cumulative_download_progress(database: sqlite3.Connection, config: ProjectConfig,
                                 queued_total: int, capture_ids=None) -> tuple[int, int]:
    """Acquired/excluded capture identities; failures are unfinished, not progress."""
    counts = _capture_progress_counts(database, config, capture_ids)
    return sum(counts.get(state, 0) for state in ('downloaded','downloaded_unscanned','skipped')), sum(counts.values())


def prepare_download_rows(
    database: sqlite3.Connection,
    config: ProjectConfig,
    patterns,
    states: tuple[str, ...] = ("pending",),
    capture_ids: list[int] | None = None,
) -> tuple[int, Iterator[sqlite3.Row]]:
    """Build a disk-backed download queue and stream rows in bounded batches.

    Candidate IDs are stored in temporary SQLite tables. This avoids both the
    project-sized Python lists used by older releases and SQLite's platform-
    dependent parameter limit when a large retry selection is supplied.
    """
    if not capture_ids:
        for signature in {cdx_query_signature(config.for_target(target)) for target in config.targets}:
            classify_indexed_captures(database, signature)
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
    _populate_download_scopes(database, config)
    if not capture_ids:
        _restore_changed_url_filter_skips(database, config)
    if not capture_ids:
        # Earlier releases stored redirect-policy refusals as nonretryable errors.
        # Recheck only exclusions whose meaning changed under the current policy;
        # keep their history until a real response establishes the new outcome.
        redirect_categories = (("external_redirect_blocked",) if config.download_external_redirects
                               else ("external_redirect_blocked", "live_redirect_blocked"))
        database.execute("""UPDATE captures SET state='pending',updated_at=? WHERE state='error'
            AND EXISTS(SELECT 1 FROM errors e WHERE e.capture_id=captures.id
                AND e.operation='download' AND e.resolved=0 AND e.ignored=0
                AND e.category IN (""" + ",".join("?" for _ in redirect_categories) + """))
            AND EXISTS(SELECT 1 FROM targets t JOIN archive_scout_download_scopes s
                ON s.pattern=t.pattern AND s.signature=captures.query_signature AND captures.timestamp BETWEEN s.start AND s.end
                WHERE t.id=captures.target_id OR EXISTS(SELECT 1 FROM capture_target_membership m
                    WHERE m.capture_id=captures.id AND m.target_id=t.id))""",
            (utc_now(), *redirect_categories))
        if config.download_external_redirects:
            # This policy is intentionally adjustable on Resume. Earlier explicit
            # redirect exclusions must not become permanent hidden omissions.
            database.execute("""UPDATE captures SET state='pending',updated_at=?
                WHERE state='skipped' AND EXISTS(SELECT 1 FROM capture_routing r
                    WHERE r.capture_id=captures.id AND r.routing='skipped_redirect_policy')
                AND EXISTS(SELECT 1 FROM targets t JOIN archive_scout_download_scopes s
                    ON s.pattern=t.pattern AND s.signature=captures.query_signature AND captures.timestamp BETWEEN s.start AND s.end
                    WHERE t.id=captures.target_id OR EXISTS(
                        SELECT 1 FROM capture_target_membership m
                        WHERE m.capture_id=captures.id AND m.target_id=t.id))""", (utc_now(),))
            database.execute("""UPDATE capture_routing SET routing='pending',updated_at=?
                WHERE routing='skipped_redirect_policy' AND EXISTS(SELECT 1 FROM captures c
                    WHERE c.id=capture_routing.capture_id AND c.state='pending')""", (utc_now(),))

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
        source = "archive_scout_capture_selection selected CROSS JOIN captures c ON c.id=selected.id"
    clauses.extend(["""EXISTS (SELECT 1 FROM targets t JOIN archive_scout_download_scopes s
            ON s.pattern=t.pattern AND s.signature=c.query_signature AND c.timestamp BETWEEN s.start AND s.end
            WHERE t.id=c.target_id OR EXISTS (SELECT 1 FROM capture_target_membership m
                WHERE m.capture_id=c.id AND m.target_id=t.id))""",
            "1=1"])
    # Explicit selections obey the same active scope as normal acquisition.
    if states:
        clauses.append("c.state IN (" + ",".join("?" for _ in states) + ")")
        params.extend(states)
        if "error" in states and not capture_ids:
            # Revisit *due* transient failures fairly with untouched captures,
            # without retrying permanent exclusions or ignoring time budgets.
            clauses.append("""(c.state!='error' OR (
                EXISTS (SELECT 1 FROM errors e WHERE e.capture_id=c.id
                        AND e.resolved=0 AND e.ignored=0 AND e.retryable=1)
                AND (NOT EXISTS(SELECT 1 FROM text_retry_schedule s WHERE s.capture_id=c.id)
                     OR EXISTS(SELECT 1 FROM text_retry_schedule s WHERE s.capture_id=c.id
                                AND s.next_eligible_at<=?))))""")
            params.append(time.time())

    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    cursor = database.execute(
        "SELECT c.id,c.original_url,c.mimetype,c.length FROM " + source + where + " ORDER BY c.id",
        params,
    )
    url_prefilter = compile_prefilter(patterns) if patterns else None
    now = utc_now()
    while True:
        chunk = cursor.fetchmany(2000)
        if not chunk:
            break
        selected_ids: list[tuple[int, int, int]] = []
        binary_ids: list[tuple[int, str, str]] = []
        keyword_skips: list[tuple[str, int]] = []
        robots_ids: list[tuple[str, int]] = []
        for row in chunk:
            capture_id = int(row["id"])
            if excluded_robots_filename(str(row["original_url"])):
                robots_ids.append((now, capture_id))
                continue
            decision = classify_indexed_resource(row["original_url"], row["mimetype"] or "")
            if decision.resource_class in {"image","video","audio","other_binary"} and decision.confident:
                binary_ids.append((capture_id, decision.resource_class, decision.reason))
                continue
            if decision.resource_class == "media_descriptor" and decision.confident:
                binary_ids.append((capture_id, decision.resource_class, decision.reason))
                continue
            if config.download_scope == "keyword_urls" and url_prefilter is not None:
                original_url = str(row["original_url"])
                normalized_url = normalize_search(original_url)
                if (
                    not url_prefilter.has_positive_rules
                    or not url_prefilter.matches({"url": original_url}, {"url": normalized_url})
                ):
                    keyword_skips.append((now, capture_id))
                    continue
            length = max(0, int(row["length"] or 0))
            selected_ids.append((capture_id, 1 if length <= 0 else 0, length))
        with database:
            if robots_ids:
                database.executemany(
                    "UPDATE captures SET state='skipped',updated_at=? WHERE id=?",
                    robots_ids,
                )
                database.executemany(
                    "UPDATE capture_routing SET routing='excluded_robots_txt',updated_at=? WHERE capture_id=?",
                    robots_ids,
                )
            if selected_ids:
                database.executemany(
                    "INSERT OR IGNORE INTO archive_scout_download_queue(id,priority,length) VALUES(?,?,?)",
                    selected_ids,
                )
            if keyword_skips:
                database.executemany(
                    "UPDATE captures SET state='skipped',updated_at=? WHERE id=?",
                    keyword_skips,
                )
                database.executemany(
                    "UPDATE capture_routing SET routing='skipped_url_filter',updated_at=? WHERE capture_id=?",
                    keyword_skips,
                )
            if binary_ids:
                database.executemany(
                    "UPDATE captures SET state='skipped',updated_at=? WHERE id=?",
                    ((now, capture_id) for capture_id, _, _ in binary_ids),
                )
                database.executemany(
                    """UPDATE capture_routing SET routing=?,updated_at=? WHERE capture_id=?""",
                    (("deferred_to_media" if config.media.enabled and kind in {"image", "video"}
                      else "unsupported_non_text" if config.media.enabled and kind in {"audio", "media_descriptor"}
                      else "skipped_non_text", now, capture_id)
                     for capture_id, kind, _ in binary_ids),
                )
                database.executemany(
                    """UPDATE capture_routing SET resource_class=?,evidence=?,confident=1,updated_at=?
                       WHERE capture_id=?""",
                    ((kind, evidence, now, capture_id) for capture_id, kind, evidence in binary_ids),
                )
    total = int(database.execute(
        "SELECT COUNT(*) FROM archive_scout_download_queue"
    ).fetchone()[0])
    if config.network.persistent_retries and not capture_ids:
        prepare_retry_queue(database)

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


def select_download_rows(
    database: sqlite3.Connection,
    config: ProjectConfig,
    patterns,
    states: tuple[str, ...] = ("pending",),
    capture_ids: list[int] | None = None,
) -> list[sqlite3.Row]:
    """Compatibility wrapper for extensions that expect an in-memory list."""
    _total, rows = prepare_download_rows(
        database, config, patterns, states=states, capture_ids=capture_ids
    )
    return list(rows)


def excluded_robots_filename(original_url: str) -> bool:
    """Exclude only the once-decoded URL pathname basename, never query text."""
    path = urllib.parse.urlsplit(str(original_url)).path
    return urllib.parse.unquote(path).rsplit("/", 1)[-1].casefold() == "robots.txt"


class NonTextResponse(Exception):
    """An archived payload was media; this is not a transport or download failure."""
    def __init__(self, resource_class: str, reason: str, content_type: str):
        super().__init__(reason)
        self.resource_class=resource_class
        self.reason=reason
        self.content_type=content_type

def fetch_parse_scan(row: sqlite3.Row, config: ProjectConfig, jobs: list[ScanJob], client: HttpClient, scan_now: bool = True) -> dict:
    original = row["original_url"]
    if excluded_robots_filename(original):
        raise ValueError("excluded_robots_txt: requested robots.txt is not eligible for text acquisition")
    url = replay_url(row["timestamp"], original)
    if isinstance(client, HttpClient):
        response = client.get(url, config.max_file_bytes,
                              redirect_validator=make_replay_redirect_validator(config, original,
                                  skip_redirects=not config.download_external_redirects))
    else:
        # Existing offline integrations can supply a fixture-only client.
        response = client.get(url, config.max_file_bytes)
    content_type = response["headers"].get("content-type") or response["headers"].get("Content-Type") or row["mimetype"] or ""
    data = response["data"]
    decision = classify_payload_kind(data, content_type, original)
    if decision.resource_class not in {"text", "media_descriptor"}:
        raise NonTextResponse(decision.resource_class, decision.reason, content_type)
    raw = decode_bytes(data if scan_now else data[:20000], content_type)
    replay_problem = classify_replay_content(raw, response["final_url"])
    if replay_problem:
        raise RuntimeError(replay_problem)
    # Publish validated replay bytes before expensive local parsing/scoring.
    # Even a failed parser or SQLite transaction must not force reacquisition.
    path = capture_path(config.output_dir, int(row["id"]), row["timestamp"], original)
    write_receipt(path, row, {"content_hash": hashlib.sha256(data).hexdigest(),
        "bytes_saved": len(data), "http_status": response["status"],
        "final_url": response["final_url"], "response_content_type": content_type})
    atomic_write_bytes(path, bytes(data))
    sync_directory(path.parent)
    if not scan_now:
        return {"capture_id": int(row["id"]), "path": path,
                "bytes_saved": len(data), "content_hash": hashlib.sha256(data).hexdigest(),
                "http_status": response["status"], "final_url": response["final_url"],
                "response_content_type": content_type, "deferred_scan": True}
    title, visible, links = parse_text_representation(raw, original, content_type)
    # Avoid a second full HTML parser pass in the download hot path. The page
    # parser above already found ordinary attributes; this lightweight source
    # pass recovers FlashVars/legacy player config. Dedicated media discovery
    # still performs the exhaustive parser pass later.
    if config.media.enabled and config.media.discover_embedded:
        embed_urls = {candidate.url for candidate in extract_embed_candidates_fast(raw, original)}
        if embed_urls:
            links = sorted(set(links).union(embed_urls))
    prepared_fields, prepared_normalized_fields = prepare_analysis_fields(original, title, visible, raw, links)
    analyses = {
        job.scan_run_id: analyze_content(
            original, title, visible, raw, links, job.patterns, job.prefilter,
            prepared_fields, prepared_normalized_fields,
        )
        for job in jobs
    }
    return {
        "capture_id": int(row["id"]),
        "path": path,
        "title": title,
        "visible": visible,
        "links": links,
        "analyses": analyses,
        "content_hash": hashlib.sha256(bytes(data)).hexdigest(),
        "normalized_hash": hash_text(prepared_normalized_fields["body"]),
        "bytes_saved": len(data),
        "http_status": response["status"],
        "final_url": response["final_url"],
        "response_content_type": content_type,
    }


def fetch_retain_streamed(row: sqlite3.Row, config: ProjectConfig, client: HttpClient) -> dict:
    """Stream the full replay to a capture-identity-bound staging file.

    Only a bounded payload prefix is retained in memory. The complete raw
    response is never published until transport completeness, content routing,
    and Wayback shell checks have passed. The local scanner handles full text.
    """
    acquisition_started = time.monotonic()
    original = str(row["original_url"])
    if excluded_robots_filename(original):
        raise ValueError("excluded_robots_txt: requested robots.txt is not eligible for text acquisition")
    path = capture_path(config.output_dir, int(row["id"]), row["timestamp"], original)
    stage = path.with_name(path.name + ".transfer")
    stage.parent.mkdir(parents=True, exist_ok=True)
    # No unvalidated prefix may be reused as if it were a complete capture.
    stage.unlink(missing_ok=True)
    try:
        response = client.download_to_path(
            replay_url(row["timestamp"], original), stage, config.max_file_bytes,
            redirect_validator=make_replay_redirect_validator(config, original,
                skip_redirects=not config.download_external_redirects),
            **({"expected_digest": str(row["digest"] or "")} if isinstance(client, HttpClient)
               and "digest" in row.keys() else {}),
            **({"retain_partial": False} if isinstance(client, HttpClient) else {}),
        )
        content_type = (response["headers"].get("content-type") or
                        response["headers"].get("Content-Type") or row["mimetype"] or "")
        preview = response["preview"]
        decision = classify_payload_kind(preview, content_type, original)
        if decision.resource_class not in {"text", "media_descriptor"}:
            raise NonTextResponse(decision.resource_class, decision.reason, content_type)
        # A recognizable archive error shell is not a saved original document.
        try:
            preview_text = decode_bytes(preview, content_type)
        except UnicodeDecodeError:
            # Prefix-only preview can split a UTF-16 surrogate/UTF-8 character.
            # The scanner validates the complete retained document locally.
            preview_text = ""
        replay_problem = classify_replay_content(preview_text, response["final_url"])
        if replay_problem:
            raise RuntimeError(replay_problem)
        if int(response["bytes"]) != stage.stat().st_size:
            raise OSError("streamed replay byte count differs from retained file")
        publish_started = time.monotonic()
        with stage.open("rb+") as stream:
            os.fsync(stream.fileno())
        write_receipt(path, row, {"content_hash": response["content_hash"],
            "bytes_saved": int(response["bytes"]), "http_status": response["status"],
            "final_url": response["final_url"], "response_content_type": content_type})
        os.replace(stage, path)
        sync_directory(path.parent)
        publish_seconds = time.monotonic() - publish_started
        return {"network_and_pacing_seconds": publish_started-acquisition_started,
            "file_fsync_publish_seconds": publish_seconds, "capture_id": int(row["id"]), "path": path,
            "bytes_saved": int(response["bytes"]), "content_hash": response["content_hash"],
            "http_status": response["status"], "final_url": response["final_url"],
            "response_content_type": content_type, "deferred_scan": True,
            "encoding_recovery": response.get("encoding_recovery", "")}
    finally:
        # A partial text replay is NOT a resumable media transfer.  Discard
        # both its bytes and HTTPX range/ETag sidecar.  A stale sidecar without
        # its prefix could otherwise cause a future request to attempt an
        # invalid partial replay or leak per-capture staging metadata.
        stage.unlink(missing_ok=True)
        stage.with_name(stage.name + ".resume.json").unlink(missing_ok=True)


def _save_payload_evidence(database: sqlite3.Connection, result: dict) -> None:
    if result.get("encoding_recovery"):
        note = record_error(database, "validation", "encoding_header_recovered",
                            str(result["encoding_recovery"]),
                            capture_id=int(result["capture_id"]), retryable=False)
        database.execute("UPDATE errors SET resolved=1 WHERE id=?", (note,))
    database.execute("""INSERT INTO capture_payload_evidence
        (capture_id,content_hash,bytes_saved,response_content_type,final_url,response_status,updated_at)
        VALUES(?,?,?,?,?,?,?) ON CONFLICT(capture_id) DO UPDATE SET
        content_hash=excluded.content_hash,bytes_saved=excluded.bytes_saved,
        response_content_type=excluded.response_content_type,
        final_url=excluded.final_url,response_status=excluded.response_status,
        updated_at=excluded.updated_at""", (int(result["capture_id"]),
        str(result["content_hash"]),int(result["bytes_saved"]),
        str(result.get("response_content_type") or ""),str(result.get("final_url") or ""),
        int(result["http_status"]),utc_now()))


def save_success(database: sqlite3.Connection, result: dict) -> None:
    if result.get("deferred_scan"):
        with database:
            database.execute(
                """UPDATE captures SET state='downloaded_unscanned',http_status=?,final_url=?,
                       bytes_saved=?,updated_at=? WHERE id=?""",
                (result["http_status"],result["final_url"],result["bytes_saved"],utc_now(),result["capture_id"]),
            )
            database.execute(
                """UPDATE capture_routing SET resource_class='text',evidence='payload:validated_text',
                       confident=1,routing='downloaded_awaiting_scan',updated_at=? WHERE capture_id=?""",
                (utc_now(),result["capture_id"]),
            )
            _save_payload_evidence(database, result)
            clear_retry(database, result["capture_id"])
            resolve_errors(database, capture_id=result["capture_id"], operations=("download",))
        discard_receipt(Path(result['path']))
        return
    with database:
        document_id = upsert_document(
            database,
            result["capture_id"],
            result["path"],
            result["title"],
            result["visible"],
            result["links"],
            result["content_hash"],
            result["normalized_hash"],
            result["bytes_saved"],
        )
        database.execute(
            "UPDATE captures SET state='downloaded',http_status=?,final_url=?,bytes_saved=?,updated_at=? WHERE id=?",
            (result["http_status"], result["final_url"], result["bytes_saved"], utc_now(), result["capture_id"]),
        )
        for scan_run_id, analysis in result["analyses"].items():
            save_match(database, int(scan_run_id), document_id, analysis)
        resolve_errors(database, capture_id=result["capture_id"], operations=("download",))
        _save_payload_evidence(database, result)
        clear_retry(database, result["capture_id"])
    discard_receipt(Path(result['path']))


def download_archive(
    config: ProjectConfig,
    database: sqlite3.Connection,
    scan_run_id: int,
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None,
    states: tuple[str, ...] = ("pending",),
    capture_ids: list[int] | None = None,
    scan_jobs: list[ScanJob] | None = None,
    acquire_only: bool = False,
) -> None:
    operation_started = time.monotonic()
    if config.download_scope == "index_only":
        if callback:
            callback(ProgressEvent("download", "Index-only mode selected; downloads skipped."))
        return
    # Defense in depth: direct callers and manual retries cannot bypass a
    # missing/unfinished inventory. One proof per batch, never per capture.
    from ..coverage import require_text_coverage
    require_text_coverage(database, config)
    jobs = [] if acquire_only else (scan_jobs or [ScanJob.create(scan_run_id, config.keyword_set_name, config.keywords)])
    if not acquire_only and (not jobs or any(not job.patterns for job in jobs)):
        raise ValueError("at least one keyword rule is required")
    combined_patterns = [item for job in jobs for item in job.patterns]
    if acquire_only and config.download_scope == "keyword_urls":
        # Download-only does not create scan runs, but URL-only filtering must
        # still honor the explicitly selected keyword box.
        selected = config.selected_keyword_sets()
        if not selected:
            raise ValueError("URL-only downloads require a selected keyword set")
        combined_patterns = ScanJob.create(0, selected[0].name, selected[0].rules).patterns
    if not capture_ids:
        with database:
            database.execute("UPDATE captures SET state='pending' WHERE state='downloading'")
    queue_started = time.monotonic()
    if callback:
        callback(ProgressEvent("download_prepare", "Preparing disk-backed text acquisition queue; committed progress is retained."))
    total, row_iter = prepare_download_rows(
        database, config, combined_patterns, states=states, capture_ids=capture_ids
    )
    progress_counts = _capture_progress_counts(database, config, capture_ids)
    cumulative_total = sum(progress_counts.values())
    retained = sum(progress_counts.get(state, 0) for state in ('downloaded','downloaded_unscanned'))
    excluded = progress_counts.get('skipped', 0)
    queue_preparation_seconds = time.monotonic()-queue_started
    initial_queue_size = total
    if not total:
        if callback:
            callback(ProgressEvent(
                "download",
                f"No captures eligible in this batch; text acquisition satisfied {retained+excluded:,}/{cumulative_total:,}; "
                f"retained {retained:,}; excluded/routed {excluded:,}. This does not certify complete inventory or scanning.",
                retained+excluded, cumulative_total,
            ))
        if not acquire_only:
            from ..scanning.pending import scan_pending_saved
            for target in config.targets:
                scan_pending_saved(database,config.for_target(target),jobs,stop_event,callback)
        return
    limiter = SharedFixedRateLimiter(config.download_delay)
    host_gate = shared_host_gate(config.rate_limit_base_pause, config.rate_limit_max_pause)
    from .session import acquisition_client
    diagnostics, _ = acquisition_client(("text_observations", id(database)), lambda: AcquisitionDiagnostics(config.output_dir, callback,
                                         enabled=config.network.diagnostics))
    diagnostics.begin_batch(callback)
    if callback and config.network.diagnostics:
        callback(ProgressEvent('acquisition_health', f'Text acquisition build: {diagnostics.build}'))

    def on_retry(attempt: int, total_attempts: int, reason: str, wait_seconds: float) -> None:
        if callback:
            rate_limited = "all Wayback requests paused" in reason
            stage = "rate_limit" if rate_limited else "download_retry"
            if rate_limited:
                limit = f"/{total_attempts}" if total_attempts else ""
                message = f"{reason}. Shared pause {attempt}{limit} for {wait_seconds:.1f}s; one recovery probe will run next…"
            else:
                message = f"{reason}. Retry {attempt}/{total_attempts} in {wait_seconds:.1f}s…"
            callback(ProgressEvent(stage, message))

    from .session import acquisition_client
    # Settings may change between explicit operations, never between retries
    # within one operation. The full transport key prevents incompatible reuse.
    client_key = (config.workers, config.download_delay, config.retries,
                  config.connect_timeout, config.read_timeout, config.user_agent,
                  json.dumps(config.network.to_payload(), sort_keys=True),
                  config.rate_limit_attempts, config.rate_limit_max_wait)
    client, owns_client = acquisition_client(client_key, lambda: HttpClient(
        limiter,
        1 if config.network.persistent_retries else config.retries,
        max(config.connect_timeout, config.read_timeout),
        config.user_agent,
        stop_event,
        retry_callback=on_retry,
        connect_timeout=config.connect_timeout,
        read_timeout=config.read_timeout,
        pool_size=config.workers,
        host_gate=host_gate,
        rate_limit_attempts=config.rate_limit_attempts,
        rate_limit_max_wait=config.rate_limit_max_wait,
        network_backend=config.network.normalized().backend,
        trust_environment=config.network.normalized().trust_environment,
        coordinated_connection_recovery=config.network.coordinated_connection_recovery,
        connection_failure_threshold=config.network.connection_failure_pause_threshold,
        connection_retry_seconds=config.network.connection_retry_seconds,
        network_callback=(lambda message: callback(ProgressEvent("network", message)) if callback else None),
        response_callback=diagnostics.response,
    ))
    client.response_callback = diagnostics.response
    client.retry_callback = on_retry
    # The network pool should retain validated bytes, not burn connection slots
    # on HTML parsing and keyword matching. A separate local scanner commits
    # score-zero coverage and matches with its own WAL connection.
    # SQLite WAL allows concurrent readers, but only one writer.  Guard the
    # two owner threads' short write transactions; a busy scanner must not
    # abort a multi-day acquisition or force another network retrieval.
    database_write_lock = threading.Lock()
    scanner_wakeup = threading.Event()
    scanner_finish = threading.Event()
    scanner_failures: list[BaseException] = []
    scanner_thread = None
    if jobs and not acquire_only:
        # Reconcile legacy gaps on the local scanner while network work starts.
        # Share its completed proof with the owner across tiny recovery batches.
        from ..scanning.pending import reconciliation_done, mark_reconciled
        needs_reconciliation = not reconciliation_done(database, config)
        reconciliation_finished = []

        def local_scanner() -> None:
            from ..scanning.pending import scan_pending_saved
            from ..database.connection import DATABASE_NAME
            connection = sqlite3.connect(config.output_dir / DATABASE_NAME, timeout=60)
            connection.row_factory = sqlite3.Row
            try:
                connection.execute("PRAGMA journal_mode=WAL")
                connection.execute("PRAGMA foreign_keys=ON")
                connection.execute("PRAGMA busy_timeout=1500")
                connection.execute("PRAGMA temp_store=FILE")
                while not stop_event.is_set():
                    scanner_wakeup.wait(2.0)
                    scanner_wakeup.clear()
                    for target in config.targets:
                        if stop_event.is_set():
                            break
                        # Bounded per-document bytes and CPU-side worker count;
                        # do not multiply network worker thread count here.
                        scan_pending_saved(connection, config.for_target(target), jobs,
                            stop_event, callback, workers=max(1, min(4, (os.cpu_count() or 2)//2)),
                            write_lock=database_write_lock, reconcile_legacy=needs_reconciliation)
                    if needs_reconciliation and not stop_event.is_set():
                        reconciliation_finished[:] = [True]
                    if scanner_finish.is_set():
                        break
            except Stopped:
                pass
            except BaseException as exc:
                scanner_failures.append(exc)
            finally:
                connection.close()
        scanner_thread = threading.Thread(target=local_scanner,
                         name="archive-scout-local-scan-supervisor", daemon=False)
        scanner_thread.start()
        scanner_wakeup.set()
    completed = matched = failures = saved_this_pass = 0
    recent_commits: deque[float] = deque()
    stage_stats = {"network_and_pacing_seconds": 0.0,
                   "file_fsync_publish_seconds": 0.0,
                   "database_commit_seconds": 0.0,
                   "samples": 0}
    started = time.monotonic()
    max_inflight = max(config.workers, config.workers * 2)
    last_completion = time.monotonic()
    last_save = time.monotonic()
    last_stall_message = time.monotonic()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=config.workers, thread_name_prefix="archive-scout") as pool:
            futures: dict[concurrent.futures.Future, sqlite3.Row] = {}
    
            fresh_admitted = 0

            def submit_available() -> None:
                nonlocal fresh_admitted, total
                slots = max_inflight - len(futures)
                if slots <= 0:
                    return
                rows: list[sqlite3.Row] = []
                for _ in range(slots):
                    if stop_event.is_set():
                        raise Stopped
                    retry_row = None
                    # Interleave one due retry after each bounded block of
                    # fresh work. Read only indexed SQLite eligibility; do
                    # not rebuild the complete acquisition queue or pool.
                    if config.network.persistent_retries and fresh_admitted >= 16 and not capture_ids:
                        retry_row = select_due_retry(database, time.time())
                        fresh_admitted = 0
                    if retry_row is not None:
                        rows.append(retry_row)
                        total += 1  # an additional physical attempt, not a new capture identity
                        continue
                    try:
                        row = next(row_iter)
                    except StopIteration:
                        break
                    fresh_admitted += 1
                    rows.append(row)
                if not rows:
                    return
                now = utc_now()
                with database_write_lock, database:
                    database.executemany("DELETE FROM archive_scout_download_queue WHERE id=?",
                                         ((int(row["id"]),) for row in rows))
                    database.executemany(
                        "UPDATE captures SET state='downloading',download_attempts=download_attempts+1,updated_at=? WHERE id=?",
                        ((now, int(row["id"])) for row in rows),
                    )
                for row in rows:
                    # Real network acquisition streams directly to disk.
                    # Nonstandard fixture transports retain the established
                    # memory-based API for compatible plugins/tests.
                    streamed = (isinstance(getattr(client, "transport", None), ResilientTransport)
                        and getattr(type(client).get, "__module__", "") == "archive_scout.cdx.client")
                    worker = fetch_retain_streamed if streamed else fetch_parse_scan
                    if streamed:
                        futures[pool.submit(worker, row, config, client)] = row
                    else:
                        futures[pool.submit(worker, row, config, jobs, client,
                            False if scanner_thread is not None else not acquire_only)] = row
    
            submit_available()
            while futures:
                if stop_event.is_set():
                    for pending in futures:
                        pending.cancel()
                    raise Stopped
                done, _ = concurrent.futures.wait(futures, timeout=5.0, return_when=concurrent.futures.FIRST_COMPLETED)
                if not done:
                    now = time.monotonic()
                    diagnostics.summary(saved_this_pass)
                    if callback and now-last_completion >= 30.0 and now-last_stall_message >= 30.0:
                        status = client.host_gate.snapshot()
                        transport_health = (client.transport.snapshot()
                            if isinstance(getattr(client, "transport", None), ResilientTransport)
                            else {})
                        callback(ProgressEvent(
                            "acquisition_stall",
                            f"No completed text requests for {now-last_completion:.0f}s; "
                            f"queued/running tasks {len(futures)}; "
                            f"workers waiting at Wayback gate {status['waiting_workers']}; "
                            f"probe active {status['recovery_probe']}; "
                            f"host pause {status['remaining_pause']:.1f}s; "
                            f"last successful save {now-last_save:.0f}s ago; "
                            f"backend waiters {transport_health.get('backend_waiters', '?')}; "
                            f"cooling backends {transport_health.get('cooling_backends', ())}; "
                            f"backend cooldown max {transport_health.get('cooldown_seconds', '?')}s. "
                            "Remaining worker occupancy may be network, filesystem, scanner, or DB work."
                        ))
                        last_stall_message = now
                    continue
                last_completion = time.monotonic()
                for future in done:
                    row = futures.pop(future)
                    outcome_state = None
                    retained_after_failure = None
                    try:
                        result = future.result()
                        try:
                            commit_started = time.monotonic()
                            with database_write_lock:
                                save_success(database, result)
                            stage_stats["samples"] += 1
                            stage_stats["database_commit_seconds"] += time.monotonic()-commit_started
                            for stage in ("network_and_pacing_seconds", "file_fsync_publish_seconds"):
                                stage_stats[stage] += float(result.get(stage, 0.0))
                        except Exception:
                            # Publish rescue state and its scan error together
                            # in the outer handler. A scanner must not race a
                            # later error write that downgrades a proven body.
                            path = Path(result.get("path", ""))
                            if path.is_file() and path.stat().st_size == result.get("bytes_saved"):
                                retained_after_failure = result
                            raise
                        saved_this_pass += 1
                        outcome_state = 'downloaded_unscanned'
                        if scanner_thread is not None:
                            scanner_wakeup.set()
                        last_save = time.monotonic()
                        recent_commits.append(last_save)
                        if "analyses" in result:
                            matched += int(any(
                                int(analysis.get("score") or 0) >= config.minimum_score
                                and not analysis.get("excluded") and not analysis.get("required_missing")
                                for analysis in result["analyses"].values()
                            ))
                    except NonTextResponse as exc:
                        outcome_state = 'skipped'
                        with database_write_lock, database:
                            database.execute("UPDATE captures SET state='skipped',updated_at=? WHERE id=?", (utc_now(),row["id"]))
                            database.execute(
                                """INSERT INTO capture_routing(capture_id,resource_class,evidence,confident,routing,updated_at)
                                   VALUES(?,?,?,?,?,?) ON CONFLICT(capture_id) DO UPDATE SET
                                   resource_class=excluded.resource_class,evidence=excluded.evidence,
                                   confident=excluded.confident,routing=excluded.routing,updated_at=excluded.updated_at""",
                                (row["id"], exc.resource_class,exc.reason,1,
                                 ("deferred_to_media" if config.media.enabled and exc.resource_class in {"image","video"}
                                  else "unsupported_non_text" if config.media.enabled else "skipped_non_text"),utc_now()),
                            )
                            if config.media.enabled and exc.resource_class in {"image","video"}:
                                from ..media.indexer import media_query_signature
                                from ..media.extensions import extension_from_url
                                from ..database.repositories import get_or_create_media_target, upsert_media_captures
                                target = get_or_create_media_target(database, "__deferred_text_replay__")
                                suffix = extension_from_url(str(row["original_url"]))
                                # Preserve the URL's real suffix (or absence
                                # of one); a fallback .mp4/.png would invent
                                # format identity for opaque historical bytes.
                                upsert_media_captures(
                                    database,[({"original": row["original_url"],"timestamp":row["timestamp"],
                                               "mimetype":exc.content_type,"statuscode":row["statuscode"],
                                               "digest":row["digest"],"length":row["length"]},
                                               exc.resource_class,suffix)], target,
                                    media_query_signature(config),source_type="text_replay_deferred",
                                )
                    except RateLimitDeferred as exc:
                        # A finite per-request rate-limit wait budget is not
                        # an explicit user Stop. Preserve the URL as due later;
                        # let independent local scans and permitted work continue.
                        failures += 1
                        diagnostics.failure(row, 'rate_limit', exc)
                        with database_write_lock, database:
                            database.execute("UPDATE captures SET state='error',updated_at=? WHERE id=?",
                                (utc_now(), row["id"]))
                            record_error(database, "download", "rate_limit", repr(exc),
                                capture_id=int(row["id"]), http_status=getattr(exc,"status",429), retryable=True)
                            record_failure(database, int(row["id"]), "rate_limit",
                                getattr(exc,"status",429), True,
                                max(config.network.retry_base_seconds, float(getattr(exc,"waited",0) or 0)),
                                config.network.retry_max_seconds)
                        if callback:
                            callback(ProgressEvent("rate_limit",
                                "Wayback request turn reached its rate-limit wait budget; "
                                "capture remains scheduled for automatic retry. Local scanning continues."))
                    except Stopped:
                        with database_write_lock, database:
                            database.execute("UPDATE captures SET state='pending',updated_at=? WHERE id=?", (utc_now(), row["id"]))
                        raise
                    except RedirectPolicyError as exc:
                        if exc.category != "redirect_skipped":
                            # Live/unapproved destinations remain explicit
                            # errors when following archived redirects is on.
                            with database_write_lock, database:
                                database.execute("UPDATE captures SET state='error',final_url=?,updated_at=? WHERE id=?",
                                    (exc.destination, utc_now(), row["id"]))
                                record_error(database, "download", exc.category, str(exc),
                                             capture_id=int(row["id"]), retryable=False)
                                database.execute("UPDATE capture_routing SET routing='failed_redirect_policy',updated_at=? WHERE capture_id=?",
                                                 (utc_now(), row["id"]))
                                clear_retry(database, int(row["id"]))
                            failures += 1
                            diagnostics.failure(row, exc.category, exc)
                        else:
                            outcome_state = 'skipped'
                            with database_write_lock, database:
                                database.execute("UPDATE captures SET state='skipped',final_url=?,updated_at=? WHERE id=?",
                                    (exc.destination, utc_now(), row["id"]))
                                database.execute("UPDATE capture_routing SET routing='skipped_redirect_policy',updated_at=? WHERE capture_id=?",
                                    (utc_now(), row["id"]))
                                clear_retry(database, int(row["id"]))
                                resolve_errors(database, capture_id=int(row["id"]), operations=("download",))
                            if callback:
                                callback(ProgressEvent("redirect_skipped", f"Skipped redirected text URL: {row['original_url']}"))
                    except Exception as exc:
                        if _storage_full(exc):
                            for pending in futures:
                                pending.cancel()
                            if callback:
                                callback(ProgressEvent("storage_full", "Storage full: stopped new acquisition; saved project checkpoints remain resumable."))
                            raise StorageFullError("storage full while retaining a capture or committing its results") from exc
                        failures += 1
                        if isinstance(exc, RedirectPolicyError):
                            category, status, retryable = exc.category, None, False
                        else:
                            category, status, retryable = classify_exception(exc)
                        if retained_after_failure is not None:
                            category, status, retryable = 'scan_commit_failure', retained_after_failure['http_status'], True
                        issue_message = site_issue_message(
                            category, str(row["original_url"]), "text download", status
                        )
                        with database_write_lock, database:
                            if retained_after_failure is not None:
                                _save_payload_evidence(database, retained_after_failure)
                                database.execute("""UPDATE captures SET state='downloaded_unscanned',
                                    bytes_saved=?,http_status=?,final_url=?,updated_at=? WHERE id=?""",
                                    (retained_after_failure['bytes_saved'],status,retained_after_failure['final_url'],utc_now(),row['id']))
                            database.execute(
                                """UPDATE captures SET state='error',http_status=?,
                                   final_url=COALESCE(?,final_url),updated_at=?
                                   WHERE id=? AND state!='downloaded_unscanned'""",
                                (status, getattr(exc, "destination", None), utc_now(), row["id"]),
                            )
                            database.execute(
                                """UPDATE capture_routing SET routing='failed',updated_at=?
                                   WHERE capture_id=? AND (SELECT state FROM captures WHERE id=capture_id)!='downloaded_unscanned'""",
                                (utc_now(), row["id"]),
                            )
                            record_error(
                                database,
                                "scan" if retained_after_failure is not None else "download",
                                category,
                                repr(exc),
                                capture_id=int(row["id"]),
                                http_status=status,
                                retryable=retryable,
                            )
                            if retained_after_failure is not None:
                                clear_retry(database,int(row['id']))
                            else:
                                record_failure(database, int(row["id"]), category, status, retryable,
                                               config.network.retry_base_seconds,
                                               config.network.retry_max_seconds)
                            if should_surface_site_issue(category):
                                record_site_issue(
                                    database,
                                    host_from_url(str(row["original_url"])),
                                    "text_download",
                                    category,
                                    issue_message,
                                    target=str(row["original_url"]),
                                    http_status=status,
                                )
                        diagnostics.failure(row, category, exc)
                        if callback and should_surface_site_issue(category):
                            callback(ProgressEvent("site_issue", issue_message))
                        if retained_after_failure is not None:
                            scanner_wakeup.set()
                    completed += 1
                    if outcome_state is None:
                        # Failures can still leave a valid retained checkpoint
                        # when a later parse/commit stage fails. Read one ID,
                        # never recount the project on the acquisition hot path.
                        outcome_state = database.execute("SELECT state FROM captures WHERE id=?", (row['id'],)).fetchone()[0]
                    was_retained = row['state'] in {'downloaded','downloaded_unscanned'}
                    retained += int(outcome_state in {'downloaded','downloaded_unscanned'}) - int(was_retained)
                    excluded += int(outcome_state == 'skipped') - int(row['state'] == 'skipped')
                    elapsed = max(0.001, time.monotonic() - started)
                    # This measures successfully committed *recent* saves,
                    # not an invocation average diluted by hours of indexing,
                    # waiting, or older network faults. No database reads.
                    now = time.monotonic()
                    diagnostics.summary(saved_this_pass)
                    while recent_commits and recent_commits[0] <= now - 30.0:
                        recent_commits.popleft()
                    rate = len(recent_commits) / min(30.0, elapsed)
                    invocation_rate = saved_this_pass / elapsed
                    cumulative_completed = max(0, min(cumulative_total, retained + excluded))
                    if callback:
                        callback(
                            ProgressEvent(
                                "download",
                                f"Text acquisition satisfied {cumulative_completed:,}/{cumulative_total:,}; "
                                f"retained {retained:,}; excluded/routed {excluded:,}; "
                                f"attempt outcomes {completed:,}/{total:,}; newly saved {saved_this_pass:,}; "
                                f"matches committed asynchronously; errors {failures:,}; recent successful saves {rate:.1f}/s (30s); "
                                f"acquisition average {invocation_rate:.1f}/s; "
                                f"batch average including preparation {saved_this_pass/max(.001,now-operation_started):.1f}/s",
                                cumulative_completed,
                                cumulative_total,
                                {
                                    "matched": matched,
                                    "failures": failures,
                                    "rate": rate,
                                    "rolling_save_rate_30s": rate,
                                    "invocation_save_rate": invocation_rate,
                                    "new_saves": saved_this_pass,
                                    "captures_satisfied": cumulative_completed,
                                    "captures_retained": retained,
                                    "captures_excluded_or_routed": excluded,
                                    "capture_total": cumulative_total,
                                    "attempt_outcomes": completed,
                                    "attempt_errors": failures,
                                    "initial_queue_size": initial_queue_size,
                                    "admitted_attempts": total,
                                    "queue_preparation_seconds": queue_preparation_seconds,
                                    "operation_save_rate": saved_this_pass/max(.001, now-operation_started),
                                    "timing_samples": stage_stats["samples"],
                                    "avg_acquisition_seconds": (stage_stats["network_and_pacing_seconds"] /
                                        max(1, stage_stats["samples"])),
                                    "avg_fsync_publish_seconds": (stage_stats["file_fsync_publish_seconds"] /
                                        max(1, stage_stats["samples"])),
                                    "avg_database_commit_seconds": (stage_stats["database_commit_seconds"] /
                                        max(1, stage_stats["samples"])),
                                },
                            )
                        )
                    submit_available()
    finally:
        # Drain local scans on clean completion, but honor Stop promptly.
        scanner_finish.set()
        scanner_wakeup.set()
        if scanner_thread is not None:
            scanner_thread.join()
        if owns_client:
            client.close()
        if scanner_thread is not None and reconciliation_finished:
            mark_reconciled(database, config)
        diagnostics.summary(saved_this_pass, force=True)
    if scanner_failures:
        raise RuntimeError("background scanner failed; retained bodies remain recoverable") from scanner_failures[0]
    # Final safeguard catches a small race between the last network commit and
    # the scanner wakeup; it does no work when the background drain succeeded.
    if not acquire_only:
        from ..scanning.pending import scan_pending_saved
        for target in config.targets:
            scan_pending_saved(database,config.for_target(target),jobs,stop_event,callback)
