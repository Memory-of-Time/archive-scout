from __future__ import annotations

import concurrent.futures
from collections import deque
import base64
import hashlib
import os
import re
import sqlite3
import threading
import time
from pathlib import Path
from typing import Callable
from urllib.parse import urlsplit

from ..cdx.client import HttpClient, RateLimitDeferred
from ..config import ProjectConfig
from ..downloads.redirects import make_replay_redirect_validator
from ..database.repositories import blocked_site_reasons, record_error, record_site_issue, save_media_success
from ..downloads.downloader import replay_url, _storage_full, StorageFullError
from ..downloads.rate_limit import SharedFixedRateLimiter, shared_host_gate
from ..downloads.validation import classify_exception
from ..downloads.diagnostics import AcquisitionDiagnostics
from ..downloads.checkpoint import sync_directory
from ..content import classify_replay_content
from ..events import ProgressEvent, Stopped
from ..site_status import host_from_url, should_surface_site_issue, site_issue_message
from ..utils import utc_now
from .indexer import media_query_signature
from .recovery import record_media_failure, clear_media_retry

INVALID_COMPONENT = re.compile(r'[<>:"/\\|?*\x00-\x1f]')


def _platform_filename(value: str, fallback: str) -> str:
    """Preserve the URL filename verbatim whenever the filesystem permits it.

    URL path segments are normally already percent-escaped, so the common case
    needs no rewriting at all. Only characters that cannot be represented as one
    portable filename are percent-escaped instead of being replaced with opaque
    ids/timestamps.
    """
    value = (value or fallback).strip()
    if not value:
        value = fallback
    out: list[str] = []
    for char in value:
        if INVALID_COMPONENT.fullmatch(char) or ord(char) < 32:
            out.append(f"%{ord(char):02X}")
        else:
            out.append(char)
    result = "".join(out).rstrip(" .")
    return result or fallback


def media_filename(original_url: str, extension: str = "") -> str:
    parsed = urlsplit(original_url)
    # Keep the raw URL spelling (including percent escapes) rather than unquoting
    # it, so photo%20one.jpg remains photo%20one.jpg on disk.
    raw_name = (parsed.path or "").rsplit("/", 1)[-1]
    fallback = "media" + (extension or "")
    return _platform_filename(raw_name, fallback)


def media_path(root: Path, row: sqlite3.Row, preserve_paths: bool = False) -> Path:
    del preserve_paths  # v1.0.5 media layout is deliberately fixed and flat.
    folder = "images" if str(row["media_kind"]).casefold() == "image" else "videos"
    filename = media_filename(str(row["original_url"]), str(row["extension"] or ""))
    return root / "media" / folder / filename


def media_replay_url(row: sqlite3.Row) -> str:
    modifier = "oe_" if str(row["extension"] or "").casefold() == ".swf" else "if_"
    return replay_url(str(row["timestamp"]), str(row["original_url"]), modifier=modifier)


def _hash_existing(path: Path) -> tuple[int, str, str]:
    """Verify an existing file in one pass; used only when it already exists."""
    sha256 = hashlib.sha256()
    sha1 = hashlib.sha1()
    size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            size += len(chunk)
            sha256.update(chunk)
            sha1.update(chunk)
    return size, sha256.hexdigest(), base64.b32encode(sha1.digest()).decode("ascii").rstrip("=")


def _media_row_value(row: sqlite3.Row | dict, name: str, default=None):
    try:
        value = row[name]
    except (IndexError, KeyError):
        return default
    return default if value is None else value


def _matching_existing_media(path: Path, row: sqlite3.Row | dict) -> tuple[int, str] | None:
    """Never claim a different capture as saved merely because its filename matches."""
    if not path.is_file():
        return None
    size, sha256, sha1_b32 = _hash_existing(path)
    verified_sha256 = str(_media_row_value(row, "content_hash", "")).lower()
    saved_size = int(_media_row_value(row, "bytes_saved", 0) or 0)
    if len(verified_sha256) == 64 and saved_size == size and sha256 == verified_sha256:
        return size, sha256
    cdx_digest = str(_media_row_value(row, "digest", "")).upper().rstrip("=")
    if len(cdx_digest) == 32 and cdx_digest == sha1_b32:
        return size, sha256
    return None


def _disambiguate_media_path(path: Path, row: sqlite3.Row | dict) -> Path:
    """Only rare basename collisions get a stable suffix; keep media folders flat."""
    identity = f'{row["original_url"]}\0{row["timestamp"]}'.encode("utf-8", "surrogatepass")
    token = hashlib.sha256(identity).hexdigest()[:16]
    return path.with_name(f"{path.stem}__{token}{path.suffix}")


def _known_binary_prefix(prefix: bytes) -> bool:
    """Positive binary evidence, never inspect arbitrary binary metadata as text."""
    return (
        prefix.startswith((b"\xff\xd8\xff", b"\x89PNG\r\n\x1a\n", b"GIF87a", b"GIF89a", b"FLV", b"ID3", b"\x1a\x45\xdf\xa3", b"OggS", b"fLaC", b"%PDF-", b"\x00\x00\x01\xba"))
        or (len(prefix) >= 12 and prefix[4:8] == b"ftyp")
        or (len(prefix) >= 12 and prefix[:4] == b"RIFF" and prefix[8:12] in (b"AVI ", b"WAVE", b"WEBP"))
        or (len(prefix) >= 8 and prefix[4:8] in (b"moov", b"mdat", b"wide", b"free"))
        or (len(prefix) >= 2 and prefix[0] == 0xff and (prefix[1] & 0xe0) == 0xe0)
    )


def _validate_media_representation(response: dict, *, path_hint: str = "") -> None:
    """Reject demonstrably non-media and incomplete transfers in constant memory.

    Unknown containers remain eligible for their configured formats; this is
    representation integrity, not a promise of universal codec playability.
    """
    headers = {str(k).casefold(): str(v).strip() for k, v in response.get("headers", {}).items()}
    size = int(response.get("bytes") or 0)
    prefix = bytes(response.get("preview") or b"")[:8192]
    status = int(response.get("status") or 0)
    if status not in (200, 206):
        raise RuntimeError("invalid_wayback_replay: media response is not successful")
    if size <= 0:
        raise RuntimeError("incomplete_media_length: empty media representation")
    content_range = headers.get("content-range", "")
    if status == 206:
        matched = re.fullmatch(r"bytes\s+(\d+)-(\d+)/(\d+)", content_range)
        if not matched:
            raise RuntimeError("incomplete_media_range: partial response has no verifiable complete range")
        start, end, length = map(int, matched.groups())
        if start != 0 or end + 1 != length or size != length:
            raise RuntimeError("incomplete_media_range: only part of the requested media was received")
    elif content_range:
        # A complete 200 response must not contradict a partial range header.
        raise RuntimeError("invalid_media_range: conflicting complete response headers")
    encoding = headers.get("content-encoding", "identity").casefold()
    declared = headers.get("content-length", "")
    if declared.isdigit() and encoding in ("", "identity") and int(declared) != size:
        raise RuntimeError("incomplete_media_length: received bytes differ from Content-Length")
    if not _known_binary_prefix(prefix):
        stripped = prefix.lstrip(b"\xef\xbb\xbf\t\r\n ").lower()
        # Recognized playlist/metafile sources are legitimate media assets,
        # even when a historical response labels them text/html. Preserving
        # the descriptor does not certify any streams it references.
        from ..classification import MEDIA_DESCRIPTOR_EXTENSIONS, path_extension
        suffix = path_extension(path_hint)
        if suffix in MEDIA_DESCRIPTOR_EXTENSIONS:
            # An XML declaration by itself is not evidence of a playable
            # descriptor; Wayback error responses can also be XML documents.
            source = re.sub(rb"^<\?xml[^>]*>\s*", b"", stripped[:2048])
            root_tag = re.match(rb"<([a-z][a-z0-9:_-]*)(?:\s|>)", source)
            root = root_tag.group(1).split(b":")[-1] if root_tag else b""
            markup_descriptors = {
                ".asx": {b"asx"}, ".wax": {b"asx"},
                ".wvx": {b"asx"}, ".wmx": {b"asx"},
                ".wpl": {b"smil", b"playlist"},
                ".smil": {b"smil"}, ".smi": {b"smil"},
                ".mpd": {b"mpd"},
            }
            valid_descriptor = root in markup_descriptors.get(suffix, set())
            valid_descriptor = valid_descriptor or (
                suffix in {".m3u", ".m3u8"} and stripped.startswith(b"#extm3u")
            ) or (suffix == ".pls" and stripped.startswith(b"[playlist]"))
            valid_descriptor = valid_descriptor or (suffix == ".vtt" and stripped.startswith(b"webvtt"))
            if valid_descriptor and b"<html" not in stripped[:1024] and b"<title>wayback machine" not in stripped[:1024]:
                return
        if (stripped.startswith((b"<!doctype html", b"<html", b"<?xml"))
                or b"<html" in stripped[:512]
                or b"<title>wayback machine" in stripped[:1024]):
            raise RuntimeError("invalid_wayback_replay: HTML/service document instead of media payload")
        if "text/html" in headers.get("content-type", "").casefold() and stripped.startswith(b"<"):
            raise RuntimeError("invalid_wayback_replay: HTML document instead of media payload")


def validate_media_representation(response: dict, *, path_hint: str = '') -> None:
    try:
        _validate_media_representation(response,path_hint=path_hint)
    except RuntimeError as exc:
        from ..network.transports import response_diagnostic
        exc.status = int(response.get('status') or 0) or None
        exc.representation_diagnostic = {**response_diagnostic(exc.status or 0,response.get('headers',{}),''),
            'decoded_bytes_received':int(response.get('bytes') or 0),
            'received_sha256':str(response.get('content_hash') or ''),
            'decoded_prefix_base64':base64.b64encode(bytes(response.get('preview') or b'')[:4096]).decode('ascii')}
        raise


def fetch_media(row: sqlite3.Row, config: ProjectConfig, client: HttpClient,
                destination: Path | None = None) -> dict:
    path = Path(destination) if destination is not None else media_path(config.output_dir, row)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Existing files may belong to a different URL or archived timestamp.
    # The CDX SHA-1 digest (or a previously committed per-capture SHA-256) is
    # required for a zero-request reuse; never use a basename alone as proof.
    if path.is_file():
        verified = _matching_existing_media(path, row)
        if verified is None:
            path = _disambiguate_media_path(path, row)
            if path.is_file():
                verified = _matching_existing_media(path, row)
                if verified is None:
                    raise FileExistsError(f"media filename collision: {path}; existing contents unverified")
        if verified is not None:
            size, digest = verified
            return {
                "id": int(row["id"]), "path": path, "bytes": size,
                "hash": digest, "status": 200, "final_url": media_replay_url(row),
            }
    temp = path.with_name(path.name + ".part")
    if isinstance(client, HttpClient):
        try:
            response = client.download_to_path(
                media_replay_url(row), temp, config.media.max_file_bytes,
                expected_digest=str(_media_row_value(row, "digest", "")),
                redirect_validator=make_replay_redirect_validator(config, str(row["original_url"])),
            )
        except TypeError as exc:
            # Compatibility with test/third-party clients that keep the old API.
            # Never retry arbitrary TypeErrors from inside a real network call.
            if "unexpected keyword argument" not in str(exc) or "expected_digest" not in str(exc):
                raise
            response = client.download_to_path(
                media_replay_url(row), temp, config.media.max_file_bytes,
            )
    else:
        response = client.download_to_path(
            media_replay_url(row), temp, config.media.max_file_bytes,
        )
    content_type = (
        response["headers"].get("content-type")
        or response["headers"].get("Content-Type")
        or ""
    ).casefold()
    try:
        validate_media_representation(response, path_hint=str(row["original_url"]))
    except Exception:
        # A rejected response is never published over previously verified bytes.
        # The error remains a recoverable capture obligation in the database.
        temp.unlink(missing_ok=True)
        raise
    # Match text acquisition's publication order: bytes reach storage before
    # the filename and database claim a successfully retained media payload.
    with temp.open('rb+') as handle:
        os.fsync(handle.fileno())
    os.replace(temp, path)
    sync_directory(path.parent)
    return {
        "id": int(row["id"]),
        "path": path,
        "bytes": int(response["bytes"]),
        "hash": str(response["content_hash"]),
        "status": response["status"],
        "final_url": response["final_url"],
    }


def iter_media_download_rows(
    database: sqlite3.Connection,
    clauses: list[str],
    params: list[object],
    batch_size: int = 1000,
):
    """Freeze one disk-backed ordered queue; never sort shrinking history again.

    The persistent index has download_attempts before length, and cannot serve
    the old COALESCE(length) ordering. Repeating that sort every 1,000 captures
    becomes quadratic over a large run. TEMP IDs stay on disk, while Python
    holds at most one batch and the bounded in-flight workers.
    """
    where = " AND ".join(clauses)
    database.execute("DROP TABLE IF EXISTS temp.archive_scout_media_download_queue")
    database.execute("""CREATE TEMP TABLE archive_scout_media_download_queue(
        id INTEGER PRIMARY KEY,priority INTEGER NOT NULL,length INTEGER NOT NULL) WITHOUT ROWID""")
    database.execute("CREATE INDEX archive_scout_media_download_order ON archive_scout_media_download_queue(priority,length,id)")
    selected = any('archive_scout_media_selection' in clause for clause in clauses)
    source = ("archive_scout_media_selection selected CROSS JOIN media_captures ON media_captures.id=selected.id"
              if selected else "media_captures")
    database.execute("""INSERT INTO archive_scout_media_download_queue
        SELECT media_captures.id,CASE WHEN COALESCE(media_captures.length,0)>0 THEN 0 ELSE 1 END,
               MAX(COALESCE(media_captures.length,0),0) FROM """ + source + " WHERE " + where, params)
    total = int(database.execute("SELECT COUNT(*) FROM archive_scout_media_download_queue").fetchone()[0])

    def rows():
        last_priority = -1
        last_length = -1
        last_id = 0
        while True:
            batch = database.execute(
                """SELECT mc.* FROM archive_scout_media_download_queue q
                   CROSS JOIN media_captures mc ON mc.id=q.id
                   WHERE (q.priority,q.length,q.id)>(?,?,?)
                   ORDER BY q.priority,q.length,q.id LIMIT ?""",
                (last_priority, last_length, last_id, max(1, int(batch_size))),
            ).fetchall()
            if not batch:
                return
            for row in batch:
                last_length = max(0, int(row["length"] or 0))
                last_priority = int(last_length <= 0)
                last_id = int(row["id"])
                yield row

    return total, rows()


def download_media(
    config: ProjectConfig,
    database: sqlite3.Connection,
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None = None,
    states: tuple[str, ...] = ("pending",),
    media_capture_ids: list[int] | None = None,
    media_signature: str | None = None,
    allow_partial_inventory: bool = False,
) -> None:
    from ..coverage import IndexCoveragePending, require_media_coverage, require_embedded_coverage, media_coverage_gaps
    direct_signature = media_query_signature(config)
    active_signature = media_signature or direct_signature
    is_direct = active_signature == direct_signature
    if is_direct:
        scope_config = config
    else:
        from dataclasses import replace
        scope_config = config.normalized()
        scope_config.media = replace(scope_config.media.normalized(), enabled=True,
                                    discover_embedded=True, allow_external_embeds=True)
        if active_signature != media_query_signature(scope_config):
            raise IndexCoveragePending("Unrecognized media inventory identity; acquisition is not admitted")
    inventory_complete = False
    try:
        if is_direct:
            require_media_coverage(database, config)
        require_embedded_coverage(database, scope_config, active_signature)
        inventory_complete = True
    except IndexCoveragePending:
        if not allow_partial_inventory:
            raise
    from .indexer import _apply_snapshot_strategy
    if inventory_complete:
        _apply_snapshot_strategy(database, active_signature, config.media.snapshot_strategy)
    clauses: list[str] = []
    params: list[object] = []
    if not inventory_complete and config.media.snapshot_strategy != 'all':
        direct_complete = is_direct and not media_coverage_gaps(database, config)
        if not direct_complete:
            clauses.append("""(media_captures.source_type IN ('embedded','external_embedded') OR EXISTS (SELECT 1 FROM media_discovery_queue q
                WHERE q.query_signature=media_captures.query_signature
                  AND q.original_url=media_captures.original_url AND q.state='indexed'))""")
        comparison = '<' if config.media.snapshot_strategy == 'earliest' else '>'
        clauses.append(f"""NOT EXISTS (SELECT 1 FROM media_captures chosen
            WHERE chosen.original_url=media_captures.original_url
              AND chosen.query_signature=media_captures.query_signature
              AND (chosen.timestamp {comparison} media_captures.timestamp
                   OR (chosen.timestamp=media_captures.timestamp AND chosen.id {comparison} media_captures.id)))""")
    database.execute("DROP TABLE IF EXISTS temp.archive_scout_media_selection")
    if media_capture_ids:
        database.execute(
            "CREATE TEMP TABLE archive_scout_media_selection(id INTEGER PRIMARY KEY) WITHOUT ROWID"
        )
        database.executemany(
            "INSERT OR IGNORE INTO archive_scout_media_selection(id) VALUES(?)",
            ((int(value),) for value in media_capture_ids),
        )
        clauses.append(
            "EXISTS (SELECT 1 FROM archive_scout_media_selection s WHERE s.id=media_captures.id)"
        )
        # Explicit retry IDs do not grant membership in a different inventory.
        clauses.append("query_signature=?")
        params.append(media_signature or media_query_signature(config))
    else:
        clauses.append("query_signature=?")
        params.append(media_signature or media_query_signature(config))
    if states:
        clauses.append("state IN (" + ",".join("?" for _ in states) + ")")
        params.extend(states)
    # Resolve MIME-vs-extension conflicts from proven text replay evidence
    # before opening media replay slots. Never skip a different timestamp.
    if 'pending' in states:
        with database:
            database.execute(
                """UPDATE media_captures SET state='skipped' WHERE query_signature=? AND state='pending'
               AND EXISTS (SELECT 1 FROM captures c JOIN capture_routing r ON r.capture_id=c.id
                 WHERE c.original_url=media_captures.original_url AND c.timestamp=media_captures.timestamp
                   AND r.resource_class='text' AND r.evidence='payload:validated_text')""",
                (media_signature or direct_signature,),
            )
    media_root = config.output_dir / "media"
    (media_root / "images").mkdir(parents=True, exist_ok=True)
    (media_root / "videos").mkdir(parents=True, exist_ok=True)
    with database:
        total, row_iter = iter_media_download_rows(database, clauses, params)
    if not total:
        if callback:
            callback(ProgressEvent("media_download", "No media captures to download.", 0, 0))
        return
    limiter = SharedFixedRateLimiter(config.download_delay)
    host_gate = shared_host_gate(config.rate_limit_base_pause, config.rate_limit_max_pause)

    def on_retry(attempt: int, total_attempts: int, reason: str, wait_seconds: float) -> None:
        if callback:
            rate_limited = "all Wayback requests paused" in reason
            stage = "rate_limit" if rate_limited else "media_retry"
            if rate_limited:
                limit = f"/{total_attempts}" if total_attempts else ""
                message = f"{reason}. Shared pause {attempt}{limit} for {wait_seconds:.1f}s; one recovery probe will run next…"
            else:
                message = f"{reason}. Retry {attempt}/{total_attempts} in {wait_seconds:.1f}s…"
            callback(ProgressEvent(stage, message))

    from ..downloads.session import acquisition_client
    diagnostics, _ = acquisition_client(('media_observations',id(database)), lambda: AcquisitionDiagnostics(config.output_dir, callback,
        enabled=config.network.diagnostics, kind='media'))
    diagnostics.begin_batch(callback)
    client, owns_client = acquisition_client(("media", id(database)), lambda: HttpClient(
        limiter,
        1 if config.network.persistent_retries else max(5, config.retries),
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
        network_callback=(lambda message: callback(ProgressEvent("network", message)) if callback else None),
        coordinated_connection_recovery=config.network.coordinated_connection_recovery,
        connection_failure_threshold=config.network.connection_failure_pause_threshold,
        connection_retry_seconds=config.network.connection_retry_seconds,
        response_callback=diagnostics.response,
    ))
    client.response_callback = diagnostics.response
    complete = errors = saved = 0
    recent_saves = deque()
    started = time.monotonic()
    max_inflight = max(config.workers, config.workers * 2)
    blocked_hosts = blocked_site_reasons(database)

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=config.workers, thread_name_prefix="archive-media") as pool:
            futures: dict[concurrent.futures.Future, tuple[sqlite3.Row, Path]] = {}
            # Only currently in-flight paths need reserving. Never hold a set
            # proportional to the entire media inventory in memory.
            in_flight_paths: set[Path] = set()

            def submit_available() -> None:
                nonlocal complete, errors
                slots = max_inflight - len(futures)
                if slots <= 0:
                    return
                rows: list[sqlite3.Row] = []
                while len(rows) < slots:
                    try:
                        row = next(row_iter)
                    except StopIteration:
                        break
                    if stop_event.is_set():
                        raise Stopped
                    host = host_from_url(str(row["original_url"] or ""))
                    blocked_reason = blocked_hosts.get(host)
                    if blocked_reason:
                        message = site_issue_message(
                            blocked_reason, str(row["original_url"]), "media download"
                        )
                        with database:
                            database.execute(
                                "UPDATE media_captures SET state='error',updated_at=? WHERE id=?",
                                (utc_now(), int(row["id"])),
                            )
                            record_error(
                                database, "media_download", blocked_reason, message,
                                media_capture_id=int(row["id"]), retryable=False,
                            )
                        complete += 1
                        errors += 1
                        if callback:
                            callback(ProgressEvent("site_issue", message))
                        continue
                    rows.append(row)
                if not rows:
                    return
                now = utc_now()
                with database:
                    database.executemany(
                        "UPDATE media_captures SET state='downloading',download_attempts=download_attempts+1,updated_at=? WHERE id=?",
                        ((now, int(row["id"])) for row in rows),
                    )
                for row in rows:
                    destination = media_path(config.output_dir, row)
                    if destination in in_flight_paths:
                        destination = _disambiguate_media_path(destination, row)
                    if destination in in_flight_paths:
                        raise RuntimeError(f"media path reservation conflict: {destination}")
                    in_flight_paths.add(destination)
                    futures[pool.submit(fetch_media, row, config, client, destination)] = (row, destination)
    
            submit_available()
    
            while futures:
                if stop_event.is_set():
                    for pending in futures:
                        pending.cancel()
                    raise Stopped
                done, _ = concurrent.futures.wait(futures, timeout=5.0, return_when=concurrent.futures.FIRST_COMPLETED)
                if not done:
                    diagnostics.summary(saved)
                    continue
                for future in done:
                    row, destination = futures.pop(future)
                    in_flight_paths.discard(destination)
                    try:
                        result = future.result()
                        with database:
                            save_media_success(
                                database, result["id"], result["path"], result["bytes"], result["hash"], result["status"], result["final_url"]
                            )
                            clear_media_retry(database, int(result["id"]))
                        saved += 1
                        recent_saves.append(time.monotonic())
                    except RateLimitDeferred as exc:
                        # Remote pause exhaustion belongs to the media retry
                        # inventory, never to the user's global Stop event.
                        errors += 1
                        diagnostics.failure(row, 'rate_limit', exc)
                        with database:
                            database.execute("UPDATE media_captures SET state='error',updated_at=? WHERE id=?",
                                (utc_now(), row["id"]))
                            record_error(database, "media_download", "rate_limit", repr(exc),
                                media_capture_id=int(row["id"]), http_status=getattr(exc,"status",429),retryable=True)
                            record_media_failure(database, int(row["id"]), "rate_limit", getattr(exc,"status",429), True, config)
                        if callback:
                            callback(ProgressEvent("rate_limit",
                                "Media capture was rate-limited; retained for later retry without stopping local work."))
                    except Stopped:
                        with database:
                            database.execute("UPDATE media_captures SET state='pending',updated_at=? WHERE id=?", (utc_now(), row["id"]))
                        raise
                    except Exception as exc:
                        if _storage_full(exc):
                            for pending in futures:
                                pending.cancel()
                            raise StorageFullError("storage full while retaining or committing media; progress preserved") from exc
                        errors += 1
                        category, status, retryable = classify_exception(exc)
                        diagnostics.failure(row, category, exc)
                        issue_message = site_issue_message(
                            category, str(row["original_url"]), "media download", status
                        )
                        with database:
                            database.execute(
                                "UPDATE media_captures SET state='error',http_status=?,updated_at=? WHERE id=?",
                                (status, utc_now(), row["id"]),
                            )
                            record_error(
                                database, "media_download", category, repr(exc), media_capture_id=int(row["id"]),
                                http_status=status, retryable=retryable
                            )
                            record_media_failure(database, int(row["id"]), category, status, retryable, config, exception=exc)
                            if should_surface_site_issue(category):
                                record_site_issue(
                                    database,
                                    host_from_url(str(row["original_url"])),
                                    "media_download",
                                    category,
                                    issue_message,
                                    target=str(row["original_url"]),
                                    http_status=status,
                                )
                        if category in {"wayback_excluded", "robots_blocked"}:
                            blocked_hosts[host_from_url(str(row["original_url"]))] = category
                        if callback and should_surface_site_issue(category):
                            callback(ProgressEvent("site_issue", issue_message))
                    complete += 1
                    elapsed = max(0.001, time.monotonic() - started)
                    now = time.monotonic()
                    while recent_saves and recent_saves[0] <= now - 30.0:
                        recent_saves.popleft()
                    rate = len(recent_saves) / min(30.0, elapsed)
                    diagnostics.summary(saved)
                    if callback:
                        callback(ProgressEvent(
                            "media_download",
                            f"Media outcomes {complete:,}/{total:,}; saved {saved:,}; errors {errors:,}; "
                            f"recent successful saves {rate:.1f}/s (30s)",
                            complete, total,
                            {"errors": errors, "new_saves": saved, "rate": rate},
                        ))
                    submit_available()
    
    
    finally:
        if owns_client:
            client.close()
        diagnostics.summary(saved, force=True)

def retry_media_errors(
    config: ProjectConfig,
    database: sqlite3.Connection,
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None = None,
    media_capture_ids: list[int] | None = None,
    allow_partial_inventory: bool = False,
) -> None:
    """Explicit bounded retry of recorded errors AND over-attempt pending work.

    No unbounded Python ID list, no automatic sweep loop, and no changes to
    normal acquisition eligibility. Retry pages are selected by stable ID.
    """
    selected = media_capture_ids if media_capture_ids is not None else config.retry_media_capture_ids
    database.execute("DROP TABLE IF EXISTS temp.archive_scout_media_retry_selection")
    selection_sql = ""
    if selected:
        database.execute(
            "CREATE TEMP TABLE archive_scout_media_retry_selection(id INTEGER PRIMARY KEY) WITHOUT ROWID"
        )
        database.executemany(
            "INSERT OR IGNORE INTO archive_scout_media_retry_selection(id) VALUES(?)",
            ((int(value),) for value in selected),
        )
        selection_sql = " AND EXISTS (SELECT 1 FROM archive_scout_media_retry_selection s WHERE s.id=mc.id)"
    last_id = 0
    active_signature = media_query_signature(config)
    error_predicate = ("(e.retryable=1 OR e.category IN ('missing_capture','oversized_response'))"
                       if selected or {'missing_capture','oversized_response'} & set(config.retry_error_categories)
                       else "e.retryable=1")
    category_sql = ""
    if config.retry_error_categories:
        category_sql = " AND e.category IN (" + ",".join("?" for _ in config.retry_error_categories) + ")"
    deliberate = bool(selected or {'missing_capture','oversized_response'} & set(config.retry_error_categories))
    latest_decision = (" AND NOT EXISTS(SELECT 1 FROM media_retry_decisions latest WHERE latest.media_capture_id=mc.id "
                       "AND latest.retryable=0" +
                       (" AND latest.category NOT IN ('missing_capture','oversized_response')" if deliberate else '') + ")")
    pending_clause = (" OR (mc.state='pending' AND mc.download_attempts>=?)"
                      if not config.retry_error_categories else "")
    while True:
        if stop_event.is_set():
            raise Stopped
        rows = database.execute(
            """SELECT mc.id FROM media_captures mc WHERE mc.query_signature=? AND mc.id>? AND
                ((mc.state='error' AND EXISTS (
                    SELECT 1 FROM errors e WHERE e.media_capture_id=mc.id
                    AND e.resolved=0 AND e.ignored=0 AND """ + error_predicate + category_sql + "))"
            + pending_clause + ")"
            + latest_decision + selection_sql + " ORDER BY mc.id LIMIT 1000",
            (active_signature, last_id, *config.retry_error_categories,
             *([config.max_attempts] if pending_clause else [])),
        ).fetchall()
        if not rows:
            break
        ids = [int(row[0]) for row in rows]
        last_id = ids[-1]
        if callback:
            callback(ProgressEvent("media_retry", f"Retrying {len(ids):,} errored or attempt-exhausted media captures"))
        download_media(config, database, stop_event, callback,
                       states=("error", "pending"), media_capture_ids=ids, allow_partial_inventory=allow_partial_inventory)
