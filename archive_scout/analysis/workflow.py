from __future__ import annotations

import concurrent.futures
import hashlib
import json
import sqlite3
import threading
import urllib.parse
from pathlib import Path
from typing import Callable

from ..cdx.client import HttpClient, RateLimitDeferred, TransientRequestError
from ..cdx.parameters import parse_cdx
from ..config import ProjectConfig
from ..content import decode_bytes
from ..constants import CDX_URL
from ..cdx.parameters import cdx_endpoints
from ..database.repositories import upsert_media_capture
from ..downloads.rate_limit import (SharedFixedRateLimiter, WAYBACK_INDEX_RATE_KEY, shared_host_gate)
from ..document_store import document_body
from ..events import ConnectivityPaused, ProgressEvent, Stopped
from ..extraction.provenance import trace_provenance
from ..extraction.regex import parse_extractor_rules, run_extractors
from ..media.extensions import extension_from_url, media_kind
from ..parsing.embeds import extract_embed_candidates
from ..parsing.forums import parse_forum_posts
from ..utils import atomic_write_lines, atomic_write_text, hash_text, json_value, utc_now
from .diffs import build_first_appearances, compare_snapshots
from .duplicates import cluster_duplicates


def _emit(callback: Callable[[ProgressEvent], None] | None, stage: str, message: str, completed: int = 0, total: int = 0) -> None:
    if callback:
        callback(ProgressEvent(stage, message, completed, total))


def _read_source(path_value: str) -> str:
    path = Path(path_value)
    try:
        return decode_bytes(path.read_bytes())
    except Exception:
        return ""


def _target_hosts(config: ProjectConfig) -> set[str]:
    hosts: set[str] = set()
    for target in config.targets:
        raw = str(target or "").strip().rstrip("*")
        if not raw:
            continue
        parsed = urllib.parse.urlsplit(raw if "://" in raw else "https://" + raw.lstrip("/"))
        host = (parsed.hostname or "").casefold().strip(".")
        if host:
            hosts.add(host)
    return hosts


def _external_allowed(host: str, allowed: list[str]) -> bool:
    host = host.casefold().strip(".")
    for domain in allowed:
        domain = domain.casefold().strip(".")
        if host == domain or host.endswith("." + domain):
            return True
    return False


def _exact_asset_params(config: ProjectConfig, url: str) -> list[tuple[str, str]]:
    params = [
        ("url", url),
        ("from", config.from_date),
        ("to", config.to_date),
        ("output", "json"),
        ("fl", "timestamp,original,mimetype,statuscode,digest,length"),
        ("matchType", "exact"),
        ("filter", "statuscode:200"),
        ("collapse", "digest"),
        ("limit", "1"),
    ]
    return params


def _lookup_external_assets(
    config: ProjectConfig,
    database: sqlite3.Connection,
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None,
) -> int:
    analysis = config.analysis.normalized()
    if not analysis.search_external_assets or not analysis.external_domains:
        return 0
    total = min(
        int(analysis.external_asset_limit),
        int(database.execute(
            """SELECT COUNT(*) FROM legacy_assets
               WHERE external=1 AND archive_status IN ('discovered','retry')"""
        ).fetchone()[0]),
    )
    if not total:
        return 0
    rows = database.execute(
        """SELECT la.id,la.document_id,la.original_url
           FROM legacy_assets la
           WHERE la.external=1 AND la.archive_status IN ('discovered','retry')
           ORDER BY la.id LIMIT ?""",
        (total,),
    )
    limiter = SharedFixedRateLimiter(config.cdx_delay, key=WAYBACK_INDEX_RATE_KEY)
    host_gate = shared_host_gate(config.rate_limit_base_pause, config.rate_limit_max_pause)
    workers = min(8, max(1, config.network.normalized().cdx_workers))

    def retry_callback(attempt: int, total_attempts: int, reason: str, wait: float) -> None:
        _emit(callback, "asset_search", f"External asset request delayed ({reason}); retry {attempt}/{total_attempts or '∞'} in {wait:.1f}s")

    client = HttpClient(
        limiter, min(config.retries, 2), min(max(config.read_timeout, 15.0), 60.0),
        config.user_agent, stop_event, retry_callback=retry_callback,
        connect_timeout=min(max(config.connect_timeout, 5.0), 30.0),
        read_timeout=min(max(config.read_timeout, 15.0), 60.0), pool_size=workers,
        host_gate=host_gate, rate_limit_attempts=0 if config.network.persistent_retries else config.rate_limit_attempts,
        rate_limit_max_wait=config.rate_limit_max_wait,
        persistent_retries=config.network.persistent_retries,
        network_backend=config.network.normalized().backend,
        trust_environment=config.network.normalized().trust_environment,
        network_callback=(lambda message: callback(ProgressEvent("network", message)) if callback else None),
    )

    def lookup(row: sqlite3.Row):
        try:
            payload = client.get_json_any(cdx_endpoints(config), _exact_asset_params(config, str(row["original_url"])))
            captures, _ = parse_cdx(payload)
            return row, captures, None
        except BaseException as exc:
            return row, [], exc

    found = completed = 0
    batch_size = max(32, workers * 8)
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers, thread_name_prefix="archive-assets") as pool:
            while completed < total:
                batch = rows.fetchmany(batch_size)
                if not batch:
                    break
                eligible: list[sqlite3.Row] = []
                blocked: list[int] = []
                for row in batch:
                    if stop_event.is_set():
                        raise Stopped
                    url = str(row["original_url"])
                    host = (urllib.parse.urlsplit(url).hostname or "").casefold()
                    if _external_allowed(host, analysis.external_domains):
                        eligible.append(row)
                    else:
                        blocked.append(int(row["id"]))
                if blocked:
                    now = utc_now()
                    with database:
                        database.executemany(
                            "UPDATE legacy_assets SET archive_status='blocked_domain',updated_at=? WHERE id=?",
                            ((now, row_id) for row_id in blocked),
                        )
                    completed += len(blocked)
                futures = [pool.submit(lookup, row) for row in eligible]
                results = []
                for future in concurrent.futures.as_completed(futures):
                    if stop_event.is_set():
                        for pending in futures:
                            pending.cancel()
                        raise Stopped
                    results.append(future.result())
                    completed += 1
                    if callback and (completed == total or completed % 25 == 0):
                        _emit(callback, "asset_search", f"Searched external assets {completed:,}/{total:,}; found {found:,}", completed, total)
                if results:
                    now = utc_now()
                    shared_pause: BaseException | None = None
                    with database:
                        for row, captures, error in results:
                            if error is not None:
                                if isinstance(error, (RateLimitDeferred, ConnectivityPaused)):
                                    database.execute(
                                        "UPDATE legacy_assets SET archive_status='retry',context=?,updated_at=? WHERE id=?",
                                        (f"{type(error).__name__}: {error}", now, row["id"]),
                                    )
                                    shared_pause = shared_pause or error
                                    continue
                                if isinstance(error, TransientRequestError):
                                    database.execute(
                                        "UPDATE legacy_assets SET archive_status='retry',context=?,updated_at=? WHERE id=?",
                                        (f"{type(error).__name__}: {error}", now, row["id"]),
                                    )
                                    continue
                                raise error
                            if not captures:
                                database.execute(
                                    "UPDATE legacy_assets SET archive_status='not_found',updated_at=? WHERE id=?",
                                    (now, row["id"]),
                                )
                                continue
                            chosen = captures[0]
                            extension = extension_from_url(chosen["original"])
                            kind = media_kind(extension, chosen.get("mimetype", "")) or "asset"
                            url = str(row["original_url"])
                            signature = "external-asset:" + hashlib.sha256(url.encode("utf-8", "replace")).hexdigest()[:20]
                            upsert_media_capture(database, chosen, None, signature, kind, extension, int(row["document_id"]), "external_asset")
                            media_row = database.execute(
                                "SELECT id FROM media_captures WHERE original_url=? AND timestamp=? AND query_signature=?",
                                (chosen["original"], chosen["timestamp"], signature),
                            ).fetchone()
                            database.execute(
                                "UPDATE legacy_assets SET archive_status='found',media_capture_id=?,updated_at=? WHERE id=?",
                                (media_row["id"] if media_row else None, now, row["id"]),
                            )
                            found += 1
                    if shared_pause is not None:
                        # Service-wide recovery belongs to the operation coordinator,
                        # not one retry row per external asset.
                        raise shared_pause
    finally:
        client.close()
    return found



def _write_reports(config: ProjectConfig, database: sqlite3.Connection, summary: dict) -> dict[str, Path]:
    report = config.report.normalized()
    folder = config.output_dir / "reports" / "analysis"
    folder.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}

    summary_path = folder / "analysis_summary.txt"
    if report.output_enabled("analysis_summary"):
        fields = report.fields_for("analysis_summary")
        lines: list[str] = []
        for field in fields:
            if field == "heading":
                lines.append("Archive Scout archive analysis")
            elif field in summary:
                lines.append(f"{field}: {summary[field]}")
        atomic_write_text(summary_path, "\n".join(lines) + ("\n" if lines else ""))
        paths["analysis_summary"] = summary_path
    else:
        summary_path.unlink(missing_ok=True)

    specs = {
        "forum_threads": (
            "forum_threads.tsv",
            ("canonical_key", "canonical_url", "title", "profile", "first_timestamp", "last_timestamp", "post_count", "document_count"),
            "SELECT canonical_key,canonical_url,title,profile,first_timestamp,last_timestamp,post_count,document_count FROM forum_threads ORDER BY first_timestamp,canonical_key",
        ),
        "extractions": (
            "extractions.tsv",
            ("document_id", "extractor", "type", "field", "value", "context"),
            "SELECT document_id,extractor_name AS extractor,extractor_type AS type,field,value,context FROM extractions ORDER BY extractor_name,value,document_id",
        ),
        "legacy_assets": (
            "legacy_assets.tsv",
            ("document_id", "url", "type", "player", "external", "archive_status", "context"),
            "SELECT document_id,original_url AS url,asset_type AS type,player,external,archive_status,context FROM legacy_assets ORDER BY original_url,document_id",
        ),
        "duplicate_groups": (
            "duplicate_groups.tsv",
            ("group_id", "method", "representative_document_id", "document_id", "similarity"),
            "SELECT dg.id AS group_id,dg.method,dg.representative_document_id,dm.document_id,dm.similarity FROM duplicate_groups dg JOIN duplicate_members dm ON dm.group_id=dg.id ORDER BY dg.id,dm.similarity DESC",
        ),
        "provenance": (
            "provenance.tsv",
            ("source_url", "source_timestamp", "mirror_url", "mirror_timestamp", "method", "similarity"),
            "SELECT cs.original_url AS source_url,pe.source_timestamp,cm.original_url AS mirror_url,pe.mirror_timestamp,pe.method,pe.similarity FROM provenance_edges pe JOIN documents ds ON ds.id=pe.source_document_id JOIN captures cs ON cs.id=ds.capture_id JOIN documents dm ON dm.id=pe.mirror_document_id JOIN captures cm ON cm.id=dm.capture_id ORDER BY pe.source_timestamp,pe.mirror_timestamp",
        ),
        "snapshot_diffs": (
            "snapshot_diffs.tsv",
            ("earlier_url", "earlier_timestamp", "later_timestamp", "summary_json"),
            "SELECT ce.original_url AS earlier_url,ce.timestamp AS earlier_timestamp,cl.timestamp AS later_timestamp,sd.summary_json FROM snapshot_diffs sd JOIN captures ce ON ce.id=sd.earlier_capture_id JOIN captures cl ON cl.id=sd.later_capture_id ORDER BY ce.original_url,ce.timestamp",
        ),
        "first_appearances": (
            "first_appearances.tsv",
            ("query", "original_url", "first_timestamp", "last_timestamp"),
            "SELECT query,original_url,first_timestamp,last_timestamp FROM first_appearances ORDER BY query,first_timestamp,original_url",
        ),
    }
    for output_name, (filename, _available_fields, query) in specs.items():
        path = folder / filename
        if not report.output_enabled(output_name):
            path.unlink(missing_ok=True)
            continue
        fields = report.fields_for(output_name)

        def report_lines(fields=fields, query=query):
            if not fields:
                return
            yield "\t".join(fields)
            for row in database.execute(query):
                values = [
                    str(row[field] if row[field] is not None else "")
                    .replace("\t", " ")
                    .replace("\r", " ")
                    .replace("\n", " ")
                    for field in fields
                ]
                yield "\t".join(values)

        atomic_write_lines(path, report_lines())
        paths[output_name] = path
    return paths


def run_analysis(
    config: ProjectConfig,
    database: sqlite3.Connection,
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None = None,
    forum_only: bool = False,
) -> dict[str, Path]:
    analysis = config.analysis.normalized()
    if stop_event.is_set():
        raise Stopped
    started = utc_now()
    cursor = database.execute(
        "INSERT INTO analysis_runs(status,started_at,metadata_json) VALUES('running',?,?)",
        (started, json.dumps(analysis.to_payload(), ensure_ascii=False, sort_keys=True)),
    )
    run_id = int(cursor.lastrowid)
    database.commit()
    summary: dict[str, int | float] = {
        "documents_processed": 0,
        "forum_threads": 0,
        "forum_posts": 0,
        "extractions": 0,
        "legacy_assets": 0,
        "external_assets_found": 0,
        "exact_duplicate_groups": 0,
        "near_duplicate_groups": 0,
        "grouped_documents": 0,
        "snapshot_pairs": 0,
        "changed_snapshot_pairs": 0,
        "first_appearances": 0,
        "provenance_edges": 0,
    }
    custom_rules = parse_extractor_rules(analysis.extractor_rules)
    hosts = _target_hosts(config)
    document_count = int(database.execute("SELECT COUNT(*) FROM documents").fetchone()[0])
    try:
        if stop_event.is_set():
            raise Stopped
        with database:
            database.execute("DELETE FROM forum_posts")
            database.execute("DELETE FROM forum_threads")
            if not forum_only:
                database.execute("DELETE FROM extractions")
                database.execute("DELETE FROM legacy_assets")
        rows = database.execute(
            """
            SELECT d.*,d.id AS document_id,c.id AS capture_id,c.original_url,c.timestamp,c.mimetype,c.detected_encoding
            FROM documents d JOIN captures c ON c.id=d.capture_id
            ORDER BY d.id
            """
        )
        thread_ids: dict[str, int] = {}
        pending_documents = 0
        for index, row in enumerate(rows, 1):
            if stop_event.is_set():
                if pending_documents:
                    database.commit()
                raise Stopped
            raw = _read_source(str(row["path"]))
            original_url = str(row["original_url"])
            now = utc_now()
            if index == 1 or index == document_count or index % 25 == 0:
                _emit(callback, "analysis", f"Analyzing document {index:,}/{document_count:,}", index, document_count)
            if analysis.reconstruct_threads:
                thread = parse_forum_posts(raw, original_url, analysis.forum_profile)
                if thread.posts:
                    database.execute(
                        """
                        INSERT INTO forum_threads(canonical_key,canonical_url,title,profile,first_timestamp,last_timestamp,post_count,document_count,created_at)
                        VALUES(?,?,?,?,?,?,0,0,?)
                        ON CONFLICT(canonical_key) DO UPDATE SET
                            canonical_url=COALESCE(excluded.canonical_url,forum_threads.canonical_url),
                            title=CASE WHEN LENGTH(excluded.title)>LENGTH(COALESCE(forum_threads.title,'')) THEN excluded.title ELSE forum_threads.title END,
                            profile=excluded.profile,
                            first_timestamp=MIN(COALESCE(forum_threads.first_timestamp,excluded.first_timestamp),excluded.first_timestamp),
                            last_timestamp=MAX(COALESCE(forum_threads.last_timestamp,excluded.last_timestamp),excluded.last_timestamp)
                        """,
                        (thread.canonical_key, thread.canonical_url, thread.title, thread.profile, row["timestamp"], row["timestamp"], now),
                    )
                    thread_id = thread_ids.get(thread.canonical_key)
                    if thread_id is None:
                        thread_id = int(database.execute(
                            "SELECT id FROM forum_threads WHERE canonical_key=?", (thread.canonical_key,)
                        ).fetchone()["id"])
                        thread_ids[thread.canonical_key] = thread_id
                    database.executemany(
                        """
                        INSERT OR IGNORE INTO forum_posts(
                            thread_id,document_id,capture_id,post_key,username,posted_at,position,body_text,body_hash,source_url
                        ) VALUES(?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            (
                                thread_id, row["document_id"], row["capture_id"], post.post_key, post.username,
                                post.posted_at, post.position, post.body_text, hash_text(post.body_text.casefold()), original_url,
                            )
                            for post in thread.posts
                        ),
                    )
            if not forum_only:
                links = json_value(row["links_json"], [])
                fields = {
                    "title": str(row["title"] or ""),
                    "body": document_body(row),
                    "url": original_url,
                    "source": raw,
                    "links": "\n".join(str(value) for value in links),
                }
                hits = run_extractors(fields, custom_rules)
                if hits:
                    database.executemany(
                        """
                        INSERT INTO extractions(document_id,extractor_name,extractor_type,field,value,context,start_offset,end_offset,created_at)
                        VALUES(?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            (
                                row["document_id"], hit.name, hit.extractor_type, hit.field, hit.value,
                                hit.context, hit.start, hit.end, now,
                            )
                            for hit in hits
                        ),
                    )
                if analysis.extract_legacy_embeds:
                    candidates = extract_embed_candidates(raw, original_url)
                    source_host = (urllib.parse.urlsplit(original_url).hostname or "").casefold()
                    asset_rows = []
                    for candidate in candidates:
                        candidate_host = (urllib.parse.urlsplit(candidate.url).hostname or "").casefold()
                        external = bool(candidate_host and candidate_host != source_host and candidate_host not in hosts)
                        asset_rows.append((
                            row["document_id"], candidate.url, candidate.url, candidate.asset_type,
                            candidate.player, int(external), candidate.context, now, now,
                        ))
                    if asset_rows:
                        database.executemany(
                            """
                            INSERT OR IGNORE INTO legacy_assets(
                                document_id,original_url,resolved_url,asset_type,player,external,archive_status,context,created_at,updated_at
                            ) VALUES(?,?,?,?,?,?, 'discovered',?,?,?)
                            """,
                            asset_rows,
                        )
            summary["documents_processed"] = int(summary["documents_processed"]) + 1
            pending_documents += 1
            if pending_documents >= 100:
                database.commit()
                pending_documents = 0
        if pending_documents:
            database.commit()

        with database:
            database.execute(
                """
                UPDATE forum_threads SET
                    post_count=(SELECT COUNT(*) FROM forum_posts fp WHERE fp.thread_id=forum_threads.id),
                    document_count=(SELECT COUNT(DISTINCT fp.document_id) FROM forum_posts fp WHERE fp.thread_id=forum_threads.id)
                """
            )
        summary["forum_threads"] = int(database.execute("SELECT COUNT(*) FROM forum_threads").fetchone()[0])
        summary["forum_posts"] = int(database.execute("SELECT COUNT(*) FROM forum_posts").fetchone()[0])
        if not forum_only:
            summary["extractions"] = int(database.execute("SELECT COUNT(*) FROM extractions").fetchone()[0])
            summary["legacy_assets"] = int(database.execute("SELECT COUNT(*) FROM legacy_assets").fetchone()[0])
            summary["external_assets_found"] = _lookup_external_assets(config, database, stop_event, callback)
            duplicate_summary = cluster_duplicates(database, analysis.duplicate_threshold, stop_event=stop_event, callback=callback)
            summary["exact_duplicate_groups"] = duplicate_summary.exact_groups
            summary["near_duplicate_groups"] = duplicate_summary.near_groups
            summary["grouped_documents"] = duplicate_summary.grouped_documents
            if analysis.compare_snapshots:
                diff_summary = compare_snapshots(database, stop_event=stop_event, callback=callback)
                summary["snapshot_pairs"] = diff_summary.compared_pairs
                summary["changed_snapshot_pairs"] = diff_summary.changed_pairs
                extracted_values = [str(row[0]) for row in database.execute("SELECT DISTINCT value FROM extractions WHERE LENGTH(value)>=3 LIMIT 5000")]
                summary["first_appearances"] = build_first_appearances(database, extracted_values)
            if analysis.build_provenance:
                summary["provenance_edges"] = trace_provenance(database)
        paths = _write_reports(config, database, summary)
        with database:
            database.execute(
                "UPDATE analysis_runs SET status='complete',completed_at=?,summary_json=? WHERE id=?",
                (utc_now(), json.dumps(summary, ensure_ascii=False, sort_keys=True), run_id),
            )
        _emit(callback, "analysis", f"Analysis complete: {summary['forum_threads']:,} threads, {summary['extractions']:,} extractions")
        return paths
    except Stopped:
        with database:
            database.execute("UPDATE analysis_runs SET status='interrupted',completed_at=? WHERE id=?", (utc_now(), run_id))
        raise
    except Exception as exc:
        with database:
            database.execute(
                "UPDATE analysis_runs SET status='failed',completed_at=?,summary_json=? WHERE id=?",
                (utc_now(), json.dumps({"error": f"{type(exc).__name__}: {exc}"}), run_id),
            )
        raise
