from __future__ import annotations

import json
import re
import sqlite3
import time
from dataclasses import fields
from pathlib import Path

from ..cdx.parameters import cdx_query_signatures
from ..classification import capture_body_coverage, capture_routing_decision
from ..config import ProjectConfig
from ..events import ProgressEvent


EMPTY_DASHBOARD = {
    # Project totals retained for backwards-compatible cards.
    "captures": 0,
    "documents": 0,
    "matches": 0,
    "errors": 0,
    "recovery_events": 0,
    "skipped_non_text": 0,
    "skipped_url_filter": 0,
    "skipped_other": 0,
    "pending": 0,
    "downloading": 0,
    "downloaded_unscanned": 0,
    "scanning": 0,
    "downloaded": 0,
    "failed_captures": 0,
    # Operation-scoped capture reconciliation.
    "operation_id": 0,
    "operation_total": 0,
    "operation_pending": 0,
    "operation_downloading": 0,
    "operation_saved_unscanned": 0,
    "operation_scanning": 0,
    "operation_scanned": 0,
    "operation_skipped": 0,
    "operation_failed": 0,
    "operation_unclassified_state": 0,
    "operation_class_text": 0,
    "operation_class_image": 0,
    "operation_class_video": 0,
    "operation_class_audio": 0,
    "operation_class_media_descriptor": 0,
    "operation_class_other_binary": 0,
    "operation_class_unknown": 0,
    "operation_downloaded": 0,
    "operation_deferred_media": 0,
    "operation_skipped_non_text": 0,
    "operation_skipped_url_filter": 0,
    "operation_skipped_other": 0,
    "operation_body_available": 0,
    "operation_body_url_only": 0,
    "operation_body_discarded": 0,
    "operation_body_partial": 0,
    "operation_body_non_text": 0,
    "latest_scan_documents": 0,
    "latest_scan_matches": 0,
    # Media pipeline totals.
    "media_candidates": 0,
    "media_selected": 0,
    "media_pending": 0,
    "media_downloading": 0,
    "media_downloaded": 0,
    "media_excluded": 0,
    "media_errors": 0,
    "media_deferred_from_text": 0,
    "media_selected_images": 0,
    "media_selected_videos": 0,
}


_STAGE_LABELS = {
    "starting": "Starting",
    "index": "Text indexing",
    "classification": "Resource classification",
    "download": "Text acquisition",
    "download_only": "Text acquisition",
    "scan": "Text scanning",
    "media_index": "Supplemental media indexing",
    "media_download": "Supplemental media download",
    "media_retry": "Supplemental media retry",
    "download_retry": "Text replay retry",
    "rate_limit": "Wayback rate-limit pause",
    "rate_limit_waiting": "Waiting for Internet Archive",
    "network": "Wayback network recovery",
    "network_waiting": "Waiting for Internet Archive",
    "report": "Report generation",
    "site_issue": "Wayback site issue",
}
_VAGUE_YEAR_PROGRESS = re.compile(r"^Indexing\s+\d+\s+year\(s\),\s*\d+\s+completed\.?$", re.IGNORECASE)


def format_progress_message(event: ProgressEvent) -> str:
    """Return a phase-explicit GUI status/activity message."""
    message = str(event.message or "").strip()
    label = _STAGE_LABELS.get(str(event.stage or ""), "")
    if str(event.stage or "") == "index" and _VAGUE_YEAR_PROGRESS.match(message):
        current = max(0, int(event.current or 0))
        total = max(current, int(event.total or 0))
        suffix = f" ({current:,}/{total:,} saved work units complete)" if total else ""
        return "Text indexing — processing the saved index plan" + suffix + "…"
    if not label or not message:
        return message or label
    lowered = message.casefold()
    if lowered.startswith(label.casefold()):
        return message
    return f"{label} — {message}"


def format_media_policy_summary(media) -> str:
    """Compact human-readable summary of the Media-tab policy for Dashboard."""
    normalized = media.normalized()
    if not normalized.enabled:
        return "Supplemental media is disabled for text/download-only runs."
    kinds = []
    if normalized.include_images:
        kinds.append("images")
    if normalized.include_videos:
        kinds.append("videos")
    kind_text = " + ".join(kinds) if kinds else "no image/video kinds enabled"
    included = ", ".join(normalized.include_extensions) if normalized.include_extensions else "none"
    excluded = ", ".join(normalized.exclude_extensions) if normalized.exclude_extensions else "none"
    return (
        f"Enabled: {kind_text} • Snapshot: {normalized.snapshot_strategy} • "
        f"Include extensions ({len(normalized.include_extensions)}): {included} • "
        f"Exclude extensions: {excluded}"
    )


class DashboardQueryInterrupted(RuntimeError):
    pass


def _count(database: sqlite3.Connection, sql: str, params: tuple = ()) -> int:
    try:
        row = database.execute(sql, params).fetchone()
        return int(row[0] or 0) if row else 0
    except sqlite3.DatabaseError as exc:
        if "interrupted" in str(exc).casefold():
            raise DashboardQueryInterrupted("dashboard query deadline exceeded") from exc
        raise


def _has_column(database: sqlite3.Connection, table: str, column: str) -> bool:
    try:
        return any(str(row[1]) == column for row in database.execute(f"PRAGMA table_info({table})"))
    except sqlite3.DatabaseError:
        return False


def _has_table(database: sqlite3.Connection, table: str) -> bool:
    try:
        return database.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone() is not None
    except sqlite3.DatabaseError:
        return False


def _latest_operation_scope(database: sqlite3.Connection) -> tuple[str, tuple[object, ...], dict[str, object]]:
    """Return a bounded SQL predicate for the newest durable operation inventory.

    Capture rows are not duplicated per operation, so scope is reconstructed from
    the frozen operation configuration using each target's effective CDX identity.
    This avoids silently mixing captures from unrelated targets/ranges in the same
    project while preserving the existing database schema and fast indexed reads.
    """
    row = database.execute(
        """SELECT id,mode,status,started_at,updated_at,config_json
           FROM operation_runs WHERE COALESCE(config_json,'')<>''
           ORDER BY id DESC LIMIT 1"""
    ).fetchone()
    if row is None:
        return "1=1", (), {"operation_id": 0, "operation_mode": "", "operation_status": "", "operation_scope": "project"}
    try:
        payload = json.loads(str(row["config_json"] or "{}"))
        allowed = {item.name for item in fields(ProjectConfig)}
        values = {key: value for key, value in payload.items() if key in allowed}
        if "output_dir" in values:
            values["output_dir"] = Path(str(values["output_dir"]))
        config = ProjectConfig(**values).normalized()
    except Exception:
        return "1=1", (), {
            "operation_id": int(row["id"]), "operation_mode": str(row["mode"] or ""),
            "operation_status": str(row["status"] or ""), "operation_scope": "project_fallback",
        }

    clauses: list[str] = []
    params: list[object] = []
    for target in config.targets:
        target_row = database.execute("SELECT id FROM targets WHERE pattern=?", (target,)).fetchone()
        if target_row is None:
            continue
        effective = config.for_target(target)
        signatures = cdx_query_signatures(effective)
        if not signatures:
            continue
        placeholders = ",".join("?" for _ in signatures)
        clauses.append(
            f"(c.target_id=? AND c.query_signature IN ({placeholders}) AND c.timestamp BETWEEN ? AND ?)"
        )
        params.extend((int(target_row[0]), *signatures, effective.from_date, effective.to_date))
    if not clauses:
        predicate = "1=0" if config.targets else "1=1"
    else:
        predicate = "(" + " OR ".join(clauses) + ")"
    return predicate, tuple(params), {
        "operation_id": int(row["id"]),
        "operation_mode": str(row["mode"] or ""),
        "operation_status": str(row["status"] or ""),
        "operation_started_at": str(row["started_at"] or ""),
        "operation_updated_at": str(row["updated_at"] or ""),
        "operation_scope": "frozen_config",
    }


def _capture_aggregate(database: sqlite3.Connection, predicate: str, params: tuple[object, ...]) -> sqlite3.Row:
    return database.execute(
        f"""SELECT
               COUNT(*) AS total,
               SUM(CASE WHEN c.state='pending' THEN 1 ELSE 0 END) AS pending,
               SUM(CASE WHEN c.state='downloading' THEN 1 ELSE 0 END) AS downloading,
               SUM(CASE WHEN c.state='downloaded_unscanned' THEN 1 ELSE 0 END) AS saved_unscanned,
               SUM(CASE WHEN c.state='scanning' THEN 1 ELSE 0 END) AS scanning,
               SUM(CASE WHEN c.state='downloaded' THEN 1 ELSE 0 END) AS scanned,
               SUM(CASE WHEN c.state='skipped' THEN 1 ELSE 0 END) AS skipped,
               SUM(CASE WHEN c.state='error' THEN 1 ELSE 0 END) AS failed,
               SUM(CASE WHEN c.state NOT IN ('pending','downloading','downloaded_unscanned','scanning','downloaded','skipped','error') THEN 1 ELSE 0 END) AS unknown_state,
               SUM(CASE WHEN COALESCE(c.resource_class,'unknown')='text' THEN 1 ELSE 0 END) AS class_text,
               SUM(CASE WHEN COALESCE(c.resource_class,'unknown')='image' THEN 1 ELSE 0 END) AS class_image,
               SUM(CASE WHEN COALESCE(c.resource_class,'unknown')='video' THEN 1 ELSE 0 END) AS class_video,
               SUM(CASE WHEN COALESCE(c.resource_class,'unknown')='audio' THEN 1 ELSE 0 END) AS class_audio,
               SUM(CASE WHEN COALESCE(c.resource_class,'unknown')='media_descriptor' THEN 1 ELSE 0 END) AS class_media_descriptor,
               SUM(CASE WHEN COALESCE(c.resource_class,'unknown')='other_binary' THEN 1 ELSE 0 END) AS class_other_binary,
               SUM(CASE WHEN COALESCE(c.resource_class,'unknown')='unknown' THEN 1 ELSE 0 END) AS class_unknown,
               SUM(CASE WHEN c.state IN ('downloaded','downloaded_unscanned','scanning') OR c.payload_availability IN ('retained','retained_unscanned','spooled_unscanned','cleanup_pending') THEN 1 ELSE 0 END) AS routed_downloaded,
               SUM(CASE WHEN c.state='skipped' AND c.skip_reason IN ('classified_media','payload_validation_deferred') THEN 1 ELSE 0 END) AS deferred_media,
               SUM(CASE WHEN c.state='skipped' AND c.skip_reason IN ('known_non_text','sniffed_non_text','unsupported_binary','classified_media_descriptor') THEN 1 ELSE 0 END) AS skipped_non_text,
               SUM(CASE WHEN c.state='skipped' AND c.skip_reason='url_keyword_filter' THEN 1 ELSE 0 END) AS skipped_url_filter,
               SUM(CASE WHEN c.state='skipped' AND COALESCE(c.skip_reason,'') NOT IN ('classified_media','payload_validation_deferred','known_non_text','sniffed_non_text','unsupported_binary','classified_media_descriptor','url_keyword_filter') THEN 1 ELSE 0 END) AS skipped_other,
               SUM(CASE WHEN COALESCE(c.resource_class,'unknown') NOT IN ('image','video','audio','media_descriptor','other_binary') AND c.payload_availability IN ('retained','retained_unscanned','spooled_unscanned','cleanup_pending') THEN 1 ELSE 0 END) AS body_available,
               SUM(CASE WHEN COALESCE(c.resource_class,'unknown') IN ('image','video','audio','media_descriptor','other_binary') THEN 1 ELSE 0 END) AS body_non_text,
               SUM(CASE WHEN c.payload_availability='discarded' THEN 1 ELSE 0 END) AS body_discarded,
               SUM(CASE WHEN c.payload_availability='partial' THEN 1 ELSE 0 END) AS body_partial,
               SUM(CASE WHEN c.payload_availability NOT IN ('retained','retained_unscanned','spooled_unscanned','cleanup_pending','discarded','partial') AND COALESCE(c.resource_class,'unknown') NOT IN ('image','video','audio','media_descriptor','other_binary') THEN 1 ELSE 0 END) AS body_url_only
           FROM captures c WHERE {predicate}""",
        params,
    ).fetchone()


def read_dashboard_counts(database_path: Path, *, max_query_seconds: float = 2.0) -> dict[str, int | bool | str | None]:
    """Read project totals plus one operation-scoped reconciliation snapshot."""
    if not database_path.exists():
        return {**EMPTY_DASHBOARD, "_exact": True, "_status": "missing_database"}
    database = sqlite3.connect(database_path.resolve().as_uri() + "?mode=ro", uri=True, timeout=0.25)
    database.row_factory = sqlite3.Row
    try:
        database.execute("PRAGMA query_only=ON")
        database.execute("PRAGMA busy_timeout=250")
        deadline = time.monotonic() + max(0.05, float(max_query_seconds))
        database.set_progress_handler(lambda: 1 if time.monotonic() >= deadline else 0, 5000)
        result: dict[str, int | bool | str | None] = {key: None for key in EMPTY_DASHBOARD}
        result["_exact"] = True
        result["_status"] = "exact"
        database.execute("BEGIN")
        try:
            # Project totals are deliberately separate from operation-scoped progress.
            # Read the universally available table totals first so a legacy or
            # partially migrated project still returns useful cards even if a
            # later optional query fails.
            result["captures"] = _count(database, "SELECT COUNT(*) FROM captures")
            result["documents"] = _count(database, "SELECT COUNT(*) FROM documents")
            if _has_column(database, "captures", "state"):
                project_row = database.execute(
                    """SELECT
                              SUM(CASE WHEN state='pending' THEN 1 ELSE 0 END) AS pending,
                              SUM(CASE WHEN state='downloading' THEN 1 ELSE 0 END) AS downloading,
                              SUM(CASE WHEN state='downloaded_unscanned' THEN 1 ELSE 0 END) AS downloaded_unscanned,
                              SUM(CASE WHEN state='scanning' THEN 1 ELSE 0 END) AS scanning,
                              SUM(CASE WHEN state='downloaded' THEN 1 ELSE 0 END) AS downloaded,
                              SUM(CASE WHEN state='error' THEN 1 ELSE 0 END) AS failed_captures
                       FROM captures"""
                ).fetchone()
                for key in ("pending", "downloading", "downloaded_unscanned", "scanning", "downloaded", "failed_captures"):
                    result[key] = int(project_row[key] or 0)
            if _has_column(database, "document_matches", "score"):
                result["matches"] = _count(
                    database,
                    "SELECT COUNT(DISTINCT document_id) FROM document_matches WHERE score>0 AND excluded=0 AND required_missing=0",
                )
            else:
                result["matches"] = _count(database, "SELECT COUNT(*) FROM document_matches")
            result["errors"] = _count(database, "SELECT COUNT(*) FROM errors WHERE resolved=0 AND ignored=0")
            # Keep this optional query after the core cards: older/minimal
            # databases intentionally surface a partial/database_error result.
            result["recovery_events"] = _count(database, "SELECT COUNT(*) FROM recovery_events")

            if _has_column(database, "captures", "skip_reason"):
                result["skipped_non_text"] = _count(
                    database,
                    """SELECT COUNT(*) FROM captures WHERE state='skipped' AND skip_reason IN
                       ('known_non_text','sniffed_non_text','unsupported_binary','classified_media','classified_media_descriptor','payload_validation_deferred')""",
                )
                result["skipped_url_filter"] = _count(
                    database, "SELECT COUNT(*) FROM captures WHERE state='skipped' AND skip_reason='url_keyword_filter'"
                )
                result["skipped_other"] = _count(
                    database,
                    """SELECT COUNT(*) FROM captures WHERE state='skipped' AND COALESCE(skip_reason,'') NOT IN
                       ('known_non_text','sniffed_non_text','unsupported_binary','classified_media','classified_media_descriptor','payload_validation_deferred','url_keyword_filter')""",
                )
            else:
                result["skipped_non_text"] = 0
                result["skipped_url_filter"] = 0
                result["skipped_other"] = _count(database, "SELECT COUNT(*) FROM captures WHERE state='skipped'")

            predicate, scope_params, meta = _latest_operation_scope(database) if _has_table(database, "operation_runs") else ("1=1", (), {"operation_id": 0, "operation_mode": "", "operation_status": "", "operation_scope": "project"})
            result.update(meta)
            operation = None
            required_capture_columns = {"state", "resource_class", "payload_availability", "skip_reason"}
            capture_columns = {str(row[1]) for row in database.execute("PRAGMA table_info(captures)")}
            if required_capture_columns.issubset(capture_columns):
                operation = _capture_aggregate(database, predicate, scope_params)
            mapping = {
                "operation_total": "total", "operation_pending": "pending", "operation_downloading": "downloading",
                "operation_saved_unscanned": "saved_unscanned", "operation_scanning": "scanning", "operation_scanned": "scanned",
                "operation_skipped": "skipped", "operation_failed": "failed", "operation_unclassified_state": "unknown_state",
                "operation_class_text": "class_text", "operation_class_image": "class_image", "operation_class_video": "class_video",
                "operation_class_audio": "class_audio", "operation_class_media_descriptor": "class_media_descriptor",
                "operation_class_other_binary": "class_other_binary", "operation_class_unknown": "class_unknown",
                "operation_downloaded": "routed_downloaded", "operation_deferred_media": "deferred_media",
                "operation_skipped_non_text": "skipped_non_text", "operation_skipped_url_filter": "skipped_url_filter",
                "operation_skipped_other": "skipped_other", "operation_body_available": "body_available",
                "operation_body_non_text": "body_non_text", "operation_body_discarded": "body_discarded",
                "operation_body_partial": "body_partial", "operation_body_url_only": "body_url_only",
            }
            if operation is not None:
                for key, column in mapping.items():
                    result[key] = int(operation[column] or 0)

            latest_scan = None
            if _has_table(database, "scan_runs"):
                latest_scan = database.execute(
                    "SELECT id,document_count,minimum_score FROM scan_runs ORDER BY id DESC LIMIT 1"
                ).fetchone()
            if latest_scan is not None:
                result["latest_scan_documents"] = int(latest_scan["document_count"] or 0)
                result["latest_scan_matches"] = _count(
                    database,
                    """SELECT COUNT(*) FROM document_matches
                       WHERE scan_run_id=? AND score>=? AND excluded=0 AND required_missing=0""",
                    (int(latest_scan["id"]), int(latest_scan["minimum_score"] or 1)),
                )
            else:
                result["latest_scan_documents"] = 0
                result["latest_scan_matches"] = 0

            try:
                media_row = database.execute(
                    """SELECT
                           COUNT(*) AS candidates,
                           SUM(CASE WHEN state NOT IN ('skipped','skipped_strategy') THEN 1 ELSE 0 END) AS selected,
                           SUM(CASE WHEN state='pending' THEN 1 ELSE 0 END) AS pending,
                           SUM(CASE WHEN state='downloading' THEN 1 ELSE 0 END) AS downloading,
                           SUM(CASE WHEN state='downloaded' THEN 1 ELSE 0 END) AS downloaded,
                           SUM(CASE WHEN state IN ('skipped','skipped_strategy') THEN 1 ELSE 0 END) AS excluded,
                           SUM(CASE WHEN state='error' THEN 1 ELSE 0 END) AS errors,
                           SUM(CASE WHEN media_kind='image' AND state NOT IN ('skipped','skipped_strategy') THEN 1 ELSE 0 END) AS images,
                           SUM(CASE WHEN media_kind='video' AND state NOT IN ('skipped','skipped_strategy') THEN 1 ELSE 0 END) AS videos
                       FROM media_captures"""
                ).fetchone()
                if media_row:
                    media_keys = (
                        "media_candidates", "media_selected", "media_pending", "media_downloading",
                        "media_downloaded", "media_excluded", "media_errors",
                        "media_selected_images", "media_selected_videos",
                    )
                    for index, key in enumerate(media_keys):
                        result[key] = int(media_row[index] or 0)
            except sqlite3.DatabaseError as exc:
                if "interrupted" in str(exc).casefold():
                    raise DashboardQueryInterrupted("dashboard query deadline exceeded") from exc

            result["media_deferred_from_text"] = _count(
                database,
                """SELECT COUNT(*) FROM media_discovery_queue
                   WHERE source_type='text_validation_deferred' AND state IN ('pending','error')""",
            )
        except DashboardQueryInterrupted:
            result["_exact"] = False
            result["_status"] = "deadline_exceeded"
        except sqlite3.DatabaseError as exc:
            result["_exact"] = False
            if "interrupted" in str(exc).casefold():
                result["_status"] = "deadline_exceeded"
            else:
                result["_status"] = "database_error"
            result["_error"] = str(exc)
            for key in EMPTY_DASHBOARD:
                if key not in result or result[key] is None:
                    result[key] = None
        finally:
            try:
                database.execute("ROLLBACK")
            except sqlite3.Error:
                pass
        return result
    finally:
        try:
            database.set_progress_handler(None, 0)
        except sqlite3.Error:
            pass
        database.close()


def _disposition_sql(value: str) -> tuple[str, tuple[object, ...]]:
    value = str(value or "").strip().casefold()
    if value in {"", "all"}:
        return "1=1", ()
    if value == "downloaded":
        return "(c.state IN ('downloaded','downloaded_unscanned','scanning') OR c.payload_availability IN ('retained','retained_unscanned','spooled_unscanned','cleanup_pending'))", ()
    if value == "deferred_to_media":
        return "(c.state='skipped' AND c.skip_reason IN ('classified_media','payload_validation_deferred'))", ()
    if value == "skipped":
        return "c.state='skipped'", ()
    if value == "failed":
        return "c.state='error'", ()
    if value == "pending":
        return "c.state='pending'", ()
    return "1=1", ()


def read_classification_rows(
    database_path: Path,
    *,
    resource_class: str = "All",
    disposition: str = "All",
    text_filter: str = "",
    limit: int = 500,
) -> list[dict[str, object]]:
    """Return bounded, filterable per-capture classification evidence.

    The view is scoped to the newest frozen operation when possible and performs
    no payload reads or filesystem stats, so it does not weaken acquisition speed.
    """
    if not database_path.exists():
        return []
    database = sqlite3.connect(database_path.resolve().as_uri() + "?mode=ro", uri=True, timeout=0.25)
    database.row_factory = sqlite3.Row
    try:
        database.execute("PRAGMA query_only=ON")
        database.execute("PRAGMA busy_timeout=250")
        scope, scope_params, _meta = _latest_operation_scope(database)
        clauses = [scope]
        params: list[object] = list(scope_params)
        class_value = str(resource_class or "All").strip().casefold()
        if class_value not in {"", "all"}:
            clauses.append("COALESCE(c.resource_class,'unknown')=?")
            params.append(class_value)
        disposition_sql, disposition_params = _disposition_sql(disposition)
        clauses.append(disposition_sql)
        params.extend(disposition_params)
        query = str(text_filter or "").strip()
        if query:
            clauses.append("(c.original_url LIKE ? OR COALESCE(c.classification_reason,'') LIKE ? OR COALESCE(c.skip_reason,'') LIKE ?)")
            pattern = f"%{query}%"
            params.extend((pattern, pattern, pattern))
        params.append(max(1, min(2000, int(limit))))
        rows = database.execute(
            """SELECT c.id,c.timestamp,c.original_url,c.mimetype,c.resource_class,c.classification_reason,
                      c.state,c.skip_reason,c.payload_availability,c.bytes_saved,c.local_path
               FROM captures c WHERE """ + " AND ".join(clauses) + " ORDER BY c.id LIMIT ?",
            tuple(params),
        ).fetchall()
        result: list[dict[str, object]] = []
        for row in rows:
            item = dict(row)
            item["routing_decision"] = capture_routing_decision(
                item.get("resource_class"), item.get("state"), item.get("skip_reason"), item.get("payload_availability")
            )
            item["body_coverage"] = capture_body_coverage(
                item.get("resource_class"), item.get("state"), item.get("payload_availability")
            )
            result.append(item)
        return result
    finally:
        database.close()
