from __future__ import annotations

import re
import sqlite3
import time
from pathlib import Path

from ..events import ProgressEvent


EMPTY_DASHBOARD = {
    "captures": 0,
    "documents": 0,
    "matches": 0,
    "errors": 0,
    "recovery_events": 0,
    "skipped_non_text": 0,
    "skipped_url_filter": 0,
    "skipped_other": 0,
    "pending": 0,
    "downloaded_unscanned": 0,
    "downloaded": 0,
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
    "network": "Wayback network recovery",
    "report": "Report generation",
    "site_issue": "Wayback site issue",
}
_VAGUE_YEAR_PROGRESS = re.compile(r"^Indexing\s+\d+\s+year\(s\),\s*\d+\s+completed\.?$", re.IGNORECASE)


def format_progress_message(event: ProgressEvent) -> str:
    """Return a phase-explicit GUI status/activity message.

    Older indexing code sometimes summarized the queue as a count of years even
    while it was actually counting Timemap pages, following resume keys, or
    handling another saved sub-plan.  The event stage is the durable source of
    truth for the operation phase, so the UI labels it explicitly and rewrites
    that legacy summary if it ever surfaces from an older/resumed queue.
    """
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


def read_dashboard_counts(database_path: Path, *, max_query_seconds: float = 2.0) -> dict[str, int | bool | str | None]:
    """Read exact totals read-only with a CPU/time deadline.

    If the deadline interrupts a query, completed counts are retained, the
    interrupted/remaining values are returned as ``None``, and ``_exact`` is
    false. The UI must never present an interrupted count as an exact zero.
    """
    if not database_path.exists():
        return {**EMPTY_DASHBOARD, "_exact": True, "_status": "missing_database"}
    database = sqlite3.connect(database_path.resolve().as_uri() + "?mode=ro", uri=True, timeout=0.25)
    try:
        database.execute("PRAGMA query_only=ON")
        database.execute("PRAGMA busy_timeout=250")
        deadline = time.monotonic() + max(0.05, float(max_query_seconds))
        database.set_progress_handler(lambda: 1 if time.monotonic() >= deadline else 0, 5000)
        result: dict[str, int | bool | str | None] = {key: None for key in EMPTY_DASHBOARD}
        result["_exact"] = True
        result["_status"] = "exact"
        # Pin every card to one SQLite read snapshot.  Without an explicit
        # transaction, each SELECT can observe a different writer commit and
        # produce impossible combinations such as captures=0, pending=1.
        database.execute("BEGIN")

        queries = [
            ("captures", "SELECT COUNT(*) FROM captures", ()),
            ("documents", "SELECT COUNT(*) FROM documents", ()),
            ("matches", "SELECT COUNT(*) FROM document_matches", ()),
            ("errors", "SELECT COUNT(*) FROM errors WHERE resolved=0 AND ignored=0", ()),
            ("pending", "SELECT COUNT(*) FROM captures WHERE state='pending'", ()),
            ("downloaded_unscanned", "SELECT COUNT(*) FROM captures WHERE state IN ('downloaded_unscanned','scanning')", ()),
            ("downloaded", "SELECT COUNT(*) FROM captures WHERE state='downloaded'", ()),
        ]
        try:
            for key, sql, params in queries:
                result[key] = _count(database, sql, params)

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

            if _has_column(database, "captures", "skip_reason"):
                result["skipped_non_text"] = _count(
                    database,
                    "SELECT COUNT(*) FROM captures WHERE state='skipped' AND skip_reason IN ('known_non_text','sniffed_non_text','unsupported_binary')",
                )
                result["skipped_url_filter"] = _count(
                    database, "SELECT COUNT(*) FROM captures WHERE state='skipped' AND skip_reason='url_keyword_filter'"
                )
                result["skipped_other"] = _count(
                    database,
                    """SELECT COUNT(*) FROM captures WHERE state='skipped' AND COALESCE(skip_reason,'') NOT IN
                       ('known_non_text','sniffed_non_text','unsupported_binary','url_keyword_filter')""",
                )
            else:
                result["skipped_non_text"] = 0
                result["skipped_url_filter"] = 0
                result["skipped_other"] = _count(database, "SELECT COUNT(*) FROM captures WHERE state='skipped'")
            result["recovery_events"] = _count(database, "SELECT COUNT(*) FROM recovery_events")
        except DashboardQueryInterrupted:
            result["_exact"] = False
            result["_status"] = "deadline_exceeded"
        except sqlite3.DatabaseError as exc:
            result["_exact"] = False
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

