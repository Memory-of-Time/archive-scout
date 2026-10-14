from __future__ import annotations

import sqlite3
import time
from pathlib import Path


EMPTY_DASHBOARD = {
    "captures": 0,
    "documents": 0,
    "matches": 0,
    "errors": 0,
    "text": 0,"image":0,"video":0,"audio":0,"media_descriptor":0,
    "other_binary":0,"unknown":0,"deferred":0,"skipped":0,"failed":0,
}


def read_dashboard_counts(database_path: Path, *, include_classification: bool = True, max_query_seconds: float = 2.0) -> dict[str, int]:
    """Explicit-refresh snapshot of persisted project totals.

    Documents counts *completed text downloads*, including those pending a
    local scan. Matches counts unique pages with qualifying positive keyword
    results, never the number of hits or repeated scan-set rows. No polling.
    """
    if not database_path.exists():
        return dict(EMPTY_DASHBOARD)
    database = sqlite3.connect(database_path.resolve().as_uri() + "?mode=ro", uri=True, timeout=0.25)
    try:
        deadline=time.monotonic()+max(0.01,float(max_query_seconds))
        database.set_progress_handler(lambda: int(time.monotonic() >= deadline),1000)
        database.execute("PRAGMA query_only=ON")
        database.execute("PRAGMA busy_timeout=250")
        row = database.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM captures),
                (SELECT COUNT(*) FROM captures WHERE state IN ('downloaded', 'downloaded_unscanned')),
                (SELECT COUNT(DISTINCT document_id) FROM document_matches
                 WHERE score > 0 AND excluded=0 AND required_missing=0),
                (SELECT COUNT(*) FROM errors WHERE resolved=0 AND ignored=0)
            """
        ).fetchone()
        if row is None:
            return dict(EMPTY_DASHBOARD)
        result = {
            "captures": int(row[0] or 0),
            "documents": int(row[1] or 0),
            "matches": int(row[2] or 0),
            "errors": int(row[3] or 0),
        }
        if not include_classification:
            return result
        counts = {}
        try:
            from ..database.classification import classification_counts
            counts=classification_counts(database)
            for kind in ("text","image","video","audio","media_descriptor","other_binary","unknown"):
                result[kind]=counts.get(kind,0)
            result["deferred"]=counts.get("route_deferred_to_media",0)
            result["skipped"]=counts.get("route_skipped_non_text",0)+counts.get("route_skipped_url_filter",0)
            result["failed"]=counts.get("route_failed",0)
        except sqlite3.OperationalError:
            pass
        result.update({
            "pending": database.execute("SELECT COUNT(*) FROM captures WHERE state='pending'").fetchone()[0],
            "downloaded_unscanned": database.execute("SELECT COUNT(*) FROM captures WHERE state='downloaded_unscanned'").fetchone()[0],
            "failed_captures": database.execute("SELECT COUNT(*) FROM captures WHERE state='error'").fetchone()[0],
            "deferred_to_media": result.get("deferred",0),
            "skipped_non_text": counts.get("route_skipped_non_text",0),
            "skipped_url_filter": counts.get("route_skipped_url_filter",0),
            "skipped_other": 0,
            "recovery_events": database.execute("SELECT COUNT(*) FROM network_events WHERE stage LIKE '%recovery%'").fetchone()[0],
        })
        media={str(row[0]):int(row[1]) for row in database.execute("SELECT state,COUNT(*) FROM media_captures GROUP BY state")}
        result.update({"media_candidates":sum(media.values()),"media_selected":sum(value for state,value in media.items() if state!='excluded'),"media_pending":media.get("pending",0),"media_downloading":media.get("downloading",0),"media_downloaded":media.get("downloaded",0),"media_excluded":media.get("excluded",0),"media_errors":media.get("error",0),"media_deferred_from_text":result.get("deferred",0)})
        for kind in ("image","video"):
            result["media_selected_"+("images" if kind=="image" else "videos")]=database.execute("SELECT COUNT(*) FROM media_captures WHERE media_kind=? AND state<>'excluded'",(kind,)).fetchone()[0]
        return result
    except sqlite3.OperationalError as exc:
        if "interrupted" not in str(exc).casefold():
            raise
        return {**{key:None for key in EMPTY_DASHBOARD},"_exact":False}
    finally:
        database.close()

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
