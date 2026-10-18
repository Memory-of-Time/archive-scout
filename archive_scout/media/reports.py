from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

from ..config import ProjectConfig
from ..downloads.downloader import replay_url
from ..utils import atomic_write_lines, utc_now


def generate_media_reports(config: ProjectConfig, database: sqlite3.Connection,
                           stop_event: threading.Event | None = None) -> dict[str, Path]:
    reports = config.output_dir / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}
    opts = config.report.normalized()

    def save(name: str, lines):
        if not opts.output_enabled(name):
            return
        fields = opts.fields_for(name)
        # The caller supplies a field dictionary per row. Never collect all
        # rows into memory; cancellation is checked as they stream.
        def selected():
            for values in lines:
                if stop_event is not None and stop_event.is_set():
                    from ..events import Stopped
                    raise Stopped
                yield "\t".join(str(values.get(field, "")) for field in fields)
        path = reports / (name + ".txt")
        atomic_write_lines(path, selected())
        paths[name] = path

    save("media_indexed", (
        {"timestamp": row["timestamp"], "media_kind": row["media_kind"],
         "extension": row["extension"] or "", "state": row["state"], "original_url": row["original_url"]}
        for row in database.execute("SELECT timestamp,media_kind,extension,state,original_url "
                                    "FROM media_captures ORDER BY media_kind,original_url,timestamp")
    ))
    save("media_downloaded", (
        {"timestamp": row["timestamp"], "media_kind": row["media_kind"],
         "bytes_saved": row["bytes_saved"], "local_file": row["path"], "original_url": row["original_url"]}
        for row in database.execute("SELECT timestamp,media_kind,bytes_saved,path,original_url FROM media_captures "
                                    "WHERE state='downloaded' ORDER BY media_kind,original_url,timestamp")
    ))
    save("media_wayback_urls", (
        {"wayback_url": replay_url(str(row["timestamp"]), str(row["original_url"]))}
        for row in database.execute("SELECT timestamp,original_url FROM media_captures "
                                    "ORDER BY media_kind,original_url,timestamp")
    ))
    save("media_errors", (
        {"last_seen": row["last_seen"], "category": row["category"],
         "attempts": row["attempt_count"], "timestamp": row["timestamp"],
         "original_url": row["original_url"], "message": row["message"]}
        for row in database.execute("""SELECT e.last_seen,e.category,e.attempt_count,e.message,mc.original_url,mc.timestamp
              FROM errors e JOIN media_captures mc ON mc.id=e.media_capture_id
              WHERE e.resolved=0 AND e.ignored=0 ORDER BY e.last_seen,e.id""")
    ))

    counts = {str(row[0]): int(row[1]) for row in database.execute(
        "SELECT state,COUNT(*) FROM media_captures GROUP BY state"
    )}
    indexed_count = sum(counts.values())
    unresolved_count = int(database.execute(
        "SELECT COUNT(*) FROM errors WHERE resolved=0 AND ignored=0 AND media_capture_id IS NOT NULL"
    ).fetchone()[0])
    over_attempt = int(database.execute(
        "SELECT COUNT(*) FROM media_captures WHERE state='pending' AND download_attempts>=?",
        (config.max_attempts,),
    ).fetchone()[0])
    if any(opts.output_enabled(name) for name in (
            "media_indexed", "media_downloaded", "media_wayback_urls", "media_errors", "media_summary")):
        path = reports / "media_unfinished_over_attempts.tsv"
        atomic_write_lines(path, (
            f"{r['id']}\t{r['download_attempts']}\t{r['timestamp']}\t{r['original_url']}"
            for r in database.execute(
                "SELECT id,download_attempts,timestamp,original_url FROM media_captures "
                "WHERE state='pending' AND download_attempts>=? ORDER BY id",
                (config.max_attempts,),
            )
        ))
        paths["media_unfinished_over_attempts"] = path
    over_attempt_errors = int(database.execute(
        "SELECT COUNT(*) FROM media_captures WHERE state='error' AND download_attempts>=?",
        (config.max_attempts,),
    ).fetchone()[0])
    summary_values = {
        "heading": "Archive Scout media report",
        "generated": f"Generated: {utc_now()}",
        "indexed_media": f"Indexed media captures: {indexed_count:,}",
        "downloaded": f"Downloaded: {counts.get('downloaded', 0):,}",
        "pending": f"Pending: {counts.get('pending', 0):,}",
        "errors": f"Errors: {counts.get('error', 0):,}",
        "unresolved_media_errors": f"Unresolved media errors: {unresolved_count:,}",
        "snapshot_strategy": f"Snapshot strategy: {config.media.snapshot_strategy}",
        "included_extensions": f"Included extensions: {', '.join(config.media.include_extensions)}",
        "excluded_extensions": f"Excluded extensions: {', '.join(config.media.exclude_extensions) or '(none)'}",
    }
    if opts.output_enabled("media_summary"):
        summary = [summary_values[key] for key in opts.fields_for("media_summary") if key in summary_values]
        summary.extend((
            f"Pending excluded by attempt ceiling (UNFINISHED): {over_attempt:,}",
            f"Errors with exhausted attempts (UNFINISHED): {over_attempt_errors:,}",
        ))
        path = reports / "media_summary.txt"
        atomic_write_lines(path, summary)
        paths["media_summary"] = path
    return paths
