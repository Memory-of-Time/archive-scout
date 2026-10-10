from __future__ import annotations

import sqlite3
from pathlib import Path

from ..config import ProjectConfig
from ..downloads.downloader import replay_url
from ..utils import atomic_write_lines, utc_now


def generate_media_reports(config: ProjectConfig, database: sqlite3.Connection) -> dict[str, Path]:
    reports = config.output_dir / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}

    path = reports / "media_indexed.txt"
    atomic_write_lines(
        path,
        (
            f"{row['timestamp']}\t{row['media_kind']}\t{row['extension'] or ''}\t{row['state']}\t{row['original_url']}"
            for row in database.execute(
                "SELECT timestamp,media_kind,extension,state,original_url "
                "FROM media_captures ORDER BY media_kind,original_url,timestamp"
            )
        ),
    )
    paths["media_indexed"] = path

    path = reports / "media_downloaded.txt"
    atomic_write_lines(
        path,
        (
            f"{row['timestamp']}\t{row['media_kind']}\t{row['bytes_saved']}\t{row['path']}\t{row['original_url']}"
            for row in database.execute(
                "SELECT timestamp,media_kind,bytes_saved,path,original_url FROM media_captures "
                "WHERE state='downloaded' ORDER BY media_kind,original_url,timestamp"
            )
        ),
    )
    paths["media_downloaded"] = path

    path = reports / "media_wayback_urls.txt"
    atomic_write_lines(
        path,
        (
            replay_url(str(row["timestamp"]), str(row["original_url"]))
            for row in database.execute(
                "SELECT timestamp,original_url FROM media_captures "
                "ORDER BY media_kind,original_url,timestamp"
            )
        ),
    )
    paths["media_wayback_urls"] = path

    path = reports / "media_errors.txt"
    atomic_write_lines(
        path,
        (
            f"{row['last_seen']}\t{row['category']}\t{row['attempt_count']}\t{row['timestamp']}\t{row['original_url']}\t{row['message']}"
            for row in database.execute(
                """
                SELECT e.last_seen,e.category,e.attempt_count,e.message,mc.original_url,mc.timestamp
                FROM errors e JOIN media_captures mc ON mc.id=e.media_capture_id
                WHERE e.resolved=0 AND e.ignored=0 ORDER BY e.last_seen,e.id
                """
            )
        ),
    )
    paths["media_errors"] = path

    counts = {str(row[0]): int(row[1]) for row in database.execute(
        "SELECT state,COUNT(*) FROM media_captures GROUP BY state"
    )}
    indexed_count = sum(counts.values())
    unresolved_count = int(database.execute(
        "SELECT COUNT(*) FROM errors WHERE resolved=0 AND ignored=0 AND media_capture_id IS NOT NULL"
    ).fetchone()[0])
    summary = [
        "Scout media report",
        f"Generated: {utc_now()}",
        f"Indexed media captures: {indexed_count:,}",
        f"Downloaded: {counts.get('downloaded', 0):,}",
        f"Pending: {counts.get('pending', 0):,}",
        f"Errors: {counts.get('error', 0):,}",
        f"Unresolved media errors: {unresolved_count:,}",
        f"Snapshot strategy: {config.media.snapshot_strategy}",
        f"Included extensions: {', '.join(config.media.include_extensions)}",
        f"Excluded extensions: {', '.join(config.media.exclude_extensions) or '(none)'}",
    ]
    path = reports / "media_summary.txt"
    atomic_write_lines(path, summary)
    paths["media_summary"] = path
    return paths
