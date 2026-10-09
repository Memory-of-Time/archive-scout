from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable, Iterator

from ..config import ProjectConfig
from ..downloads.downloader import replay_url
from ..utils import atomic_write_lines, utc_now

MEDIA_REPORT_FILENAMES = {
    "media_indexed": "media_indexed.txt",
    "media_downloaded": "media_downloaded.txt",
    "media_wayback_urls": "media_wayback_urls.txt",
    "media_errors": "media_errors.txt",
    "media_summary": "media_summary.txt",
}


def _tab_line(values: dict[str, object], fields: list[str]) -> str:
    return "\t".join(str(values.get(field, "") if values.get(field, "") is not None else "") for field in fields)


def _write_or_remove(
    reports: Path,
    name: str,
    enabled: bool,
    lines: Iterable[str],
) -> Path | None:
    path = reports / MEDIA_REPORT_FILENAMES[name]
    if not enabled:
        path.unlink(missing_ok=True)
        return None
    atomic_write_lines(path, lines)
    return path


def generate_media_reports(config: ProjectConfig, database: sqlite3.Connection) -> dict[str, Path]:
    report = config.report.normalized()
    reports = config.output_dir / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}

    indexed_fields = report.fields_for("media_indexed")

    def indexed_lines() -> Iterator[str]:
        if not indexed_fields:
            return
        for row in database.execute(
            "SELECT timestamp,media_kind,extension,state,original_url "
            "FROM media_captures ORDER BY media_kind,original_url,timestamp"
        ):
            yield _tab_line(
                {
                    "timestamp": row["timestamp"],
                    "media_kind": row["media_kind"],
                    "extension": row["extension"] or "",
                    "state": row["state"],
                    "original_url": row["original_url"],
                },
                indexed_fields,
            )

    path = _write_or_remove(reports, "media_indexed", report.output_enabled("media_indexed"), indexed_lines())
    if path:
        paths["media_indexed"] = path

    downloaded_fields = report.fields_for("media_downloaded")

    def downloaded_lines() -> Iterator[str]:
        if not downloaded_fields:
            return
        for row in database.execute(
            "SELECT timestamp,media_kind,bytes_saved,path,original_url FROM media_captures "
            "WHERE state='downloaded' ORDER BY media_kind,original_url,timestamp"
        ):
            yield _tab_line(
                {
                    "timestamp": row["timestamp"],
                    "media_kind": row["media_kind"],
                    "bytes_saved": row["bytes_saved"],
                    "local_file": row["path"],
                    "original_url": row["original_url"],
                },
                downloaded_fields,
            )

    path = _write_or_remove(
        reports, "media_downloaded", report.output_enabled("media_downloaded"), downloaded_lines()
    )
    if path:
        paths["media_downloaded"] = path

    wayback_fields = report.fields_for("media_wayback_urls")

    def media_wayback_lines() -> Iterator[str]:
        if not wayback_fields:
            return
        for row in database.execute(
            "SELECT timestamp,original_url FROM media_captures ORDER BY media_kind,original_url,timestamp"
        ):
            yield _tab_line(
                {"wayback_url": replay_url(str(row["timestamp"]), str(row["original_url"]))},
                wayback_fields,
            )

    path = _write_or_remove(
        reports, "media_wayback_urls", report.output_enabled("media_wayback_urls"), media_wayback_lines()
    )
    if path:
        paths["media_wayback_urls"] = path

    error_fields = report.fields_for("media_errors")

    def error_lines() -> Iterator[str]:
        if not error_fields:
            return
        for row in database.execute(
            """
            SELECT e.last_seen,e.category,e.attempt_count,e.message,mc.original_url,mc.timestamp
            FROM errors e JOIN media_captures mc ON mc.id=e.media_capture_id
            WHERE e.resolved=0 AND e.ignored=0 ORDER BY e.last_seen,e.id
            """
        ):
            yield _tab_line(
                {
                    "last_seen": row["last_seen"],
                    "category": row["category"],
                    "attempts": row["attempt_count"],
                    "timestamp": row["timestamp"],
                    "original_url": row["original_url"],
                    "message": row["message"],
                },
                error_fields,
            )

    path = _write_or_remove(reports, "media_errors", report.output_enabled("media_errors"), error_lines())
    if path:
        paths["media_errors"] = path

    summary_fields = report.fields_for("media_summary")

    def summary_lines() -> Iterator[str]:
        if not summary_fields:
            return
        counts = {
            str(row[0]): int(row[1])
            for row in database.execute("SELECT state,COUNT(*) FROM media_captures GROUP BY state")
        }
        indexed_count = sum(counts.values())
        unresolved_count = int(database.execute(
            "SELECT COUNT(*) FROM errors WHERE resolved=0 AND ignored=0 AND media_capture_id IS NOT NULL"
        ).fetchone()[0])
        values = {
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
        for field in summary_fields:
            yield values[field]

    path = _write_or_remove(reports, "media_summary", report.output_enabled("media_summary"), summary_lines())
    if path:
        paths["media_summary"] = path
    return paths
