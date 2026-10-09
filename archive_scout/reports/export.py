from __future__ import annotations

import csv
import json
import os
import sqlite3
import tempfile
import zipfile
from pathlib import Path

from ..database.repositories import iter_result_rows
from ..downloads.downloader import replay_url
from ..utils import atomic_text_writer, json_value

EXPORT_FIELDS = [
    "match_id", "score", "review_status", "title", "timestamp", "original_url", "wayback_url",
    "local_file", "hits", "snippets", "note", "tags",
]


def _record(row: sqlite3.Row) -> dict:
    return {
        "match_id": row["id"],
        "score": row["score"],
        "review_status": row["review_status"],
        "title": row["title"] or "",
        "timestamp": row["timestamp"],
        "original_url": row["original_url"],
        "wayback_url": replay_url(row["timestamp"], row["original_url"]),
        "local_file": row["path"],
        "hits": json_value(row["hits_json"], {}),
        "snippets": json_value(row["snippets_json"], []),
        "note": row["note"],
        "tags": row["tags"],
    }


def _records(
    database: sqlite3.Connection,
    scan_run_id: int,
    review_status: str,
    minimum_score: int,
    search: str,
):
    for row in iter_result_rows(database, scan_run_id, minimum_score, review_status, search):
        yield _record(row)


def export_scan(
    database: sqlite3.Connection,
    scan_run_id: int,
    destination: Path,
    format_name: str,
    review_status: str = "",
    minimum_score: int = 0,
    search: str = "",
) -> Path:
    format_name = format_name.casefold()
    if format_name not in {"json", "csv", "md", "markdown"}:
        raise ValueError(f"unsupported export format: {format_name}")

    with atomic_text_writer(destination) as handle:
        records = _records(database, scan_run_id, review_status, minimum_score, search)
        if format_name == "json":
            handle.write("[\n")
            first = True
            for record in records:
                if not first:
                    handle.write(",\n")
                handle.write(json.dumps(record, ensure_ascii=False, indent=2))
                first = False
            handle.write("\n]\n")
        elif format_name == "csv":
            writer = csv.DictWriter(handle, fieldnames=EXPORT_FIELDS)
            writer.writeheader()
            for record in records:
                output = dict(record)
                output["hits"] = json.dumps(output["hits"], ensure_ascii=False)
                output["snippets"] = json.dumps(output["snippets"], ensure_ascii=False)
                writer.writerow(output)
        else:
            handle.write("# Archive Scout review export\n")
            for index, record in enumerate(records, 1):
                handle.write("\n\n")
                handle.write(f"## {index}. {record['title'] or '(untitled)'}\n")
                handle.write(f"- Score: {record['score']}\n")
                handle.write(f"- Review: {record['review_status']}\n")
                handle.write(f"- Timestamp: {record['timestamp']}\n")
                handle.write(f"- Original: {record['original_url']}\n")
                handle.write(f"- Wayback: {record['wayback_url']}\n")
                handle.write(f"- Tags: {record['tags'] or '(none)'}\n")
                handle.write(f"- Note: {record['note'] or '(none)'}\n\n")
                handle.write("Snippets:\n")
                for snippet in record["snippets"]:
                    handle.write(f"- {snippet}\n")
            handle.write("\n")
    return destination


def export_review_package(
    database: sqlite3.Connection,
    scan_run_id: int,
    destination: Path,
    review_status: str = "",
    minimum_score: int = 0,
    search: str = "",
) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=destination.stem + ".", dir=str(destination.parent)) as temp:
        root = Path(temp)
        json_path = root / f"scan-{scan_run_id}.json"
        csv_path = root / f"scan-{scan_run_id}.csv"
        md_path = root / f"scan-{scan_run_id}.md"
        export_scan(database, scan_run_id, json_path, "json", review_status, minimum_score, search)
        export_scan(database, scan_run_id, csv_path, "csv", review_status, minimum_score, search)
        export_scan(database, scan_run_id, md_path, "markdown", review_status, minimum_score, search)
        temporary_zip = root / destination.name
        with zipfile.ZipFile(temporary_zip, "w", zipfile.ZIP_DEFLATED) as archive:
            for path in (json_path, csv_path, md_path):
                archive.write(path, path.name)
        os.replace(temporary_zip, destination)
    return destination
