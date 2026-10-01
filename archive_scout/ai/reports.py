from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path

from ..database.repositories import ai_result_rows
from ..utils import atomic_text_writer


def generate_ai_reports(output_dir: Path, database: sqlite3.Connection, ai_run_id: int, minimum_relevance: int = 0) -> dict[str, Path]:
    reports = output_dir / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    rows = ai_result_rows(database, ai_run_id, minimum_relevance, limit=1_000_000)
    base = reports / f"ai_relevance_{int(ai_run_id)}"
    csv_path = base.with_suffix(".csv")
    json_path = base.with_suffix(".json")
    md_path = base.with_suffix(".md")

    with atomic_text_writer(csv_path) as handle:
        writer = csv.writer(handle)
        writer.writerow(["relevance", "confidence", "archive_score", "timestamp", "category", "title", "url", "reason", "evidence"])
        for row in rows:
            writer.writerow([
                row["relevance_score"], row["confidence"], row["archive_score"], row["timestamp"], row["category"],
                row["title"], row["original_url"], row["reason"], row["evidence"],
            ])

    with atomic_text_writer(json_path) as handle:
        json.dump([
            {
                "relevance": int(row["relevance_score"]),
                "confidence": float(row["confidence"]),
                "archive_score": int(row["archive_score"]),
                "timestamp": row["timestamp"],
                "category": row["category"],
                "title": row["title"],
                "url": row["original_url"],
                "reason": row["reason"],
                "evidence": row["evidence"],
            }
            for row in rows
        ], handle, ensure_ascii=False, indent=2)
        handle.write("\n")

    with atomic_text_writer(md_path) as handle:
        handle.write("# Archive Scout AI relevance results\n\n")
        handle.write(f"AI run: {int(ai_run_id)}\n\n")
        for index, row in enumerate(rows, 1):
            handle.write(f"## {index}. {row['title'] or row['original_url']}\n\n")
            handle.write(f"- Relevance: {int(row['relevance_score'])}/100\n")
            handle.write(f"- Confidence: {float(row['confidence']):.2f}\n")
            handle.write(f"- Archive Scout score: {int(row['archive_score'])}\n")
            handle.write(f"- Timestamp: {row['timestamp']}\n")
            handle.write(f"- URL: {row['original_url']}\n")
            if row["category"]:
                handle.write(f"- Category: {row['category']}\n")
            handle.write(f"\n{row['reason'] or ''}\n\n")
            if row["evidence"]:
                handle.write(f"Evidence summary: {row['evidence']}\n\n")
    return {"ai_csv": csv_path, "ai_json": json_path, "ai_markdown": md_path}
