from __future__ import annotations

import sqlite3
from pathlib import Path

from ..utils import atomic_write_lines, utc_now


def _run_label(database: sqlite3.Connection, scan_run_id: int) -> str:
    row = database.execute(
        """
        SELECT sr.name,ks.name AS keyword_set_name
        FROM scan_runs sr JOIN keyword_sets ks ON ks.id=sr.keyword_set_id
        WHERE sr.id=?
        """,
        (scan_run_id,),
    ).fetchone()
    if not row:
        raise ValueError(f"scan run {scan_run_id} does not exist")
    return f"{scan_run_id} — {row['name']} — {row['keyword_set_name']}"


def generate_scan_comparison(
    database: sqlite3.Connection,
    first_scan_id: int,
    second_scan_id: int,
    destination: Path,
) -> Path:
    if first_scan_id == second_scan_id:
        raise ValueError("select two different scan runs")

    shared = int(database.execute(
        """
        SELECT COUNT(*) FROM document_matches first
        JOIN document_matches second ON second.document_id=first.document_id
        WHERE first.scan_run_id=? AND second.scan_run_id=?
        """,
        (first_scan_id, second_scan_id),
    ).fetchone()[0])
    only_first = int(database.execute(
        """
        SELECT COUNT(*) FROM document_matches first
        WHERE first.scan_run_id=? AND NOT EXISTS(
            SELECT 1 FROM document_matches second
            WHERE second.scan_run_id=? AND second.document_id=first.document_id
        )
        """,
        (first_scan_id, second_scan_id),
    ).fetchone()[0])
    only_second = int(database.execute(
        """
        SELECT COUNT(*) FROM document_matches second
        WHERE second.scan_run_id=? AND NOT EXISTS(
            SELECT 1 FROM document_matches first
            WHERE first.scan_run_id=? AND first.document_id=second.document_id
        )
        """,
        (second_scan_id, first_scan_id),
    ).fetchone()[0])

    def lines():
        yield "Archive Scout scan comparison"
        yield f"Generated: {utc_now()}"
        yield f"First: {_run_label(database, first_scan_id)}"
        yield f"Second: {_run_label(database, second_scan_id)}"
        yield f"Documents in both: {shared:,}"
        yield f"Only in first: {only_first:,}"
        yield f"Only in second: {only_second:,}"
        yield ""
        yield "SCORE CHANGES"
        for row in database.execute(
            """
            SELECT first.score AS first_score,second.score AS second_score,
                   c.timestamp,c.original_url,d.title
            FROM document_matches first
            JOIN document_matches second ON second.document_id=first.document_id
            JOIN documents d ON d.id=first.document_id
            JOIN captures c ON c.id=d.capture_id
            WHERE first.scan_run_id=? AND second.scan_run_id=?
            ORDER BY ABS(second.score-first.score) DESC,first.document_id
            """,
            (first_scan_id, second_scan_id),
        ):
            delta = int(row["second_score"]) - int(row["first_score"])
            yield "\t".join(
                [
                    f"delta={delta:+d}",
                    f"first={row['first_score']}",
                    f"second={row['second_score']}",
                    row["timestamp"],
                    row["original_url"],
                    row["title"] or "(untitled)",
                ]
            )
        yield ""
        yield "ONLY IN FIRST"
        for row in database.execute(
            """
            SELECT first.score,c.timestamp,c.original_url,d.title
            FROM document_matches first
            JOIN documents d ON d.id=first.document_id
            JOIN captures c ON c.id=d.capture_id
            WHERE first.scan_run_id=? AND NOT EXISTS(
                SELECT 1 FROM document_matches second
                WHERE second.scan_run_id=? AND second.document_id=first.document_id
            )
            ORDER BY first.document_id
            """,
            (first_scan_id, second_scan_id),
        ):
            yield f"{row['score']}\t{row['timestamp']}\t{row['original_url']}\t{row['title'] or '(untitled)'}"
        yield ""
        yield "ONLY IN SECOND"
        for row in database.execute(
            """
            SELECT second.score,c.timestamp,c.original_url,d.title
            FROM document_matches second
            JOIN documents d ON d.id=second.document_id
            JOIN captures c ON c.id=d.capture_id
            WHERE second.scan_run_id=? AND NOT EXISTS(
                SELECT 1 FROM document_matches first
                WHERE first.scan_run_id=? AND first.document_id=second.document_id
            )
            ORDER BY second.document_id
            """,
            (second_scan_id, first_scan_id),
        ):
            yield f"{row['score']}\t{row['timestamp']}\t{row['original_url']}\t{row['title'] or '(untitled)'}"

    atomic_write_lines(destination, lines())
    return destination
