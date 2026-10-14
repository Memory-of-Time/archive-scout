from __future__ import annotations

import difflib
import json
import sqlite3
from collections import defaultdict
from dataclasses import dataclass

from ..scanning.automaton import LiteralAutomaton
from ..utils import clean_space, utc_now


@dataclass(slots=True)
class DiffSummary:
    compared_pairs: int = 0
    changed_pairs: int = 0
    first_appearances: int = 0


def _summary(earlier: str, later: str) -> dict:
    earlier_lines = [value for line in earlier.splitlines() if (value := clean_space(line))]
    later_lines = [value for line in later.splitlines() if (value := clean_space(line))]
    matcher = difflib.SequenceMatcher(None, earlier, later, autojunk=False)
    earlier_set = set(earlier_lines)
    later_set = set(later_lines)
    added = sorted(later_set - earlier_set)
    removed = sorted(earlier_set - later_set)
    return {
        "similarity": round(matcher.ratio(), 6),
        "earlier_chars": len(earlier),
        "later_chars": len(later),
        "added_lines": added[:200],
        "removed_lines": removed[:200],
        "added_count": len(added),
        "removed_count": len(removed),
    }


def compare_snapshots(database: sqlite3.Connection) -> DiffSummary:
    summary = DiffSummary()
    previous: sqlite3.Row | None = None
    with database:
        database.execute("DELETE FROM snapshot_diffs")
        for row in database.execute(
            """
            SELECT c.id AS capture_id,c.original_url,c.timestamp,d.body_text,d.normalized_hash
            FROM captures c JOIN documents d ON d.id=c.document_id
            WHERE c.state='downloaded'
            ORDER BY c.original_url,c.timestamp,c.id
            """
        ):
            if previous is not None and previous["original_url"] == row["original_url"]:
                previous_hash = str(previous["normalized_hash"] or "")
                current_hash = str(row["normalized_hash"] or "")
                if previous_hash and previous_hash == current_hash:
                    result = {
                        "similarity": 1.0,
                        "earlier_chars": len(str(previous["body_text"] or "")),
                        "later_chars": len(str(row["body_text"] or "")),
                        "added_lines": [], "removed_lines": [], "added_count": 0, "removed_count": 0,
                    }
                else:
                    result = _summary(str(previous["body_text"] or ""), str(row["body_text"] or ""))
                database.execute(
                    """
                    INSERT INTO snapshot_diffs(earlier_capture_id,later_capture_id,summary_json,created_at)
                    VALUES(?,?,?,?)
                    ON CONFLICT(earlier_capture_id,later_capture_id) DO UPDATE SET
                        summary_json=excluded.summary_json,created_at=excluded.created_at
                    """,
                    (previous["capture_id"], row["capture_id"], json.dumps(result, ensure_ascii=False), utc_now()),
                )
                summary.compared_pairs += 1
                if result["similarity"] < 0.999999:
                    summary.changed_pairs += 1
            previous = row
    return summary


def build_first_appearances(database: sqlite3.Connection, queries: list[str]) -> int:
    queries = list(dict.fromkeys(value.strip() for value in queries if value.strip()))
    if not queries:
        return 0
    needle_queries: dict[str, list[str]] = {}
    for query in queries:
        needle_queries.setdefault(query.casefold(), []).append(query)
    automaton = LiteralAutomaton(needle_queries)
    count = 0

    def flush(original_url: str | None, matches: dict[str, tuple[sqlite3.Row, sqlite3.Row]]) -> int:
        if original_url is None or not matches:
            return 0
        database.executemany(
            """
            INSERT INTO first_appearances(
                query,original_url,first_capture_id,first_timestamp,last_capture_id,last_timestamp,created_at
            ) VALUES(?,?,?,?,?,?,?)
            """,
            (
                (
                    query,
                    original_url,
                    first["capture_id"],
                    first["timestamp"],
                    last["capture_id"],
                    last["timestamp"],
                    utc_now(),
                )
                for query, (first, last) in matches.items()
            ),
        )
        return len(matches)

    with database:
        database.execute("DELETE FROM first_appearances")
        current_url: str | None = None
        matches: dict[str, tuple[sqlite3.Row, sqlite3.Row]] = {}
        for row in database.execute(
            """
            SELECT c.id AS capture_id,c.original_url,c.timestamp,d.title,d.body_text,d.links_json
            FROM captures c JOIN documents d ON d.id=c.document_id
            ORDER BY c.original_url,c.timestamp,c.id
            """
        ):
            original_url = str(row["original_url"])
            if current_url is not None and original_url != current_url:
                count += flush(current_url, matches)
                matches.clear()
            current_url = original_url
            haystack = " ".join(
                (str(row["title"] or ""), str(row["body_text"] or ""), str(row["links_json"] or ""))
            ).casefold()
            for needle in automaton.find(haystack):
                for query in needle_queries.get(needle, ()):
                    first, _last = matches.get(query, (row, row))
                    matches[query] = (first, row)
        count += flush(current_url, matches)
    return count

