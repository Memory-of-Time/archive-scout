from __future__ import annotations

import json
import sqlite3
from collections import Counter
from dataclasses import dataclass

from ..document_store import document_body
from ..scanning.automaton import LiteralAutomaton
from ..utils import clean_space, utc_now
from ..events import Stopped, ProgressEvent


@dataclass(slots=True)
class DiffSummary:
    compared_pairs: int = 0
    changed_pairs: int = 0
    first_appearances: int = 0


def _summary(earlier: str, later: str, stop_event=None) -> dict:
    """Complete linear-work anchored-block comparison with explicit semantics.

    Common prefix/suffix characters and identical 64-character middle blocks
    contribute to similarity. It is a block-overlap measure, not the old greedy
    character SequenceMatcher ratio. Exact change detection is independent of
    that descriptive metric, including reordered text and tiny large-file edits.
    """
    def stopped():
        if stop_event is not None and stop_event.is_set():
            raise Stopped
    stopped()
    shorter = min(len(earlier), len(later))
    prefix = 0
    while prefix + 4096 <= shorter and earlier[prefix:prefix+4096] == later[prefix:prefix+4096]:
        stopped()
        prefix += 4096
    while prefix < shorter and earlier[prefix] == later[prefix]:
        prefix += 1
    suffix = 0
    while suffix + 4096 <= shorter - prefix and earlier[len(earlier)-suffix-4096:len(earlier)-suffix] == later[len(later)-suffix-4096:len(later)-suffix]:
        stopped()
        suffix += 4096
    while suffix < shorter - prefix and earlier[len(earlier)-suffix-1] == later[len(later)-suffix-1]:
        suffix += 1
    def blocks(text):
        counts = Counter()
        end = len(text) - suffix
        for offset in range(prefix, end, 64):
            if offset % 65536 == prefix % 65536:
                stopped()
            counts[text[offset:min(offset+64, end)]] += 1
        return counts
    a, b = blocks(earlier), blocks(later)
    matches = prefix + suffix + sum(len(block) * min(count, b.get(block, 0)) for block, count in a.items())
    denominator = len(earlier) + len(later)
    def lines(text):
        result = set()
        for line in text.splitlines():
            stopped()
            value = clean_space(line)
            if value:
                result.add(value)
        return result
    earlier_lines, later_lines = lines(earlier), lines(later)
    added, removed = sorted(later_lines - earlier_lines), sorted(earlier_lines - later_lines)
    return {
        "similarity": round(2 * matches / denominator, 6) if denominator else 1.0,
        "similarity_method": "anchored_character_blocks_64_v1",
        "changed": earlier != later,
        "earlier_chars": len(earlier), "later_chars": len(later),
        "added_lines": added[:200], "removed_lines": removed[:200],
        "added_count": len(added), "removed_count": len(removed),
        "summary_line_limit": 200,
    }


def compare_snapshots(database: sqlite3.Connection, *, stop_event=None, callback=None) -> DiffSummary:
    summary = DiffSummary()
    previous: sqlite3.Row | None = None
    with database:
        database.execute("DROP TABLE IF EXISTS temp.archive_scout_diff_work")
        database.execute("CREATE TEMP TABLE archive_scout_diff_work(earlier_capture_id INTEGER,later_capture_id INTEGER,summary_json TEXT,created_at TEXT)")
        for row in database.execute(
            """
            SELECT d.*,c.id AS capture_id,c.original_url,c.timestamp,c.mimetype,c.detected_encoding
            FROM captures c JOIN documents d ON d.id=c.document_id
            WHERE c.state='downloaded'
            ORDER BY c.original_url,c.timestamp,c.id
            """
        ):
            if stop_event is not None and stop_event.is_set():
                raise Stopped
            if previous is not None and previous["original_url"] == row["original_url"]:
                previous_hash = str(previous["normalized_hash"] or "")
                current_hash = str(row["normalized_hash"] or "")
                if previous_hash and previous_hash == current_hash:
                    result = {
                        "similarity": 1.0, "changed": False,
                        "similarity_method": "anchored_character_blocks_64_v1",
                        "earlier_chars": len(document_body(previous)),
                        "later_chars": len(document_body(row)),
                        "added_lines": [], "removed_lines": [], "added_count": 0, "removed_count": 0,
                    }
                else:
                    result = _summary(document_body(previous), document_body(row), stop_event)
                database.execute(
                    """
                    INSERT INTO archive_scout_diff_work(earlier_capture_id,later_capture_id,summary_json,created_at)
                    VALUES(?,?,?,?)
                    """,
                    (previous["capture_id"], row["capture_id"], json.dumps(result, ensure_ascii=False), utc_now()),
                )
                summary.compared_pairs += 1
                if result.get("changed", False):
                    summary.changed_pairs += 1
                if callback and summary.compared_pairs % 100 == 0:
                    callback(ProgressEvent('snapshot_differences', f'Compared {summary.compared_pairs:,} snapshot pairs', summary.compared_pairs, None))
            previous = row
        if stop_event is not None and stop_event.is_set():
            raise Stopped
        database.execute("DELETE FROM snapshot_diffs")
        database.execute("INSERT INTO snapshot_diffs(earlier_capture_id,later_capture_id,summary_json,created_at) SELECT * FROM archive_scout_diff_work")
        database.execute("DROP TABLE archive_scout_diff_work")
    if callback:
        callback(ProgressEvent('snapshot_differences', f'Compared {summary.compared_pairs:,} snapshot pairs', summary.compared_pairs, summary.compared_pairs))
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
            SELECT d.*,c.id AS capture_id,c.original_url,c.timestamp,c.mimetype,c.detected_encoding
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
                (str(row["title"] or ""), document_body(row), str(row["links_json"] or ""))
            ).casefold()
            for needle in automaton.find(haystack):
                for query in needle_queries.get(needle, ()):
                    first, _last = matches.get(query, (row, row))
                    matches[query] = (first, row)
        count += flush(current_url, matches)
    return count

