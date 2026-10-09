from __future__ import annotations

import argparse
import json
import tempfile
import time
import tracemalloc
import sys
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from archive_scout.cdx.client import parse_cdx_text_rows
from archive_scout.database.connection import DATABASE_NAME, open_database
from archive_scout.database.repositories import result_rows, upsert_captures
from archive_scout.reports.export import export_scan
from archive_scout.scanning.keywords import compile_keywords, compile_prefilter
from archive_scout.scanning.scoring import analyze_content, prepare_analysis_fields
from archive_scout.utils import utc_now


def measure(name: str, operation):
    tracemalloc.start()
    started = time.perf_counter()
    value = operation()
    elapsed = time.perf_counter() - started
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return {
        "name": name,
        "elapsed_seconds": round(elapsed, 6),
        "peak_memory_bytes": peak,
        "result": value,
    }


def cdx_payload(start: int, row_count: int) -> bytearray:
    return bytearray(
        b"\n".join(
            f"20010101000000 text/html 200 D{index} 123 http://example.com/page/{index}".encode()
            for index in range(start, start + row_count)
        )
        + b"\n\nresume-token\n"
    )


def cdx_chunks(total_rows: int, chunk_rows: int):
    for start in range(0, total_rows, chunk_rows):
        yield start, min(chunk_rows, total_rows - start)


def seed_results(database, root: Path, row_count: int, body_bytes: int) -> int:
    now = utc_now()
    body = "benchmark body " + ("x" * max(0, body_bytes - 15))
    with database:
        database.execute(
            "INSERT INTO keyword_sets(name,fingerprint,keywords_json,rules_json,created_at,updated_at) VALUES(?,?,?,?,?,?)",
            ("Benchmark", "benchmark-fingerprint", "[]", "[]", now, now),
        )
        keyword_set_id = int(database.execute("SELECT id FROM keyword_sets").fetchone()[0])
        database.execute(
            "INSERT INTO scan_runs(keyword_set_id,name,status,minimum_score,started_at,source_operation) VALUES(?,?,'complete',1,?,'benchmark')",
            (keyword_set_id, "Benchmark", now),
        )
        scan_run_id = int(database.execute("SELECT id FROM scan_runs").fetchone()[0])
        database.execute(
            "INSERT OR IGNORE INTO targets(pattern,settings_json,created_at) VALUES('example.com/*','{}',?)",
            (now,),
        )
        target_id = int(database.execute("SELECT id FROM targets").fetchone()[0])
        database.executemany(
            """
            INSERT INTO captures(
                original_url,timestamp,target_id,query_signature,mimetype,statuscode,digest,length,state,created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                (
                    f"http://example.com/result/{index}",
                    f"2001{index % 12 + 1:02d}{index % 28 + 1:02d}000000",
                    target_id,
                    "benchmark-results",
                    "text/html",
                    "200",
                    f"R{index}",
                    len(body),
                    "downloaded",
                    now,
                    now,
                )
                for index in range(row_count)
            ),
        )
        capture_ids = [int(row[0]) for row in database.execute(
            "SELECT id FROM captures WHERE query_signature='benchmark-results' ORDER BY id"
        )]
        database.executemany(
            """
            INSERT INTO documents(
                capture_id,path,title,body_text,links_json,content_hash,normalized_hash,size_bytes,created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                (
                    capture_id,
                    str(root / f"result-{index}.html"),
                    f"Title {index}",
                    body,
                    "[]",
                    f"content-{index}",
                    f"normalized-{index}",
                    len(body),
                    now,
                    now,
                )
                for index, capture_id in enumerate(capture_ids)
            ),
        )
        document_ids = [int(row[0]) for row in database.execute("SELECT id FROM documents ORDER BY id")]
        database.executemany(
            """
            INSERT INTO document_matches(
                scan_run_id,document_id,score,hits_json,fields_json,snippets_json,interesting_links_json,
                excluded,required_missing,proximity_json,created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                (
                    scan_run_id,
                    document_id,
                    index % 100,
                    "{}",
                    "{}",
                    '["benchmark snippet"]',
                    "[]",
                    0,
                    0,
                    "{}",
                    now,
                    now,
                )
                for index, document_id in enumerate(document_ids)
            ),
        )
        match_ids = [int(row[0]) for row in database.execute("SELECT id FROM document_matches ORDER BY id")]
        database.executemany(
            "INSERT INTO reviews(match_id,status) VALUES(?,'unreviewed')",
            ((match_id,) for match_id in match_ids),
        )
        database.executemany(
            "INSERT INTO notes(match_id,text,created_at,updated_at) VALUES(?,?,?,?)",
            ((match_id, "benchmark note", now, now) for match_id in match_ids),
        )
        database.execute("INSERT OR IGNORE INTO tags(name) VALUES('benchmark')")
        tag_id = int(database.execute("SELECT id FROM tags WHERE name='benchmark'").fetchone()[0])
        database.executemany(
            "INSERT INTO match_tags(match_id,tag_id) VALUES(?,?)",
            ((match_id, tag_id) for match_id in match_ids),
        )
    return scan_run_id


def run(args: argparse.Namespace) -> dict:
    def parse_rows():
        parsed_count = 0
        for start, count in cdx_chunks(args.cdx_rows, args.cdx_chunk_rows):
            parsed_count += len(parse_cdx_text_rows(cdx_payload(start, count), "https://example.invalid/cdx").rows)
        return parsed_count

    def literal_prefilter():
        patterns = compile_keywords(
            [f"benchmark-keyword-{index}" for index in range(args.keyword_patterns - 1)]
            + ["lost media archive"]
        )
        prefilter = compile_prefilter(patterns)
        matched = 0
        for index in range(args.keyword_searches):
            value = "ordinary archived page text"
            if index == args.keyword_searches - 1:
                value += " lost media archive"
            matched += int(prefilter.matches({"body": value}, {"body": value}))
        return matched

    def literal_scoring():
        patterns = compile_keywords(
            [f"benchmark-keyword-{index}" for index in range(args.keyword_patterns - 1)]
            + ["lost media archive"]
        )
        prefilter = compile_prefilter(patterns)
        raw = "<html><body>ordinary archived page text lost media archive</body></html>"
        visible = "ordinary archived page text lost media archive"
        fields, normalized_fields = prepare_analysis_fields(
            "http://example.com", "", visible, raw, []
        )
        score = 0
        for _index in range(args.keyword_score_runs):
            score += int(
                analyze_content(
                    "http://example.com", "", visible, raw, [], patterns, prefilter,
                    fields, normalized_fields,
                )["score"]
            )
        return score

    results = [
        measure("parse_cdx_chunks", parse_rows),
        measure("literal_prefilter", literal_prefilter),
        measure("literal_scoring", literal_scoring),
    ]
    with tempfile.TemporaryDirectory(prefix="archive-scout-benchmark-") as temp:
        root = Path(temp)
        database = open_database(root)
        with database:
            target_cursor = database.execute(
                "INSERT INTO targets(pattern,settings_json,created_at) VALUES(?,?,?)",
                ("benchmark.example/*", "{}", utc_now()),
            )
        target_id = int(target_cursor.lastrowid)

        def insert_rows():
            before = database.total_changes
            for start, count in cdx_chunks(args.cdx_rows, args.cdx_chunk_rows):
                parsed = parse_cdx_text_rows(cdx_payload(start, count), "https://example.invalid/cdx")
                with database:
                    upsert_captures(database, parsed.rows, target_id, "benchmark-cdx")
            return database.total_changes - before

        unchanged_cdx_writes = 0
        if not args.skip_cdx_upsert:
            results.append(measure("parse_and_upsert_cdx_chunks", insert_rows))

            def repeat_insert_rows():
                before = database.total_changes
                for start, count in cdx_chunks(args.cdx_rows, args.cdx_chunk_rows):
                    parsed = parse_cdx_text_rows(cdx_payload(start, count), "https://example.invalid/cdx")
                    with database:
                        upsert_captures(database, parsed.rows, target_id, "benchmark-cdx")
                return database.total_changes - before

            repeated = measure("repeat_parse_and_noop_upsert", repeat_insert_rows)
            unchanged_cdx_writes = int(repeated["result"])
            results.append(repeated)
        scan_run_id = seed_results(database, root, args.result_rows, args.body_bytes)
        results.append(measure("browse_500", lambda: len(result_rows(database, scan_run_id, limit=500))))
        export_path = root / "benchmark-export.json"
        results.append(measure(
            "export_json",
            lambda: (export_scan(database, scan_run_id, export_path, "json"), export_path.stat().st_size)[1],
        ))
        database.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        database_size = (root / DATABASE_NAME).stat().st_size
        inserted_cdx_rows = int(database.execute(
            "SELECT COUNT(*) FROM captures WHERE query_signature='benchmark-cdx'"
        ).fetchone()[0])
        duplicate_work = 0 if args.skip_cdx_upsert else args.cdx_rows - inserted_cdx_rows
        database.close()

    chunk_count = (args.cdx_rows + args.cdx_chunk_rows - 1) // args.cdx_chunk_rows
    return {
        "fixture": {
            "cdx_rows": args.cdx_rows,
            "cdx_chunk_rows": args.cdx_chunk_rows,
            "result_rows": args.result_rows,
            "body_bytes": args.body_bytes,
            "live_network": False,
            "cdx_upsert_skipped": args.skip_cdx_upsert,
            "keyword_patterns": args.keyword_patterns,
            "keyword_searches": args.keyword_searches,
            "keyword_score_runs": args.keyword_score_runs,
        },
        "metrics": results,
        "database_size_bytes": database_size,
        "database_transactions": 2 if args.skip_cdx_upsert else chunk_count + 2,
        "cdx_rows_inserted": inserted_cdx_rows,
        "http_attempts": 0,
        "duplicate_work": duplicate_work,
        "unchanged_cdx_writes": unchanged_cdx_writes,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run deterministic Archive Scout offline benchmarks.")
    parser.add_argument("--cdx-rows", type=int, default=100_000)
    parser.add_argument("--cdx-chunk-rows", type=int, default=50_000)
    parser.add_argument("--skip-cdx-upsert", action="store_true")
    parser.add_argument("--result-rows", type=int, default=10_000)
    parser.add_argument("--body-bytes", type=int, default=8192)
    parser.add_argument("--keyword-patterns", type=int, default=5000)
    parser.add_argument("--keyword-searches", type=int, default=1000)
    parser.add_argument("--keyword-score-runs", type=int, default=100)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if (
        args.cdx_rows < 1
        or args.cdx_chunk_rows < 1
        or args.result_rows < 1
        or args.body_bytes < 1
        or args.keyword_patterns < 1
        or args.keyword_searches < 1
        or args.keyword_score_runs < 1
    ):
        parser.error("row counts, chunk sizes, body size, and keyword counts must be positive")
    report = run(args)
    text = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    print(text, end="")


if __name__ == "__main__":
    main()
