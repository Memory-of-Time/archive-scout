from __future__ import annotations

import argparse
import inspect
import json
import shutil
import tempfile
import threading
import time
import tracemalloc
from pathlib import Path
from unittest import mock
import sys

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from archive_scout.cdx.indexer import index_archive
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.config import ProjectConfig
from archive_scout.constants import SCHEMA_VERSION, VERSION
from archive_scout.database.connection import open_database
from archive_scout.downloads.downloader import prepare_acquisition_rows, download_archive_only, download_archive
from archive_scout.network import transports
from archive_scout.scanning.hitlist import search_with_hitlist
from archive_scout.scanning.jobs import ScanJob
from archive_scout.database.repositories import get_or_create_keyword_set, start_scan_run
from archive_scout.utils import utc_now


def timed(fn):
    tracemalloc.start()
    start = time.perf_counter()
    value = fn()
    elapsed = time.perf_counter() - start
    _cur, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return elapsed, peak, value


def range_request_fixture() -> dict:
    with tempfile.TemporaryDirectory(prefix="archive-scout-audit3-range-") as temp:
        root = Path(temp)
        config = ProjectConfig(
            output_dir=root,
            targets=["example.com/*"],
            keywords=[],
            from_date="2000",
            to_date="2019",
            cdx_delay=0,
        ).normalized()
        database = open_database(root)
        calls: list[dict[str, str]] = []

        def fake_get(_self, _urls, params, max_bytes=0, prefer_text=False):
            del max_bytes, prefer_text
            values = dict(params)
            calls.append(values)
            if values.get("showNumPages") == "true":
                return 1
            return []

        elapsed, peak, _ = timed(lambda: _patched_index(fake_get, config, database))
        database.close()
        return {
            "elapsed_seconds": elapsed,
            "peak_memory_bytes": peak,
            "logical_requests": len(calls),
            "page_count_requests": sum(1 for item in calls if item.get("showNumPages") == "true"),
            "data_requests": sum(1 for item in calls if item.get("showNumPages") != "true"),
            "distinct_windows": len({(item.get("from"), item.get("to")) for item in calls}),
        }


def _patched_index(fake_get, config, database):
    with mock.patch("archive_scout.cdx.client.HttpClient.get_cdx_any", new=fake_get):
        index_archive(config, database, threading.Event())


def selector_fixture(rows: int) -> dict:
    with tempfile.TemporaryDirectory(prefix="archive-scout-audit3-selector-") as temp:
        root = Path(temp)
        config = ProjectConfig(
            output_dir=root,
            targets=["example.com/*"],
            keywords=[],
            from_date="2001",
            to_date="2001",
        ).normalized()
        database = open_database(root)
        signature = cdx_query_signature(config)
        now = utc_now()
        with database:
            database.executemany(
                """INSERT INTO captures(
                       original_url,timestamp,query_signature,mimetype,statuscode,length,state,created_at,updated_at
                   ) VALUES(?, '20010101000000', ?, 'text/html','200',?,'pending',?,?)""",
                ((f"http://example.com/{index}.html", signature, 100 + (index % 4000), now, now) for index in range(rows)),
            )

        def enumerate_once():
            total, iterator, _stats = prepare_acquisition_rows(database, config, None)
            return total, sum(1 for _ in iterator)

        progress = {"callbacks": 0}
        database.set_progress_handler(lambda: progress.__setitem__("callbacks", progress["callbacks"] + 1) or 0, 1000)
        first_elapsed, first_peak, first_value = timed(enumerate_once)
        first_vm = progress["callbacks"] * 1000
        progress["callbacks"] = 0
        second_elapsed, second_peak, second_value = timed(enumerate_once)
        second_vm = progress["callbacks"] * 1000
        database.set_progress_handler(None, 0)
        database.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        db_size = (root / "archive_scout.sqlite3").stat().st_size
        database.close()
        return {
            "rows": rows,
            "first_pass_seconds": first_elapsed,
            "warm_pass_seconds": second_elapsed,
            "first_peak_memory_bytes": first_peak,
            "warm_peak_memory_bytes": second_peak,
            "first_vm_steps_approx": first_vm,
            "warm_vm_steps_approx": second_vm,
            "first_result": list(first_value),
            "warm_result": list(second_value),
            "database_size_bytes": db_size,
        }


def hitlist_fixture(files: int) -> dict:
    with tempfile.TemporaryDirectory(prefix="archive-scout-audit3-hitlist-") as temp:
        root = Path(temp)
        capture_dir = root / "captures" / "2001" / "01"
        capture_dir.mkdir(parents=True)
        database = open_database(root)
        now = utc_now()
        columns = {row[1] for row in database.execute("PRAGMA table_info(captures)")}
        has_availability = "payload_availability" in columns
        with database:
            for index in range(files):
                path = capture_dir / f"page-{index}.txt"
                # Every requested literal exists in raw source, so a correct
                # Hitlist guard must avoid DOM rendering for this workload.
                path.write_text(
                    f"<html><body>alpha beta gamma page {index}</body></html>", encoding="utf-8"
                )
                base_columns = "original_url,timestamp,query_signature,mimetype,statuscode,length,state,local_path,created_at,updated_at"
                values = [
                    f"http://example.com/{index}.html", "20010101000000", "bench-hitlist", "text/html", "200",
                    path.stat().st_size, "downloaded", str(path), now, now,
                ]
                if has_availability:
                    base_columns += ",payload_availability,resource_class,resource_classifier_revision"
                    values += ["retained", "text", 1]
                placeholders = ",".join("?" for _ in values)
                database.execute(f"INSERT INTO captures({base_columns}) VALUES({placeholders})", values)
        elapsed, peak, result = timed(
            lambda: search_with_hitlist(root, database, ["alpha", "beta", "gamma"], threading.Event())
        )
        database.close()
        return {
            "files": files,
            "elapsed_seconds": elapsed,
            "peak_memory_bytes": peak,
            "local_checked": int(result["local_checked"]),
            "matches": int(result["matches"]),
        }


def binary_prefix_fixture(payload_bytes: int) -> dict:
    signature = inspect.signature(transports._write_limited)
    supports_validator = "preview_validator" in signature.parameters
    with tempfile.TemporaryDirectory(prefix="archive-scout-audit3-prefix-") as temp:
        destination = Path(temp) / "binary.part"
        consumed = {"bytes": 0}
        chunk_size = 8192

        def chunks():
            remaining = payload_bytes
            first = True
            while remaining > 0:
                size = min(chunk_size, remaining)
                if first:
                    chunk = b"\x89PNG\r\n\x1a\n" + b"x" * max(0, size - 8)
                    first = False
                else:
                    chunk = b"x" * size
                consumed["bytes"] += len(chunk)
                remaining -= len(chunk)
                yield chunk

        rejected = False
        started = time.perf_counter()
        if supports_validator:
            from archive_scout.classification import classify_payload_prefix

            def validator(_headers, prefix):
                decision = classify_payload_prefix(prefix, "text/plain", "http://example.com/file")
                return decision.resource_class if decision.confident and decision.resource_class != "text" else None

            try:
                transports._write_limited(
                    chunks(), destination, payload_bytes + 1, threading.Event(),
                    preview_bytes=64 * 1024, preview_validator=validator,
                )
            except Exception as exc:
                if exc.__class__.__name__ == "PreviewRejected":
                    rejected = True
                else:
                    raise
        else:
            transports._write_limited(chunks(), destination, payload_bytes + 1, threading.Event())
            # Audit2 classification occurred after the stream completed.
            rejected = True
            destination.unlink(missing_ok=True)
        elapsed = time.perf_counter() - started
        return {
            "fixture_bytes": payload_bytes,
            "bytes_consumed_before_rejection": consumed["bytes"],
            "rejected": rejected,
            "elapsed_seconds": elapsed,
        }



class _BenchmarkReplayClient:
    calls = 0
    body = b"<html><body>needle benchmark payload</body></html>"

    def __init__(self, *args, **kwargs):
        del args, kwargs

    def close(self):
        return None

    def metrics_snapshot(self):
        return {
            "request_starts": type(self).calls, "request_completions": type(self).calls,
            "request_failures": 0, "network_bytes": type(self).calls * len(type(self).body),
            "retry_waits": 0, "rate_limit_events": 0, "pacing_wait_seconds": 0.0,
            "host_gate_wait_seconds": 0.0, "retry_wait_seconds": 0.0,
            "rate_limit_wait_seconds": 0.0, "network_seconds": 0.0,
        }

    def download_to_path(self, url, destination, max_bytes, compute_hash=False, **kwargs):
        del max_bytes, compute_hash
        type(self).calls += 1
        body = type(self).body
        headers = {"content-type": "text/html; charset=utf-8"}
        validator = kwargs.get("preview_validator")
        if validator is not None:
            rejected = validator(headers, body)
            if rejected:
                raise RuntimeError(str(rejected))
        Path(destination).parent.mkdir(parents=True, exist_ok=True)
        Path(destination).write_bytes(body)
        return {
            "headers": headers, "preview": body, "bytes": len(body), "content_hash": "",
            "status": 200, "final_url": url,
        }


def _seed_pending_text(database, signature: str, count: int) -> None:
    now = utc_now()
    with database:
        database.executemany(
            """INSERT INTO captures(
                   original_url,timestamp,query_signature,mimetype,statuscode,length,state,created_at,updated_at
               ) VALUES(?, '20010101000000', ?, 'text/html','200',1024,'pending',?,?)""",
            ((f"http://example.com/page-{index}.html", signature, now, now) for index in range(count)),
        )


def retention_fixture(count: int) -> dict:
    result: dict[str, object] = {}
    with tempfile.TemporaryDirectory(prefix="archive-scout-audit3-keep-") as temp:
        root = Path(temp)
        config = ProjectConfig(root, ["example.com/*"], [], from_date="2001", to_date="2001", workers=10, download_delay=0).normalized()
        database = open_database(root)
        signature = cdx_query_signature(config)
        _seed_pending_text(database, signature, count)
        _BenchmarkReplayClient.calls = 0
        elapsed, peak, summary = timed(lambda: _patched_download_only(config, database))
        files = list((root / "captures").rglob("*.txt"))
        result["keep"] = {
            "elapsed_seconds": elapsed, "peak_memory_bytes": peak,
            "http_starts": _BenchmarkReplayClient.calls, "files_remaining": len(files),
            "bytes_remaining": sum(path.stat().st_size for path in files),
            "downloaded": int(summary.get("downloaded", 0)),
        }
        database.close()

    if "text_retention" in getattr(ProjectConfig, "__dataclass_fields__", {}):
        with tempfile.TemporaryDirectory(prefix="archive-scout-audit3-discard-") as temp:
            root = Path(temp)
            config = ProjectConfig(
                root, ["example.com/*"], ["needle"], from_date="2001", to_date="2001",
                workers=10, scan_workers=2, download_delay=0, text_retention="discard_after_scan",
                discard_spool_mb=32,
            ).normalized()
            database = open_database(root)
            signature = cdx_query_signature(config)
            _seed_pending_text(database, signature, count)
            with database:
                keyword_set_id = get_or_create_keyword_set(database, "Benchmark", ["needle"])
                scan_run_id = start_scan_run(database, keyword_set_id, "Benchmark", 1, "benchmark")
            job = ScanJob.create(scan_run_id, "Benchmark", ["needle"])
            _BenchmarkReplayClient.calls = 0
            elapsed, peak, _ = timed(lambda: _patched_discard(config, database, scan_run_id, job))
            files = list((root / "captures").rglob("*.txt"))
            result["discard"] = {
                "elapsed_seconds": elapsed, "peak_memory_bytes": peak,
                "http_starts": _BenchmarkReplayClient.calls, "files_remaining": len(files),
                "bytes_remaining": sum(path.stat().st_size for path in files),
                "documents": int(database.execute("SELECT COUNT(*) FROM documents").fetchone()[0]),
                "matches": int(database.execute("SELECT COUNT(*) FROM document_matches").fetchone()[0]),
                "discarded": int(database.execute("SELECT COUNT(*) FROM captures WHERE payload_availability='discarded'").fetchone()[0]),
            }
            database.close()
    return result


def _patched_download_only(config, database):
    with mock.patch("archive_scout.downloads.downloader.HttpClient", _BenchmarkReplayClient):
        return download_archive_only(config, database, threading.Event(), None)


def _patched_discard(config, database, scan_run_id, job):
    with mock.patch("archive_scout.downloads.downloader.HttpClient", _BenchmarkReplayClient):
        return download_archive(config, database, scan_run_id, threading.Event(), None, scan_jobs=[job])


def run(args) -> dict:
    return {
        "version": VERSION,
        "schema": SCHEMA_VERSION,
        "live_network": False,
        "range_20_year": range_request_fixture(),
        "selector": selector_fixture(args.selector_rows),
        "hitlist": hitlist_fixture(args.hitlist_files),
        "ambiguous_binary": binary_prefix_fixture(args.binary_bytes),
        "retention": retention_fixture(args.retention_captures),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Offline Audit3 before/after benchmark fixtures")
    parser.add_argument("--selector-rows", type=int, default=200000)
    parser.add_argument("--hitlist-files", type=int, default=300)
    parser.add_argument("--binary-bytes", type=int, default=512 * 1024)
    parser.add_argument("--retention-captures", type=int, default=200)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = run(args)
    text = json.dumps(result, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
