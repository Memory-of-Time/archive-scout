from __future__ import annotations

import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from archive_scout.cdx.client import HttpClient, RateLimitDeferred
from archive_scout.config import KeywordSetConfig, ProjectConfig
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, upsert_captures
from archive_scout.downloads.downloader import cumulative_download_progress
from archive_scout.downloads.rate_limit import (
    FixedRateLimiter,
    SharedFixedRateLimiter,
    reset_shared_traffic_state_for_tests,
    shared_host_gate,
)
from archive_scout.events import ConnectivityPaused
from archive_scout.network.transports import TransportResponse
from archive_scout.operations import is_recoverable_pause, run_project
from archive_scout.scanning.jobs import ScanJob


class _Single503Transport:
    def request(self, url, headers, max_bytes, stop_event):
        return TransportResponse(
            status=503,
            headers={},
            final_url=url,
            data=b"temporarily unavailable",
            backend="test",
            elapsed=0.0,
        )

    def close(self):
        pass


class V101AuditTests(unittest.TestCase):
    def setUp(self):
        reset_shared_traffic_state_for_tests()

    def test_resume_reenters_index_queue_before_download(self):
        with tempfile.TemporaryDirectory() as temp:
            config = ProjectConfig(
                output_dir=Path(temp),
                targets=["example.com/*"],
                keywords=["needle"],
                keyword_sets=[KeywordSetConfig("set", ["needle"], True)],
                from_date="2008",
                to_date="2008",
            )
            order: list[str] = []
            job = ScanJob.create(1, "set", ["needle"])
            with (
                patch("archive_scout.operations.index_archive", side_effect=lambda *a, **k: order.append("index")),
                patch("archive_scout.operations.prepare_scan_jobs", return_value=[job]),
                patch("archive_scout.operations.download_archive", side_effect=lambda *a, **k: order.append("download")),
                patch("archive_scout.operations.finish_jobs"),
                patch("archive_scout.operations.generate_job_reports", return_value={}),
            ):
                run_project(config, "resume", threading.Event())
            self.assertEqual(order, ["index", "download"])

    def test_process_wide_host_gate_is_shared(self):
        first = shared_host_gate(30, 300)
        second = shared_host_gate(60, 600)
        self.assertIs(first, second)
        self.assertGreaterEqual(first.base_pause, 60)
        self.assertGreaterEqual(first.max_pause, 600)

    def test_shared_request_limiter_coordinates_independent_clients(self):
        first = SharedFixedRateLimiter(0.04)
        second = SharedFixedRateLimiter(0.04)
        stop = threading.Event()
        started = time.monotonic()
        with first.slot(stop):
            pass
        with second.slot(stop):
            pass
        self.assertGreaterEqual(time.monotonic() - started, 0.025)

    def test_headerless_503_enters_coordinated_pause(self):
        stop = threading.Event()
        client = HttpClient(
            FixedRateLimiter(0),
            retries=1,
            timeout=1,
            user_agent="test",
            stop_event=stop,
            host_gate=shared_host_gate(0.01, 0.01),
            rate_limit_attempts=1,
            rate_limit_max_wait=1,
            transport=_Single503Transport(),
        )
        with self.assertRaises(RateLimitDeferred) as ctx:
            client.get("https://web.archive.org/test", 1024)
        self.assertEqual(ctx.exception.status, 503)
        client.close()

    def test_zero_pause_budget_normalizes_to_finite_safety_defaults(self):
        with tempfile.TemporaryDirectory() as temp:
            config = ProjectConfig(
                output_dir=Path(temp),
                targets=["example.com/*"],
                keywords=["needle"],
                rate_limit_max_wait=0,
                rate_limit_attempts=0,
            ).normalized()
        self.assertEqual(config.rate_limit_max_wait, 900.0)
        self.assertEqual(config.rate_limit_attempts, 8)


    def test_resume_progress_is_cumulative_from_persisted_capture_states(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(
                output_dir=root,
                targets=["example.com/*"],
                keywords=["needle"],
                from_date="2008",
                to_date="2008",
            ).normalized()
            database = open_database(root)
            target_id = get_or_create_target(database, "example.com/*")
            signature = cdx_query_signature(config)
            rows = [
                {"original": f"http://example.com/{i}.html", "timestamp": f"20080101{i:06d}", "mimetype": "text/html", "statuscode": "200", "digest": str(i), "length": "10"}
                for i in range(5)
            ]
            upsert_captures(database, rows, target_id, signature)
            ids = [row[0] for row in database.execute("SELECT id FROM captures ORDER BY id")]
            database.execute("UPDATE captures SET state='downloaded' WHERE id IN (?,?,?)", ids[:3])
            database.commit()
            completed, total = cumulative_download_progress(database, config, queued_total=2)
            database.close()
        self.assertEqual((completed, total), (3, 5))

    def test_operation_progress_is_persisted_for_integrations(self):
        import json
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(
                output_dir=root,
                targets=["example.com/*"],
                keywords=["needle"],
                keyword_sets=[KeywordSetConfig("set", ["needle"], True)],
                from_date="2008",
                to_date="2008",
            )
            def fake_index(config, database, stop_event, callback):
                callback(__import__("archive_scout.events", fromlist=["ProgressEvent"]).ProgressEvent(
                    "index", "Saved index progress", 2, 5
                ))
            with patch("archive_scout.operations.index_archive", side_effect=fake_index):
                run_project(config, "index", threading.Event())
            database = open_database(root)
            row = database.execute(
                "SELECT message,progress_json FROM operation_runs ORDER BY id DESC LIMIT 1"
            ).fetchone()
            database.close()
        payload = json.loads(row["progress_json"])
        self.assertEqual(row["message"], "Index complete")
        self.assertEqual(payload, {"completed": 2, "total": 5, "stage": "index"})

    def test_recoverable_pause_contract(self):
        self.assertTrue(is_recoverable_pause(ConnectivityPaused("saved")))
        self.assertTrue(is_recoverable_pause(RateLimitDeferred("saved", status=503, waited=10)))
        self.assertFalse(is_recoverable_pause(RuntimeError("broken")))


if __name__ == "__main__":
    unittest.main()
