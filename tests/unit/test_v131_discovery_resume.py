from __future__ import annotations

import io
import json
import tempfile
import threading
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from unittest.mock import MagicMock, patch

from archive_scout.cdx.client import TransientRequestError
from archive_scout.cdx.discovery import run_fast_discovery
from archive_scout.cli import cli_main
from archive_scout.events import Stopped


class DiscoveryTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.source = self.root / "domains.txt"
        self.output = self.root / "discovery"
        self.source.write_text("FIRST.test\nfirst.test\nnot/a/domain\nsecond.test\nthird.test\n", encoding="utf-8")
        self.filter = r"(?i).*/sample[.]jpg(?:[?&].*)?$"
        self.stop = threading.Event()

    def test_live_output_resume_deferred_cap_and_no_project_state(self):
        calls = []
        header = ["urlkey", "timestamp", "original", "statuscode"]

        def get(url, *_args):
            params = parse_qs(urlsplit(url).query)
            host = params["url"][0]
            calls.append(host)
            self.assertEqual(params["filter"], ["original:" + self.filter])
            self.assertEqual(params["from"], ["20010101000000"])
            self.assertEqual(params["to"], ["20031231235959"])
            self.assertEqual(params["matchType"], ["domain"])
            self.assertEqual(params["limit"], ["100"])
            if host == "second.test":
                raise TransientRequestError("gateway timeout", status=504)
            rows = [header, ["key", "20010912000000", f"http://media.{host}/sample.jpg", "200"]]
            if host == "third.test":
                rows.extend([[], ["resume-key"]])
            return {"status": 200, "data": json.dumps(rows).encode()}

        client = self.enterContext(patch("archive_scout.cdx.discovery.HttpClient")).return_value
        client.host_gate.remaining.return_value = 0
        client.get.side_effect = get
        self.enterContext(patch("archive_scout.cdx.discovery.IMPORT_BATCH", 1))
        self.enterContext(patch("archive_scout.cdx.discovery.WORKERS", 1))
        from archive_scout.cdx import discovery
        persist = discovery._save_results
        saved = []

        def stop_after_first(*args):
            count = persist(*args)
            if count and not saved:
                live = next(self.output.glob("*/matching_urls.txt"))
                self.assertEqual(live.read_text(), "http://media.first.test/sample.jpg\n")
                saved.append(live)
                self.stop.set()
            return count

        with patch("archive_scout.cdx.discovery._save_results", side_effect=stop_after_first):
            with self.assertRaises(Stopped):
                run_fast_discovery(self.source, self.output, self.filter, stop_event=self.stop)
        self.assertEqual(calls, ["first.test"])
        self.stop.clear()
        reports, counts = run_fast_discovery(self.source, self.output, "&filter=original:" + self.filter)
        self.assertTrue(reports["urls"].samefile(saved[0]))
        self.assertEqual(calls, ["first.test", "second.test", "third.test", "second.test"])
        self.assertEqual(counts, {"searched": 1, "deferred": 1, "limited": 1, "matches": 2,
                                  "invalid_input_lines": 1, "input_read_to_end": True})
        self.assertIn("resume-key", reports["deferred"].read_text())
        self.assertIn("504", reports["deferred"].read_text())
        self.assertFalse((self.output / "archive_scout.sqlite3").exists())
        reports["urls"].write_text("", encoding="utf-8")
        run_fast_discovery(self.source, self.output, self.filter)
        self.assertEqual(len(calls), 4)
        self.assertEqual(len(reports["urls"].read_text().splitlines()), 2)

    def test_invalid_response_cannot_be_reported_as_empty(self):
        client = self.enterContext(patch("archive_scout.cdx.discovery.HttpClient")).return_value
        client.host_gate.remaining.return_value = 0
        client.get.return_value = {"status": 200, "data": b'{"error":"service down"}'}
        reports, counts = run_fast_discovery(self.source, self.output, self.filter)
        self.assertEqual(counts["deferred"], 3)
        self.assertNotIn("searched", counts)
        self.assertIn("Invalid CDX response", reports["deferred"].read_text())

    def test_cli_json_uses_standalone_operation(self):
        with patch("archive_scout.cdx.discovery.run_fast_discovery",
                   return_value=({"urls": self.output}, {"matches": 3, "limited": 1})) as run:
            with redirect_stdout(io.StringIO()) as stdout:
                code = cli_main(["discover-urls", str(self.source), "--output-dir", str(self.output),
                                 "--url-filter", self.filter, "--format", "json"])
            self.assertEqual(code, 0)
            self.assertEqual(json.loads(stdout.getvalue())["coverage"]["matches"], 3)
            self.assertEqual(run.call_args.args[3:5], ("2001", "2003"))

    def test_gui_starts_discovery_without_requiring_project_keywords(self):
        from archive_scout.ui.main_window import ArchiveScoutApp
        app = MagicMock()
        app.worker_thread = None
        app.discovery_options.return_value = (self.source, self.output, self.filter, "2001", "2003")
        with patch("archive_scout.ui.main_window.ensure_frozen_bundle_available"), \
             patch("archive_scout.ui.main_window.threading.Thread") as worker:
            ArchiveScoutApp.start(app, override_mode="discover_urls")
        app.build_config.assert_not_called()
        self.assertEqual(worker.call_args.kwargs["target"], app.run_discovery_worker)
        self.assertEqual(worker.call_args.kwargs["args"], app.discovery_options.return_value)


class AcquisitionTests(unittest.TestCase):
    def test_first_window_is_ready_before_rest_of_inventory_is_classified(self):
        from archive_scout.cdx.parameters import cdx_query_signature
        from archive_scout.config import ProjectConfig
        from archive_scout.database.connection import open_database
        from archive_scout.database.repositories import get_or_create_target, upsert_captures
        from archive_scout.downloads.downloader import prepare_download_rows

        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        db = open_database(root)
        self.addCleanup(db.close)
        config = ProjectConfig(output_dir=root, targets=["first.test"], keywords=[], from_year=2001, to_year=2003).normalized()
        target = get_or_create_target(db, config.targets[0])
        sig = cdx_query_signature(config)
        rows = [("20010912000000", f"http://first.test/{i}.html", "text/html", "200", "", "20") for i in range(2000)]
        rows.append(("20010912000000", "http://first.test/sample.jpg", "text/html", "200", "", "10"))
        upsert_captures(db, rows, target, sig)
        db.commit()

        stop = threading.Event()
        _, windows = prepare_download_rows(db, config, [], progressive=True, stop_event=stop)
        first, skipped = next(windows)
        self.assertEqual((len(first), skipped), (2000, 0))
        self.assertEqual(db.execute("SELECT COUNT(*) FROM capture_routing").fetchone()[0], 2000)
        stop.set()
        db.execute("DELETE FROM archive_scout_download_queue")
        db.commit()
        with self.assertRaises(Stopped):
            next(windows)
        stop.clear()
        _, windows = prepare_download_rows(db, config, [], progressive=True, stop_event=stop)
        first, _ = next(windows)
        db.execute("DELETE FROM archive_scout_download_queue")
        db.commit()
        second, skipped = next(windows)
        self.assertEqual((len(second), skipped), (0, 1))
        self.assertEqual(db.execute("SELECT state FROM captures WHERE id=2001").fetchone()[0], "skipped")
        self.assertEqual(db.execute("SELECT resource_class FROM capture_routing WHERE capture_id=2001").fetchone()[0], "image")

    def test_old_body_completion_does_not_clear_newer_backend_refusals(self):
        from archive_scout.network import transports
        from archive_scout.network.transports import ResilientTransport, TransportResponse

        stop = threading.Event()
        transport = ResilientTransport(pool_size=2, connect_timeout=1, read_timeout=1, mode="httpx", trust_env=False)
        self.addCleanup(transport.close)

        def response(url, _headers, _max_bytes, _stop):
            transports._backend_received_headers(200)
            for _ in range(3):
                newer = transport._await_backend("httpx", stop)
                transport._backend_result(newer, False)
            return TransportResponse(200, {}, url, b"old body", "httpx", 0.1)

        with patch.object(transport.backends["httpx"], "request", side_effect=response):
            transport.request("http://first.test/", {}, 256, stop)
        self.assertEqual(transport.dispatch_slots(1), 0)
        self.assertIn("httpx", transport.snapshot()["cooling_backends"])
