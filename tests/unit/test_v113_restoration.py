"""v1.1.3 regression checks for selective rollback improvements."""
from __future__ import annotations

import os
from contextlib import closing
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from archive_scout.config import ProjectConfig, load_project_config, save_project_config
from archive_scout.network.transports import ResilientTransport, TransportResponse
from archive_scout.ui.widgets import WheelRouter, ScrollablePage


class RestorationTests(unittest.TestCase):
    def test_new_retained_jobs_default_to_acquire_first(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(root, ["example.com/*"], ["needle"]).normalized()
            self.assertFalse(config.scan_overlap)
            save_project_config(config)
            self.assertFalse(load_project_config(root / "project.json").scan_overlap)

    def test_existing_project_explicit_overlap_is_preserved(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(root, ["example.com/*"], ["needle"], scan_overlap=True)
            save_project_config(config)
            self.assertTrue(load_project_config(root / "project.json").scan_overlap)

    def test_fallback_backend_periodically_rechecks_healthy_primary(self):
        backend_calls: list[str] = []

        class Backend:
            def __init__(self, name): self.name = name
            def request(self, url, headers, max_bytes, stop_event, **kwargs):
                backend_calls.append(self.name)
                return TransportResponse(200, {}, url, b"fine", self.name, 0.01)

        tr = ResilientTransport.__new__(ResilientTransport)
        tr.lock = threading.Lock()
        tr.callback = None
        tr.attempt_context_factory = None
        tr.backends = {name: Backend(name) for name in ("httpx", "urllib3")}
        tr.order = ["httpx", "urllib3"]
        tr.cooldown_until = {}
        tr.last_success = "urllib3"
        tr._fallback_successes = 31
        tr.request("https://example.com/a", {}, 100, threading.Event())
        self.assertEqual(backend_calls, ["urllib3"])
        self.assertEqual(tr._fallback_successes, 32)
        tr.request("https://example.com/b", {}, 100, threading.Event())
        self.assertEqual(backend_calls, ["urllib3", "httpx"])
        self.assertEqual(tr.last_success, "httpx")
        self.assertEqual(tr._fallback_successes, 0)

    def test_failed_primary_probe_is_not_retried_every_request(self):
        calls = []
        class Primary:
            def request(self, *args, **kwargs):
                calls.append("primary")
                raise OSError("offline")
        class Secondary:
            def request(self, url, *args, **kwargs):
                calls.append("secondary")
                return TransportResponse(200, {}, url, b"ok", "urllib3", 0.01)
        tr = ResilientTransport.__new__(ResilientTransport)
        tr.lock = threading.Lock(); tr.callback = None; tr.attempt_context_factory = None
        tr.backends = {"httpx": Primary(), "urllib3": Secondary()}
        tr.order = ["httpx", "urllib3"]; tr.last_success = "urllib3"
        tr.cooldown_until = {}; tr._fallback_successes = 32
        tr.request("https://example.com/a", {}, 100, threading.Event())
        self.assertEqual(calls, ["primary", "secondary"])
        tr.request("https://example.com/b", {}, 100, threading.Event())
        self.assertEqual(calls, ["primary", "secondary", "secondary"])

    def test_fractional_wheel_accumulates_without_excess_speed(self):
        router = WheelRouter.__new__(WheelRouter)
        router.root = object(); router._wheel_residual = {}
        widget = object()
        with patch("archive_scout.ui.widgets.sys.platform", "win32"):
            self.assertEqual(router._units(SimpleNamespace(widget=widget, delta=-40)), 0)
            self.assertEqual(router._units(SimpleNamespace(widget=widget, delta=-40)), 0)
            self.assertEqual(router._units(SimpleNamespace(widget=widget, delta=-40)), 1)
            self.assertEqual(router._units(SimpleNamespace(widget=widget, delta=-1200)), 3)
        with patch("archive_scout.ui.widgets.sys.platform", "darwin"):
            router._wheel_residual.clear()
            self.assertEqual(router._units(SimpleNamespace(widget=widget, delta=-1)), 0)
            self.assertEqual(router._units(SimpleNamespace(widget=widget, delta=-1)), 0)
            self.assertEqual(router._units(SimpleNamespace(widget=widget, delta=-1)), 1)

    def test_fresh_save_progress_only_counts_post_commit_remote_files(self):
        from archive_scout.cdx.parameters import cdx_query_signature
        from archive_scout.database.connection import open_database
        from archive_scout.downloads.downloader import download_archive_only
        from archive_scout.utils import utc_now

        class Client:
            def __init__(self, *args, **kwargs): pass
            def close(self): pass
            def download_to_path(self, url, path, *args, **kwargs):
                body = b"<html><body>archive body</body></html>"
                Path(path).write_bytes(body)
                return {"headers": {"content-type": "text/html"}, "preview": body,
                        "bytes": len(body), "content_hash": "", "status": 200, "final_url": url}

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = ProjectConfig(root, ["example.com/*"], [], from_date="2001", to_date="2001", workers=1).normalized()
            # Windows cannot delete an open SQLite file during TemporaryDirectory cleanup.
            with closing(open_database(root)) as db:
                now = utc_now()
                with db:
                    db.executemany("""INSERT INTO captures(original_url,timestamp,query_signature,mimetype,
                        statuscode,length,state,created_at,updated_at)
                        VALUES(?,'20010101000000',?,'text/html','200',100,'pending',?,?)""",
                        [(f"http://example.com/{i}", cdx_query_signature(cfg), now, now) for i in range(2)])
                events = []
                with patch("archive_scout.downloads.downloader.HttpClient", Client):
                    result = download_archive_only(cfg, db, threading.Event(), events.append)
                self.assertEqual(result["downloaded"], 2)
                details = [event.detail for event in events if event.stage == "download_only"]
                self.assertEqual(details[-1]["fresh_committed"], 2)
                self.assertEqual(details[-1]["fresh_committed_bytes"], 2 * len(b"<html><body>archive body</body></html>"))
                self.assertGreater(details[-1]["fresh_save_rate_60s"], 0)
                self.assertEqual(db.execute("SELECT COUNT(*) FROM captures WHERE state='downloaded_unscanned'").fetchone()[0], 2)

    @unittest.skipUnless(os.environ.get("DISPLAY") or os.name == "nt", "Tk display not available")
    def test_wide_form_is_accessible_and_focus_does_not_jump_when_visible(self):
        import tkinter as tk
        from tkinter import ttk
        try:
            root = tk.Tk()
        except tk.TclError as exc:
            self.skipTest(f"Tk display unavailable: {exc}")
        self.addCleanup(root.destroy)
        root.geometry("500x350")
        page = ScrollablePage(root)
        page.pack(fill="both", expand=True)
        ttk.Label(page.body, text="wide", width=130).pack()
        field = ttk.Entry(page.body)
        field.pack(anchor="w")
        root.update()
        self.assertTrue(page.can_scroll_x(1))
        old = page.canvas.yview()
        page.reveal(field)
        self.assertEqual(page.canvas.yview(), old)


if __name__ == "__main__":
    unittest.main()
