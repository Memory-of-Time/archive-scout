from __future__ import annotations

import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from archive_scout.cdx import indexer, parallel
from archive_scout.cdx.client import RateLimitDeferred
from archive_scout.config import NetworkConfig, ProjectConfig
from archive_scout.constants import SCHEMA_VERSION, VERSION
from archive_scout.downloads.rate_limit import (
    RecoveryDeadlineExceeded,
    SharedFixedRateLimiter,
    SharedHostGate,
    WAYBACK_INDEX_RATE_KEY,
    reset_shared_traffic_state_for_tests,
)
from archive_scout.media import indexer as media_indexer
from archive_scout.ui.widgets import ScrollablePage, WheelRouter


class V101RateAndScrollingAuditTests(unittest.TestCase):
    def tearDown(self):
        reset_shared_traffic_state_for_tests()

    def test_release_identity_keeps_schema_11(self):
        self.assertEqual(VERSION, "1.0.7")
        self.assertEqual(SCHEMA_VERSION, 12)

    def test_text_paged_service_pause_stops_admission_and_preserves_cursor(self):
        cfg = ProjectConfig(
            Path("."), ["example.com/*"], [],
            network=NetworkConfig(index_strategy="paged", cdx_workers=1),
        ).normalized()
        current = indexer.PendingWindow(
            "20010101000000", "20011231235959",
            strategy="paged", page_count=1000, page_blocks=9,
        )
        calls: list[int] = []
        deferred = RateLimitDeferred("service paused", waited=900, incident_id=4)

        def fail(_client, _urls, params, **_kwargs):
            calls.append(int(dict(params)["page"]))
            raise deferred

        client = mock.MagicMock()
        with mock.patch.object(parallel, "request_cdx_json_rows", side_effect=fail):
            with self.assertRaises(RateLimitDeferred) as caught:
                indexer._request_paged_batch(client, cfg, "example.com/*", current, threading.Event())
        self.assertIs(caught.exception, deferred)
        self.assertEqual(calls, [0])
        self.assertEqual(current.page, 0)
        self.assertEqual(current.retry_pages, [])
        self.assertEqual(current.page_failures, {})

    def test_media_paged_service_pause_stops_admission_and_preserves_cursor(self):
        cfg = ProjectConfig(
            Path("."), ["example.com/*"], [],
            network=NetworkConfig(index_strategy="paged", cdx_workers=1),
        ).normalized()
        current = indexer.PendingWindow(
            "20010101000000", "20011231235959",
            strategy="paged", page_count=1000, page_blocks=9,
        )
        calls: list[int] = []
        deferred = RateLimitDeferred("service paused", waited=900, incident_id=5)

        def fail(_client, _urls, params, **_kwargs):
            calls.append(int(dict(params)["page"]))
            raise deferred

        client = mock.MagicMock()
        with mock.patch.object(parallel, "request_cdx_json_rows", side_effect=fail):
            with self.assertRaises(RateLimitDeferred) as caught:
                media_indexer._request_media_paged_batch(
                    cfg, client, "example.com/*", current, ["jpg"], threading.Event()
                )
        self.assertIs(caught.exception, deferred)
        self.assertEqual(calls, [0])
        self.assertEqual(current.page, 0)
        self.assertEqual(current.retry_pages, [])
        self.assertEqual(current.page_failures, {})

    def test_shared_gate_wait_is_bounded_by_incident_deadline(self):
        clock = [1000.0]
        gate = SharedHostGate()
        with mock.patch("archive_scout.downloads.rate_limit.time.monotonic", side_effect=lambda: clock[0]), \
             mock.patch("archive_scout.downloads.rate_limit.time.time", side_effect=lambda: 10_000.0 + (clock[0] - 1000.0)), \
             mock.patch("archive_scout.downloads.rate_limit.random.uniform", return_value=1.0):
            gate.signal_rate_limit(retry_after=120, reason="HTTP 429")
            with mock.patch.object(
                gate.condition, "wait",
                side_effect=lambda timeout: clock.__setitem__(0, clock[0] + timeout),
            ):
                with self.assertRaises(RecoveryDeadlineExceeded) as caught:
                    gate.acquire_request(threading.Event(), deadline=1030.0)
        self.assertAlmostEqual(clock[0], 1030.0, places=6)
        self.assertEqual(caught.exception.incident_id, 1)
        self.assertGreater(caught.exception.eligible_at_epoch, 10_000.0)

    def test_coalesced_rate_limit_incident_adapts_spacing_once(self):
        reset_shared_traffic_state_for_tests()
        limiter = SharedFixedRateLimiter(2.5, key=WAYBACK_INDEX_RATE_KEY)
        gate = SharedHostGate()
        applied = []
        clock = [1000.0]
        with mock.patch("archive_scout.downloads.rate_limit.time.monotonic", side_effect=lambda: clock[0]), \
             mock.patch("archive_scout.downloads.rate_limit.time.time", side_effect=lambda: 10_000.0 + (clock[0] - 1000.0)), \
             mock.patch("archive_scout.downloads.rate_limit.random.uniform", return_value=1.0):
            for _ in range(10):
                _remaining, incident_id, _eligible, _new = gate.signal_rate_limit()
                applied.append(limiter.note_rate_limit(incident_id))
                clock[0] += 3.0  # deliberately beyond the old 2s coalescing window
        self.assertEqual(applied.count(True), 1)
        self.assertEqual(gate.snapshot()["incidents"], 1)
        self.assertEqual(limiter.effective_delay, 5.0)

    def test_visible_focus_reveal_leaves_scroll_position_unchanged(self):
        page = ScrollablePage.__new__(ScrollablePage)
        page.canvas = SimpleNamespace(
            winfo_rooty=lambda: 100,
            winfo_height=lambda: 600,
            winfo_fpixels=lambda _value: 1.333,
            bbox=lambda _value: (0, 0, 900, 2000),
            yview=lambda: (0.15, 0.45),
            yview_moveto=mock.Mock(),
        )
        page.body = SimpleNamespace(winfo_reqheight=lambda: 2000)
        visible = SimpleNamespace(winfo_rooty=lambda: 400, winfo_height=lambda: 30)
        page.reveal(visible)
        page.canvas.yview_moveto.assert_not_called()

    def test_native_event_reaching_boundary_does_not_scroll_parent_same_event(self):
        outer = SimpleNamespace(can_scroll_y=lambda _units: True, scroll_y=mock.Mock())
        native = SimpleNamespace(
            yview=lambda: (0.8, 1.0),
            master=None,
            _archive_scout_scroll_owner=outer,
        )
        router = WheelRouter.__new__(WheelRouter)
        router.root = SimpleNamespace(winfo_containing=lambda _x, _y: native)
        router._wheel_residual = {}
        router._native_views = {(id(native), "y"): (0.7, 0.9)}
        event = SimpleNamespace(widget=native, x_root=10, y_root=10, delta=-120, num=None)
        with mock.patch.object(WheelRouter, "_native_scrollable", return_value=True):
            router._wheel(event)
            outer.scroll_y.assert_not_called()
            # A later event that begins with the native surface already at its
            # boundary may bubble to the outer page.
            router._wheel(event)
        outer.scroll_y.assert_called_once_with(1)


if __name__ == "__main__":
    unittest.main()
