"""v1.2.2 regressions: native CDX formats, manual-only GUI, truthful speed."""
from __future__ import annotations

import json
import sqlite3
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from archive_scout.cdx.client import HttpClient, CDXRows
from archive_scout.downloads.rate_limit import FixedRateLimiter
from archive_scout.network.transports import TransportResponse
from archive_scout.ui.main_window import ArchiveScoutApp


HEADER = ['timestamp', 'original', 'mimetype', 'statuscode', 'digest', 'length']
PAYLOAD = [HEADER, ['20010101000000', 'http://example.org/', 'text/html', '200', 'digest', '12']]


class _FormatTransport:
    def __init__(self, body: bytes):
        self.body = body
        self.urls: list[str] = []
    def request(self, url, _headers, _maximum, _stop):
        self.urls.append(url)
        return TransportResponse(200, {'content-type': 'application/json'}, url, self.body, 'mock', 0.001)
    def close(self):
        pass


class CDXFormatTests(unittest.TestCase):
    def test_json_timemap_is_requested_once_even_when_text_preferred(self):
        transport = _FormatTransport(json.dumps(PAYLOAD).encode('utf-8'))
        client = HttpClient(FixedRateLimiter(0), 1, 5, 'test', threading.Event(), transport=transport)
        try:
            rows = client.get_cdx_rows_any(
                ('https://web.archive.org/web/timemap/json',), [('output','txt'), ('fl', ','.join(HEADER))],
                prefer_text=True,
            )
            self.assertEqual(len(rows.rows), 1)
            self.assertEqual(len(transport.urls), 1)
            self.assertIn('output=txt', transport.urls[0])  # endpoint controls actual format
        finally:
            client.close()

    def test_json_body_on_generic_text_endpoint_is_parsed_without_retry(self):
        transport = _FormatTransport(json.dumps(PAYLOAD).encode('utf-8'))
        client = HttpClient(FixedRateLimiter(0), 1, 5, 'test', threading.Event(), transport=transport)
        try:
            result = client.get_cdx_rows_any(
                ('https://web.archive.org/cdx/search/cdx',), [('output','json')], prefer_text=True,
            )
            self.assertEqual(result.rows[0][0], '20010101000000')
            self.assertEqual(len(transport.urls), 1)
        finally:
            client.close()

    def test_legacy_dict_rows_use_native_json_without_second_request(self):
        transport = _FormatTransport(json.dumps(PAYLOAD).encode('utf-8'))
        client = HttpClient(FixedRateLimiter(0), 1, 5, 'test', threading.Event(), transport=transport)
        try:
            result = client.get_cdx_any(('https://web.archive.org/web/timemap/json',), [('output','txt')], prefer_text=True)
            self.assertEqual(result[1][1], 'http://example.org/')
            self.assertEqual(len(transport.urls), 1)
        finally:
            client.close()

    def test_true_truncation_is_not_accepted_as_complete(self):
        # Return malformed JSON through both format attempts; never silently
        # record a complete inventory or interpret truncated JSON as CDX text.
        from archive_scout.cdx.client import TransientRequestError
        transport = _FormatTransport(json.dumps(PAYLOAD).encode('utf-8')[:-3])
        client = HttpClient(FixedRateLimiter(0), 1, 5, 'test', threading.Event(), transport=transport)
        try:
            with self.assertRaises((TransientRequestError, ValueError, RuntimeError)):
                client.get_cdx_rows_any(('https://web.archive.org/web/timemap/json',), [('output','json')], prefer_text=True)
        finally:
            client.close()


class GUISourceContracts(unittest.TestCase):
    def test_manual_dashboard_does_not_poll_and_auto_refresh_is_coalesced(self):
        from archive_scout.ui.dashboard_refresh import DashboardRefreshController
        controller = DashboardRefreshController(mode='manual')
        self.assertFalse(controller.automatic_due(0, visible=True, operation_active=True))
        self.assertIsNone(controller.begin(0))
        token = controller.begin(0, manual=True)
        self.assertIsNotNone(token)
        self.assertIsNone(controller.begin(1, manual=True))
        self.assertTrue(controller.finish(token))
        controller.configure('auto', 10)
        self.assertFalse(controller.automatic_due(5, visible=True, operation_active=True))
        self.assertTrue(controller.automatic_due(10, visible=True, operation_active=True))


class GUIThemeTests(unittest.TestCase):
    def test_dark_scroll_canvas_and_native_scaling(self):
        import tkinter as tk
        from archive_scout.ui.theme import DARK, apply_theme, apply_text_theme
        from archive_scout.ui.widgets import ScrollablePage
        try:
            root = tk.Tk()
        except tk.TclError:
            self.skipTest('No desktop display available')
        try:
            original_scaling = float(root.tk.call('tk', 'scaling'))
            page = ScrollablePage(root, horizontal=True)
            page.pack(fill='both', expand=True)
            apply_theme(root, 'dark', 1.0)
            apply_text_theme(root, DARK)
            self.assertEqual(page.canvas.cget('background'), DARK['bg'])
            self.assertAlmostEqual(float(root.tk.call('tk','scaling')), original_scaling, places=2)
        finally:
            root.destroy()
            del page, root
            import gc
            gc.collect()


if __name__ == '__main__':
    unittest.main()
